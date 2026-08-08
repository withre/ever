//! Topic management — logical topics over a shared append-only log.
//!
//! Topics are names registered in an in-memory index. Each topic tracks
//! which global offsets in the shared log belong to it. The log stores
//! all events from all topics sequentially.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;
const store = @import("store.zig");
const Log = store.Log;
const Event = store.Event;

fn StringArrayHashMap(comptime V: type) type {
    if (@hasDecl(std, "StringArrayHashMapUnmanaged")) return std.StringArrayHashMapUnmanaged(V);
    return std.array_hash_map.String(V);
}

pub const TopicError = error{
    AlreadyExists,
    NotFound,
    InvalidName,
    TopicDeleted,
    /// A client publish carried the reserved tombstone key. Distinct from
    /// the topic-state errors above: this is an argument defect, and a
    /// caller should be able to tell the two apart.
    ReservedKey,
    /// A client publish carried an empty value. Empty values mark the
    /// store's own internal records and are skipped by every read path,
    /// so accepting one would return an offset for an event no consumer
    /// can ever see.
    EmptyValue,
};

/// Sentinel key written to the log to mark a topic as soft-deleted.
///
/// Reserved against client publishes: `deleteTopic` writes this key with an
/// empty value and `rebuildIndex` reads the pair back as "topic deleted", so
/// a client able to write it could destroy any topic — invisibly, until the
/// next restart. `validatePublishInput` is the guard; internal writers call
/// `log.append` directly and are deliberately not subject to it.
const deletion_marker_key = "__ever_tombstone__";

/// Client-facing explanation for `TopicError.ReservedKey`. Lives here rather
/// than at each protocol edge so the TCP and HTTP paths cannot drift apart.
pub const reserved_key_message =
    "\"" ++ deletion_marker_key ++ "\" is a reserved key and cannot be published";

/// Client-facing explanation for `TopicError.EmptyValue`. Names the remedy,
/// because the caller most likely to hit this is building a heartbeat or tick
/// and reaching for the cheapest possible payload.
pub const empty_value_message =
    "empty values are reserved for internal markers and are not readable; " ++
    "publish a non-empty payload (use \"{}\" if you need a contentless signal)";

/// Reject a client publish that no client may make, whatever the topic's
/// state. Pure and cheap, so `publish` runs it before taking the lock and
/// before the topic checks: a publish that would be refused on its own terms
/// should not first be told the topic does not exist.
fn validatePublishInput(key: ?[]const u8, value: []const u8) TopicError!void {
    if (key) |k| {
        if (std.mem.eql(u8, k, deletion_marker_key)) return TopicError.ReservedKey;
    }
    if (value.len == 0) return TopicError.EmptyValue;
}

pub const Config = struct {
    max_segment_size: u64 = 64 * 1024 * 1024,
    sync_on_append: bool = true,
};

/// Information about a topic for listing purposes.
pub const TopicListEntry = struct {
    name: []const u8,
    deleted: bool,
};

/// Per-topic index: tracks which global offsets belong to this topic.
const TopicIndex = struct {
    offsets: std.ArrayList(u64) = .empty,
    /// Indices into `offsets` that hold marker records (empty value),
    /// ascending. One entry for the creation marker, one per tombstone, plus
    /// any historical empty-value user publish — so typically one, and never
    /// large. Maintained at exactly the two sites that maintain
    /// `non_marker_count`: `publish` and `rebuildIndex`.
    ///
    /// Its only purpose is to let `fetch`/`fetchPattern` translate a
    /// topic-local skip count (which counts non-markers) into an index into
    /// `offsets` (which counts records) without reading anything off disk.
    /// See `firstIndexForSkip` and air/v0.1/subscribe-fetch-seek.org.
    marker_positions: std.ArrayList(u32) = .empty,
    /// Count of non-marker events in this topic. A "marker" is any event
    /// whose value is empty (creation markers from createTopic, tombstones
    /// from deleteTopic, and — by historical quirk — user publishes with an
    /// empty value). Kept in sync with the skip logic in `fetch`/`fetchPattern`
    /// so hook cursors can be set to "tip" (== non_marker_count) to skip all
    /// currently-visible events.
    non_marker_count: u64 = 0,

    fn deinit(self: *TopicIndex, allocator: Allocator) void {
        self.offsets.deinit(allocator);
        self.marker_positions.deinit(allocator);
    }

    /// Record that the entry just appended to `offsets` is a marker.
    fn noteMarker(self: *TopicIndex, allocator: Allocator) !void {
        try self.marker_positions.append(allocator, @intCast(self.offsets.items.len - 1));
    }

    /// Index into `offsets` of the `skip`-th non-marker event.
    ///
    /// `skip` counts only non-marker events, `offsets` counts every record,
    /// so the answer is `skip` shifted right by the number of markers at or
    /// before it — the standard insert-position shift, exact because
    /// `marker_positions` is ascending.
    ///
    /// Callers must still skip markers they encounter while reading forward
    /// and must still not count them. That is deliberate: if this bookkeeping
    /// is ever stale, the seek lands slightly early or late and the read loop
    /// still returns the right events. A wrong assumption degrades to slow,
    /// never to incorrect.
    fn firstIndexForSkip(self: *const TopicIndex, skip: u64) usize {
        if (skip == 0) return 0;
        var i: u64 = skip;
        for (self.marker_positions.items) |mp| {
            if (mp <= i) i += 1 else break;
        }
        return @intCast(@min(i, self.offsets.items.len));
    }
};

/// Manages topics as a logical layer over a single shared Log.
pub const TopicManager = struct {
    allocator: Allocator,
    log: Log,
    topics: StringArrayHashMap(TopicIndex),
    deleted_topics: std.StringHashMap(void),
    /// Blocking, not spinning: a thread that loses this lock parks on a futex
    /// instead of burning a core. That matters because `fetchPatternByOffset`
    /// holds it across a whole log scan, so the waiter is a publisher and the
    /// wait can be long. Non-reentrant, as before -- the `*Locked` entry-point
    /// variants still exist for exactly that reason, and violating the
    /// discipline now deadlocks quietly rather than spinning visibly.
    /// See air/v0.1/store-blocking-locks.org.
    mutex: std.Io.Mutex = .init,
    /// Publication epoch: bumped under `mutex` on every event appended
    /// through `publish`, and by `wakeAllWaiters`.
    ///
    /// The condition variable is only the notification; *this* is the state.
    /// A waiter records the epoch before it looks for events and waits for it
    /// to move, so an append that lands between the look and the wait is
    /// never slept through -- the waiter finds the epoch already changed and
    /// returns without parking. See air/v0.1/subscribe-notify-on-append.org.
    append_epoch: u64 = 0,
    append_cond: std.Io.Condition = .init,
    /// Test-only fault injection: when true, `createTopicLocked` fails just
    /// before appending the topic-creation marker, simulating a log-append
    /// failure. Never set in production code.
    test_fail_marker_append: bool = false,

    pub fn init(allocator: Allocator, io: Io, dir: Dir, config: Config) !TopicManager {
        const log = try Log.init(allocator, io, dir, .{
            .max_segment_size = config.max_segment_size,
            .sync_on_append = config.sync_on_append,
        });

        var manager = TopicManager{
            .allocator = allocator,
            .log = log,
            .topics = .empty,
            .deleted_topics = std.StringHashMap(void).init(allocator),
            .mutex = .init,
        };

        // Rebuild topic index from log contents
        try manager.rebuildIndex();

        return manager;
    }

    pub fn deinit(self: *TopicManager) void {
        self.deleted_topics.deinit();
        var iter = self.topics.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.topics.deinit(self.allocator);
        self.log.deinit();
    }

    fn lock(self: *TopicManager) void {
        self.mutex.lockUncancelable(self.log.io);
    }

    fn unlock(self: *TopicManager) void {
        self.mutex.unlock(self.log.io);
    }

    /// Public lock used by the hook-registration path to compute a "tip"
    /// cursor and insert the hook under the same critical section that
    /// `publish` uses (single-writer discipline). This guarantees no publish
    /// can interleave between the tip read and hook insertion. The ordering
    /// guarantee is consistency under the store's single-writer publish
    /// discipline, not strict atomicity in the formal sense.
    pub fn lockForHookRegistration(self: *TopicManager) void {
        self.lock();
    }

    pub fn unlockForHookRegistration(self: *TopicManager) void {
        self.unlock();
    }


    /// Return the current tip for `pattern` — the cursor value the hook
    /// daemon should use as a starting point so only events published after
    /// registration are delivered. Caller must hold the registration lock.
    ///
    /// The semantics differ by pattern shape and match the corresponding
    /// fetch path used by the hook daemon:
    ///
    /// - **Exact topic**: returns `non_marker_count` of that topic. Used as
    ///   a topic-local skip count by `fetch(...)`. Future events on this
    ///   topic are delivered exactly once.
    ///
    /// - **Prefix or wildcard pattern**: returns the current global log
    ///   offset (`log.nextOffset()`). Used as a global-offset cursor by
    ///   `fetchPatternByOffset(...)`. Any event published from now on —
    ///   on any matching topic, *including topics that don't exist yet*
    ///   — is delivered exactly once. This avoids the B1 wildcard bug
    ///   where a single per-topic skip count could swallow events on
    ///   low-count or newly-created matching topics.
    pub fn tipForPatternLocked(self: *TopicManager, pattern: []const u8) u64 {
        if (!isPatternShape(pattern)) {
            if (self.topics.getPtr(pattern)) |idx| return idx.non_marker_count;
            return 0;
        }

        return self.log.nextOffset();
    }

    /// Convenience for callers that don't need the registration ordering
    /// guarantee — acquires and releases the lock internally.
    ///
    /// **test-only:** server code (and any caller that needs the tip read
    /// to be ordered with the hook insertion against concurrent publishes)
    /// must use `lockForHookRegistration` + `tipForPatternLocked` and then
    /// insert the hook before unlocking. This wrapper drops the lock before
    /// returning, so a concurrent publish can interleave between the read
    /// and any subsequent action by the caller.
    pub fn tipForPattern(self: *TopicManager, pattern: []const u8) u64 {
        self.lock();
        defer self.unlock();
        return self.tipForPatternLocked(pattern);
    }

    /// Register a new topic. Writes a marker event to the log so the
    /// topic survives restart even if no events are published to it.
    pub fn createTopic(self: *TopicManager, name: []const u8) !void {
        self.lock();
        defer self.unlock();
        try self.createTopicLocked(name);
    }

    /// Body of `createTopic` — caller must already hold the TopicManager
    /// mutex (e.g. via `lockForHookRegistration`). Split out because the
    /// mutex is a non-reentrant spinlock: calling `createTopic` while
    /// holding the lock deadlocks. Used by the server's atomic
    /// create-topic-plus-register-hook path.
    pub fn createTopicLocked(self: *TopicManager, name: []const u8) !void {
        try validateTopicName(name);
        if (self.topics.contains(name)) return TopicError.AlreadyExists;

        if (self.test_fail_marker_append) return error.InjectedMarkerAppendFailure;

        // Write a marker event so rebuildIndex discovers this topic on restart
        const offset = try self.log.append(name, null, "");
        const owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned);
        var idx: TopicIndex = .{};
        try idx.offsets.append(self.allocator, offset);
        try idx.noteMarker(self.allocator);
        try self.topics.put(self.allocator, owned, idx);
    }

    /// Soft-delete a topic. The topic stays in the index but is marked as
    /// deleted. Historical events remain readable; new publishes are rejected.
    /// A tombstone marker is written to the log so the state survives restart.
    pub fn deleteTopic(self: *TopicManager, name: []const u8) !void {
        self.lock();
        defer self.unlock();
        if (!self.topics.contains(name)) return TopicError.NotFound;
        if (self.deleted_topics.contains(name)) return TopicError.NotFound;

        // Write a tombstone marker to the log so rebuildIndex picks it up.
        const idx = self.topics.getPtr(name).?;
        const offset = try self.log.append(name, deletion_marker_key, "");
        try idx.offsets.append(self.allocator, offset);
        try idx.noteMarker(self.allocator);

        // Mark as deleted (key points to the owned string inside `topics`).
        const owned_key = self.topics.getKey(name).?;
        try self.deleted_topics.put(owned_key, {});
    }

    /// Check if a topic exists (including soft-deleted topics).
    pub fn hasTopic(self: *TopicManager, name: []const u8) bool {
        self.lock();
        defer self.unlock();
        return self.topics.contains(name);
    }

    /// Like `hasTopic`, but the caller must already hold the TopicManager
    /// mutex (e.g. via `lockForHookRegistration`).
    pub fn hasTopicLocked(self: *TopicManager, name: []const u8) bool {
        return self.topics.contains(name);
    }

    /// Check if a topic is soft-deleted.
    pub fn isTopicDeleted(self: *TopicManager, name: []const u8) bool {
        self.lock();
        defer self.unlock();
        return self.deleted_topics.contains(name);
    }

    /// List all topic names. Caller owns the returned slice and strings.
    pub fn listTopics(self: *TopicManager, allocator: Allocator) ![]TopicListEntry {
        self.lock();
        defer self.unlock();
        const keys = self.topics.keys();
        const result = try allocator.alloc(TopicListEntry, keys.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |e| allocator.free(e.name);
            allocator.free(result);
        }
        for (keys, 0..) |key, i| {
            result[i] = .{
                .name = try allocator.dupe(u8, key),
                .deleted = self.deleted_topics.contains(key),
            };
            initialized = i + 1;
        }
        return result;
    }

    /// Return all topic names matching a subscription pattern.
    pub fn matchTopics(self: *TopicManager, allocator: Allocator, input: []const u8) ![][]const u8 {
        self.lock();
        defer self.unlock();
        var matched: std.ArrayList([]const u8) = .empty;
        errdefer { for (matched.items) |m| allocator.free(m); matched.deinit(allocator); }
        for (self.topics.keys()) |key| {
            if (matchTopic(input, key))
                try matched.append(allocator, try allocator.dupe(u8, key));
        }
        return matched.toOwnedSlice(allocator);
    }

    /// Publish an event to a topic. Returns the global offset.
    /// Returns TopicDeleted if the topic has been soft-deleted.
    ///
    /// This is the choke point for *client* publishes — both the TCP and the
    /// HTTP handler funnel through here, while the store's own marker writes
    /// (`createTopic`, `deleteTopic`) call `self.log.append` directly. So the
    /// input guard belongs here and needs no bypass mechanism.
    pub fn publish(self: *TopicManager, topic_name: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        // Phase 0: reject inputs reserved for the store itself. Lock-free.
        try validatePublishInput(key, value);
        // Phase 1: validate under lock
        {
            self.lock();
            defer self.unlock();
            if (self.deleted_topics.contains(topic_name)) return TopicError.TopicDeleted;
            if (!self.topics.contains(topic_name)) return TopicError.NotFound;
        }
        // Phase 2: disk I/O without lock (log has its own mutex)
        const offset = try self.log.append(topic_name, key, value);
        // Phase 3: update index under lock
        {
            self.lock();
            defer self.unlock();
            const idx = self.topics.getPtr(topic_name) orelse return TopicError.NotFound;
            try idx.offsets.append(self.allocator, offset);
            if (value.len != 0) idx.non_marker_count += 1 else try idx.noteMarker(self.allocator);
            // Same critical section as the index update, so a reader that
            // observes the epoch also observes the event.
            self.append_epoch += 1;
        }
        // Phase 4: wake anyone waiting for this. Outside the lock, so woken
        // readers do not immediately contend with the thread that woke them.
        self.append_cond.broadcast(self.log.io);
        return offset;
    }

    /// Current publication epoch, for use as a wait baseline.
    ///
    /// Read this *before* looking for events, never after. An append landing
    /// between the look and the read would otherwise be folded into the
    /// baseline, and the waiter would sleep through the event it wanted.
    pub fn appendEpoch(self: *TopicManager) u64 {
        self.lock();
        defer self.unlock();
        return self.append_epoch;
    }

    /// Block until the publication epoch moves away from `baseline`, or until
    /// `timeout_ms` elapses, whichever comes first. Returns the epoch seen on
    /// waking, to be used as the next baseline.
    ///
    /// Returning immediately when the epoch has already moved is not an
    /// optimisation, it is the correctness property: it is what makes a
    /// wakeup delivered before the waiter parked impossible to lose.
    ///
    /// A wake means "look again", never "here is your event". Spurious
    /// wakeups, wakeups for another topic, and events taken by another reader
    /// all resolve the same way, by re-reading.
    pub fn waitForAppend(self: *TopicManager, baseline: u64, timeout_ms: u32) u64 {
        self.lock();
        defer self.unlock();
        if (self.append_epoch != baseline) return self.append_epoch;
        self.append_cond.waitTimeout(self.log.io, &self.mutex, .{ .duration = .{
            .raw = .fromMilliseconds(timeout_ms),
            .clock = .boot,
        } }) catch {}; // Timeout and cancelation are both "look again".
        return self.append_epoch;
    }

    /// Wake every waiter whether or not anything was published. Used by
    /// shutdown, which must not wait out each subscriber's block window.
    ///
    /// Bumps the epoch as well as broadcasting, so a waiter that has decided
    /// to wait but has not parked yet also returns immediately instead of
    /// sleeping out its timeout.
    pub fn wakeAllWaiters(self: *TopicManager) void {
        {
            self.lock();
            defer self.unlock();
            self.append_epoch += 1;
        }
        self.append_cond.broadcast(self.log.io);
    }

    /// Fetch events for a single topic by topic-local offset range.
    /// Skips internal marker events (empty value from createTopic).
    pub fn fetch(self: *TopicManager, allocator: Allocator, topic_name: []const u8, start: u64, max_count: u32) ![]Event {
        // Phase 1: seek to the cursor and copy only the tail we will read.
        //
        // `start` counts non-marker events; `offsets` counts records. The
        // index knows where its markers are, so the translation is arithmetic
        // rather than I/O — we do NOT read the skipped events off disk to
        // discover they are skipped. See air/v0.1/subscribe-fetch-seek.org.
        const offsets_copy = blk: {
            self.lock();
            defer self.unlock();
            const idx = self.topics.getPtr(topic_name) orelse return TopicError.NotFound;
            const from = idx.firstIndexForSkip(start);
            break :blk try allocator.dupe(u64, idx.offsets.items[from..]);
        };
        defer allocator.free(offsets_copy);

        // Phase 2: read from log WITHOUT holding the lock
        var events: std.ArrayList(Event) = .empty;
        errdefer { for (events.items) |e| store.freeEvent(allocator, e); events.deinit(allocator); }

        var i: usize = 0;
        while (i < offsets_copy.len and events.items.len < max_count) : (i += 1) {
            const event = (try self.log.read(allocator, offsets_copy[i])) orelse continue;
            // Skip marker events (creation markers and tombstones). Still done
            // here, and still not counted, so a stale seek cannot return the
            // wrong events — only a few too many reads.
            if (event.value.len == 0) {
                store.freeEvent(allocator, event);
                continue;
            }
            errdefer store.freeEvent(allocator, event);
            try events.append(allocator, event);
        }
        return events.toOwnedSlice(allocator);
    }

    /// Fetch events for a single topic strictly *after* a global log offset
    /// — the resume cursor behind `--after-offset` / `?after_offset=`. The
    /// topic index already stores each event's global offset in ascending
    /// order (`idx.offsets`), so we binary-search for the first stored
    /// offset `> after_offset` and read forward from there, skipping
    /// marker events (creation markers and tombstones). No new state.
    pub fn fetchAfterOffset(self: *TopicManager, allocator: Allocator, topic_name: []const u8, after_offset: u64, max_count: u32) ![]Event {
        // Phase 1: copy the relevant tail of the offset index under lock
        const offsets_copy = blk: {
            self.lock();
            defer self.unlock();
            const idx = self.topics.getPtr(topic_name) orelse return TopicError.NotFound;
            const items = idx.offsets.items;
            // Binary search: first index with items[i] > after_offset
            // (equivalently >= after_offset + 1; avoids overflow at maxInt).
            var lo: usize = 0;
            var hi: usize = items.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (items[mid] <= after_offset) lo = mid + 1 else hi = mid;
            }
            break :blk try allocator.dupe(u64, items[lo..]);
        };
        defer allocator.free(offsets_copy);

        // Phase 2: read from log WITHOUT holding the lock
        var events: std.ArrayList(Event) = .empty;
        errdefer { for (events.items) |e| store.freeEvent(allocator, e); events.deinit(allocator); }

        for (offsets_copy) |off| {
            if (events.items.len >= max_count) break;
            const event = (try self.log.read(allocator, off)) orelse continue;
            // Skip marker events (creation markers and tombstones)
            if (event.value.len == 0) {
                store.freeEvent(allocator, event);
                continue;
            }
            errdefer store.freeEvent(allocator, event);
            try events.append(allocator, event);
        }
        return events.toOwnedSlice(allocator);
    }

    /// Non-marker event count of an exact topic, or `null` if the topic
    /// doesn't exist. Read under the manager lock. Populates the
    /// `topic_events` field of fetch responses so clients can distinguish
    /// "start beyond end of topic" from "topic is empty".
    pub fn topicEventCount(self: *TopicManager, topic_name: []const u8) ?u64 {
        self.lock();
        defer self.unlock();
        if (self.topics.getPtr(topic_name)) |idx| return idx.non_marker_count;
        return null;
    }

    /// Fetch events matching `pattern`, starting at a global log offset.
    /// Used by the hook daemon for wildcard/prefix hooks, where the cursor
    /// is interpreted as the next global log offset to consider (rather than
    /// a per-topic skip count). This naturally handles topics that exist
    /// but have a low event count, and topics that didn't exist at the time
    /// the cursor was captured — every future event is delivered exactly
    /// once in publish order.
    ///
    /// Marker events (empty value: createTopic markers and tombstones) are
    /// skipped silently — they are not user-visible publishes.
    pub fn fetchPatternByOffset(
        self: *TopicManager,
        allocator: Allocator,
        pattern: []const u8,
        start_offset: u64,
        max_count: u32,
    ) ![]Event {
        // We read from the log directly. Hold the manager mutex while reading
        // so log appends and readBatch don't race; readBatch itself is not
        // thread-safe against concurrent appends.
        self.lock();
        defer self.unlock();

        var events: std.ArrayList(Event) = .empty;
        errdefer { for (events.items) |e| store.freeEvent(allocator, e); events.deinit(allocator); }

        var cursor = start_offset;
        const next = self.log.nextOffset();
        while (events.items.len < max_count and cursor < next) {
            // Read in chunks to bound memory; readBatch returns up to chunk size.
            // The `+ 16` slack lets us read a few extra entries to absorb
            // marker/tombstone skips so the buffer can still reach `max_count`
            // deliverable events without an extra round-trip.
            const chunk: u32 = @intCast(@min(@as(u64, max_count) - events.items.len + 16, 256));
            const batch = try self.log.readBatch(allocator, cursor, chunk);
            defer allocator.free(batch);
            if (batch.len == 0) break;
            for (batch, 0..) |evt, idx| {
                if (evt.value.len == 0) {
                    // Marker / tombstone — not user-visible.
                    cursor = evt.offset + 1;
                    store.freeEvent(allocator, evt);
                    continue;
                }
                if (!matchTopic(pattern, evt.topic)) {
                    cursor = evt.offset + 1;
                    store.freeEvent(allocator, evt);
                    continue;
                }
                if (events.items.len >= max_count) {
                    // Buffer full. Free this and the remaining un-iterated
                    // batch entries and bail out. We deliberately do NOT
                    // advance `cursor` past these events: this fn returns
                    // only delivered events, and the daemon advances the
                    // hook's persistent cursor based on `event.offset + 1`
                    // of those — so undelivered offsets are re-read on the
                    // next call.
                    store.freeEvent(allocator, evt);
                    for (batch[idx + 1 ..]) |rest| store.freeEvent(allocator, rest);
                    break;
                }
                cursor = evt.offset + 1;
                errdefer store.freeEvent(allocator, evt);
                try events.append(allocator, evt);
            }
        }
        return events.toOwnedSlice(allocator);
    }

    /// Count events matching `pattern` from a global log offset — the
    /// count-only sibling of `fetchPatternByOffset` (same scan-from-cursor
    /// loop: skip markers, `matchTopic`, tally instead of allocating event
    /// bodies). Used by `list_hooks` to derive a hook's Pending backlog.
    ///
    /// The scan stops tallying at `cap` so a hook far behind a large log
    /// cannot trigger an unbounded scan; callers render a result equal to
    /// `cap` as "cap+". The result is a point-in-time snapshot racing live
    /// publishes — fine for a status display.
    pub fn countPatternByOffset(
        self: *TopicManager,
        allocator: Allocator,
        pattern: []const u8,
        start_offset: u64,
        cap: u32,
    ) !u64 {
        // Same locking rationale as fetchPatternByOffset: readBatch is not
        // thread-safe against concurrent appends.
        self.lock();
        defer self.unlock();

        var count: u64 = 0;
        var cursor = start_offset;
        const next = self.log.nextOffset();
        while (count < cap and cursor < next) {
            const batch = try self.log.readBatch(allocator, cursor, 256);
            defer allocator.free(batch);
            if (batch.len == 0) break;
            for (batch) |evt| {
                defer store.freeEvent(allocator, evt);
                if (count >= cap) continue; // cap reached — just free the rest
                cursor = evt.offset + 1;
                if (evt.value.len == 0) continue; // marker / tombstone
                if (!matchTopic(pattern, evt.topic)) continue;
                count += 1;
            }
        }
        return count;
    }

    /// Fetch events across all topics matching a pattern.
    /// Skips internal marker events (creation markers and tombstones).
    pub fn fetchPattern(self: *TopicManager, allocator: Allocator, pattern: []const u8, start: u64, max_count: u32) ![]Event {
        // Phase 1: collect all matching offsets under lock
        const OffsetBatch = struct { offsets: []u64 };
        var batches: std.ArrayList(OffsetBatch) = .empty;
        defer {
            for (batches.items) |b| allocator.free(b.offsets);
            batches.deinit(allocator);
        }
        {
            self.lock();
            defer self.unlock();
            var topic_iter = self.topics.iterator();
            while (topic_iter.next()) |entry| {
                if (!matchTopic(pattern, entry.key_ptr.*)) continue;
                // Same seek as `fetch`, per matching topic: `start` is applied
                // to each topic independently, which is the behaviour this
                // function already had.
                const from = entry.value_ptr.firstIndexForSkip(start);
                const duped = try allocator.dupe(u64, entry.value_ptr.offsets.items[from..]);
                try batches.append(allocator, .{ .offsets = duped });
            }
        }

        // Phase 2: read from log WITHOUT holding the lock
        var events: std.ArrayList(Event) = .empty;
        errdefer { for (events.items) |e| store.freeEvent(allocator, e); events.deinit(allocator); }

        for (batches.items) |batch| {
            const offsets = batch.offsets;
            var i: usize = 0;
            while (i < offsets.len and events.items.len < max_count) : (i += 1) {
                const event = (try self.log.read(allocator, offsets[i])) orelse continue;
                // Markers are skipped and not counted; see `fetch`.
                if (event.value.len == 0) {
                    store.freeEvent(allocator, event);
                    continue;
                }
                errdefer store.freeEvent(allocator, event);
                try events.append(allocator, event);
            }
        }
        return events.toOwnedSlice(allocator);
    }

    // ── Private ─────────────────────────────────────────────────────────

    /// Scan the entire log to rebuild the topic index on startup.
    fn rebuildIndex(self: *TopicManager) !void {
        var offset: u64 = 0;
        while (offset < self.log.nextOffset()) {
            const event = (try self.log.read(self.allocator, offset)) orelse break;
            defer store.freeEvent(self.allocator, event);

            // Ensure topic exists in index
            const gop = try self.topics.getOrPut(self.allocator, event.topic);
            if (!gop.found_existing) {
                gop.key_ptr.* = try self.allocator.dupe(u8, event.topic);
                gop.value_ptr.* = .{};
            }
            try gop.value_ptr.offsets.append(self.allocator, offset);
            if (event.value.len != 0) gop.value_ptr.non_marker_count += 1 else try gop.value_ptr.noteMarker(self.allocator);

            // Detect deletion tombstone markers
            if (event.key) |key| {
                if (std.mem.eql(u8, key, deletion_marker_key) and event.value.len == 0) {
                    const owned_key = gop.key_ptr.*;
                    try self.deleted_topics.put(owned_key, {});
                }
            }

            offset += 1;
        }
    }
};

// ── Pattern matching ────────────────────────────────────────────────────────

/// Match a topic name against a subscription input.
///   "."          → all topics
///   "prefix."    → prefix match
///   contains "*" → segment pattern
///   otherwise    → exact match
pub fn matchTopic(input: []const u8, topic_name: []const u8) bool {
    if (input.len == 0 or topic_name.len == 0) return false;
    if (input.len == 1 and input[0] == '.') return true;
    if (input[input.len - 1] == '.') {
        if (topic_name.len <= input.len - 1) return false;
        return std.mem.startsWith(u8, topic_name, input);
    }
    if (std.mem.indexOfScalar(u8, input, '*') != null) return matchSegmentPattern(input, topic_name);
    return std.mem.eql(u8, input, topic_name);
}

fn matchSegmentPattern(pattern: []const u8, name: []const u8) bool {
    var pat_iter = std.mem.splitScalar(u8, pattern, '.');
    var name_iter = std.mem.splitScalar(u8, name, '.');
    while (true) {
        const p = pat_iter.next();
        const n = name_iter.next();
        if (p == null and n == null) return true;
        if (p == null or n == null) return false;
        if (std.mem.eql(u8, p.?, "*")) continue;
        if (!std.mem.eql(u8, p.?, n.?)) return false;
    }
}

/// True if `input` is a pattern — a trailing-dot prefix (including ".")
/// or a '*' wildcard — rather than an exact topic name.
pub fn isPatternShape(input: []const u8) bool {
    return std.mem.indexOfScalar(u8, input, '*') != null or
        (input.len > 0 and input[input.len - 1] == '.');
}

pub fn validateTopicName(name: []const u8) TopicError!void {
    if (name.len == 0 or name.len > 255) return TopicError.InvalidName;
    if (name[0] == '.' or name[name.len - 1] == '.') return TopicError.InvalidName;
    var prev_dot = false;
    for (name) |c| {
        const is_dot = c == '.';
        if (is_dot and prev_dot) return TopicError.InvalidName;
        prev_dot = is_dot;
        const valid = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '.' or c == '-' or c == '_';
        if (!valid) return TopicError.InvalidName;
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "matchTopic exact" {
    try std.testing.expect(matchTopic("a.b", "a.b"));
    try std.testing.expect(!matchTopic("a.b", "a.c"));
    try std.testing.expect(!matchTopic("a.b", "a.b.c"));
}
test "matchTopic prefix" {
    try std.testing.expect(matchTopic("a.", "a.b"));
    try std.testing.expect(matchTopic("a.", "a.b.c"));
    try std.testing.expect(!matchTopic("a.", "a"));
    try std.testing.expect(matchTopic(".", "x"));
}
test "matchTopic wildcard" {
    try std.testing.expect(matchTopic("a.*", "a.b"));
    try std.testing.expect(!matchTopic("a.*", "a.b.c"));
    try std.testing.expect(!matchTopic("a.*", "a"));
    try std.testing.expect(matchTopic("*", "x"));
    try std.testing.expect(!matchTopic("*", "x.y"));
    try std.testing.expect(matchTopic("a.*.c", "a.b.c"));
}
test "matchTopic edge" {
    try std.testing.expect(!matchTopic("", "a"));
    try std.testing.expect(!matchTopic("a", ""));
}

test "TopicManager publish and fetch" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t1");
    try tm.createTopic("t2");
    _ = try tm.publish("t1", "k", "v1");
    _ = try tm.publish("t2", null, "v2");
    _ = try tm.publish("t1", null, "v3");

    // Fetch t1 — should see 2 events
    const e1 = try tm.fetch(std.testing.allocator, "t1", 0, 10);
    defer { for (e1) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(e1); }
    try std.testing.expectEqual(@as(usize, 2), e1.len);
    try std.testing.expectEqualStrings("v1", e1[0].value);
    try std.testing.expectEqualStrings("v3", e1[1].value);

    // Fetch t2 — should see 1 event
    const e2 = try tm.fetch(std.testing.allocator, "t2", 0, 10);
    defer { for (e2) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(e2); }
    try std.testing.expectEqual(@as(usize, 1), e2.len);
    try std.testing.expectEqualStrings("v2", e2[0].value);
}

test "TopicManager fetchAfterOffset: mid-stream, at-tip, beyond-tip" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t1");
    const g1 = try tm.publish("t1", null, "v1");
    const g2 = try tm.publish("t1", null, "v2");
    const g3 = try tm.publish("t1", null, "v3");
    _ = g1;

    // Mid-stream: strictly after the 2nd event → exactly the 3rd.
    const mid = try tm.fetchAfterOffset(std.testing.allocator, "t1", g2, 10);
    defer { for (mid) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(mid); }
    try std.testing.expectEqual(@as(usize, 1), mid.len);
    try std.testing.expectEqualStrings("v3", mid[0].value);
    try std.testing.expectEqual(g3, mid[0].offset);

    // At-tip: strictly after the last event → nothing.
    const tip = try tm.fetchAfterOffset(std.testing.allocator, "t1", g3, 10);
    defer std.testing.allocator.free(tip);
    try std.testing.expectEqual(@as(usize, 0), tip.len);

    // Beyond-tip: an offset past everything → nothing, no error.
    const beyond = try tm.fetchAfterOffset(std.testing.allocator, "t1", g3 + 1000, 10);
    defer std.testing.allocator.free(beyond);
    try std.testing.expectEqual(@as(usize, 0), beyond.len);

    // Before everything (the creation marker's offset region) → all events.
    const all = try tm.fetchAfterOffset(std.testing.allocator, "t1", 0, 10);
    defer { for (all) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(all); }
    try std.testing.expectEqual(@as(usize, 3), all.len);

    // Missing topic → NotFound.
    try std.testing.expectError(TopicError.NotFound, tm.fetchAfterOffset(std.testing.allocator, "nope", 0, 10));
}

test "TopicManager fetchAfterOffset: markers and tombstones interleaved" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t1"); // creation marker in t1's index
    const g1 = try tm.publish("t1", null, "v1");
    try tm.createTopic("t2"); // interleaved global offset on another topic
    _ = try tm.publish("t2", null, "other");
    const g2 = try tm.publish("t1", null, "v2");
    try tm.deleteTopic("t1"); // tombstone marker appended to t1's index

    // Resuming after g1 skips the tombstone and yields only v2.
    const after1 = try tm.fetchAfterOffset(std.testing.allocator, "t1", g1, 10);
    defer { for (after1) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(after1); }
    try std.testing.expectEqual(@as(usize, 1), after1.len);
    try std.testing.expectEqualStrings("v2", after1[0].value);
    try std.testing.expectEqual(g2, after1[0].offset);

    // Resuming after g2 crosses only markers → nothing.
    const after2 = try tm.fetchAfterOffset(std.testing.allocator, "t1", g2, 10);
    defer std.testing.allocator.free(after2);
    try std.testing.expectEqual(@as(usize, 0), after2.len);
}

test "TopicManager fetchPatternByOffset serves the after_offset pattern path" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("agent.a");
    try tm.createTopic("agent.b");
    _ = try tm.publish("agent.a", null, "a1");
    const g2 = try tm.publish("agent.b", null, "b1");
    const g3 = try tm.publish("agent.a", null, "a2");

    // Server maps `after_offset` → start_offset = after_offset + 1.
    const events = try tm.fetchPatternByOffset(std.testing.allocator, "agent.", g2 + 1, 10);
    defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("a2", events[0].value);
    try std.testing.expectEqual(g3, events[0].offset);
}

test "TopicManager countPatternByOffset counts from cursor, excludes markers" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    // Creation markers and a tombstone interleaved with real events — none
    // of the markers may be tallied.
    try tm.createTopic("agent.a");
    try tm.createTopic("agent.b");
    try tm.createTopic("other.x");
    _ = try tm.publish("agent.a", null, "a1");
    const g_b1 = try tm.publish("agent.b", null, "b1");
    _ = try tm.publish("other.x", null, "x1"); // non-matching topic
    _ = try tm.publish("agent.a", null, "a2");
    try tm.createTopic("agent.late"); // marker after the cursor positions below
    _ = try tm.publish("agent.late", null, "l1");

    // From offset 0: all four matching non-marker events.
    try std.testing.expectEqual(@as(u64, 4), try tm.countPatternByOffset(std.testing.allocator, "agent.", 0, 1000));
    // From mid-log: only events at offset >= g_b1 + 1 (a2, l1).
    try std.testing.expectEqual(@as(u64, 2), try tm.countPatternByOffset(std.testing.allocator, "agent.", g_b1 + 1, 1000));
    // From the tip: nothing pending.
    try std.testing.expectEqual(@as(u64, 0), try tm.countPatternByOffset(std.testing.allocator, "agent.", tm.tipForPattern("agent."), 1000));
    // Wildcard matches exactly one segment: a two-segment suffix is counted
    // by the prefix pattern but not by the wildcard.
    try tm.createTopic("agent.deep.q");
    _ = try tm.publish("agent.deep.q", null, "d1");
    try std.testing.expectEqual(@as(u64, 5), try tm.countPatternByOffset(std.testing.allocator, "agent.", 0, 1000));
    try std.testing.expectEqual(@as(u64, 4), try tm.countPatternByOffset(std.testing.allocator, "agent.*", 0, 1000));
}

test "TopicManager countPatternByOffset honours the cap" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("agent.a");
    var i: usize = 0;
    while (i < 7) : (i += 1) _ = try tm.publish("agent.a", null, "v");

    // Cap below the true backlog — the tally stops at the cap.
    try std.testing.expectEqual(@as(u64, 3), try tm.countPatternByOffset(std.testing.allocator, "agent.", 0, 3));
    // Cap above the true backlog — exact count.
    try std.testing.expectEqual(@as(u64, 7), try tm.countPatternByOffset(std.testing.allocator, "agent.", 0, 1000));
}

test "TopicManager topicEventCount" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t1");
    try std.testing.expectEqual(@as(?u64, 0), tm.topicEventCount("t1"));
    _ = try tm.publish("t1", null, "v1");
    _ = try tm.publish("t1", null, "v2");
    try std.testing.expectEqual(@as(?u64, 2), tm.topicEventCount("t1"));
    try std.testing.expectEqual(@as(?u64, null), tm.topicEventCount("missing"));
}

test "TopicManager fetchPattern" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("agent.build");
    try tm.createTopic("agent.test");
    try tm.createTopic("file.changes");
    _ = try tm.publish("agent.build", null, "b1");
    _ = try tm.publish("agent.test", null, "t1");
    _ = try tm.publish("file.changes", null, "f1");

    const events = try tm.fetchPattern(std.testing.allocator, "agent.", 0, 10);
    defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, 2), events.len);
}

test "TopicManager recovery" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    { var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
      defer tm.deinit();
      try tm.createTopic("persist");
      _ = try tm.publish("persist", null, "hello");
    }
    { var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
      defer tm.deinit();
      try std.testing.expect(tm.hasTopic("persist"));
      const events = try tm.fetch(std.testing.allocator, "persist", 0, 10);
      defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
      try std.testing.expectEqual(@as(usize, 1), events.len);
      try std.testing.expectEqualStrings("hello", events[0].value);
    }
}

test "TopicManager create delete list" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("a");
    try tm.createTopic("b");
    try std.testing.expectError(TopicError.AlreadyExists, tm.createTopic("a"));
    const list = try tm.listTopics(std.testing.allocator);
    defer { for (list) |l| std.testing.allocator.free(l.name); std.testing.allocator.free(list); }
    try std.testing.expectEqual(@as(usize, 2), list.len);

    // Soft-delete: topic still exists but is marked deleted
    try tm.deleteTopic("a");
    try std.testing.expect(tm.hasTopic("a"));
    try std.testing.expect(tm.isTopicDeleted("a"));
    // Cannot delete again
    try std.testing.expectError(TopicError.NotFound, tm.deleteTopic("a"));
}

test "TopicManager createTopicLocked works under the registration lock" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    // The mutex is a non-reentrant spinlock — calling createTopic here
    // would deadlock. createTopicLocked is the reentrancy-safe body.
    tm.lockForHookRegistration();
    try tm.createTopicLocked("t.locked");
    try std.testing.expectError(TopicError.AlreadyExists, tm.createTopicLocked("t.locked"));
    try std.testing.expect(tm.hasTopicLocked("t.locked"));
    tm.unlockForHookRegistration();

    // Topic created under the lock behaves like any other topic.
    try std.testing.expect(tm.hasTopic("t.locked"));
    _ = try tm.publish("t.locked", null, "v");
    const events = try tm.fetch(std.testing.allocator, "t.locked", 0, 10);
    defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, 1), events.len);
}

test "isPatternShape distinguishes exact names from patterns" {
    try std.testing.expect(!isPatternShape("a.b"));
    try std.testing.expect(!isPatternShape("agent.results"));
    try std.testing.expect(isPatternShape("a."));
    try std.testing.expect(isPatternShape("."));
    try std.testing.expect(isPatternShape("a.*"));
    try std.testing.expect(isPatternShape("*"));
    try std.testing.expect(isPatternShape("a.*.c"));
}

test "TopicManager soft-delete blocks publish but allows fetch" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t1");
    _ = try tm.publish("t1", "k", "before-delete");
    try tm.deleteTopic("t1");

    // Publish should fail
    try std.testing.expectError(TopicError.TopicDeleted, tm.publish("t1", null, "after-delete"));

    // Fetch should still work and return the pre-deletion event
    const events = try tm.fetch(std.testing.allocator, "t1", 0, 10);
    defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqualStrings("before-delete", events[0].value);
}

test "TopicManager soft-delete survives restart" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    {
        var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("persist");
        _ = try tm.publish("persist", null, "data");
        try tm.deleteTopic("persist");
    }
    {
        var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try std.testing.expect(tm.hasTopic("persist"));
        try std.testing.expect(tm.isTopicDeleted("persist"));

        // Should still be readable
        const events = try tm.fetch(std.testing.allocator, "persist", 0, 10);
        defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
        try std.testing.expectEqual(@as(usize, 1), events.len);
        try std.testing.expectEqualStrings("data", events[0].value);

        // Should not be publishable
        try std.testing.expectError(TopicError.TopicDeleted, tm.publish("persist", null, "nope"));
    }
}

test "TopicManager tipForPattern: exact topic" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t.demo");
    try std.testing.expectEqual(@as(u64, 0), tm.tipForPattern("t.demo"));

    _ = try tm.publish("t.demo", null, "one");
    _ = try tm.publish("t.demo", null, "two");
    try std.testing.expectEqual(@as(u64, 2), tm.tipForPattern("t.demo"));

    // Unknown topic: tip is 0 (future events still caught when the topic is created)
    try std.testing.expectEqual(@as(u64, 0), tm.tipForPattern("t.does-not-exist"));
}

test "TopicManager tipForPattern: prefix and wildcard" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("agent.one");
    try tm.createTopic("agent.two");
    try tm.createTopic("file.a");
    _ = try tm.publish("agent.one", null, "a");
    _ = try tm.publish("agent.one", null, "b");
    _ = try tm.publish("agent.one", null, "c");
    _ = try tm.publish("agent.two", null, "x");
    _ = try tm.publish("file.a", null, "z");

    // For prefix/wildcard patterns, tip is the global log offset — i.e. the
    // next offset that will be assigned. 3 createTopic markers + 5 publishes
    // = 8 entries already written, so next offset is 8.
    try std.testing.expectEqual(@as(u64, 8), tm.tipForPattern("agent."));
    try std.testing.expectEqual(@as(u64, 8), tm.tipForPattern("agent.*"));
    try std.testing.expectEqual(@as(u64, 8), tm.tipForPattern("."));
}

test "TopicManager tipForPattern: tombstones and markers are excluded" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t.x"); // creation marker
    _ = try tm.publish("t.x", null, "real");
    // Tip counts only real events, not the creation marker.
    try std.testing.expectEqual(@as(u64, 1), tm.tipForPattern("t.x"));
}

test "TopicManager tip survives restart via rebuildIndex" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    {
        var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("persist.tip");
        _ = try tm.publish("persist.tip", null, "a");
        _ = try tm.publish("persist.tip", null, "b");
    }
    {
        var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try std.testing.expectEqual(@as(u64, 2), tm.tipForPattern("persist.tip"));
    }
}

test "TopicManager publish rejects the reserved tombstone key" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("victim");
    const before = tm.log.nextOffset();

    // The reported exploit: the exact pair `deleteTopic` writes.
    try std.testing.expectError(TopicError.ReservedKey, tm.publish("victim", deletion_marker_key, ""));
    // And with a non-empty value, so the reserved-key guard is shown to stand
    // on its own rather than being masked by the empty-value guard.
    try std.testing.expectError(TopicError.ReservedKey, tm.publish("victim", deletion_marker_key, "x"));

    // Nothing was appended: a rejected publish must not reach the log.
    try std.testing.expectEqual(before, tm.log.nextOffset());
    try std.testing.expect(!tm.isTopicDeleted("victim"));

    // The topic is still writable, and an ordinary key is untouched by the
    // guard (confirming the check is on the reserved value, not on keys).
    _ = try tm.publish("victim", "ordinary", "v");
    try std.testing.expectEqual(before + 1, tm.log.nextOffset());
}

test "TopicManager publish rejects empty values" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t");
    const before = tm.log.nextOffset();

    try std.testing.expectError(TopicError.EmptyValue, tm.publish("t", null, ""));
    try std.testing.expectError(TopicError.EmptyValue, tm.publish("t", "ordinary", ""));
    try std.testing.expectEqual(before, tm.log.nextOffset());

    // Regression: the normal case still works, including the contentless
    // signal the error message points callers at.
    _ = try tm.publish("t", null, "{}");
    _ = try tm.publish("t", "k", "v");
    try std.testing.expectEqual(before + 2, tm.log.nextOffset());

    const events = try tm.fetch(std.testing.allocator, "t", 0, 10);
    defer { for (events) |e| store.freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, 2), events.len);
}

test "TopicManager publish guard runs before the topic-state checks" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    // A malformed publish is rejected on its own terms rather than reporting
    // NotFound for a topic the caller was never allowed to write that to.
    try std.testing.expectError(TopicError.ReservedKey, tm.publish("no-such-topic", deletion_marker_key, "x"));
    try std.testing.expectError(TopicError.EmptyValue, tm.publish("no-such-topic", null, ""));
    // Control: without the reserved input, the same call does report NotFound,
    // so the assertions above could have failed the other way.
    try std.testing.expectError(TopicError.NotFound, tm.publish("no-such-topic", null, "v"));
}

test "internal marker writes are unaffected by the publish guard" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // createTopic and deleteTopic write exactly the key/value pairs the guard
    // rejects, but call log.append directly. If the guard ever grew a path
    // into them, the state below would not survive a rebuild.
    {
        var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("gone");
        try tm.createTopic("stays");
        _ = try tm.publish("gone", null, "data");
        try tm.deleteTopic("gone");
    }
    {
        var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        // The creation marker still registers a topic with no events...
        try std.testing.expect(tm.hasTopic("stays"));
        try std.testing.expect(!tm.isTopicDeleted("stays"));
        // ...and the tombstone still deletes, permanently.
        try std.testing.expect(tm.hasTopic("gone"));
        try std.testing.expect(tm.isTopicDeleted("gone"));
        try std.testing.expectError(TopicError.TopicDeleted, tm.publish("gone", null, "nope"));
        try std.testing.expectError(TopicError.AlreadyExists, tm.createTopic("gone"));
    }
}

test "TopicManager listTopics shows deleted flag" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("alive");
    try tm.createTopic("dead");
    try tm.deleteTopic("dead");

    const list = try tm.listTopics(std.testing.allocator);
    defer { for (list) |l| std.testing.allocator.free(l.name); std.testing.allocator.free(list); }
    try std.testing.expectEqual(@as(usize, 2), list.len);

    var found_alive = false;
    var found_dead = false;
    for (list) |entry| {
        if (std.mem.eql(u8, entry.name, "alive")) {
            try std.testing.expect(!entry.deleted);
            found_alive = true;
        }
        if (std.mem.eql(u8, entry.name, "dead")) {
            try std.testing.expect(entry.deleted);
            found_dead = true;
        }
    }
    try std.testing.expect(found_alive);
    try std.testing.expect(found_dead);
}
