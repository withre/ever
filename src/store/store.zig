//! Shared append-only event log — the core storage primitive of Ever.
//!
//! All events from all topics are written sequentially to a single log.
//! Each event record includes its topic name. Topics are a logical
//! concept backed by an in-memory index, not separate files.
//!
//! Thread-safe: a mutex serializes all writes. With `sync_on_append` the
//! fsync is NOT performed under that mutex: appenders that arrive while a
//! sync is in flight queue their records and share the next sync (group
//! commit, air/v0.1/log-group-commit.org). Every record is still durable
//! before its offset is returned.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;

/// A single event record.
pub const Event = struct {
    offset: u64,
    timestamp: i64,
    topic: []const u8,
    key: ?[]const u8,
    value: []const u8,

    /// Header: offset(8) + timestamp(8) + topic_len(2) + key_len(4) + val_len(4) = 26 bytes.
    pub const header_size: usize = 26;

    /// Encode this event into `buf`. Returns the encoded slice, or error if buffer too small.
    pub fn encode(self: Event, buf: []u8) error{BufferTooSmall}![]u8 {
        const key_bytes = self.key orelse &[_]u8{};
        const total = header_size + self.topic.len + key_bytes.len + self.value.len;
        if (buf.len < total) return error.BufferTooSmall;

        std.mem.writeInt(u64, buf[0..8], self.offset, .little);
        std.mem.writeInt(i64, buf[8..16], self.timestamp, .little);
        std.mem.writeInt(u16, buf[16..18], @intCast(self.topic.len), .little);
        std.mem.writeInt(u32, buf[18..22], @intCast(key_bytes.len), .little);
        std.mem.writeInt(u32, buf[22..26], @intCast(self.value.len), .little);
        var pos: usize = header_size;
        @memcpy(buf[pos .. pos + self.topic.len], self.topic);
        pos += self.topic.len;
        @memcpy(buf[pos .. pos + key_bytes.len], key_bytes);
        pos += key_bytes.len;
        @memcpy(buf[pos .. pos + self.value.len], self.value);

        return buf[0..total];
    }

    /// Decode an event from raw bytes. Returns CorruptRecord if data is malformed.
    pub fn decode(data: []const u8) error{CorruptRecord}!Event {
        if (data.len < header_size) return error.CorruptRecord;

        const offset = std.mem.readInt(u64, data[0..8], .little);
        const timestamp = std.mem.readInt(i64, data[8..16], .little);
        const topic_len: usize = std.mem.readInt(u16, data[16..18], .little);
        const key_len: usize = std.mem.readInt(u32, data[18..22], .little);
        const val_len: usize = std.mem.readInt(u32, data[22..26], .little);

        const total = header_size + topic_len + key_len + val_len;
        if (data.len < total) return error.CorruptRecord;

        var pos: usize = header_size;
        const topic = data[pos .. pos + topic_len];
        pos += topic_len;
        const key = if (key_len > 0) data[pos .. pos + key_len] else null;
        pos += key_len;
        const value = data[pos .. pos + val_len];

        return .{ .offset = offset, .timestamp = timestamp, .topic = topic, .key = key, .value = value };
    }

    /// Total byte size of this event when encoded (header + topic + key + value).
    pub fn recordSize(self: Event) usize {
        const key_len = if (self.key) |k| k.len else 0;
        return header_size + self.topic.len + key_len + self.value.len;
    }

    /// Compute record size from header bytes without decoding the full event.
    pub fn recordSizeFromHeader(header: *const [header_size]u8) usize {
        const topic_len: usize = std.mem.readInt(u16, header[16..18], .little);
        const key_len: usize = std.mem.readInt(u32, header[18..22], .little);
        const val_len: usize = std.mem.readInt(u32, header[22..26], .little);
        return header_size + topic_len + key_len + val_len;
    }
};

const Segment = struct {
    base_offset: u64,
    file: File,
    size: u64,
    positions: std.ArrayList(u64) = .empty,

    fn eventCount(self: *const Segment) u64 {
        return self.positions.items.len;
    }

    fn deinit(self: *Segment, allocator: Allocator, io: Io) void {
        self.file.close(io);
        self.positions.deinit(allocator);
    }
};

/// Name of the advisory lock file `Config.exclusive` acquires in the data
/// directory. `ever status` probes it (status.zig `checkLockHeld`) and the
/// CLI's data-dir listing special-cases it.
pub const lock_file_name = "ever.lock";

pub const Config = struct {
    max_segment_size: u64 = 64 * 1024 * 1024,
    sync_on_append: bool = true,
    /// Take `ever.lock` (flock LOCK_EX|LOCK_NB) in `Log.init`, before any
    /// segment is read, and hold it for the Log's lifetime. `ever start`
    /// sets this; an embedder that is the sole writer of its directory
    /// should too. Init fails with error.StoreLocked if another opener
    /// holds it. The lock belongs to the open file description: it is
    /// released when the Log is deinited or the process dies, and it is
    /// inherited by a child that is forked without exec — createFile opens
    /// O_CLOEXEC, so exec'd children (hooks) are safe.
    exclusive: bool = false,
};

/// The store's exclusion primitive, in one place so the CLI and an embedder
/// cannot drift apart: create-or-open `ever.lock` and take a non-blocking
/// exclusive flock on it. The lock is held until the returned file is
/// closed. (air/v0.1/embedded-store-marker.org)
fn acquireLockFile(io: Io, dir: Dir) !File {
    const lock_file = dir.createFile(io, lock_file_name, .{ .read = true, .truncate = false }) catch
        return error.LockFileUnavailable;
    errdefer lock_file.close(io);
    const LOCK_EX = 2;
    const LOCK_NB = 4;
    if (std.os.linux.flock(lock_file.handle, LOCK_EX | LOCK_NB) != 0)
        return error.StoreLocked;
    try verifyLockFileIdentity(io, dir, lock_file);
    return lock_file;
}

/// After a successful flock, confirm the file we hold is still the file at
/// the path. An flock is released by the kernel when its last holder dies,
/// so a held lock is a live process essentially always — but anything that
/// replaces `ever.lock` between a live holder's open and ours (a user
/// following old "remove if stale" advice, a cleanup script, a restore from
/// backup) leaves the holder locking an unlinked inode while the path names
/// a fresh one, and both openers then pass every check they have and write
/// one directory. fstat and stat agree on the inode iff nobody swapped the
/// file in between; refusing on mismatch turns swap-then-start from silent
/// corruption into an error, for two syscalls at init.
/// (air/v0.1/lock-identity-and-stale-message.org)
pub fn verifyLockFileIdentity(io: Io, dir: Dir, lock_file: File) !void {
    const fd_st = try lock_file.stat(io);
    const path_st = dir.statFile(io, lock_file_name, .{}) catch
        return error.LockFileVanished;
    // `Io.File.Stat` does not expose the device at this std revision; the
    // inode alone still catches every same-filesystem swap.
    if (fd_st.inode != path_st.inode)
        return error.LockFileReplaced;
}

/// Shared append-only log. Thread-safe for appends.
pub const Log = struct {
    allocator: Allocator,
    io: Io,
    dir: Dir,
    segments: std.ArrayList(Segment) = .empty,
    next_offset: u64 = 0,
    config: Config,
    /// Blocking, not spinning. See the note on `TopicManager.mutex` and
    /// air/v0.1/store-blocking-locks.org.
    mutex: std.Io.Mutex = .init,
    /// Group commit state (air/v0.1/log-group-commit.org), all guarded by
    /// `mutex`. `synced_watermark` counts in global offsets: every record
    /// with `offset < synced_watermark` has been covered by a completed
    /// fsync issued after its write. Monotonic — the one correctness rule
    /// of the design is that an appender compares its own end-offset
    /// (`offset + 1`) against this watermark, never against "some sync
    /// finished", because a sync that started before the record was
    /// written proves nothing about it. Only meaningful when
    /// `config.sync_on_append` is set.
    synced_watermark: u64 = 0,
    /// True while a leader appender is fsyncing outside the mutex. At most
    /// one sync is in flight at a time; appenders that find this set park
    /// on `sync_cond` and re-check the watermark when woken.
    sync_leader_active: bool = false,
    sync_cond: std.Io.Condition = .init,
    /// Test-only: fsyncs issued by `append` (leader or rotation), mutated
    /// under `mutex`, read after joining the threads that appended. Lets a
    /// test assert "one fsync covered more than one append" without timing
    /// anything.
    test_sync_count: if (builtin.is_test) u64 else void = if (builtin.is_test) 0 else {},
    /// Test-only: artificial delay (ns) before each leader fsync, widening
    /// the window in which followers queue so batches form deterministically.
    test_sync_delay_ns: if (builtin.is_test) u64 else void = if (builtin.is_test) 0 else {},
    /// Test-only: incremented when a leader elects itself while a
    /// non-active segment still holds unsynced records. The rotation-time
    /// sync exists to make this impossible — a leader in that state would
    /// fsync the wrong file and then claim those records durable.
    test_stale_leader_count: if (builtin.is_test) u64 else void = if (builtin.is_test) 0 else {},
    /// The flock on `ever.lock`, held when `config.exclusive` is set;
    /// closing it (deinit) releases the lock.
    lock_file: ?File = null,

    pub fn init(allocator: Allocator, io: Io, dir: Dir, config: Config) !Log {
        var log = Log{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .config = config,
            .mutex = .init,
        };
        // Exclusion comes first: a refused opener must never have read
        // segment state it cannot trust.
        if (config.exclusive) log.lock_file = try acquireLockFile(io, dir);
        errdefer if (log.lock_file) |lf| lf.close(io);
        try log.recover();
        // Recovery trusts what it reads off disk, so the durability
        // watermark starts past it: the first append must not think it
        // owes a sync for records it never wrote.
        log.synced_watermark = log.next_offset;
        return log;
    }

    pub fn deinit(self: *Log) void {
        for (self.segments.items) |*seg| seg.deinit(self.allocator, self.io);
        self.segments.deinit(self.allocator);
        if (self.lock_file) |lf| lf.close(self.io);
    }

    /// Append an event. Thread-safe. Returns the global offset.
    ///
    /// With `sync_on_append`, the record is durable before the offset is
    /// returned, but the mutex is not held across the fsync that makes it
    /// so. The first appender to need a sync becomes the *leader* and
    /// fsyncs outside the lock; appenders that arrive meanwhile write their
    /// records under the lock and wait, and the next sync covers them all
    /// at once (group commit, air/v0.1/log-group-commit.org). An appender
    /// that arrives alone syncs immediately — nobody ever waits for a
    /// batch to form.
    pub fn append(self: *Log, topic: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        self.mutex.lockUncancelable(self.io);

        const offset = self.appendLocked(topic, key, value) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };

        if (!self.config.sync_on_append) {
            self.mutex.unlock(self.io);
            return offset;
        }

        const my_end = offset + 1;
        while (self.synced_watermark < my_end) {
            if (self.sync_leader_active) {
                // A sync is in flight, but it may have started before our
                // record was written, so its completion proves nothing
                // about us: park and re-check the watermark. Condition
                // wakeups are a re-check loop, never a delivery.
                self.sync_cond.waitUncancelable(self.io, &self.mutex);
                continue;
            }
            // Become the leader for everything written so far. `target` is
            // captured under the mutex after all those writes completed,
            // so the fsync below is ordered after them and covers them.
            // The file handle is copied out because the segment list may
            // grow (rotation) while the mutex is released; rotation keeps
            // unsynced records confined to the active segment, so this one
            // file is always the right one to sync.
            self.sync_leader_active = true;
            const target = self.next_offset;
            const file = self.activeSegment().file;
            if (builtin.is_test) {
                self.test_sync_count += 1;
                if (self.synced_watermark < self.activeSegment().base_offset)
                    self.test_stale_leader_count += 1;
            }
            self.mutex.unlock(self.io);
            self.testSyncDelay();
            // A failed sync is discarded exactly as it was when it ran
            // under the mutex; reporting it belongs to
            // air/v0.1/sync-failure-reporting.org — noting that group
            // commit raises the stakes, since one failure now covers every
            // record in (watermark, target], not one caller's.
            file.sync(self.io) catch {};
            self.mutex.lockUncancelable(self.io);
            self.sync_leader_active = false;
            if (target > self.synced_watermark) self.synced_watermark = target;
            self.mutex.unlock(self.io);
            // Wake followers whether or not the watermark moved: some may
            // be waiting only for the leader slot so one of them can take
            // over. Outside the lock, like TopicManager's append_cond —
            // the Condition's epoch keeps an about-to-park waiter from
            // sleeping through it.
            self.sync_cond.broadcast(self.io);
            // `target >= my_end` always: it was captured after our own
            // write, so the sync we just did covers us.
            return offset;
        }
        self.mutex.unlock(self.io);
        return offset;
    }

    /// The write half of `append`: encode and write the record, index it,
    /// advance `next_offset`. Caller holds `mutex`. Does not sync — with
    /// `sync_on_append` that happens in `append`'s group-commit phase,
    /// outside the lock.
    fn appendLocked(self: *Log, topic: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        const offset = self.next_offset;
        const event = Event{
            .offset = offset,
            .timestamp = getMilliTimestamp(),
            .topic = topic,
            .key = key,
            .value = value,
        };
        const rec_size = event.recordSize();

        if (self.segments.items.len == 0 or
            self.activeSegment().size + rec_size > self.config.max_segment_size)
        {
            // Group commit's file invariant: records above
            // `synced_watermark` live only in the active segment, so a
            // sync leader has exactly one file to fsync. Rotating with
            // unsynced records in the outgoing segment would break that —
            // a later leader would sync the new file and then claim
            // records sitting unsynced in the old one — so sync it before
            // switching. Under the mutex, but rotation happens once per
            // `max_segment_size` bytes, not once per append.
            if (self.config.sync_on_append and self.segments.items.len != 0 and
                self.synced_watermark < self.next_offset)
            {
                if (builtin.is_test) self.test_sync_count += 1;
                self.activeSegment().file.sync(self.io) catch {};
                self.synced_watermark = self.next_offset;
                self.sync_cond.broadcast(self.io);
            }
            try self.createSegment(offset);
        }

        var buf: [4096]u8 = undefined;
        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |hb| self.allocator.free(hb);

        const write_buf = if (rec_size <= buf.len) &buf else blk: {
            heap_buf = try self.allocator.alloc(u8, rec_size);
            break :blk heap_buf.?;
        };

        const encoded = try event.encode(write_buf);
        const seg = self.activeSegmentMut();

        try seg.positions.append(self.allocator, seg.size);
        seg.file.writePositionalAll(self.io, encoded, seg.size) catch |err| {
            _ = seg.positions.pop();
            return err;
        };

        seg.size += rec_size;
        self.next_offset = offset + 1;
        return offset;
    }

    /// Test-only counter of records actually read off disk, incremented by
    /// `read` and `readBatch`. Exists so a test can assert that a fetch
    /// *seeks* rather than reading and discarding everything before its
    /// cursor (air/v0.1/subscribe-fetch-seek.org). Asserting on a read count
    /// is stable; asserting on wall-clock time is not.
    pub var test_read_count: if (builtin.is_test) usize else void = if (builtin.is_test) 0 else {};

    inline fn countRead(n: usize) void {
        if (builtin.is_test) test_read_count += n;
    }

    /// Test-only slow-device simulation for the leader fsync. A no-op in
    /// production builds and when `test_sync_delay_ns` is zero.
    inline fn testSyncDelay(self: *const Log) void {
        if (builtin.is_test) {
            if (self.test_sync_delay_ns != 0) {
                _ = std.os.linux.nanosleep(&.{
                    .sec = @intCast(self.test_sync_delay_ns / 1_000_000_000),
                    .nsec = @intCast(self.test_sync_delay_ns % 1_000_000_000),
                }, null);
            }
        }
    }

    /// Read a single event by global offset. Returns null if not found.
    /// NOT thread-safe on its own — caller must serialize with appends
    /// (e.g., via TopicManager's mutex).
    pub fn read(self: *Log, allocator: Allocator, offset: u64) !?Event {
        countRead(1);
        if (offset >= self.next_offset) return null;
        const seg = self.findSegmentForOffset(offset) orelse return null;
        const local_idx = offset - seg.base_offset;
        if (local_idx >= seg.positions.items.len) return null;
        return try self.readEventAt(allocator, seg, seg.positions.items[@intCast(local_idx)]);
    }

    /// Read a batch of events by global offset range.
    /// NOT thread-safe on its own — caller must serialize with appends
    /// (e.g., via TopicManager's mutex).
    pub fn readBatch(self: *Log, allocator: Allocator, start_offset: u64, max_count: u32) ![]Event {
        var events: std.ArrayList(Event) = .empty;
        errdefer {
            for (events.items) |evt| freeEvent(allocator, evt);
            events.deinit(allocator);
        }

        var offset = start_offset;
        var remaining: u32 = max_count;
        while (remaining > 0 and offset < self.next_offset) {
            const seg = self.findSegmentForOffset(offset) orelse break;
            const local_start = offset - seg.base_offset;
            const available = seg.eventCount() - local_start;
            const to_read: u64 = @min(available, remaining);

            countRead(@intCast(to_read));
            for (0..to_read) |i| {
                const event = try self.readEventAt(allocator, seg, seg.positions.items[@intCast(local_start + i)]);
                errdefer freeEvent(allocator, event);
                try events.append(allocator, event);
            }
            offset += to_read;
            remaining -= @intCast(to_read);
        }
        return events.toOwnedSlice(allocator);
    }

    pub fn nextOffset(self: *const Log) u64 {
        return self.next_offset;
    }

    // ── Private ─────────────────────────────────────────────────────────

    fn activeSegment(self: *Log) *const Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }
    fn activeSegmentMut(self: *Log) *Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }

    fn findSegmentForOffset(self: *Log, offset: u64) ?*Segment {
        const items = self.segments.items;
        if (items.len == 0) return null;
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (items[mid].base_offset <= offset) lo = mid + 1 else hi = mid;
        }
        if (lo == 0) return null;
        return &self.segments.items[lo - 1];
    }

    fn readEventAt(self: *Log, allocator: Allocator, seg: *const Segment, file_pos: u64) !Event {
        var header_buf: [Event.header_size]u8 = undefined;
        const hdr_read = seg.file.readPositionalAll(self.io, &header_buf, file_pos) catch return error.CorruptRecord;
        if (hdr_read < Event.header_size) return error.CorruptRecord;

        const payload_size = Event.recordSizeFromHeader(&header_buf) - Event.header_size;
        const payload = try allocator.alloc(u8, payload_size);
        errdefer allocator.free(payload);

        if (payload_size > 0) {
            const n = seg.file.readPositionalAll(self.io, payload, file_pos + Event.header_size) catch {
                allocator.free(payload);
                return error.CorruptRecord;
            };
            if (n < payload_size) { allocator.free(payload); return error.CorruptRecord; }
        }

        const topic_len: usize = std.mem.readInt(u16, header_buf[16..18], .little);
        const key_len: usize = std.mem.readInt(u32, header_buf[18..22], .little);

        var pos: usize = 0;
        const topic = payload[pos .. pos + topic_len]; pos += topic_len;
        const key: ?[]const u8 = if (key_len > 0) payload[pos .. pos + key_len] else null; pos += key_len;
        const value = payload[pos..];

        return .{
            .offset = std.mem.readInt(u64, header_buf[0..8], .little),
            .timestamp = std.mem.readInt(i64, header_buf[8..16], .little),
            .topic = topic,
            .key = key,
            .value = value,
        };
    }

    fn createSegment(self: *Log, base_offset: u64) !void {
        var name_buf: [40]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "{d:0>20}.log", .{base_offset}) catch unreachable;
        const file = self.dir.createFile(self.io, name, .{ .read = true, .truncate = false }) catch |err| return err;
        const stat = file.stat(self.io) catch |err| { file.close(self.io); return err; };
        try self.segments.append(self.allocator, .{ .base_offset = base_offset, .file = file, .size = stat.size });
    }

    fn recover(self: *Log) !void {
        var seg_names: std.ArrayList([]u8) = .empty;
        defer { for (seg_names.items) |n| self.allocator.free(n); seg_names.deinit(self.allocator); }

        var iter = self.dir.iterate();
        while (try iter.next(self.io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".log")) continue;
            const copy = try self.allocator.dupe(u8, entry.name);
            try seg_names.append(self.allocator, copy);
        }
        std.mem.sort([]u8, seg_names.items, {}, struct {
            fn f(_: void, a: []u8, b: []u8) bool { return std.mem.order(u8, a, b) == .lt; }
        }.f);

        for (seg_names.items) |name| {
            const base_offset = std.fmt.parseInt(u64, name[0 .. name.len - 4], 10) catch continue;
            const file = self.dir.openFile(self.io, name, .{ .mode = .read_write }) catch continue;
            const stat = file.stat(self.io) catch { file.close(self.io); continue; };
            var seg = Segment{ .base_offset = base_offset, .file = file, .size = stat.size };
            self.buildPositionIndex(&seg) catch { seg.deinit(self.allocator, self.io); continue; };
            if (seg.eventCount() > 0) self.next_offset = base_offset + seg.eventCount();
            try self.segments.append(self.allocator, seg);
        }
    }

    fn buildPositionIndex(self: *Log, seg: *Segment) !void {
        var pos: u64 = 0;
        var header_buf: [Event.header_size]u8 = undefined;
        while (pos < seg.size) {
            const n = seg.file.readPositionalAll(self.io, &header_buf, pos) catch break;
            if (n < Event.header_size) break;
            const total = Event.recordSizeFromHeader(&header_buf);
            if (pos + total > seg.size) break;
            try seg.positions.append(self.allocator, pos);
            pos += total;
        }
    }
};

/// Free an event returned by Log.read/readBatch.
/// The payload (topic + key + value) is a single contiguous allocation.
pub fn freeEvent(allocator: Allocator, event: Event) void {
    // topic is always present and starts the payload allocation
    const total = event.topic.len + (if (event.key) |k| k.len else 0) + event.value.len;
    if (total > 0) allocator.free(event.topic.ptr[0..total]);
}

fn getMilliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.REALTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "Event encode and decode round-trip" {
    const event = Event{ .offset = 42, .timestamp = 1711234567890, .topic = "agent.tasks", .key = "test-key", .value = "hello world" };
    var buf: [256]u8 = undefined;
    const encoded = try event.encode(&buf);
    const decoded = try Event.decode(encoded);
    try std.testing.expectEqual(@as(u64, 42), decoded.offset);
    try std.testing.expectEqualStrings("agent.tasks", decoded.topic);
    try std.testing.expectEqualStrings("test-key", decoded.key.?);
    try std.testing.expectEqualStrings("hello world", decoded.value);
}

test "Event encode and decode without key" {
    const event = Event{ .offset = 0, .timestamp = 1000, .topic = "t", .key = null, .value = "data" };
    var buf: [256]u8 = undefined;
    const decoded = try Event.decode(try event.encode(&buf));
    try std.testing.expectEqualStrings("t", decoded.topic);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.key);
    try std.testing.expectEqualStrings("data", decoded.value);
}

test "Log append and read" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 4096, .sync_on_append = false });
    defer log.deinit();

    const off = try log.append("topic.a", "key1", "value1");
    try std.testing.expectEqual(@as(u64, 0), off);

    const event = (try log.read(std.testing.allocator, 0)).?;
    defer freeEvent(std.testing.allocator, event);
    try std.testing.expectEqualStrings("topic.a", event.topic);
    try std.testing.expectEqualStrings("key1", event.key.?);
    try std.testing.expectEqualStrings("value1", event.value);
}

test "Log interleaved topics and readBatch" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 4096, .sync_on_append = false });
    defer log.deinit();

    _ = try log.append("a", null, "e0");
    _ = try log.append("b", null, "e1");
    _ = try log.append("a", null, "e2");

    const events = try log.readBatch(std.testing.allocator, 0, 10);
    defer { for (events) |e| freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }

    try std.testing.expectEqual(@as(usize, 3), events.len);
    try std.testing.expectEqualStrings("a", events[0].topic);
    try std.testing.expectEqualStrings("b", events[1].topic);
    try std.testing.expectEqualStrings("a", events[2].topic);
}

test "Log segment rotation" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 80, .sync_on_append = false });
    defer log.deinit();

    _ = try log.append("t", null, "a");
    _ = try log.append("t", null, "b");
    _ = try log.append("t", null, "c");
    try std.testing.expect(log.segments.items.len >= 2);

    const events = try log.readBatch(std.testing.allocator, 0, 10);
    defer { for (events) |e| freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, 3), events.len);
}

test "Log recovery after reopen" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    { var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 4096, .sync_on_append = false });
      defer log.deinit();
      _ = try log.append("t1", "k1", "v1");
      _ = try log.append("t2", "k2", "v2");
      _ = try log.append("t1", null, "v3");
    }
    { var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 4096, .sync_on_append = false });
      defer log.deinit();
      try std.testing.expectEqual(@as(u64, 3), log.nextOffset());
      const e = (try log.read(std.testing.allocator, 1)).?;
      defer freeEvent(std.testing.allocator, e);
      try std.testing.expectEqualStrings("t2", e.topic);
      try std.testing.expectEqualStrings("v2", e.value);
      try std.testing.expectEqual(@as(u64, 3), try log.append("t1", null, "v4"));
    }
}

test "Log read non-existent offset returns null" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 4096, .sync_on_append = false });
    defer log.deinit();
    try std.testing.expectEqual(@as(?Event, null), try log.read(std.testing.allocator, 0));
}

// ── Config.exclusive (air/v0.1/embedded-store-marker.org) ───────────────────
//
// flock attaches to the open file description, so two opens in one process
// contend exactly as two processes do — these tests need no subprocess.

test "Log exclusive: a second exclusive opener is refused until the first releases" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true });
    try std.testing.expectError(error.StoreLocked, Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true }));
    log.deinit();

    // deinit closed the lock file, which released the flock: takable again.
    var log2 = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true });
    log2.deinit();
}

test "Log without exclusive: two openers still succeed" {
    // Default false keeps every existing library caller's behaviour — and
    // keeps the unsafe two-opener case reachable for the future shared mode
    // (multi-process-access.org).
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var a = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer a.deinit();
    var b = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer b.deinit();
}

test "Log exclusive: interlocks with a hand-rolled ever.lock holder" {
    // The interim an embedder runs today: flock `ever.lock` by name, no
    // Config field. This test fails the day the convention moves (filename,
    // primitive, or flag) — which is the point of having it in Ever's tree
    // rather than only in an embedder's.
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const holder = try tmp.dir.createFile(io, "ever.lock", .{ .read = true, .truncate = false });
    defer holder.close(io);
    const LOCK_EX = 2;
    const LOCK_NB = 4;
    try std.testing.expectEqual(@as(usize, 0), std.os.linux.flock(holder.handle, LOCK_EX | LOCK_NB));

    try std.testing.expectError(error.StoreLocked, Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true }));
}

test "verifyLockFileIdentity: detects a lock file swapped under a live holder" {
    // The deterministic form of reverie's T4: hold the fd, unlink and
    // recreate the path from outside, run the check on the stale fd. The
    // old inode stays alive while our fd holds it, so the recreated file
    // cannot reuse its number.
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const held = try tmp.dir.createFile(io, "ever.lock", .{ .read = true, .truncate = false });
    defer held.close(io);
    const LOCK_EX = 2;
    const LOCK_NB = 4;
    try std.testing.expectEqual(@as(usize, 0), std.os.linux.flock(held.handle, LOCK_EX | LOCK_NB));

    // Intact: fd and path name the same inode.
    try verifyLockFileIdentity(io, tmp.dir, held);

    // Deleted: the path no longer resolves.
    try tmp.dir.deleteFile(io, "ever.lock");
    try std.testing.expectError(error.LockFileVanished, verifyLockFileIdentity(io, tmp.dir, held));

    // Recreated: the path resolves to a different inode than the one we
    // locked — the two-stores-on-one-directory state, caught.
    const replacement = try tmp.dir.createFile(io, "ever.lock", .{ .read = true, .truncate = false });
    defer replacement.close(io);
    try std.testing.expectError(error.LockFileReplaced, verifyLockFileIdentity(io, tmp.dir, held));
}

// ── Group commit (air/v0.1/log-group-commit.org) ────────────────────────
//
// The spec's acceptance is the concurrency table moving — total throughput
// rising with publisher count — but a wall-clock assertion in a unit test
// measures the scheduler, not the design. The design's observable is the
// sync count: with the mutex held across each fsync, N appends issue
// exactly N fsyncs no matter how they overlap; with group commit,
// overlapping appends share them. So these tests assert on
// `test_sync_count` (the batching) and on `synced_watermark` ordering (the
// guarantee), both deterministic, with `test_sync_delay_ns` standing in
// for the slow device that makes batches form.

/// Appends `count` records and checks, after each returned offset, that a
/// sync had covered it by the time `append` returned. The watermark is
/// monotonic, so sampling it after the return can only over-approximate:
/// if this check fails, the ordering rule was definitely violated.
const GroupCommitWorker = struct {
    log: *Log,
    topic: []const u8,
    count: usize,
    ok: bool = false,

    fn run(self: *GroupCommitWorker) void {
        var i: usize = 0;
        while (i < self.count) : (i += 1) {
            const off = self.log.append(self.topic, null, "v") catch return;
            self.log.mutex.lockUncancelable(self.log.io);
            const wm = self.log.synced_watermark;
            self.log.mutex.unlock(self.log.io);
            if (wm < off + 1) return;
        }
        self.ok = true;
    }
};

test "Log group commit: concurrent appenders share fsyncs, durably" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = true });
    defer log.deinit();
    // Make each sync visibly slow, so appenders pile up behind the leader
    // — the queue a batch forms from under real concurrency.
    log.test_sync_delay_ns = 2 * 1_000_000;

    const n_threads = 8;
    const per_thread = 10;
    const total = n_threads * per_thread;

    var workers: [n_threads]GroupCommitWorker = undefined;
    for (&workers) |*w| w.* = .{ .log = &log, .topic = "gc.share", .count = per_thread };
    var threads: [n_threads]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, GroupCommitWorker.run, .{&workers[i]});
    for (&threads) |*t| t.join();

    // Every append returned only after a sync covered it.
    for (&workers) |*w| try std.testing.expect(w.ok);
    try std.testing.expectEqual(@as(u64, total), log.nextOffset());
    try std.testing.expect(log.synced_watermark >= total);

    // The point of the design: at least one fsync covered more than one
    // append. The pre-group-commit tree — fsync under the mutex, once per
    // append — issues exactly `total` and fails this line; that tree is
    // this assertion's negative control.
    try std.testing.expect(log.test_sync_count < total);
    try std.testing.expectEqual(@as(u64, 0), log.test_stale_leader_count);
}

test "Log group commit: a lone appender syncs immediately, once per append" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = true });
    defer log.deinit();

    for (0..5) |i| {
        const off = try log.append("gc.solo", null, "v");
        try std.testing.expectEqual(@as(u64, i), off);
        try std.testing.expect(log.synced_watermark >= off + 1);
    }
    // Exactly one fsync per append: with nobody queued there is nothing to
    // batch and nothing may be deferred. Fewer would mean a sync was
    // skipped — the durability trade this design explicitly is not — and
    // this count is also what catches a variant that waits for a batch to
    // form before syncing.
    try std.testing.expectEqual(@as(u64, 5), log.test_sync_count);
}

test "Log group commit: rotation mid-batch keeps the guarantee" {
    // Tiny segments force rotations while leaders are mid-fsync. The
    // rotation-time sync keeps unsynced records confined to the active
    // segment; without it, a leader elected across the boundary fsyncs the
    // new file and then advances the watermark over records sitting
    // unsynced in the old one — which `test_stale_leader_count` records.
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .max_segment_size = 128, .sync_on_append = true });
    defer log.deinit();
    log.test_sync_delay_ns = 1 * 1_000_000;

    const n_threads = 4;
    const per_thread = 8;
    const total = n_threads * per_thread;

    var workers: [n_threads]GroupCommitWorker = undefined;
    for (&workers) |*w| w.* = .{ .log = &log, .topic = "gc.rot", .count = per_thread };
    var threads: [n_threads]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, GroupCommitWorker.run, .{&workers[i]});
    for (&threads) |*t| t.join();

    for (&workers) |*w| try std.testing.expect(w.ok);
    try std.testing.expect(log.segments.items.len >= 2);
    try std.testing.expectEqual(@as(u64, total), log.nextOffset());
    try std.testing.expect(log.synced_watermark >= total);
    try std.testing.expectEqual(@as(u64, 0), log.test_stale_leader_count);

    // And the records all landed, across every segment.
    const events = try log.readBatch(std.testing.allocator, 0, total);
    defer { for (events) |e| freeEvent(std.testing.allocator, e); std.testing.allocator.free(events); }
    try std.testing.expectEqual(@as(usize, total), events.len);
}

test "Log exclusive: the identity check does not false-positive on a healthy init" {
    // The swap itself can only be injected between flock and check, which
    // has no test hook; the deterministic detection test above runs the
    // check on a stale fd directly. What the composed path must guarantee
    // is the other direction: an ordinary exclusive init passes its own
    // wired-in identity check.
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true });
    log.deinit();
}
