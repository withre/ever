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

pub const TopicError = error{
    AlreadyExists,
    NotFound,
    InvalidName,
};

pub const Config = struct {
    max_segment_size: u64 = 64 * 1024 * 1024,
    sync_on_append: bool = true,
};

/// Per-topic index: tracks which global offsets belong to this topic.
const TopicIndex = struct {
    offsets: std.ArrayList(u64) = .empty,

    fn deinit(self: *TopicIndex, allocator: Allocator) void {
        self.offsets.deinit(allocator);
    }
};

/// Manages topics as a logical layer over a single shared Log.
pub const TopicManager = struct {
    allocator: Allocator,
    log: Log,
    topics: std.StringArrayHashMap(TopicIndex),
    mutex: std.atomic.Mutex ,

    pub fn init(allocator: Allocator, io: Io, dir: Dir, config: Config) !TopicManager {
        const log = try Log.init(allocator, io, dir, .{
            .max_segment_size = config.max_segment_size,
            .sync_on_append = config.sync_on_append,
        });

        var manager = TopicManager{
            .allocator = allocator,
            .log = log,
            .topics = std.StringArrayHashMap(TopicIndex).init(allocator),
            .mutex = .unlocked,
        };

        // Rebuild topic index from log contents
        try manager.rebuildIndex();

        return manager;
    }

    pub fn deinit(self: *TopicManager) void {
        var iter = self.topics.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.topics.deinit();
        self.log.deinit();
    }

    fn lock(self: *TopicManager) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Register a new topic. Writes a marker event to the log so the
    /// topic survives restart even if no events are published to it.
    pub fn createTopic(self: *TopicManager, name: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        try validateTopicName(name);
        if (self.topics.contains(name)) return TopicError.AlreadyExists;

        // Write a marker event so rebuildIndex discovers this topic on restart
        const offset = try self.log.append(name, null, "");
        const owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned);
        var idx: TopicIndex = .{};
        try idx.offsets.append(self.allocator, offset);
        try self.topics.put(owned, idx);
    }

    /// Remove a topic from the index. Log data stays (no compaction yet).
    pub fn deleteTopic(self: *TopicManager, name: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.topics.fetchSwapRemove(name) orelse return TopicError.NotFound;
        var idx = entry.value;
        idx.deinit(self.allocator);
        self.allocator.free(entry.key);
    }

    /// Check if a topic exists.
    pub fn hasTopic(self: *TopicManager, name: []const u8) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.topics.contains(name);
    }

    /// List all topic names. Caller owns the returned slice and strings.
    pub fn listTopics(self: *TopicManager, allocator: Allocator) ![][]const u8 {
        self.lock();
        defer self.mutex.unlock();
        const keys = self.topics.keys();
        const result = try allocator.alloc([]const u8, keys.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |s| allocator.free(s);
            allocator.free(result);
        }
        for (keys, 0..) |key, i| {
            result[i] = try allocator.dupe(u8, key);
            initialized = i + 1;
        }
        return result;
    }

    /// Return all topic names matching a subscription pattern.
    pub fn matchTopics(self: *TopicManager, allocator: Allocator, input: []const u8) ![][]const u8 {
        self.lock();
        defer self.mutex.unlock();
        var matched: std.ArrayList([]const u8) = .empty;
        errdefer { for (matched.items) |m| allocator.free(m); matched.deinit(allocator); }
        for (self.topics.keys()) |key| {
            if (matchTopic(input, key))
                try matched.append(allocator, try allocator.dupe(u8, key));
        }
        return matched.toOwnedSlice(allocator);
    }

    /// Publish an event to a topic. Returns the global offset.
    pub fn publish(self: *TopicManager, topic_name: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        self.lock();
        defer self.mutex.unlock();
        const idx = self.topics.getPtr(topic_name) orelse return TopicError.NotFound;
        const offset = try self.log.append(topic_name, key, value);
        try idx.offsets.append(self.allocator, offset);
        return offset;
    }

    /// Fetch events for a single topic by topic-local offset range.
    /// Skips internal marker events (empty value from createTopic).
    pub fn fetch(self: *TopicManager, allocator: Allocator, topic_name: []const u8, start: u64, max_count: u32) ![]Event {
        self.lock();
        defer self.mutex.unlock();
        const idx = self.topics.getPtr(topic_name) orelse return TopicError.NotFound;
        const offsets = idx.offsets.items;

        var events: std.ArrayList(Event) = .empty;
        errdefer { for (events.items) |e| store.freeEvent(allocator, e); events.deinit(allocator); }

        var skipped: u64 = 0;
        var i: usize = 0;
        while (i < offsets.len and events.items.len < max_count) : (i += 1) {
            const event = (try self.log.read(allocator, offsets[i])) orelse continue;
            // Skip marker events (empty value from createTopic)
            if (event.value.len == 0) {
                if (skipped < start) skipped += 1;
                store.freeEvent(allocator, event);
                continue;
            }
            if (i - skipped < start) { store.freeEvent(allocator, event); continue; }
            errdefer store.freeEvent(allocator, event);
            try events.append(allocator, event);
        }
        return events.toOwnedSlice(allocator);
    }

    /// Fetch events across all topics matching a pattern.
    /// Skips internal marker events (empty value from createTopic).
    pub fn fetchPattern(self: *TopicManager, allocator: Allocator, pattern: []const u8, start: u64, max_count: u32) ![]Event {
        self.lock();
        defer self.mutex.unlock();
        var events: std.ArrayList(Event) = .empty;
        errdefer { for (events.items) |e| store.freeEvent(allocator, e); events.deinit(allocator); }

        var topic_iter = self.topics.iterator();
        while (topic_iter.next()) |entry| {
            if (!matchTopic(pattern, entry.key_ptr.*)) continue;
            const offsets = entry.value_ptr.offsets.items;

            var skipped: u64 = 0;
            var i: usize = 0;
            while (i < offsets.len and events.items.len < max_count) : (i += 1) {
                const event = (try self.log.read(allocator, offsets[i])) orelse continue;
                if (event.value.len == 0) {
                    if (skipped < start) skipped += 1;
                    store.freeEvent(allocator, event);
                    continue;
                }
                if (i - skipped < start) { store.freeEvent(allocator, event); continue; }
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
            const gop = try self.topics.getOrPut(event.topic);
            if (!gop.found_existing) {
                gop.key_ptr.* = try self.allocator.dupe(u8, event.topic);
                gop.value_ptr.* = .{};
            }
            try gop.value_ptr.offsets.append(self.allocator, offset);
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
    defer { for (list) |l| std.testing.allocator.free(l); std.testing.allocator.free(list); }
    try std.testing.expectEqual(@as(usize, 2), list.len);

    try tm.deleteTopic("a");
    try std.testing.expect(!tm.hasTopic("a"));
    try std.testing.expectError(TopicError.NotFound, tm.deleteTopic("a"));
}
