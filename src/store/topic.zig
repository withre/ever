//! Topic management — named streams of events, each backed by an append-only log.

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

/// Manages a collection of named topics, each with its own event log.
pub const TopicManager = struct {
    allocator: Allocator,
    io: Io,
    dir: Dir, // The root data dir
    topics_dir: Dir,
    topics: std.StringArrayHashMap(*Log),
    config: Config,

    /// Initialize the topic manager. Opens data directories.
    pub fn init(allocator: Allocator, io: Io, dir: Dir, config: Config) !TopicManager {
        const topics_dir = dir.createDirPathOpen(io, "topics", .{
            .open_options = .{ .iterate = true },
        }) catch |err| return err;

        var manager = TopicManager{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .topics_dir = topics_dir,
            .topics = std.StringArrayHashMap(*Log).init(allocator),
            .config = config,
        };

        try manager.loadRegistry();

        return manager;
    }

    pub fn deinit(self: *TopicManager) void {
        var iter = self.topics.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.topics.deinit();
        self.topics_dir.close(self.io);
    }

    /// Create a new topic. Returns error if it already exists.
    pub fn createTopic(self: *TopicManager, name: []const u8) !void {
        try validateTopicName(name);

        if (self.topics.contains(name)) return TopicError.AlreadyExists;

        // Create topic directory under topics/
        self.topics_dir.createDir(self.io, name, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const topic_dir = self.topics_dir.openDir(self.io, name, .{ .iterate = true }) catch |err| return err;

        // Initialize the log
        const log = try self.allocator.create(Log);
        errdefer self.allocator.destroy(log);

        log.* = Log.init(self.allocator, self.io, topic_dir, .{
            .max_segment_size = self.config.max_segment_size,
            .sync_on_append = self.config.sync_on_append,
        }) catch |err| {
            topic_dir.close(self.io);
            return err;
        };

        const name_owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_owned);

        try self.topics.put(name_owned, log);
    }

    /// Delete a topic and its data.
    pub fn deleteTopic(self: *TopicManager, name: []const u8) !void {
        const entry = self.topics.fetchSwapRemove(name) orelse return TopicError.NotFound;

        entry.value.deinit();
        self.allocator.destroy(entry.value);
        self.allocator.free(entry.key);

        // Remove the directory
        self.topics_dir.deleteTree(self.io, name) catch {};
    }

    /// Get a topic's log for reading/writing. Returns error if not found.
    pub fn getTopic(self: *TopicManager, name: []const u8) !*Log {
        return self.topics.get(name) orelse TopicError.NotFound;
    }

    /// List all topic names. Caller owns the returned slice and its strings.
    pub fn listTopics(self: *TopicManager, allocator: Allocator) ![][]const u8 {
        const keys = self.topics.keys();
        const result = try allocator.alloc([]const u8, keys.len);
        errdefer allocator.free(result);

        for (keys, 0..) |key, i| {
            result[i] = try allocator.dupe(u8, key);
        }

        return result;
    }

    /// Return all topic names matching a subscription input.
    /// Caller owns the returned slice and its strings.
    pub fn matchTopics(self: *TopicManager, allocator: Allocator, input: []const u8) ![][]const u8 {
        var matched: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (matched.items) |m| allocator.free(m);
            matched.deinit(allocator);
        }

        for (self.topics.keys()) |key| {
            if (matchTopic(input, key)) {
                try matched.append(allocator, try allocator.dupe(u8, key));
            }
        }

        return matched.toOwnedSlice(allocator);
    }

    /// Publish an event to a topic. Creates the log entry and returns the offset.
    pub fn publish(self: *TopicManager, topic_name: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        const log = try self.getTopic(topic_name);
        return try log.append(key, value);
    }

    // --- Private helpers ---

    fn loadRegistry(self: *TopicManager) !void {
        // Scan the topics directory for existing topic subdirectories
        var iter = self.topics_dir.iterate();
        while (try iter.next(self.io)) |entry| {
            if (entry.kind != .directory) continue;

            const topic_dir = self.topics_dir.openDir(self.io, entry.name, .{ .iterate = true }) catch continue;

            const log = self.allocator.create(Log) catch {
                topic_dir.close(self.io);
                continue;
            };

            log.* = Log.init(self.allocator, self.io, topic_dir, .{
                .max_segment_size = self.config.max_segment_size,
                .sync_on_append = self.config.sync_on_append,
            }) catch {
                self.allocator.destroy(log);
                topic_dir.close(self.io);
                continue;
            };

            const name_owned = self.allocator.dupe(u8, entry.name) catch {
                log.deinit();
                self.allocator.destroy(log);
                continue;
            };

            self.topics.put(name_owned, log) catch {
                self.allocator.free(name_owned);
                log.deinit();
                self.allocator.destroy(log);
            };
        }
    }
};

/// Match a topic name against a subscription input.
///
/// Three modes:
///   "."              → matches all topics
///   "prefix."        → prefix match (topics starting with "prefix.")
///   contains "*"     → segment pattern (* = exactly one segment)
///   otherwise        → exact match
pub fn matchTopic(input: []const u8, topic_name: []const u8) bool {
    if (input.len == 0 or topic_name.len == 0) return false;

    // Mode 1: "." alone matches everything
    if (input.len == 1 and input[0] == '.') return true;

    // Mode 2: trailing dot = prefix match
    if (input[input.len - 1] == '.') {
        // Check if topic starts with the prefix (including the dot)
        if (topic_name.len <= input.len - 1) return false;
        return std.mem.startsWith(u8, topic_name, input);
    }

    // Mode 3: contains * = segment pattern match
    if (std.mem.indexOfScalar(u8, input, '*') != null) {
        return matchSegmentPattern(input, topic_name);
    }

    // Mode 4: exact match
    return std.mem.eql(u8, input, topic_name);
}

/// Segment-by-segment pattern matching. * matches exactly one segment.
fn matchSegmentPattern(pattern: []const u8, name: []const u8) bool {
    var pat_iter = std.mem.splitScalar(u8, pattern, '.');
    var name_iter = std.mem.splitScalar(u8, name, '.');

    while (true) {
        const pat_seg = pat_iter.next();
        const name_seg = name_iter.next();

        if (pat_seg == null and name_seg == null) return true; // both exhausted
        if (pat_seg == null or name_seg == null) return false; // length mismatch

        if (std.mem.eql(u8, pat_seg.?, "*")) continue; // * matches any single segment
        if (!std.mem.eql(u8, pat_seg.?, name_seg.?)) return false; // literal mismatch
    }
}

/// Validate a topic name.
pub fn validateTopicName(name: []const u8) TopicError!void {
    if (name.len == 0 or name.len > 255) return TopicError.InvalidName;
    if (name[0] == '.' or name[name.len - 1] == '.') return TopicError.InvalidName;

    var prev_dot = false;
    for (name) |c| {
        const is_dot = c == '.';
        if (is_dot and prev_dot) return TopicError.InvalidName;
        prev_dot = is_dot;

        const valid = (c >= 'a' and c <= 'z') or
            (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or
            c == '.' or c == '-' or c == '_';
        if (!valid) return TopicError.InvalidName;
    }
}

// --- Tests ---

test "matchTopic exact match" {
    try std.testing.expect(matchTopic("agent.tasks", "agent.tasks"));
    try std.testing.expect(!matchTopic("agent.tasks", "agent.build"));
    try std.testing.expect(!matchTopic("agent.tasks", "agent.tasks.x"));
    try std.testing.expect(matchTopic("agent", "agent"));
}

test "matchTopic prefix match (trailing dot)" {
    try std.testing.expect(matchTopic("agent.", "agent.tasks"));
    try std.testing.expect(matchTopic("agent.", "agent.build"));
    try std.testing.expect(matchTopic("agent.", "agent.tasks.build"));
    try std.testing.expect(!matchTopic("agent.", "agent")); // prefix requires more after
    try std.testing.expect(matchTopic("agent.tasks.", "agent.tasks.build"));
    try std.testing.expect(!matchTopic("agent.tasks.", "agent.tasks")); // must have more
    try std.testing.expect(matchTopic(".", "agent")); // dot alone = all
    try std.testing.expect(matchTopic(".", "a.b.c"));
}

test "matchTopic segment wildcard (*)" {
    try std.testing.expect(matchTopic("agent.*", "agent.tasks"));
    try std.testing.expect(matchTopic("agent.*", "agent.build"));
    try std.testing.expect(!matchTopic("agent.*", "agent.tasks.build")); // * is one segment
    try std.testing.expect(!matchTopic("agent.*", "agent")); // * requires a segment
    try std.testing.expect(matchTopic("*", "agent"));
    try std.testing.expect(!matchTopic("*", "agent.tasks")); // * is one segment
    try std.testing.expect(matchTopic("*.*", "agent.tasks"));
    try std.testing.expect(matchTopic("*.tasks", "agent.tasks"));
    try std.testing.expect(matchTopic("*.tasks", "build.tasks"));
    try std.testing.expect(!matchTopic("*.tasks", "tasks")); // * requires a segment
    try std.testing.expect(matchTopic("agent.*.complete", "agent.build.complete"));
    try std.testing.expect(!matchTopic("agent.*.complete", "agent.x.y.complete")); // * is one segment
}

test "matchTopic edge cases" {
    try std.testing.expect(!matchTopic("", "agent"));
    try std.testing.expect(!matchTopic("agent", ""));
    try std.testing.expect(!matchTopic("*", ""));
    try std.testing.expect(!matchTopic(".", ""));
}

test "TopicManager matchTopics" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
        .sync_on_append = false,
    });
    defer manager.deinit();

    try manager.createTopic("agent.tasks");
    try manager.createTopic("agent.build");
    try manager.createTopic("file.changes");

    // Prefix match
    {
        const matched = try manager.matchTopics(std.testing.allocator, "agent.");
        defer {
            for (matched) |m| std.testing.allocator.free(m);
            std.testing.allocator.free(matched);
        }
        try std.testing.expectEqual(@as(usize, 2), matched.len);
    }

    // All
    {
        const matched = try manager.matchTopics(std.testing.allocator, ".");
        defer {
            for (matched) |m| std.testing.allocator.free(m);
            std.testing.allocator.free(matched);
        }
        try std.testing.expectEqual(@as(usize, 3), matched.len);
    }

    // Wildcard
    {
        const matched = try manager.matchTopics(std.testing.allocator, "*.tasks");
        defer {
            for (matched) |m| std.testing.allocator.free(m);
            std.testing.allocator.free(matched);
        }
        try std.testing.expectEqual(@as(usize, 1), matched.len);
        try std.testing.expectEqualStrings("agent.tasks", matched[0]);
    }

    // Exact
    {
        const matched = try manager.matchTopics(std.testing.allocator, "file.changes");
        defer {
            for (matched) |m| std.testing.allocator.free(m);
            std.testing.allocator.free(matched);
        }
        try std.testing.expectEqual(@as(usize, 1), matched.len);
    }

    // No match
    {
        const matched = try manager.matchTopics(std.testing.allocator, "nope.*");
        defer std.testing.allocator.free(matched);
        try std.testing.expectEqual(@as(usize, 0), matched.len);
    }
}

test "validateTopicName accepts valid names" {
    try validateTopicName("agent-tasks");
    try validateTopicName("file-changes");
    try validateTopicName("my_topic_123");
    try validateTopicName("a");
}

test "validateTopicName rejects invalid names" {
    try std.testing.expectError(TopicError.InvalidName, validateTopicName(""));
    try std.testing.expectError(TopicError.InvalidName, validateTopicName(".leading"));
    try std.testing.expectError(TopicError.InvalidName, validateTopicName("trailing."));
    try std.testing.expectError(TopicError.InvalidName, validateTopicName("double..dot"));
    try std.testing.expectError(TopicError.InvalidName, validateTopicName("bad/slash"));
    try std.testing.expectError(TopicError.InvalidName, validateTopicName("bad space"));
}

test "TopicManager create and publish" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
        .max_segment_size = 4096,
        .sync_on_append = false,
    });
    defer manager.deinit();

    try manager.createTopic("test-topic");

    const offset = try manager.publish("test-topic", "key", "value");
    try std.testing.expectEqual(@as(u64, 0), offset);

    // Read back
    const log = try manager.getTopic("test-topic");
    const event = (try log.read(std.testing.allocator, 0)).?;
    defer store.freeEvent(std.testing.allocator, event);
    try std.testing.expectEqualStrings("value", event.value);
}

test "TopicManager duplicate create fails" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
        .sync_on_append = false,
    });
    defer manager.deinit();

    try manager.createTopic("t1");
    try std.testing.expectError(TopicError.AlreadyExists, manager.createTopic("t1"));
}

test "TopicManager delete topic" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
        .sync_on_append = false,
    });
    defer manager.deinit();

    try manager.createTopic("to-delete");
    try manager.deleteTopic("to-delete");
    try std.testing.expectError(TopicError.NotFound, manager.getTopic("to-delete"));
}

test "TopicManager list topics" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
        .sync_on_append = false,
    });
    defer manager.deinit();

    try manager.createTopic("alpha");
    try manager.createTopic("beta");

    const topics = try manager.listTopics(std.testing.allocator);
    defer {
        for (topics) |t| std.testing.allocator.free(t);
        std.testing.allocator.free(topics);
    }

    try std.testing.expectEqual(@as(usize, 2), topics.len);
}

test "TopicManager recovery after reopen" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // First session: create topic and publish
    {
        var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
            .max_segment_size = 4096,
            .sync_on_append = false,
        });
        defer manager.deinit();

        try manager.createTopic("persistent");
        _ = try manager.publish("persistent", null, "hello");
    }

    // Second session: verify recovery
    {
        var manager = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{
            .max_segment_size = 4096,
            .sync_on_append = false,
        });
        defer manager.deinit();

        const log = try manager.getTopic("persistent");
        try std.testing.expectEqual(@as(u64, 1), log.nextOffset());

        const event = (try log.read(std.testing.allocator, 0)).?;
        defer store.freeEvent(std.testing.allocator, event);
        try std.testing.expectEqualStrings("hello", event.value);
    }
}
