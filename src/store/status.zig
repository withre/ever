//! Store status — offline inspection of an Ever data directory.
//!
//! Reads the data directory directly (no running server needed).
//! Scans segment files, counts events, discovers topics, reads hooks,
//! and checks lock status.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;
const Event = @import("store.zig").Event;

pub const TopicInfo = struct {
    name: []const u8,
    events: u64,
    deleted: bool = false,
};

pub const HookInfo = struct {
    id: u64,
    pattern: []const u8,
    command: []const u8,
    cursor: u64,
};

pub const StoreStatus = struct {
    data_dir: []const u8,
    segments: u64,
    total_bytes: u64,
    total_events: u64,
    topics: []TopicInfo,
    hooks: []HookInfo,
    lock_held: bool,

    pub fn deinit(self: *StoreStatus, alloc: Allocator) void {
        for (self.topics) |t| alloc.free(t.name);
        alloc.free(self.topics);
        for (self.hooks) |h| {
            alloc.free(h.pattern);
            alloc.free(h.command);
        }
        alloc.free(self.hooks);
    }
};

/// Scan a data directory and return store status.
pub fn getStatus(alloc: Allocator, io_: Io, data_dir_path: []const u8) !StoreStatus {
    const dir = Dir.cwd().openDir(io_, data_dir_path, .{ .iterate = true }) catch
        return error.CannotOpenDir;
    defer dir.close(io_);
    return getStatusFromDirWithPath(alloc, io_, dir, data_dir_path);
}

/// Scan a data directory (given as Dir) and return store status.
fn getStatusFromDir(alloc: Allocator, io_: Io, dir: Dir) !StoreStatus {
    // For tests, we don't have a path string, so use a placeholder
    return getStatusFromDirWithPath(alloc, io_, dir, ".");
}

/// Scan a data directory (given as Dir) and return store status with path.
fn getStatusFromDirWithPath(alloc: Allocator, io_: Io, dir: Dir, data_dir_path: []const u8) !StoreStatus {

    // 1. Scan *.log segments — count and sum sizes
    var segments: u64 = 0;
    var total_bytes: u64 = 0;
    {
        var iter = dir.iterate();
        while (try iter.next(io_)) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".log")) continue;
            segments += 1;
            const file = dir.openFile(io_, entry.name, .{ .mode = .read_only }) catch continue;
            defer file.close(io_);
            const stat = file.stat(io_) catch continue;
            total_bytes += stat.size;
        }
    }

    // 2. Scan shared log — read headers only to count events and discover topics
    var topic_counts = std.StringArrayHashMap(u64).init(alloc);
    defer {
        for (topic_counts.keys()) |k| alloc.free(k);
        topic_counts.deinit();
    }
    // Track which topics have been soft-deleted (tombstone marker)
    var deleted_set = std.StringHashMap(void).init(alloc);
    defer deleted_set.deinit(); // keys point into topic_counts, no separate free

    var total_events: u64 = 0;
    {
        // Collect and sort segment file names
        var seg_names: std.ArrayList([]u8) = .empty;
        defer {
            for (seg_names.items) |n| alloc.free(n);
            seg_names.deinit(alloc);
        }

        var iter2 = dir.iterate();
        while (try iter2.next(io_)) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".log")) continue;
            try seg_names.append(alloc, try alloc.dupe(u8, entry.name));
        }
        std.mem.sort([]u8, seg_names.items, {}, struct {
            fn f(_: void, a: []u8, b: []u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.f);

        for (seg_names.items) |name| {
            const file = dir.openFile(io_, name, .{ .mode = .read_only }) catch continue;
            defer file.close(io_);
            const stat = file.stat(io_) catch continue;
            const file_size = stat.size;

            var pos: u64 = 0;
            while (pos + Event.header_size <= file_size) {
                var header_buf: [Event.header_size]u8 = undefined;
                const n = file.readPositionalAll(io_, &header_buf, pos) catch break;
                if (n < Event.header_size) break;

                const topic_len: usize = std.mem.readInt(u16, header_buf[16..18], .little);
                const key_len: usize = std.mem.readInt(u32, header_buf[18..22], .little);
                const val_len: usize = std.mem.readInt(u32, header_buf[22..26], .little);
                const rec_size = Event.header_size + topic_len + key_len + val_len;

                if (pos + rec_size > file_size) break;

                // Read topic name
                if (topic_len > 0 and topic_len <= 255) {
                    var topic_buf: [255]u8 = undefined;
                    const tn = file.readPositionalAll(io_, topic_buf[0..topic_len], pos + Event.header_size) catch break;
                    if (tn == topic_len) {
                        const topic_name = topic_buf[0..topic_len];
                        // Only count as user event if value is non-empty
                        const gop = try topic_counts.getOrPut(topic_name);
                        if (!gop.found_existing) {
                            gop.key_ptr.* = try alloc.dupe(u8, topic_name);
                            gop.value_ptr.* = 0;
                        }
                        if (val_len > 0) {
                            gop.value_ptr.* += 1;
                            total_events += 1;
                        }

                        // Check for deletion tombstone marker:
                        // key == "__ever_tombstone__" and val_len == 0
                        if (val_len == 0 and key_len == 18) {
                            const tombstone_key = "__ever_tombstone__";
                            var key_buf: [18]u8 = undefined;
                            const kn = file.readPositionalAll(io_, &key_buf, pos + Event.header_size + topic_len) catch break;
                            if (kn == 18 and std.mem.eql(u8, &key_buf, tombstone_key)) {
                                try deleted_set.put(gop.key_ptr.*, {});
                            }
                        }
                    }
                }

                pos += rec_size;
            }
        }
    }

    // Build topics list
    const keys = topic_counts.keys();
    const values = topic_counts.values();
    const topics = try alloc.alloc(TopicInfo, keys.len);
    for (keys, values, 0..) |k, v, i| {
        topics[i] = .{ .name = try alloc.dupe(u8, k), .events = v, .deleted = deleted_set.contains(k) };
    }

    // 3. Read hooks.json
    const hooks = readHooks(alloc, io_, dir) catch try alloc.alloc(HookInfo, 0);

    // 4. Check lock status
    const lock_held = checkLockHeld(io_, dir);

    return .{
        .data_dir = data_dir_path,
        .segments = segments,
        .total_bytes = total_bytes,
        .total_events = total_events,
        .topics = topics,
        .hooks = hooks,
        .lock_held = lock_held,
    };
}

fn readHooks(alloc: Allocator, io_: Io, dir: Dir) ![]HookInfo {
    const file = dir.openFile(io_, "hooks.json", .{ .mode = .read_only }) catch return try alloc.alloc(HookInfo, 0);
    defer file.close(io_);

    const stat = file.stat(io_) catch return try alloc.alloc(HookInfo, 0);
    if (stat.size == 0 or stat.size > 1024 * 1024) return try alloc.alloc(HookInfo, 0);

    const buf = try alloc.alloc(u8, @intCast(stat.size));
    defer alloc.free(buf);

    const n = file.readPositionalAll(io_, buf, 0) catch return try alloc.alloc(HookInfo, 0);
    if (n == 0) return try alloc.alloc(HookInfo, 0);

    // New format: command is a string
    const HookJson = struct {
        id: u64,
        pattern: []const u8,
        command: []const u8,
        cwd: []const u8,
        cursor: u64,
        created_at: i64 = 0,
        once: bool = false,
    };

    const HookFileJson = struct {
        hooks: []const HookJson,
        next_id: u64,
    };

    // Legacy format: command is an array of strings
    const HookJsonLegacy = struct {
        id: u64,
        pattern: []const u8,
        command: []const []const u8,
        cwd: []const u8,
        cursor: u64,
        created_at: i64 = 0,
        once: bool = false,
    };

    const HookFileLegacy = struct {
        hooks: []const HookJsonLegacy,
        next_id: u64,
    };

    const data = buf[0..n];

    // Try new format first (command as string)
    if (std.json.parseFromSlice(HookFileJson, alloc, data, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    })) |parsed| {
        defer parsed.deinit();

        var hooks = try alloc.alloc(HookInfo, parsed.value.hooks.len);
        var initialized: usize = 0;
        errdefer {
            for (hooks[0..initialized]) |h| {
                alloc.free(h.pattern);
                alloc.free(h.command);
            }
            alloc.free(hooks);
        }

        for (parsed.value.hooks, 0..) |h, i| {
            hooks[i] = .{
                .id = h.id,
                .pattern = try alloc.dupe(u8, h.pattern),
                .command = try alloc.dupe(u8, h.command),
                .cursor = h.cursor,
            };
            initialized = i + 1;
        }
        return hooks;
    } else |_| {}

    // Fall back to legacy format (command as array of words)
    const legacy = std.json.parseFromSlice(HookFileLegacy, alloc, data, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return try alloc.alloc(HookInfo, 0);
    defer legacy.deinit();

    var hooks = try alloc.alloc(HookInfo, legacy.value.hooks.len);
    var initialized: usize = 0;
    errdefer {
        for (hooks[0..initialized]) |h| {
            alloc.free(h.pattern);
            alloc.free(h.command);
        }
        alloc.free(hooks);
    }

    for (legacy.value.hooks, 0..) |h, i| {
        // Join command array into a single display string
        var cmd_display: std.ArrayList(u8) = .empty;
        defer cmd_display.deinit(alloc);
        for (h.command, 0..) |arg, j| {
            if (j > 0) try cmd_display.append(alloc, ' ');
            try cmd_display.appendSlice(alloc, arg);
        }

        hooks[i] = .{
            .id = h.id,
            .pattern = try alloc.dupe(u8, h.pattern),
            .command = try cmd_display.toOwnedSlice(alloc),
            .cursor = h.cursor,
        };
        initialized = i + 1;
    }

    return hooks;
}

fn checkLockHeld(io_: Io, dir: Dir) bool {
    const file = dir.openFile(io_, "ever.lock", .{ .mode = .read_write }) catch return false;
    defer file.close(io_);

    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    const LOCK_UN: i32 = 8;
    const result = std.os.linux.flock(file.handle, LOCK_EX | LOCK_NB);
    if (result == 0) {
        // Lock acquired — no server running. Release immediately.
        _ = std.os.linux.flock(file.handle, LOCK_UN);
        return false;
    }
    // Lock failed — server is running
    return true;
}

/// Format human-readable size string (e.g., "12.4 MB").
fn formatSize(buf: []u8, bytes: u64) []const u8 {
    if (bytes < 1024) {
        return std.fmt.bufPrint(buf, "{d} B", .{bytes}) catch "? B";
    } else if (bytes < 1024 * 1024) {
        const kb = @as(f64, @floatFromInt(bytes)) / 1024.0;
        return std.fmt.bufPrint(buf, "{d:.1} KB", .{kb}) catch "? KB";
    } else if (bytes < 1024 * 1024 * 1024) {
        const mb = @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
        return std.fmt.bufPrint(buf, "{d:.1} MB", .{mb}) catch "? MB";
    } else {
        const gb = @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0 * 1024.0);
        return std.fmt.bufPrint(buf, "{d:.1} GB", .{gb}) catch "? GB";
    }
}

/// Format a number with comma separators (e.g., 48291 → "48,291").
fn formatNumber(buf: []u8, n: u64) []const u8 {
    // First print the raw number
    var raw_buf: [20]u8 = undefined;
    const raw = std.fmt.bufPrint(&raw_buf, "{d}", .{n}) catch return "?";

    if (raw.len <= 3) {
        @memcpy(buf[0..raw.len], raw);
        return buf[0..raw.len];
    }

    // Insert commas
    var pos: usize = 0;
    const first_group = raw.len % 3;
    if (first_group > 0) {
        @memcpy(buf[pos .. pos + first_group], raw[0..first_group]);
        pos += first_group;
    }
    var i: usize = first_group;
    while (i < raw.len) {
        if (pos > 0) {
            buf[pos] = ',';
            pos += 1;
        }
        @memcpy(buf[pos .. pos + 3], raw[i .. i + 3]);
        pos += 3;
        i += 3;
    }
    return buf[0..pos];
}

/// Print human-readable store status to stderr.
pub fn printHuman(status: *const StoreStatus) void {
    var size_buf: [32]u8 = undefined;
    var num_buf: [32]u8 = undefined;

    std.debug.print("\nEver Store: {s}\n", .{status.data_dir});
    std.debug.print("══════════════════════════════════════\n\n", .{});

    std.debug.print("  Log\n", .{});
    std.debug.print("    Segments:    {d}\n", .{status.segments});
    std.debug.print("    Total size:  {s}\n", .{formatSize(&size_buf, status.total_bytes)});
    std.debug.print("    Events:      {s}\n", .{formatNumber(&num_buf, status.total_events)});

    std.debug.print("\n  Topics ({d})\n", .{status.topics.len});
    if (status.topics.len == 0) {
        std.debug.print("    (none)\n", .{});
    } else {
        // First pass: find max topic name width (including marker)
        var max_name_w: usize = 0;
        for (status.topics) |t| {
            const w = t.name.len + if (t.deleted) @as(usize, 10) else @as(usize, 0); // " (deleted)" = 10
            if (w > max_name_w) max_name_w = w;
        }
        // Ensure minimum column width and add padding
        if (max_name_w < 20) max_name_w = 20;
        max_name_w += 2; // extra spacing before count

        // Second pass: print with alignment
        for (status.topics) |t| {
            var evt_buf: [32]u8 = undefined;
            const evt_str = formatNumber(&evt_buf, t.events);
            const marker: []const u8 = if (t.deleted) " (deleted)" else "";
            const name_w = t.name.len + marker.len;
            const padding = if (max_name_w > name_w) max_name_w - name_w else 0;
            const suffix: []const u8 = if (t.events == 1) " event" else " events";
            // Print: indent + name + marker + padding + right-aligned count + suffix
            std.debug.print("    {s}{s}", .{ t.name, marker });
            var i: usize = 0;
            while (i < padding) : (i += 1) std.debug.print(" ", .{});
            std.debug.print("{s:>6}{s}\n", .{ evt_str, suffix });
        }
    }

    std.debug.print("\n  Hooks ({d})\n", .{status.hooks.len});
    if (status.hooks.len == 0) {
        std.debug.print("    (none)\n", .{});
    } else {
        // First pass: find max widths for pattern and command columns
        var max_pat_w: usize = 0;
        var max_cmd_w: usize = 0;
        for (status.hooks) |h| {
            if (h.pattern.len > max_pat_w) max_pat_w = h.pattern.len;
            if (h.command.len > max_cmd_w) max_cmd_w = h.command.len;
        }
        if (max_pat_w < 10) max_pat_w = 10;
        if (max_cmd_w < 10) max_cmd_w = 10;
        max_pat_w += 2;
        max_cmd_w += 2;

        // Second pass: print with alignment
        for (status.hooks) |h| {
            var cursor_buf: [32]u8 = undefined;
            const cursor_str = formatNumber(&cursor_buf, h.cursor);
            // Print id
            std.debug.print("    #{d:<3} {s}", .{ h.id, h.pattern });
            // Pad pattern
            var p: usize = h.pattern.len;
            while (p < max_pat_w) : (p += 1) std.debug.print(" ", .{});
            std.debug.print("→ {s}", .{h.command});
            // Pad command
            var c: usize = h.command.len;
            while (c < max_cmd_w) : (c += 1) std.debug.print(" ", .{});
            std.debug.print("cursor: {s}\n", .{cursor_str});
        }
    }

    if (status.lock_held) {
        std.debug.print("\n  Lock: held (server running)\n\n", .{});
    } else {
        std.debug.print("\n  Lock: not held\n\n", .{});
    }
}

/// Print JSON store status to stderr.
pub fn printJson(status: *const StoreStatus, alloc: Allocator) void {
    var json: std.ArrayList(u8) = .empty;
    defer json.deinit(alloc);

    json.appendSlice(alloc, "{\n  \"data_dir\": \"") catch return;
    json.appendSlice(alloc, status.data_dir) catch return;
    json.appendSlice(alloc, "\",\n  \"log\": {\n") catch return;

    appendFmt(&json, alloc, "    \"segments\": {d},\n", .{status.segments});
    appendFmt(&json, alloc, "    \"total_bytes\": {d},\n", .{status.total_bytes});
    appendFmt(&json, alloc, "    \"total_events\": {d}\n", .{status.total_events});
    json.appendSlice(alloc, "  },\n  \"topics\": [\n") catch return;

    for (status.topics, 0..) |t, i| {
        json.appendSlice(alloc, "    {\"name\": \"") catch return;
        json.appendSlice(alloc, t.name) catch return;
        appendFmt(&json, alloc, "\", \"events\": {d}", .{t.events});
        if (t.deleted) {
            json.appendSlice(alloc, ", \"deleted\": true") catch return;
        }
        json.appendSlice(alloc, "}") catch return;
        if (i + 1 < status.topics.len) {
            json.appendSlice(alloc, ",\n") catch return;
        } else {
            json.appendSlice(alloc, "\n") catch return;
        }
    }

    json.appendSlice(alloc, "  ],\n  \"hooks\": [\n") catch return;

    for (status.hooks, 0..) |h, i| {
        appendFmt(&json, alloc, "    {{\"id\": {d}, \"pattern\": \"", .{h.id});
        json.appendSlice(alloc, h.pattern) catch return;
        json.appendSlice(alloc, "\", \"command\": \"") catch return;
        appendJsonEscaped(&json, alloc, h.command);
        appendFmt(&json, alloc, "\", \"cursor\": {d}}}", .{h.cursor});
        if (i + 1 < status.hooks.len) {
            json.appendSlice(alloc, ",\n") catch return;
        } else {
            json.appendSlice(alloc, "\n") catch return;
        }
    }

    json.appendSlice(alloc, "  ],\n") catch return;
    if (status.lock_held) {
        json.appendSlice(alloc, "  \"lock_held\": true\n") catch return;
    } else {
        json.appendSlice(alloc, "  \"lock_held\": false\n") catch return;
    }
    json.appendSlice(alloc, "}\n") catch return;

    std.debug.print("{s}", .{json.items});
}

fn appendFmt(json: *std.ArrayList(u8), alloc: Allocator, comptime fmt: []const u8, args: anytype) void {
    var buf: [256]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, fmt, args) catch return;
    json.appendSlice(alloc, s) catch return;
}

fn appendJsonEscaped(json: *std.ArrayList(u8), alloc: Allocator, s: []const u8) void {
    for (s) |c| switch (c) {
        '"' => json.appendSlice(alloc, "\\\"") catch return,
        '\\' => json.appendSlice(alloc, "\\\\") catch return,
        '\n' => json.appendSlice(alloc, "\\n") catch return,
        '\t' => json.appendSlice(alloc, "\\t") catch return,
        '\r' => json.appendSlice(alloc, "\\r") catch return,
        else => json.append(alloc, c) catch return,
    };
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "formatNumber" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("0", formatNumber(&buf, 0));
    try std.testing.expectEqualStrings("42", formatNumber(&buf, 42));
    try std.testing.expectEqualStrings("999", formatNumber(&buf, 999));
    try std.testing.expectEqualStrings("1,000", formatNumber(&buf, 1000));
    try std.testing.expectEqualStrings("48,291", formatNumber(&buf, 48291));
    try std.testing.expectEqualStrings("1,000,000", formatNumber(&buf, 1_000_000));
}

test "formatSize" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("0 B", formatSize(&buf, 0));
    try std.testing.expectEqualStrings("512 B", formatSize(&buf, 512));
    try std.testing.expectEqualStrings("1.0 KB", formatSize(&buf, 1024));
    try std.testing.expectEqualStrings("1.0 MB", formatSize(&buf, 1024 * 1024));
}

test "getStatus on data dir with events" {
    const alloc = std.testing.allocator;
    const test_io = std.testing.io;

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Create a TopicManager, publish some events, then close it
    const topic_mod = @import("topic.zig");
    {
        var tm = try topic_mod.TopicManager.init(alloc, test_io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("status.test");
        _ = try tm.publish("status.test", null, "event1");
        _ = try tm.publish("status.test", null, "event2");
    }

    // Now scan status — we need the path string, but tmpDir doesn't give us one easily.
    // Instead, test the scanning logic directly by calling the internal functions.
    // We'll exercise getStatus via its full path in the CLI integration test.
    // For unit testing, just verify the helper functions work.

    // Verify segments exist by iterating the dir
    var seg_count: u64 = 0;
    var iter = tmp.dir.iterate();
    while (try iter.next(test_io)) |entry| {
        if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".log")) seg_count += 1;
    }
    try std.testing.expect(seg_count >= 1);
}

test "getStatus end-to-end with real data" {
    const alloc = std.testing.allocator;
    const test_io = std.testing.io;
    const TopicManager = @import("topic.zig").TopicManager;

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Create a TopicManager, publish some events, then close it
    {
        var tm = try TopicManager.init(alloc, test_io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("agent.tasks");
        try tm.createTopic("agent.build");
        _ = try tm.publish("agent.tasks", null, "{\"task\":\"test1\"}");
        _ = try tm.publish("agent.tasks", null, "{\"task\":\"test2\"}");
        _ = try tm.publish("agent.build", null, "{\"build\":\"success\"}");
        _ = try tm.publish("agent.tasks", null, "{\"task\":\"test3\"}");
    }

    // Now getStatus should see the data
    // We need to use the actual path to tmp.dir
    // Since tmpDir doesn't give us the path directly, we'll test the internal functions
    // by scanning the dir directly
    var status = try getStatusFromDirWithPath(alloc, test_io, tmp.dir, ".");
    defer status.deinit(alloc);

    // Verify segment count and total events
    try std.testing.expect(status.segments >= 1);
    try std.testing.expectEqual(@as(u64, 4), status.total_events);

    // Verify topics
    try std.testing.expectEqual(@as(usize, 2), status.topics.len);
    
    // Find agent.tasks and verify count
    var found_tasks = false;
    var found_build = false;
    for (status.topics) |t| {
        if (std.mem.eql(u8, t.name, "agent.tasks")) {
            try std.testing.expectEqual(@as(u64, 3), t.events);
            found_tasks = true;
        }
        if (std.mem.eql(u8, t.name, "agent.build")) {
            try std.testing.expectEqual(@as(u64, 1), t.events);
            found_build = true;
        }
    }
    try std.testing.expect(found_tasks);
    try std.testing.expect(found_build);

    // Verify lock is not held (no server running)
    try std.testing.expectEqual(false, status.lock_held);
}
