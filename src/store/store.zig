//! Append-only event log — the core storage primitive of Ever.
//!
//! Events are sequentially written to segment files with monotonically
//! increasing offsets. Segments are rotated when they exceed a configurable
//! maximum size.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;

/// A single event record stored in the log.
pub const Event = struct {
    offset: u64,
    timestamp: i64,
    key: ?[]const u8,
    value: []const u8,

    /// Header size: offset(8) + timestamp(8) + key_len(4) + val_len(4) = 24 bytes.
    pub const header_size: usize = 24;

    /// Encode this event record into bytes.
    pub fn encode(self: Event, buf: []u8) error{BufferTooSmall}![]u8 {
        const key_bytes = self.key orelse &[_]u8{};
        const total = header_size + key_bytes.len + self.value.len;
        if (buf.len < total) return error.BufferTooSmall;

        std.mem.writeInt(u64, buf[0..8], self.offset, .little);
        std.mem.writeInt(i64, buf[8..16], self.timestamp, .little);
        std.mem.writeInt(u32, buf[16..20], @intCast(key_bytes.len), .little);
        std.mem.writeInt(u32, buf[20..24], @intCast(self.value.len), .little);
        @memcpy(buf[24 .. 24 + key_bytes.len], key_bytes);
        @memcpy(buf[24 + key_bytes.len .. total], self.value);

        return buf[0..total];
    }

    /// Decode an event record from bytes.
    pub fn decode(data: []const u8) error{CorruptRecord}!Event {
        if (data.len < header_size) return error.CorruptRecord;

        const offset = std.mem.readInt(u64, data[0..8], .little);
        const timestamp = std.mem.readInt(i64, data[8..16], .little);
        const key_len: usize = std.mem.readInt(u32, data[16..20], .little);
        const val_len: usize = std.mem.readInt(u32, data[20..24], .little);

        const total = header_size + key_len + val_len;
        if (data.len < total) return error.CorruptRecord;

        const key = if (key_len > 0) data[24 .. 24 + key_len] else null;
        const value = data[24 + key_len .. total];

        return .{
            .offset = offset,
            .timestamp = timestamp,
            .key = key,
            .value = value,
        };
    }

    /// Total size of this event record in bytes.
    pub fn totalSize(self: Event) usize {
        const key_len = if (self.key) |k| k.len else 0;
        return header_size + key_len + self.value.len;
    }
};

/// Metadata for a single log segment.
const Segment = struct {
    base_offset: u64,
    file: File,
    size: u64,
};

/// Configuration for the event log.
pub const Config = struct {
    max_segment_size: u64 = 64 * 1024 * 1024, // 64MB
    sync_on_append: bool = true,
};

/// Append-only event log with segment rotation.
pub const Log = struct {
    allocator: Allocator,
    io: Io,
    dir: Dir,
    segments: std.ArrayList(Segment) = .empty,
    next_offset: u64,
    config: Config,

    /// Initialize a log in the given directory.
    pub fn init(allocator: Allocator, io: Io, dir: Dir, config: Config) !Log {
        var log = Log{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .segments = .empty,
            .next_offset = 0,
            .config = config,
        };

        try log.recover();

        return log;
    }

    pub fn deinit(self: *Log) void {
        for (self.segments.items) |seg| {
            seg.file.close(self.io);
        }
        self.segments.deinit(self.allocator);
    }

    /// Append an event to the log. Returns the assigned offset.
    pub fn append(self: *Log, key: ?[]const u8, value: []const u8) !u64 {
        const offset = self.next_offset;
        const timestamp = getMilliTimestamp();

        const event = Event{
            .offset = offset,
            .timestamp = timestamp,
            .key = key,
            .value = value,
        };

        const record_size = event.totalSize();

        // Rotate if needed
        if (self.segments.items.len == 0 or
            self.activeSegment().size + record_size > self.config.max_segment_size)
        {
            try self.createSegment(offset);
        }

        // Encode and write
        var buf: [4096]u8 = undefined;
        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |hb| self.allocator.free(hb);

        const write_buf = if (record_size <= buf.len)
            &buf
        else blk: {
            heap_buf = try self.allocator.alloc(u8, record_size);
            break :blk heap_buf.?;
        };

        const encoded = try event.encode(write_buf);

        const seg = self.activeSegmentMut();
        seg.file.writePositionalAll(self.io, encoded, seg.size) catch |err| {
            return err;
        };

        if (self.config.sync_on_append) {
            seg.file.sync(self.io) catch {};
        }

        seg.size += record_size;
        self.next_offset = offset + 1;

        return offset;
    }

    /// Read a single event by offset. Returns null if not found.
    pub fn read(self: *Log, allocator: Allocator, offset: u64) !?Event {
        if (offset >= self.next_offset) return null;

        // Find the segment containing this offset
        const seg_idx = self.findSegment(offset) orelse return null;
        const seg = &self.segments.items[seg_idx];

        // Scan the segment to find the offset
        var pos: u64 = 0;
        var header_buf: [Event.header_size]u8 = undefined;

        while (pos < seg.size) {
            const bytes_read = seg.file.readPositionalAll(self.io, &header_buf, pos) catch break;
            if (bytes_read < Event.header_size) break;

            const event_offset = std.mem.readInt(u64, header_buf[0..8], .little);
            const key_len: usize = std.mem.readInt(u32, header_buf[16..20], .little);
            const val_len: usize = std.mem.readInt(u32, header_buf[20..24], .little);
            const total_size = Event.header_size + key_len + val_len;

            if (event_offset == offset) {
                // Read the full record
                const record = try allocator.alloc(u8, total_size);
                errdefer allocator.free(record);
                const full_read = seg.file.readPositionalAll(self.io, record, pos) catch {
                    allocator.free(record);
                    return error.CorruptRecord;
                };
                if (full_read < total_size) {
                    allocator.free(record);
                    return error.CorruptRecord;
                }
                const decoded = try Event.decode(record);
                // Make owned copies of key/value
                const key_copy: ?[]const u8 = if (decoded.key) |k| blk: {
                    const copy = try allocator.alloc(u8, k.len);
                    @memcpy(copy, k);
                    break :blk copy;
                } else null;
                const val_copy = try allocator.alloc(u8, decoded.value.len);
                @memcpy(val_copy, decoded.value);
                allocator.free(record);
                return .{
                    .offset = decoded.offset,
                    .timestamp = decoded.timestamp,
                    .key = key_copy,
                    .value = val_copy,
                };
            }

            pos += total_size;
        }

        return null;
    }

    /// Read a batch of events starting from offset.
    /// Caller owns the returned slice and each event's key/value memory.
    pub fn readBatch(self: *Log, allocator: Allocator, start_offset: u64, max_count: u32) ![]Event {
        var events: std.ArrayList(Event) = .empty;
        errdefer {
            for (events.items) |evt| {
                if (evt.key) |k| allocator.free(k);
                allocator.free(evt.value);
            }
            events.deinit(allocator);
        }

        var current_offset = start_offset;
        var count: u32 = 0;

        while (count < max_count and current_offset < self.next_offset) {
            if (try self.read(allocator, current_offset)) |event| {
                try events.append(allocator, event);
                count += 1;
            }
            current_offset += 1;
        }

        return events.toOwnedSlice(allocator);
    }

    /// Current next offset (total number of events written).
    pub fn nextOffset(self: *const Log) u64 {
        return self.next_offset;
    }

    // --- Private helpers ---

    fn activeSegment(self: *Log) *const Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }

    fn activeSegmentMut(self: *Log) *Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }

    fn findSegment(self: *Log, offset: u64) ?usize {
        var result: ?usize = null;
        for (self.segments.items, 0..) |seg, i| {
            if (seg.base_offset <= offset) {
                result = i;
            }
        }
        return result;
    }

    fn createSegment(self: *Log, base_offset: u64) !void {
        var name_buf: [40]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "{d:0>20}.log", .{base_offset}) catch unreachable;

        const file = self.dir.createFile(self.io, name, .{
            .read = true,
            .truncate = false,
        }) catch |err| {
            return err;
        };

        const stat = file.stat(self.io) catch |err| {
            file.close(self.io);
            return err;
        };

        try self.segments.append(self.allocator, .{
            .base_offset = base_offset,
            .file = file,
            .size = stat.size,
        });
    }

    fn recover(self: *Log) !void {
        // Collect existing segment files
        var seg_names: std.ArrayList([]u8) = .empty;
        defer {
            for (seg_names.items) |name| self.allocator.free(name);
            seg_names.deinit(self.allocator);
        }

        var iter = self.dir.iterate();
        while (try iter.next(self.io)) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".log")) continue;
            const name_copy = try self.allocator.alloc(u8, entry.name.len);
            @memcpy(name_copy, entry.name);
            try seg_names.append(self.allocator, name_copy);
        }

        // Sort by name (sorts by base offset due to zero-padding)
        std.mem.sort([]u8, seg_names.items, {}, struct {
            fn lessThan(_: void, a: []u8, b: []u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);

        // Open each segment
        for (seg_names.items) |name| {
            const stem = name[0 .. name.len - 4]; // strip ".log"
            const base_offset = std.fmt.parseInt(u64, stem, 10) catch continue;

            const file = self.dir.openFile(self.io, name, .{ .mode = .read_write }) catch continue;
            const stat = file.stat(self.io) catch {
                file.close(self.io);
                continue;
            };

            try self.segments.append(self.allocator, .{
                .base_offset = base_offset,
                .file = file,
                .size = stat.size,
            });
        }

        // Recover next_offset by scanning the last segment
        if (self.segments.items.len > 0) {
            const last_seg = &self.segments.items[self.segments.items.len - 1];
            var pos: u64 = 0;
            var header_buf: [Event.header_size]u8 = undefined;

            while (pos < last_seg.size) {
                const bytes_read = last_seg.file.readPositionalAll(self.io, &header_buf, pos) catch break;
                if (bytes_read < Event.header_size) break;

                const key_len: usize = std.mem.readInt(u32, header_buf[16..20], .little);
                const val_len: usize = std.mem.readInt(u32, header_buf[20..24], .little);
                const total_size = Event.header_size + key_len + val_len;

                if (pos + total_size > last_seg.size) break;

                const event_offset = std.mem.readInt(u64, header_buf[0..8], .little);
                self.next_offset = event_offset + 1;
                pos += total_size;
            }
        }
    }
};

/// Get current time in milliseconds since Unix epoch.
fn getMilliTimestamp() i64 {
    const ts = std.posix.clock_gettime(.REALTIME) catch return 0;
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

// --- Tests ---

test "Event encode and decode round-trip" {
    const event = Event{
        .offset = 42,
        .timestamp = 1711234567890,
        .key = "test-key",
        .value = "hello world",
    };

    var buf: [256]u8 = undefined;
    const encoded = try event.encode(&buf);

    const decoded = try Event.decode(encoded);
    try std.testing.expectEqual(@as(u64, 42), decoded.offset);
    try std.testing.expectEqual(@as(i64, 1711234567890), decoded.timestamp);
    try std.testing.expectEqualStrings("test-key", decoded.key.?);
    try std.testing.expectEqualStrings("hello world", decoded.value);
}

test "Event encode and decode without key" {
    const event = Event{
        .offset = 0,
        .timestamp = 1000,
        .key = null,
        .value = "data",
    };

    var buf: [256]u8 = undefined;
    const encoded = try event.encode(&buf);
    const decoded = try Event.decode(encoded);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.key);
    try std.testing.expectEqualStrings("data", decoded.value);
}

test "Log append and read single event" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
        .max_segment_size = 4096,
        .sync_on_append = false,
    });
    defer log.deinit();

    const offset = try log.append("key1", "value1");
    try std.testing.expectEqual(@as(u64, 0), offset);
    try std.testing.expectEqual(@as(u64, 1), log.nextOffset());

    const event = (try log.read(std.testing.allocator, 0)).?;
    defer {
        if (event.key) |k| std.testing.allocator.free(k);
        std.testing.allocator.free(event.value);
    }
    try std.testing.expectEqual(@as(u64, 0), event.offset);
    try std.testing.expectEqualStrings("key1", event.key.?);
    try std.testing.expectEqualStrings("value1", event.value);
}

test "Log append multiple events and readBatch" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
        .max_segment_size = 4096,
        .sync_on_append = false,
    });
    defer log.deinit();

    _ = try log.append(null, "event-0");
    _ = try log.append(null, "event-1");
    _ = try log.append(null, "event-2");

    const events = try log.readBatch(std.testing.allocator, 0, 10);
    defer {
        for (events) |evt| {
            if (evt.key) |k| std.testing.allocator.free(k);
            std.testing.allocator.free(evt.value);
        }
        std.testing.allocator.free(events);
    }

    try std.testing.expectEqual(@as(usize, 3), events.len);
    try std.testing.expectEqualStrings("event-0", events[0].value);
    try std.testing.expectEqualStrings("event-1", events[1].value);
    try std.testing.expectEqualStrings("event-2", events[2].value);
}

test "Log segment rotation" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Small segment size to force rotation
    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
        .max_segment_size = 60, // ~2 small events per segment
        .sync_on_append = false,
    });
    defer log.deinit();

    _ = try log.append(null, "a");
    _ = try log.append(null, "b");
    _ = try log.append(null, "c");

    // Should have multiple segments
    try std.testing.expect(log.segments.items.len >= 2);

    // All events should still be readable
    const events = try log.readBatch(std.testing.allocator, 0, 10);
    defer {
        for (events) |evt| {
            if (evt.key) |k| std.testing.allocator.free(k);
            std.testing.allocator.free(evt.value);
        }
        std.testing.allocator.free(events);
    }
    try std.testing.expectEqual(@as(usize, 3), events.len);
}

test "Log recovery after reopen" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Write some events
    {
        var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
            .max_segment_size = 4096,
            .sync_on_append = false,
        });
        defer log.deinit();

        _ = try log.append("k1", "v1");
        _ = try log.append("k2", "v2");
        _ = try log.append(null, "v3");
    }

    // Reopen and verify recovery
    {
        var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
            .max_segment_size = 4096,
            .sync_on_append = false,
        });
        defer log.deinit();

        try std.testing.expectEqual(@as(u64, 3), log.nextOffset());

        const event = (try log.read(std.testing.allocator, 1)).?;
        defer {
            if (event.key) |k| std.testing.allocator.free(k);
            std.testing.allocator.free(event.value);
        }
        try std.testing.expectEqualStrings("k2", event.key.?);
        try std.testing.expectEqualStrings("v2", event.value);

        // New appends should continue from recovered offset
        const new_offset = try log.append(null, "v4");
        try std.testing.expectEqual(@as(u64, 3), new_offset);
    }
}

test "Log read non-existent offset returns null" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
        .max_segment_size = 4096,
        .sync_on_append = false,
    });
    defer log.deinit();

    const result = try log.read(std.testing.allocator, 0);
    try std.testing.expectEqual(@as(?Event, null), result);
}
