//! Append-only event log — the core storage primitive of Ever.
//!
//! Events are sequentially written to segment files with monotonically
//! increasing offsets. Each segment maintains an in-memory position index
//! for O(1) offset lookups.

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

    /// Decode an event record from bytes. Returns slices into `data`.
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

/// Metadata for a single log segment, with in-memory position index.
const Segment = struct {
    base_offset: u64,
    file: File,
    size: u64,
    /// Maps (offset - base_offset) → byte position in file. O(1) lookup.
    positions: std.ArrayList(u64) = .empty,

    fn eventCount(self: *const Segment) u64 {
        return self.positions.items.len;
    }

    fn deinit(self: *Segment, allocator: Allocator, io: Io) void {
        self.file.close(io);
        self.positions.deinit(allocator);
    }
};

/// Configuration for the event log.
pub const Config = struct {
    max_segment_size: u64 = 64 * 1024 * 1024, // 64MB
    sync_on_append: bool = true,
};

/// Append-only event log with segment rotation and O(1) reads.
pub const Log = struct {
    allocator: Allocator,
    io: Io,
    dir: Dir,
    segments: std.ArrayList(Segment) = .empty,
    next_offset: u64,
    config: Config,

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
        for (self.segments.items) |*seg| {
            seg.deinit(self.allocator, self.io);
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

        // Encode into stack buffer or heap
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

        // Record position in index BEFORE writing
        try seg.positions.append(self.allocator, seg.size);

        seg.file.writePositionalAll(self.io, encoded, seg.size) catch |err| {
            // Rollback position index on write failure
            _ = seg.positions.pop();
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
    /// Caller owns the returned key/value memory.
    pub fn read(self: *Log, allocator: Allocator, offset: u64) !?Event {
        if (offset >= self.next_offset) return null;

        const seg = self.findSegmentForOffset(offset) orelse return null;
        const local_idx = offset - seg.base_offset;
        if (local_idx >= seg.positions.items.len) return null;

        const file_pos = seg.positions.items[@intCast(local_idx)];

        return try self.readEventAt(allocator, seg, file_pos);
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

        var offset = start_offset;
        var remaining: u32 = max_count;

        while (remaining > 0 and offset < self.next_offset) {
            const seg = self.findSegmentForOffset(offset) orelse break;
            const local_start = offset - seg.base_offset;
            const available = seg.eventCount() - local_start;
            const to_read: u64 = @min(available, remaining);

            // Read contiguous events from this segment
            for (0..to_read) |i| {
                const local_idx = local_start + i;
                const file_pos = seg.positions.items[@intCast(local_idx)];
                const event = try self.readEventAt(allocator, seg, file_pos);
                errdefer {
                    if (event.key) |k| allocator.free(k);
                    allocator.free(event.value);
                }
                try events.append(allocator, event);
            }

            offset += to_read;
            remaining -= @intCast(to_read);
        }

        return events.toOwnedSlice(allocator);
    }

    /// Current next offset (total number of events written).
    pub fn nextOffset(self: *const Log) u64 {
        return self.next_offset;
    }

    // ── Private helpers ─────────────────────────────────────────────────

    fn activeSegment(self: *Log) *const Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }

    fn activeSegmentMut(self: *Log) *Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }

    /// Binary search for the segment containing `offset`.
    fn findSegmentForOffset(self: *Log, offset: u64) ?*Segment {
        const items = self.segments.items;
        if (items.len == 0) return null;

        // Binary search: find the last segment with base_offset <= offset
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (items[mid].base_offset <= offset) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) return null;
        return &self.segments.items[lo - 1];
    }

    /// Read and decode a single event at a known file position.
    fn readEventAt(self: *Log, allocator: Allocator, seg: *const Segment, file_pos: u64) !Event {
        // Read header first
        var header_buf: [Event.header_size]u8 = undefined;
        const hdr_read = seg.file.readPositionalAll(self.io, &header_buf, file_pos) catch
            return error.CorruptRecord;
        if (hdr_read < Event.header_size) return error.CorruptRecord;

        const key_len: usize = std.mem.readInt(u32, header_buf[16..20], .little);
        const val_len: usize = std.mem.readInt(u32, header_buf[20..24], .little);
        const payload_size = key_len + val_len;

        // Read payload (key + value)
        const payload = try allocator.alloc(u8, payload_size);
        errdefer allocator.free(payload);

        if (payload_size > 0) {
            const payload_read = seg.file.readPositionalAll(self.io, payload, file_pos + Event.header_size) catch {
                allocator.free(payload);
                return error.CorruptRecord;
            };
            if (payload_read < payload_size) {
                allocator.free(payload);
                return error.CorruptRecord;
            }
        }

        const offset_val = std.mem.readInt(u64, header_buf[0..8], .little);
        const timestamp = std.mem.readInt(i64, header_buf[8..16], .little);

        // Return event with slices into the payload buffer.
        // Caller frees event.value which is the full payload allocation.
        // We pack key and value contiguously so one free covers both.
        const key: ?[]const u8 = if (key_len > 0) payload[0..key_len] else null;
        const value = payload[key_len..];

        return .{
            .offset = offset_val,
            .timestamp = timestamp,
            .key = key,
            .value = value,
        };
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
        // Collect existing segment file names
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

        std.mem.sort([]u8, seg_names.items, {}, struct {
            fn lessThan(_: void, a: []u8, b: []u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);

        for (seg_names.items) |name| {
            const stem = name[0 .. name.len - 4];
            const base_offset = std.fmt.parseInt(u64, stem, 10) catch continue;

            const file = self.dir.openFile(self.io, name, .{ .mode = .read_write }) catch continue;
            const stat = file.stat(self.io) catch {
                file.close(self.io);
                continue;
            };

            var seg = Segment{
                .base_offset = base_offset,
                .file = file,
                .size = stat.size,
            };

            // Build position index by scanning the segment once
            self.buildPositionIndex(&seg) catch {
                seg.deinit(self.allocator, self.io);
                continue;
            };

            if (seg.eventCount() > 0) {
                self.next_offset = base_offset + seg.eventCount();
            }

            try self.segments.append(self.allocator, seg);
        }
    }

    /// Scan a segment file once to build the in-memory position index.
    fn buildPositionIndex(self: *Log, seg: *Segment) !void {
        var pos: u64 = 0;
        var header_buf: [Event.header_size]u8 = undefined;

        while (pos < seg.size) {
            const bytes_read = seg.file.readPositionalAll(self.io, &header_buf, pos) catch break;
            if (bytes_read < Event.header_size) break;

            const key_len: usize = std.mem.readInt(u32, header_buf[16..20], .little);
            const val_len: usize = std.mem.readInt(u32, header_buf[20..24], .little);
            const total_size = Event.header_size + key_len + val_len;

            if (pos + total_size > seg.size) break;

            try seg.positions.append(self.allocator, pos);
            pos += total_size;
        }
    }
};

/// Get current time in milliseconds since Unix epoch.
fn getMilliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.REALTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

// ── Tests ───────────────────────────────────────────────────────────────────

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
        // value allocation covers both key and value (contiguous payload)
        std.testing.allocator.free(event.value.ptr[0 .. (if (event.key) |k| k.len else 0) + event.value.len]);
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
            freeEvent(std.testing.allocator, evt);
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

    var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
        .max_segment_size = 60,
        .sync_on_append = false,
    });
    defer log.deinit();

    _ = try log.append(null, "a");
    _ = try log.append(null, "b");
    _ = try log.append(null, "c");

    try std.testing.expect(log.segments.items.len >= 2);

    const events = try log.readBatch(std.testing.allocator, 0, 10);
    defer {
        for (events) |evt| freeEvent(std.testing.allocator, evt);
        std.testing.allocator.free(events);
    }
    try std.testing.expectEqual(@as(usize, 3), events.len);
}

test "Log recovery after reopen" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

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

    {
        var log = try Log.init(std.testing.allocator, io, tmp.dir, .{
            .max_segment_size = 4096,
            .sync_on_append = false,
        });
        defer log.deinit();

        try std.testing.expectEqual(@as(u64, 3), log.nextOffset());

        const event = (try log.read(std.testing.allocator, 1)).?;
        defer freeEvent(std.testing.allocator, event);
        try std.testing.expectEqualStrings("k2", event.key.?);
        try std.testing.expectEqualStrings("v2", event.value);

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

/// Helper to free an event returned by read/readBatch.
/// The payload (key + value) is a single contiguous allocation.
pub fn freeEvent(allocator: Allocator, event: Event) void {
    const key_len = if (event.key) |k| k.len else 0;
    const total_len = key_len + event.value.len;
    if (total_len > 0) {
        // The payload starts at key (or value if no key)
        const ptr = if (event.key) |k| k.ptr else event.value.ptr;
        allocator.free(ptr[0..total_len]);
    }
}
