//! Shared append-only event log — the core storage primitive of Ever.
//!
//! All events from all topics are written sequentially to a single log.
//! Each event record includes its topic name. Topics are a logical
//! concept backed by an in-memory index, not separate files.
//!
//! Thread-safe: a mutex serializes all writes.

const std = @import("std");
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

pub const Config = struct {
    max_segment_size: u64 = 64 * 1024 * 1024,
    sync_on_append: bool = true,
};

/// Shared append-only log. Thread-safe for appends.
pub const Log = struct {
    allocator: Allocator,
    io: Io,
    dir: Dir,
    segments: std.ArrayList(Segment) = .empty,
    next_offset: u64 = 0,
    config: Config,
    mutex: std.atomic.Mutex ,

    pub fn init(allocator: Allocator, io: Io, dir: Dir, config: Config) !Log {
        var log = Log{
            .allocator = allocator,
            .io = io,
            .dir = dir,
            .config = config,
            .mutex = .unlocked,
        };
        try log.recover();
        return log;
    }

    pub fn deinit(self: *Log) void {
        for (self.segments.items) |*seg| seg.deinit(self.allocator, self.io);
        self.segments.deinit(self.allocator);
    }

    /// Append an event. Thread-safe. Returns the global offset.
    pub fn append(self: *Log, topic: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();

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

        if (self.config.sync_on_append) seg.file.sync(self.io) catch {};

        seg.size += rec_size;
        self.next_offset = offset + 1;
        return offset;
    }

    /// Read a single event by global offset. Returns null if not found.
    /// NOT thread-safe on its own — caller must serialize with appends
    /// (e.g., via TopicManager's mutex).
    pub fn read(self: *Log, allocator: Allocator, offset: u64) !?Event {
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
