//! Wire protocol for Ever inter-process communication.
//!
//! Binary frame format:
//! ┌───────────┬──────────┬──────────┬──────────────────┐
//! │ version   │ msg_type │ body_len │ body             │
//! │ u8        │ u8       │ u32 LE   │ [body_len]u8     │
//! └───────────┴──────────┴──────────┴──────────────────┘
//!
//! Bodies are JSON-encoded.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const PROTOCOL_VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 6; // version(1) + msg_type(1) + body_len(4)
pub const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024; // 16MB

pub const MessageType = enum(u8) {
    // Requests
    publish = 0x01,
    fetch = 0x02,
    create_topic = 0x03,
    delete_topic = 0x04,
    list_topics = 0x05,
    ack = 0x06,
    register_hook = 0x07,
    unregister_hook = 0x08,
    list_hooks = 0x09,
    add_timer = 0x0A,
    remove_timer = 0x0B,
    list_timers = 0x0C,
    timer_info = 0x0D,
    hook_ps = 0x0E,
    hook_logs = 0x0F,
    status = 0x10,

    // Responses
    publish_ok = 0x81,
    fetch_ok = 0x82,
    create_topic_ok = 0x83,
    delete_topic_ok = 0x84,
    list_topics_ok = 0x85,
    ack_ok = 0x86,
    register_hook_ok = 0x87,
    unregister_hook_ok = 0x88,
    list_hooks_ok = 0x89,
    add_timer_ok = 0x8A,
    remove_timer_ok = 0x8B,
    list_timers_ok = 0x8C,
    timer_info_ok = 0x8D,
    hook_ps_ok = 0x8E,
    hook_logs_ok = 0x8F,
    status_ok = 0x90,

    // Error
    error_response = 0xFF,
};

pub const Frame = struct {
    version: u8,
    msg_type: MessageType,
    body: []const u8,
};

// --- Request/Response types ---

pub const PublishRequest = struct {
    topic: []const u8,
    key: ?[]const u8 = null,
    value: []const u8,
};

pub const PublishResponse = struct {
    offset: u64,
};

pub const FetchRequest = struct {
    topic: ?[]const u8 = null,
    pattern: ?[]const u8 = null,
    offset: u64 = 0,
    max_count: u32 = 100,
    block_ms: u32 = 0,
};

pub const EventData = struct {
    offset: u64,
    timestamp: i64,
    key: ?[]const u8 = null,
    value: []const u8,
    topic: ?[]const u8 = null,
};

pub const FetchResponse = struct {
    events: []const EventData,
};

pub const TopicRequest = struct {
    topic: []const u8,
};

pub const TopicInfoItem = struct {
    name: []const u8,
    deleted: bool = false,
};

pub const ListTopicsResponse = struct {
    topics: []const TopicInfoItem,
};

pub const AckRequest = struct {
    topic: []const u8,
    group: []const u8,
    offset: u64,
};

pub const ErrorResponse = struct {
    code: u16,
    message: []const u8,
};

// --- Hook request/response types ---

pub const RegisterHookRequest = struct {
    pattern: []const u8,
    command: []const u8,
    cwd: []const u8,
    once: bool = false,
    env: ?[]const []const u8 = null,
    name: ?[]const u8 = null,
    /// Starting cursor for the new hook (topic-local skip count).
    /// `null` — the default — means "current tip", resolved atomically by
    /// the server against the TopicManager's publish lock so no event can
    /// slip between tip-read and hook insertion. Pass `0` for a full
    /// historical replay, or an explicit value for partial replay.
    start_cursor: ?u64 = null,
    /// When true, `pattern` must be an exact topic name; the server creates
    /// the topic AND registers the hook in one critical section under the
    /// TopicManager's publish lock, so no event can precede the hook. Fails
    /// (registering nothing) if the topic already exists. Defaulted so old
    /// clients are unaffected. Mutually exclusive with `start_cursor`.
    create_topic: bool = false,
};

pub const RegisterHookResponse = struct {
    id: u64,
};

pub const UnregisterHookRequest = struct {
    id: u64,
};

pub const HookInfo = struct {
    id: u64,
    name: ?[]const u8 = null,
    pattern: []const u8,
    command: []const u8,
    cwd: []const u8,
    cursor: u64,
    once: bool = false,
    env: ?[]const []const u8 = null,
};

pub const ListHooksResponse = struct {
    hooks: []const HookInfo,
};

// --- Timer request/response types ---

pub const AddTimerRequest = struct {
    name: []const u8,
    schedule_type: []const u8, // "interval" or "cron"
    schedule_value: []const u8, // e.g. "5m" or "0 3 * * *"
    topic: []const u8,
    payload: []const u8 = "{}",
    persistent: bool = true,
};

pub const RemoveTimerRequest = struct {
    name: []const u8,
};

pub const TimerInfoRequest = struct {
    name: []const u8,
};

pub const TimerInfoData = struct {
    name: []const u8,
    schedule: []const u8,
    topic: []const u8,
    payload: []const u8,
    last_fired_at: i64,
    fire_count: u64,
    persistent: bool,
    created_at: i64,
};

pub const ListTimersResponse = struct {
    timers: []const TimerInfoData,
};

pub const TimerInfoResponse = struct {
    timer: TimerInfoData,
};

// --- Hook process/log request/response types ---

pub const HookPsRequest = struct {
    _unused: u8 = 0,
};

pub const HookProcessInfo = struct {
    hook_id: u64,
    pid: i32,
    pattern: []const u8,
    command: []const u8,
    start_time: i64,
    log_path: []const u8,
};

pub const HookPsResponse = struct {
    processes: []const HookProcessInfo,
};

pub const HookLogsRequest = struct {
    hook_id: u64,
    max_bytes: u32 = 65536,
};

pub const HookLogsResponse = struct {
    hook_id: u64,
    log_path: []const u8,
    content: []const u8,
};

// --- Status request/response types ---

pub const StatusRequest = struct {
    _unused: u8 = 0, // no parameters; mirrors HookPsRequest
};

pub const StatusTopicInfo = struct {
    name: []const u8,
    events: u64,
    deleted: bool = false,
};

pub const StatusHookInfo = struct {
    id: u64,
    pattern: []const u8,
    command: []const u8,
    cursor: u64,
};

pub const StatusResponse = struct {
    data_dir: []const u8,
    segments: u64,
    total_bytes: u64,
    total_events: u64,
    topics: []const StatusTopicInfo,
    hooks: []const StatusHookInfo,
    timer_count: u64,
    uptime_ms: u64,
};

// --- Encoding/Decoding ---

/// Encode a frame header and body to a buffer. Returns the encoded slice.
pub fn encodeFrame(buf: []u8, msg_type: MessageType, body: []const u8) error{BufferTooSmall}![]u8 {
    const total = HEADER_SIZE + body.len;
    if (buf.len < total) return error.BufferTooSmall;

    buf[0] = PROTOCOL_VERSION;
    buf[1] = @intFromEnum(msg_type);
    std.mem.writeInt(u32, buf[2..6], @intCast(body.len), .little);
    @memcpy(buf[HEADER_SIZE .. HEADER_SIZE + body.len], body);

    return buf[0..total];
}

/// Encode a frame and write it to a posix fd.
pub fn writeFrame(fd: std.posix.fd_t, msg_type: MessageType, body: []const u8) !void {
    var header: [HEADER_SIZE]u8 = undefined;
    header[0] = PROTOCOL_VERSION;
    header[1] = @intFromEnum(msg_type);
    std.mem.writeInt(u32, header[2..6], @intCast(body.len), .little);

    // Write header then body
    try writeAll(fd, &header);
    try writeAll(fd, body);
}

/// Read a frame from a posix fd. Returns null on clean EOF.
/// Caller owns the returned body slice.
pub fn readFrame(allocator: Allocator, fd: std.posix.fd_t) !?Frame {
    var header: [HEADER_SIZE]u8 = undefined;
    const header_read = readAll(fd, &header) catch return null;
    if (header_read == 0) return null; // Clean EOF
    if (header_read < HEADER_SIZE) return error.IncompleteHeader;

    const version = header[0];
    if (version != PROTOCOL_VERSION) return error.UnsupportedVersion;

    const msg_type: MessageType = @enumFromInt(header[1]);
    const body_len = std.mem.readInt(u32, header[2..6], .little);
    if (body_len > MAX_MESSAGE_SIZE) return error.MessageTooLarge;

    const body = try allocator.alloc(u8, body_len);
    errdefer allocator.free(body);

    const body_read = readAll(fd, body) catch {
        allocator.free(body);
        return null;
    };
    if (body_read < body_len) {
        allocator.free(body);
        return error.IncompleteBody;
    }

    return .{
        .version = version,
        .msg_type = msg_type,
        .body = body,
    };
}

/// Encode a typed message to JSON bytes.
pub fn encodeBody(allocator: Allocator, value: anytype) ![]u8 {
    return std.json.Stringify.valueAlloc(allocator, value, .{});
}

/// Decode JSON bytes to a typed message.
pub fn decodeBody(comptime T: type, allocator: Allocator, body: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, allocator, body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

// --- Error code helpers ---

pub const ErrorCode = struct {
    pub const bad_request: u16 = 400;
    pub const not_found: u16 = 404;
    pub const conflict: u16 = 409;
    pub const too_large: u16 = 413;
    pub const internal: u16 = 500;
};

// --- Low-level posix I/O helpers ---

fn writeAll(fd: std.posix.fd_t, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const remaining = data[written..];
        const rc = std.os.linux.write(fd, remaining.ptr, remaining.len);
        if (@as(isize, @bitCast(rc)) < 0) {
            const errno: u16 = @truncate(@as(usize, @bitCast(-@as(isize, @bitCast(rc)))));
            return switch (errno) {
                32 => error.BrokenPipe, // EPIPE
                104 => error.ConnectionResetByPeer, // ECONNRESET
                11 => continue, // EAGAIN
                else => error.Unexpected,
            };
        }
        if (rc == 0) return error.BrokenPipe;
        written += rc;
    }
}

fn readAll(fd: std.posix.fd_t, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const n = std.posix.read(fd, buf[total..]) catch |err| return err;
        if (n == 0) return total; // EOF
        total += n;
    }
    return total;
}

// --- Tests ---

test "frame encode and decode round-trip" {
    var buf: [1024]u8 = undefined;
    const body = "hello world";
    const encoded = try encodeFrame(&buf, .publish, body);

    // Parse header
    try std.testing.expectEqual(PROTOCOL_VERSION, encoded[0]);
    try std.testing.expectEqual(MessageType.publish, @as(MessageType, @enumFromInt(encoded[1])));
    const body_len = std.mem.readInt(u32, encoded[2..6], .little);
    try std.testing.expectEqual(@as(u32, 11), body_len);
    try std.testing.expectEqualStrings("hello world", encoded[6..]);
}

test "body encode and decode PublishRequest" {
    const req = PublishRequest{
        .topic = "test-topic",
        .key = "my-key",
        .value = "{\"data\":1}",
    };

    const body = try encodeBody(std.testing.allocator, req);
    defer std.testing.allocator.free(body);

    const parsed = try decodeBody(PublishRequest, std.testing.allocator, body);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("test-topic", parsed.value.topic);
    try std.testing.expectEqualStrings("my-key", parsed.value.key.?);
    try std.testing.expectEqualStrings("{\"data\":1}", parsed.value.value);
}

test "body encode and decode FetchResponse" {
    const events = [_]EventData{
        .{ .offset = 0, .timestamp = 1000, .key = null, .value = "v0" },
        .{ .offset = 1, .timestamp = 1001, .key = "k1", .value = "v1" },
    };

    const resp = FetchResponse{ .events = &events };

    const body = try encodeBody(std.testing.allocator, resp);
    defer std.testing.allocator.free(body);

    const parsed = try decodeBody(FetchResponse, std.testing.allocator, body);
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 2), parsed.value.events.len);
    try std.testing.expectEqualStrings("v0", parsed.value.events[0].value);
    try std.testing.expectEqualStrings("k1", parsed.value.events[1].key.?);
}

test "body encode and decode StatusResponse round-trip" {
    const topics = [_]StatusTopicInfo{
        .{ .name = "agent.tasks", .events = 42 },
        .{ .name = "agent.old", .events = 3, .deleted = true },
    };
    const hooks = [_]StatusHookInfo{
        .{ .id = 7, .pattern = "agent.", .command = "./notify.sh", .cursor = 12 },
    };
    const resp = StatusResponse{
        .data_dir = "/var/lib/ever",
        .segments = 2,
        .total_bytes = 4096,
        .total_events = 45,
        .topics = &topics,
        .hooks = &hooks,
        .timer_count = 3,
        .uptime_ms = 123456,
    };

    const body = try encodeBody(std.testing.allocator, resp);
    defer std.testing.allocator.free(body);

    const parsed = try decodeBody(StatusResponse, std.testing.allocator, body);
    defer parsed.deinit();

    try std.testing.expectEqualStrings("/var/lib/ever", parsed.value.data_dir);
    try std.testing.expectEqual(@as(u64, 2), parsed.value.segments);
    try std.testing.expectEqual(@as(u64, 4096), parsed.value.total_bytes);
    try std.testing.expectEqual(@as(u64, 45), parsed.value.total_events);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.topics.len);
    try std.testing.expectEqualStrings("agent.tasks", parsed.value.topics[0].name);
    try std.testing.expectEqual(@as(u64, 42), parsed.value.topics[0].events);
    try std.testing.expectEqual(false, parsed.value.topics[0].deleted);
    try std.testing.expectEqual(true, parsed.value.topics[1].deleted);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hooks.len);
    try std.testing.expectEqualStrings("agent.", parsed.value.hooks[0].pattern);
    try std.testing.expectEqualStrings("./notify.sh", parsed.value.hooks[0].command);
    try std.testing.expectEqual(@as(u64, 12), parsed.value.hooks[0].cursor);
    try std.testing.expectEqual(@as(u64, 3), parsed.value.timer_count);
    try std.testing.expectEqual(@as(u64, 123456), parsed.value.uptime_ms);
}

test "status message type codepoints" {
    try std.testing.expectEqual(@as(u8, 0x10), @intFromEnum(MessageType.status));
    try std.testing.expectEqual(@as(u8, 0x90), @intFromEnum(MessageType.status_ok));
}

test "frame encode buffer too small" {
    var buf: [4]u8 = undefined;
    const result = encodeFrame(&buf, .publish, "hello");
    try std.testing.expectError(error.BufferTooSmall, result);
}
