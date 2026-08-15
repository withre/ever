//! Client library for connecting to the Ever store.
//!
//! Provides Publisher and Subscriber functionality through a unified Client.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;
const protocol = @import("../protocol/message.zig");
const store = @import("../store/store.zig");
const status_mod = @import("../store/status.zig");

pub const FetchResult = struct {
    events: []Event,
    /// Total non-marker events in the topic — present for exact-topic
    /// fetches, `null` for pattern fetches. See `FetchResponse.topic_events`.
    topic_events: ?u64 = null,
    /// Global log offset the server scanned up to, exclusive — present for
    /// pattern-after-offset fetches, `null` when the request did not scan
    /// (and from an older server, which is exactly "behave as today").
    /// Resume strictly after `max(last delivered offset, scan_watermark - 1)`
    /// so a quiet pattern advances past examined ranges without deliveries.
    /// See `FetchResponse.scan_watermark`.
    scan_watermark: ?u64 = null,
    allocator: Allocator,

    pub fn deinit(self: *FetchResult) void {
        for (self.events) |evt| {
            if (evt.key) |k| self.allocator.free(k);
            if (evt.topic) |t| self.allocator.free(t);
            self.allocator.free(evt.value);
        }
        self.allocator.free(self.events);
    }
};

pub const Event = struct {
    offset: u64,
    timestamp: i64,
    key: ?[]const u8,
    value: []const u8,
    topic: ?[]const u8 = null,
};

pub const HookInfoOwned = struct {
    id: u64,
    name: ?[]const u8 = null,
    pattern: []const u8,
    command: []const u8,
    cwd: []const u8,
    cursor: u64,
    once: bool = false,
    env: ?[]const []const u8 = null,
    // Execution counters (hook-failure-visibility). Defaulted — an old store
    // that omits them yields zeros/nulls.
    fired_count: u64 = 0,
    failure_count: u64 = 0,
    last_exit_status: ?u8 = null,
    last_failed_offset: ?u64 = null,
    // Derived listing fields (hook-list-legibility). `pending` is capped
    // server-side (renders "1000+" at the cap); `cursor_kind` tags the
    // polymorphic `cursor` ("topic_local" | "global"). Always heap-duped by
    // `listHooks`; an old store that omits them yields 0 / "topic_local".
    pending: u64 = 0,
    cursor_kind: []const u8,

    pub fn deinit(self: HookInfoOwned, allocator: Allocator) void {
        if (self.name) |n| allocator.free(n);
        allocator.free(self.pattern);
        allocator.free(self.command);
        allocator.free(self.cwd);
        allocator.free(self.cursor_kind);
        if (self.env) |env| {
            for (env) |e| allocator.free(e);
            allocator.free(env);
        }
    }
};

pub const ListHooksResult = struct {
    hooks: []HookInfoOwned,
    allocator: Allocator,

    pub fn deinit(self: *ListHooksResult) void {
        for (self.hooks) |h| h.deinit(self.allocator);
        self.allocator.free(self.hooks);
    }
};

pub const HookProcessInfoOwned = struct {
    hook_id: u64,
    pid: i32,
    pattern: []const u8,
    command: []const u8,
    start_time: i64,
    log_path: []const u8,

    pub fn deinit(self: HookProcessInfoOwned, allocator: Allocator) void {
        allocator.free(self.pattern);
        allocator.free(self.command);
        allocator.free(self.log_path);
    }
};

pub const HookPsResult = struct {
    processes: []HookProcessInfoOwned,
    allocator: Allocator,

    pub fn deinit(self: *HookPsResult) void {
        for (self.processes) |p| p.deinit(self.allocator);
        self.allocator.free(self.processes);
    }
};

pub const HookLogsResult = struct {
    hook_id: u64,
    log_path: []const u8,
    content: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *HookLogsResult) void {
        self.allocator.free(self.log_path);
        self.allocator.free(self.content);
    }
};

/// Owned result of a server status query. Wraps a `StoreStatus` whose
/// fields (including `data_dir`) are deep-copied from the wire response.
pub const StatusResult = struct {
    status: status_mod.StoreStatus,
    allocator: Allocator,

    pub fn deinit(self: *StatusResult) void {
        self.allocator.free(self.status.data_dir);
        self.status.deinit(self.allocator);
    }
};

pub const TimerInfo = struct {
    name: []const u8,
    schedule: []const u8,
    topic: []const u8,
    payload: []const u8,
    last_fired_at: i64,
    fire_count: u64,
    persistent: bool,
    created_at: i64,
};

pub const ListTimersResult = struct {
    timers: []TimerInfo,
    allocator: Allocator,

    pub fn deinit(self: *ListTimersResult) void {
        for (self.timers) |t| {
            self.allocator.free(t.name);
            self.allocator.free(t.schedule);
            self.allocator.free(t.topic);
            self.allocator.free(t.payload);
        }
        self.allocator.free(self.timers);
    }
};

/// Both fields of a server `error_response`, retained on the `Client` for
/// the caller who wants the text beside the tag. `message` is owned by the
/// client's allocator and lives until the next request (or `deinit`).
pub const ServerErrorInfo = struct { code: u16, message: []const u8 };

/// A server `error_response`, mapped tag-by-tag from the protocol's codes
/// (400, 404, 409, 413, 500) so a caller's `switch` reads like the protocol.
/// `error.ServerError` is the fallback: a code this client predates, or a
/// body that did not parse (`lastError().?.code == 0` in that case).
pub const ServerError = error{ BadRequest, NotFound, Conflict, TooLarge, Internal, ServerError };

/// Unified client for publishing and subscribing to the Ever store.
///
/// Library contract: no method writes to stdout or stderr. A server
/// `error_response` surfaces as a `ServerError` tag with both wire fields
/// recoverable via `lastError()`; loss of the connection — EOF or reset
/// while reading the reply, EPIPE/ECONNRESET while writing the request —
/// is always the one tag `error.ConnectionClosed`.
pub const Client = struct {
    allocator: Allocator,
    io: Io,
    stream: net.Stream,
    /// Set when a request returns a `ServerError` tag; cleared (and freed)
    /// at the start of the next request and in `deinit`. `code == 0` with a
    /// fixed message when the error_response body did not parse.
    last_error: ?ServerErrorInfo = null,

    /// The retained `{code, message}` of the server error the most recent
    /// request ended in, or null if it did not end in one.
    pub fn lastError(self: *const Client) ?ServerErrorInfo {
        return self.last_error;
    }

    fn clearLastError(self: *Client) void {
        if (self.last_error) |e| {
            self.allocator.free(e.message);
            self.last_error = null;
        }
    }

    /// Write a request frame, folding the write side's names for "peer
    /// gone" (EPIPE, ECONNRESET) into the read side's
    /// `error.ConnectionClosed` — one condition, one tag. Doubles as the
    /// start of a request: clears the previously retained server error.
    fn send(self: *Client, msg_type: protocol.MessageType, body: []const u8) !void {
        self.clearLastError();
        protocol.writeFrame(self.fd(), msg_type, body) catch |err| switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => return error.ConnectionClosed,
            else => |e| return e,
        };
    }

    /// Parse an error_response frame body, retain `{code, message}` for
    /// `lastError`, and map the code onto `ServerError`. Prints nothing:
    /// the library's whole report is the error value and `lastError()`.
    /// (On out-of-memory the retained message is empty, never absent.)
    fn handleErrorResponse(self: *Client, body: []const u8) ServerError {
        const parsed = protocol.decodeBody(protocol.ErrorResponse, self.allocator, body) catch {
            self.last_error = .{ .code = 0, .message = self.allocator.dupe(u8, "server returned an error (could not parse details)") catch "" };
            return error.ServerError;
        };
        defer parsed.deinit();
        self.last_error = .{ .code = parsed.value.code, .message = self.allocator.dupe(u8, parsed.value.message) catch "" };
        return switch (parsed.value.code) {
            protocol.ErrorCode.bad_request => error.BadRequest,
            protocol.ErrorCode.not_found => error.NotFound,
            protocol.ErrorCode.conflict => error.Conflict,
            protocol.ErrorCode.too_large => error.TooLarge,
            protocol.ErrorCode.internal => error.Internal,
            else => error.ServerError,
        };
    }

    /// Connect to the Ever store at the given address and port.
    pub fn connect(allocator: Allocator, io: Io, address: []const u8, port: u16) !Client {
        const ip4 = try net.Ip4Address.parse(address, port);
        const ip_address: net.IpAddress = .{ .ip4 = ip4 };
        const stream = try ip_address.connect(io, .{ .mode = .stream });

        return .{
            .allocator = allocator,
            .io = io,
            .stream = stream,
        };
    }

    /// Close the connection to the store and free the retained error, if any.
    pub fn deinit(self: *Client) void {
        self.clearLastError();
        self.stream.close(self.io);
    }

    /// Get the underlying fd for protocol operations.
    fn fd(self: *Client) std.posix.fd_t {
        return self.stream.socket.handle;
    }

    /// Publish an event to a topic. Returns the assigned offset.
    pub fn publish(self: *Client, topic_name: []const u8, key: ?[]const u8, value: []const u8) !u64 {
        const req = protocol.PublishRequest{
            .topic = topic_name,
            .key = key,
            .value = value,
        };

        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.publish, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .publish_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.PublishResponse, self.allocator, frame.body);
        defer parsed.deinit();

        return parsed.value.offset;
    }

    /// Fetch a batch of events from a single topic.
    pub fn fetch(self: *Client, topic_name: []const u8, offset: u64, max_count: u32) !FetchResult {
        return self.doFetch(.{ .topic = topic_name, .offset = offset, .max_count = max_count });
    }

    /// Fetch events from all topics matching a pattern (trailing dot, *, or .).
    pub fn fetchPattern(self: *Client, pattern: []const u8, offset: u64, max_count: u32) !FetchResult {
        return self.doFetch(.{ .pattern = pattern, .offset = offset, .max_count = max_count });
    }

    /// Fetch with blocking — server holds until events arrive or timeout.
    pub fn fetchBlocking(self: *Client, topic_name: ?[]const u8, pattern: ?[]const u8, offset: u64, max_count: u32, block_ms: u32) !FetchResult {
        return self.doFetch(.{
            .topic = topic_name,
            .pattern = pattern,
            .offset = offset,
            .max_count = max_count,
            .block_ms = block_ms,
        });
    }

    /// Fetch events strictly *after* a global log offset — the resume
    /// cursor for "continue after what I just saw". Works for exact topics
    /// and patterns. `block_ms == 0` returns immediately; otherwise the
    /// server holds until events arrive or the timeout elapses.
    pub fn fetchAfter(self: *Client, topic_name: ?[]const u8, pattern: ?[]const u8, after_offset: u64, max_count: u32, block_ms: u32) !FetchResult {
        return self.doFetch(.{
            .topic = topic_name,
            .pattern = pattern,
            .after_offset = after_offset,
            .max_count = max_count,
            .block_ms = block_ms,
        });
    }

    fn doFetch(self: *Client, req: protocol.FetchRequest) !FetchResult {
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.fetch, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .fetch_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.FetchResponse, self.allocator, frame.body);
        defer parsed.deinit();

        var events = try self.allocator.alloc(Event, parsed.value.events.len);
        errdefer self.allocator.free(events);

        var initialized: usize = 0;
        errdefer {
            for (events[0..initialized]) |evt| {
                if (evt.key) |k| self.allocator.free(k);
                if (evt.topic) |t| self.allocator.free(t);
                self.allocator.free(evt.value);
            }
        }

        for (parsed.value.events, 0..) |evt, i| {
            const key_copy: ?[]const u8 = if (evt.key) |k| blk: {
                break :blk try self.allocator.dupe(u8, k);
            } else null;
            errdefer if (key_copy) |k| self.allocator.free(k);

            const topic_copy: ?[]const u8 = if (evt.topic) |t| blk: {
                break :blk try self.allocator.dupe(u8, t);
            } else null;
            errdefer if (topic_copy) |t| self.allocator.free(t);

            const val_copy = try self.allocator.dupe(u8, evt.value);

            events[i] = .{
                .offset = evt.offset,
                .timestamp = evt.timestamp,
                .key = key_copy,
                .value = val_copy,
                .topic = topic_copy,
            };
            initialized = i + 1;
        }

        return .{
            .events = events,
            .topic_events = parsed.value.topic_events,
            .scan_watermark = parsed.value.scan_watermark,
            .allocator = self.allocator,
        };
    }

    /// Create a topic on the server.
    pub fn createTopic(self: *Client, name: []const u8) !void {
        const req = protocol.TopicRequest{ .topic = name };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.create_topic, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .create_topic_ok) return error.UnexpectedResponse;
    }

    /// Delete a topic on the server.
    pub fn deleteTopic(self: *Client, name: []const u8) !void {
        const req = protocol.TopicRequest{ .topic = name };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.delete_topic, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .delete_topic_ok) return error.UnexpectedResponse;
    }

    /// Register a hook on the server.
    pub fn registerHook(self: *Client, pattern: []const u8, command: []const u8, cwd: []const u8) !u64 {
        return self.registerHookFull(pattern, command, cwd, false, null);
    }

    /// Register a hook with full options (once, env, name).
    pub fn registerHookFull(self: *Client, pattern: []const u8, command: []const u8, cwd: []const u8, once: bool, env: ?[]const []const u8) !u64 {
        return self.registerHookFullNamed(pattern, command, cwd, once, env, null);
    }

    /// Register a hook with full options including optional name.
    /// Uses server-default start cursor (= current tip).
    pub fn registerHookFullNamed(self: *Client, pattern: []const u8, command: []const u8, cwd: []const u8, once: bool, env: ?[]const []const u8, name: ?[]const u8) !u64 {
        return self.registerHookFullNamedCursor(pattern, command, cwd, once, env, name, null);
    }

    /// Register a hook with an explicit start cursor. `start_cursor == null`
    /// asks the server to resolve "current tip" atomically; `0` replays all
    /// history; any other value starts at that topic-local offset.
    pub fn registerHookFullNamedCursor(self: *Client, pattern: []const u8, command: []const u8, cwd: []const u8, once: bool, env: ?[]const []const u8, name: ?[]const u8, start_cursor: ?u64) !u64 {
        return self.sendRegisterHook(.{
            .pattern = pattern,
            .command = command,
            .cwd = cwd,
            .once = once,
            .env = env,
            .name = name,
            .start_cursor = start_cursor,
        });
    }

    /// Atomically create the exact topic `topic` AND register a hook on it
    /// in one server-side critical section — no event can be published to
    /// the topic before the hook is armed. Fails (registering nothing) if
    /// the topic already exists, including tombstoned names.
    pub fn registerHookCreateTopic(self: *Client, topic: []const u8, command: []const u8, cwd: []const u8, once: bool, env: ?[]const []const u8, name: ?[]const u8) !u64 {
        return self.sendRegisterHook(.{
            .pattern = topic,
            .command = command,
            .cwd = cwd,
            .once = once,
            .env = env,
            .name = name,
            .create_topic = true,
        });
    }

    fn sendRegisterHook(self: *Client, req: protocol.RegisterHookRequest) !u64 {
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.register_hook, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .register_hook_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.RegisterHookResponse, self.allocator, frame.body);
        defer parsed.deinit();
        return parsed.value.id;
    }

    /// Unregister a hook by ID on the server.
    pub fn removeHook(self: *Client, id: u64) !void {
        const req = protocol.UnregisterHookRequest{ .id = id };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.unregister_hook, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .unregister_hook_ok) return error.UnexpectedResponse;
    }

    /// List all hooks on the server.
    pub fn listHooks(self: *Client) !ListHooksResult {
        try self.send(.list_hooks, "{}");

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .list_hooks_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.ListHooksResponse, self.allocator, frame.body);
        defer parsed.deinit();

        // Deep copy the hooks
        const hooks = try self.allocator.alloc(HookInfoOwned, parsed.value.hooks.len);
        errdefer self.allocator.free(hooks);

        var initialized: usize = 0;
        errdefer {
            for (hooks[0..initialized]) |h| h.deinit(self.allocator);
        }

        for (parsed.value.hooks, 0..) |h, i| {
            // Deep copy env if present
            const env_copy: ?[]const []const u8 = if (h.env) |env| blk: {
                const ec = try self.allocator.alloc([]const u8, env.len);
                var env_copied: usize = 0;
                errdefer {
                    for (ec[0..env_copied]) |e| self.allocator.free(e);
                    self.allocator.free(ec);
                }
                for (env, 0..) |item, j| {
                    ec[j] = try self.allocator.dupe(u8, item);
                    env_copied = j + 1;
                }
                break :blk ec;
            } else null;

            hooks[i] = .{
                .id = h.id,
                .name = if (h.name) |n| try self.allocator.dupe(u8, n) else null,
                .pattern = try self.allocator.dupe(u8, h.pattern),
                .command = try self.allocator.dupe(u8, h.command),
                .cwd = try self.allocator.dupe(u8, h.cwd),
                .cursor = h.cursor,
                .once = h.once,
                .env = env_copy,
                .fired_count = h.fired_count,
                .failure_count = h.failure_count,
                .last_exit_status = h.last_exit_status,
                .last_failed_offset = h.last_failed_offset,
                .pending = h.pending,
                .cursor_kind = try self.allocator.dupe(u8, h.cursor_kind),
            };
            initialized = i + 1;
        }

        return .{ .hooks = hooks, .allocator = self.allocator };
    }

    /// List all topics on the server.
    pub fn listTopics(self: *Client) ![][]const u8 {
        try self.send(.list_topics, "{}");

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .list_topics_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.ListTopicsResponse, self.allocator, frame.body);
        defer parsed.deinit();

        var result = try self.allocator.alloc([]const u8, parsed.value.topics.len);
        errdefer self.allocator.free(result);

        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |t| self.allocator.free(t);
        }

        for (parsed.value.topics, 0..) |t, i| {
            if (t.deleted) {
                // Append " (deleted)" marker to the name for display
                const name_len = t.name.len;
                const suffix = " (deleted)";
                const buf = try self.allocator.alloc(u8, name_len + suffix.len);
                @memcpy(buf[0..name_len], t.name);
                @memcpy(buf[name_len..], suffix);
                result[i] = buf;
            } else {
                result[i] = try self.allocator.dupe(u8, t.name);
            }
            initialized = i + 1;
        }

        return result;
    }

    /// Add a timer to the server.
    pub fn addTimer(self: *Client, name: []const u8, schedule_type: []const u8, schedule_value: []const u8, topic: []const u8, payload: []const u8, persistent: bool) !void {
        const req = protocol.AddTimerRequest{
            .name = name,
            .schedule_type = schedule_type,
            .schedule_value = schedule_value,
            .topic = topic,
            .payload = payload,
            .persistent = persistent,
        };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.add_timer, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .add_timer_ok) return error.UnexpectedResponse;
    }

    /// Remove a timer from the server.
    pub fn removeTimer(self: *Client, name: []const u8) !void {
        const req = protocol.RemoveTimerRequest{ .name = name };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.remove_timer, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .remove_timer_ok) return error.UnexpectedResponse;
    }

    /// List all timers on the server.
    pub fn listTimers(self: *Client) !ListTimersResult {
        try self.send(.list_timers, "{}");

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .list_timers_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.ListTimersResponse, self.allocator, frame.body);
        defer parsed.deinit();

        // Deep copy the timers
        const timers = try self.allocator.alloc(TimerInfo, parsed.value.timers.len);
        errdefer self.allocator.free(timers);

        var initialized: usize = 0;
        errdefer {
            for (timers[0..initialized]) |t| {
                self.allocator.free(t.name);
                self.allocator.free(t.schedule);
                self.allocator.free(t.topic);
                self.allocator.free(t.payload);
            }
        }

        for (parsed.value.timers, 0..) |t, i| {
            timers[i] = .{
                .name = try self.allocator.dupe(u8, t.name),
                .schedule = try self.allocator.dupe(u8, t.schedule),
                .topic = try self.allocator.dupe(u8, t.topic),
                .payload = try self.allocator.dupe(u8, t.payload),
                .last_fired_at = t.last_fired_at,
                .fire_count = t.fire_count,
                .persistent = t.persistent,
                .created_at = t.created_at,
            };
            initialized = i + 1;
        }

        return .{ .timers = timers, .allocator = self.allocator };
    }

    /// List running hook processes.
    pub fn hookPs(self: *Client) !HookPsResult {
        try self.send(.hook_ps, "{}");

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .hook_ps_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.HookPsResponse, self.allocator, frame.body);
        defer parsed.deinit();

        const procs = try self.allocator.alloc(HookProcessInfoOwned, parsed.value.processes.len);
        errdefer self.allocator.free(procs);

        var initialized: usize = 0;
        errdefer for (procs[0..initialized]) |p| p.deinit(self.allocator);

        for (parsed.value.processes, 0..) |p, i| {
            procs[i] = .{
                .hook_id = p.hook_id,
                .pid = p.pid,
                .pattern = try self.allocator.dupe(u8, p.pattern),
                .command = try self.allocator.dupe(u8, p.command),
                .start_time = p.start_time,
                .log_path = try self.allocator.dupe(u8, p.log_path),
            };
            initialized = i + 1;
        }

        return .{ .processes = procs, .allocator = self.allocator };
    }

    /// Get log output for a hook by ID (most recent execution).
    pub fn hookLogs(self: *Client, hook_id: u64) !HookLogsResult {
        const req = protocol.HookLogsRequest{ .hook_id = hook_id };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.hook_logs, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .hook_logs_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.HookLogsResponse, self.allocator, frame.body);
        defer parsed.deinit();

        return .{
            .hook_id = parsed.value.hook_id,
            .log_path = try self.allocator.dupe(u8, parsed.value.log_path),
            .content = try self.allocator.dupe(u8, parsed.value.content),
            .allocator = self.allocator,
        };
    }

    /// Query live store statistics from the server. The returned status has
    /// `source = .server` and `lock_held = true` (true by construction: a
    /// server answered over the wire). Caller owns the result — call `deinit`.
    pub fn status(self: *Client) !StatusResult {
        try self.send(.status, "{}");

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .status_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.StatusResponse, self.allocator, frame.body);
        defer parsed.deinit();

        const data_dir = try self.allocator.dupe(u8, parsed.value.data_dir);
        errdefer self.allocator.free(data_dir);

        const topics = try self.allocator.alloc(status_mod.TopicInfo, parsed.value.topics.len);
        var topics_initialized: usize = 0;
        errdefer {
            for (topics[0..topics_initialized]) |t| self.allocator.free(t.name);
            self.allocator.free(topics);
        }
        for (parsed.value.topics) |t| {
            topics[topics_initialized] = .{
                .name = try self.allocator.dupe(u8, t.name),
                .events = t.events,
                .deleted = t.deleted,
            };
            topics_initialized += 1;
        }

        const hooks = try self.allocator.alloc(status_mod.HookInfo, parsed.value.hooks.len);
        var hooks_initialized: usize = 0;
        errdefer {
            for (hooks[0..hooks_initialized]) |h| {
                self.allocator.free(h.pattern);
                self.allocator.free(h.command);
            }
            self.allocator.free(hooks);
        }
        for (parsed.value.hooks) |h| {
            const pattern = try self.allocator.dupe(u8, h.pattern);
            errdefer self.allocator.free(pattern);
            const command = try self.allocator.dupe(u8, h.command);
            hooks[hooks_initialized] = .{
                .id = h.id,
                .pattern = pattern,
                .command = command,
                .cursor = h.cursor,
            };
            hooks_initialized += 1;
        }

        return .{
            .status = .{
                .data_dir = data_dir,
                .segments = parsed.value.segments,
                .total_bytes = parsed.value.total_bytes,
                .total_events = parsed.value.total_events,
                .topics = topics,
                .hooks = hooks,
                .lock_held = true,
                .source = .server,
                .uptime_ms = parsed.value.uptime_ms,
                .timer_count = parsed.value.timer_count,
            },
            .allocator = self.allocator,
        };
    }

    /// Get info about a specific timer.
    pub fn timerInfo(self: *Client, name: []const u8) !TimerInfo {
        const req = protocol.TimerInfoRequest{ .name = name };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try self.send(.timer_info, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return self.handleErrorResponse(frame.body);
        if (frame.msg_type != .timer_info_ok) return error.UnexpectedResponse;

        const parsed = try protocol.decodeBody(protocol.TimerInfoResponse, self.allocator, frame.body);
        defer parsed.deinit();

        const t = parsed.value.timer;
        return .{
            .name = try self.allocator.dupe(u8, t.name),
            .schedule = try self.allocator.dupe(u8, t.schedule),
            .topic = try self.allocator.dupe(u8, t.topic),
            .payload = try self.allocator.dupe(u8, t.payload),
            .last_fired_at = t.last_fired_at,
            .fire_count = t.fire_count,
            .persistent = t.persistent,
            .created_at = t.created_at,
        };
    }
};

test "Client struct compiles" {
    _ = Client;
    _ = FetchResult;
    _ = Event;
}
