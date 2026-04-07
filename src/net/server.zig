//! TCP server for the Ever store.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;
const protocol = @import("../protocol/message.zig");
const topic_mod = @import("../store/topic.zig");
const TopicManager = topic_mod.TopicManager;
const store = @import("../store/store.zig");
const hooks_mod = @import("../store/hooks.zig");
const HookTable = hooks_mod.HookTable;
const timers_mod = @import("../store/timers.zig");
const TimerTable = timers_mod.TimerTable;

pub const Config = struct {
    address: []const u8 = "127.0.0.1",
    port: u16 = 7890,
    max_connections: u32 = 1024,
};

/// Global server pointer for signal handler access.
var global_server: ?*Server = null;

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    topic_manager: *TopicManager,
    hook_table: ?*HookTable,
    timer_table: ?*TimerTable,
    config: Config,
    net_server: ?net.Server,
    shutdown_requested: std.atomic.Value(bool),
    active_connections: std.atomic.Value(u32),

    /// Create a new server. Call `run()` to start accepting connections.
    pub fn init(allocator: Allocator, io: Io, topic_manager: *TopicManager, config: Config) !Server {
        return .{
            .allocator = allocator,
            .io = io,
            .topic_manager = topic_manager,
            .hook_table = null,
            .timer_table = null,
            .config = config,
            .net_server = null,
            .shutdown_requested = std.atomic.Value(bool).init(false),
            .active_connections = std.atomic.Value(u32).init(0),
        };
    }

    /// Attach a hook table for server-side hook management.
    pub fn setHookTable(self: *Server, ht: *HookTable) void {
        self.hook_table = ht;
    }

    /// Attach a timer table for server-side timer management.
    pub fn setTimerTable(self: *Server, tt: *TimerTable) void {
        self.timer_table = tt;
    }

    /// Release server resources (closes the listener socket).
    pub fn deinit(self: *Server) void {
        if (self.net_server) |*s| { s.deinit(self.io); self.net_server = null; }
    }

    /// Start the accept loop. Blocks until `shutdown()` is called.
    pub fn run(self: *Server) !void {
        const ip4 = try net.Ip4Address.parse(self.config.address, self.config.port);
        const address: net.IpAddress = .{ .ip4 = ip4 };
        self.net_server = try address.listen(self.io, .{ .reuse_address = false });

        while (!self.shutdown_requested.load(.acquire)) {
            const stream = self.net_server.?.accept(self.io) catch |err| switch (err) {
                error.ConnectionAborted => continue,
                else => { if (self.shutdown_requested.load(.acquire)) break; return err; },
            };

            // Enforce max_connections limit
            const current = self.active_connections.load(.acquire);
            if (current >= self.config.max_connections) {
                var s = stream;
                // Send error before closing
                sendError(self.allocator, s.socket.handle, protocol.ErrorCode.internal, "too many connections") catch {};
                s.close(self.io);
                continue;
            }

            _ = self.active_connections.fetchAdd(1, .acq_rel);
            const thread = std.Thread.spawn(.{}, handleConnection, .{ self, stream }) catch {
                _ = self.active_connections.fetchSub(1, .acq_rel);
                var s = stream; s.close(self.io); continue;
            };
            thread.detach();
        }
    }

    /// Install SIGINT/SIGTERM handlers that trigger graceful shutdown.
    pub fn installSignalHandlers(self: *Server) void {
        global_server = self;
        const act = std.os.linux.Sigaction{
            .handler = .{ .handler = signalHandler },
            .mask = std.os.linux.sigemptyset(),
            .flags = 0,
        };
        _ = std.os.linux.sigaction(.INT, &act, null);
        _ = std.os.linux.sigaction(.TERM, &act, null);
    }

    /// Signal the server to stop accepting connections, close the listener,
    /// and wait for in-flight connections to drain (up to 5 seconds).
    pub fn shutdown(self: *Server) void {
        self.shutdown_requested.store(true, .release);
        if (self.net_server) |*s| { s.deinit(self.io); self.net_server = null; }

        // Wait for in-flight connections to finish (max 5 seconds)
        var waited_ms: u32 = 0;
        while (self.active_connections.load(.acquire) > 0 and waited_ms < 5000) {
            _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 100_000_000 }, null); // 100ms
            waited_ms += 100;
        }
        const remaining = self.active_connections.load(.acquire);
        if (remaining > 0) {
            std.debug.print("Shutdown: {d} connections did not drain within 5s.\n", .{remaining});
        }
    }

    fn handleConnection(self: *Server, stream: net.Stream) void {
        var s = stream;
        defer {
            s.close(self.io);
            _ = self.active_connections.fetchSub(1, .acq_rel);
        }
        const fd = s.socket.handle;
        while (!self.shutdown_requested.load(.acquire)) {
            const frame = protocol.readFrame(self.allocator, fd) catch break;
            if (frame == null) break;
            defer self.allocator.free(frame.?.body);
            self.handleFrame(frame.?, fd) catch break;
        }
    }

    fn handleFrame(self: *Server, frame: protocol.Frame, fd: std.posix.fd_t) !void {
        switch (frame.msg_type) {
            .publish => try self.handlePublish(frame.body, fd),
            .fetch => try self.handleFetch(frame.body, fd),
            .create_topic => try self.handleCreateTopic(frame.body, fd),
            .delete_topic => try self.handleDeleteTopic(frame.body, fd),
            .list_topics => try self.handleListTopics(fd),
            .ack => try protocol.writeFrame(fd, .ack_ok, "{}"),
            .register_hook => try self.handleRegisterHook(frame.body, fd),
            .unregister_hook => try self.handleUnregisterHook(frame.body, fd),
            .list_hooks => try self.handleListHooks(fd),
            .add_timer => try self.handleAddTimer(frame.body, fd),
            .remove_timer => try self.handleRemoveTimer(frame.body, fd),
            .list_timers => try self.handleListTimers(fd),
            .timer_info => try self.handleTimerInfo(frame.body, fd),
            else => try sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "unknown request type"),
        }
    }

    fn handlePublish(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const parsed = protocol.decodeBody(protocol.PublishRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid publish request");
        defer parsed.deinit();
        const req = parsed.value;

        const offset = self.topic_manager.publish(req.topic, req.key, req.value) catch |err| return switch (err) {
            error.NotFound => sendError(self.allocator, fd, protocol.ErrorCode.not_found, "topic not found"),
            error.TopicDeleted => sendError(self.allocator, fd, protocol.ErrorCode.conflict, "topic is deleted"),
            else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "publish failed"),
        };

        const resp_body = try protocol.encodeBody(self.allocator, protocol.PublishResponse{ .offset = offset });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .publish_ok, resp_body);
    }

    fn handleFetch(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const parsed = protocol.decodeBody(protocol.FetchRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid fetch request");
        defer parsed.deinit();
        const req = parsed.value;

        // Blocking fetch: retry until events found or timeout
        const block_ms = req.block_ms;
        var elapsed_ms: u32 = 0;
        const sleep_interval_ms: u32 = 100; // 100ms poll interval

        while (true) {
            // Resolve events — either by pattern or single topic
            const events = if (req.pattern) |pattern|
                self.topic_manager.fetchPattern(self.allocator, pattern, req.offset, req.max_count) catch
                    return sendError(self.allocator, fd, protocol.ErrorCode.internal, "fetch failed")
            else if (req.topic) |topic_name|
                self.topic_manager.fetch(self.allocator, topic_name, req.offset, req.max_count) catch |err| return switch (err) {
                    error.NotFound => sendError(self.allocator, fd, protocol.ErrorCode.not_found, "topic not found"),
                    else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "fetch failed"),
                }
            else
                return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "missing topic or pattern");

            // If we have events or not blocking, send response
            if (events.len > 0 or block_ms == 0 or elapsed_ms >= block_ms or self.shutdown_requested.load(.acquire)) {
                defer {
                    for (events) |evt| store.freeEvent(self.allocator, evt);
                    self.allocator.free(events);
                }

                // Convert to protocol format
                const event_data = try self.allocator.alloc(protocol.EventData, events.len);
                defer self.allocator.free(event_data);
                for (events, 0..) |evt, i| {
                    event_data[i] = .{
                        .offset = evt.offset,
                        .timestamp = evt.timestamp,
                        .key = evt.key,
                        .value = evt.value,
                        .topic = evt.topic,
                    };
                }

                const resp_body = try protocol.encodeBody(self.allocator, protocol.FetchResponse{ .events = event_data });
                defer self.allocator.free(resp_body);
                try protocol.writeFrame(fd, .fetch_ok, resp_body);
                return;
            }

            // No events yet, free and sleep
            self.allocator.free(events); // empty slice

            // Sleep using linux nanosleep
            const ts = std.os.linux.timespec{
                .sec = 0,
                .nsec = @as(i64, sleep_interval_ms) * 1_000_000,
            };
            _ = std.os.linux.nanosleep(&ts, null);
            elapsed_ms += sleep_interval_ms;
        }
    }

    fn handleCreateTopic(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const parsed = protocol.decodeBody(protocol.TopicRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid request");
        defer parsed.deinit();
        self.topic_manager.createTopic(parsed.value.topic) catch |err| return switch (err) {
            error.AlreadyExists => sendError(self.allocator, fd, protocol.ErrorCode.conflict, "topic already exists"),
            error.InvalidName => sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid topic name"),
            else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "create topic failed"),
        };
        try protocol.writeFrame(fd, .create_topic_ok, "{}");
    }

    fn handleDeleteTopic(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const parsed = protocol.decodeBody(protocol.TopicRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid request");
        defer parsed.deinit();
        self.topic_manager.deleteTopic(parsed.value.topic) catch |err| return switch (err) {
            error.NotFound => sendError(self.allocator, fd, protocol.ErrorCode.not_found, "topic not found"),
            else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "delete topic failed"),
        };
        try protocol.writeFrame(fd, .delete_topic_ok, "{}");
    }

    fn handleListTopics(self: *Server, fd: std.posix.fd_t) !void {
        const topics = self.topic_manager.listTopics(self.allocator) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "list topics failed");
        defer { for (topics) |t| self.allocator.free(t.name); self.allocator.free(topics); }

        const topic_infos = try self.allocator.alloc(protocol.TopicInfoItem, topics.len);
        defer self.allocator.free(topic_infos);
        for (topics, 0..) |t, i| {
            topic_infos[i] = .{ .name = t.name, .deleted = t.deleted };
        }

        const resp_body = try protocol.encodeBody(self.allocator, protocol.ListTopicsResponse{ .topics = topic_infos });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .list_topics_ok, resp_body);
    }

    fn handleRegisterHook(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const ht = self.hook_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "hooks not enabled");

        const parsed = protocol.decodeBody(protocol.RegisterHookRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid register hook request");
        defer parsed.deinit();
        const req = parsed.value;

        const id = ht.addFull(req.pattern, req.command, req.cwd, req.once, req.env) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "failed to register hook");

        const resp_body = try protocol.encodeBody(self.allocator, protocol.RegisterHookResponse{ .id = id });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .register_hook_ok, resp_body);
    }

    fn handleUnregisterHook(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const ht = self.hook_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "hooks not enabled");

        const parsed = protocol.decodeBody(protocol.UnregisterHookRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid unregister hook request");
        defer parsed.deinit();

        ht.remove(parsed.value.id) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.not_found, "hook not found");

        try protocol.writeFrame(fd, .unregister_hook_ok, "{}");
    }

    fn handleListHooks(self: *Server, fd: std.posix.fd_t) !void {
        const ht = self.hook_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "hooks not enabled");

        const hooks = ht.snapshot(self.allocator) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "list hooks failed");
        defer hooks_mod.freeHookSnapshot(self.allocator, hooks);

        const hook_infos = try self.allocator.alloc(protocol.HookInfo, hooks.len);
        defer self.allocator.free(hook_infos);

        for (hooks, 0..) |hook, i| {
            hook_infos[i] = .{
                .id = hook.id,
                .pattern = hook.pattern,
                .command = hook.command,
                .cwd = hook.cwd,
                .cursor = hook.cursor,
                .once = hook.once,
                .env = hook.env,
            };
        }

        const resp_body = try protocol.encodeBody(self.allocator, protocol.ListHooksResponse{ .hooks = hook_infos });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .list_hooks_ok, resp_body);
    }

    fn handleAddTimer(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const tt = self.timer_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "timers not enabled");

        const parsed = protocol.decodeBody(protocol.AddTimerRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid add timer request");
        defer parsed.deinit();
        const req = parsed.value;

        // Parse schedule
        const schedule: timers_mod.Schedule = if (std.mem.eql(u8, req.schedule_type, "interval")) blk: {
            const secs = timers_mod.parseDuration(req.schedule_value) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid duration format");
            break :blk .{ .interval = secs };
        } else if (std.mem.eql(u8, req.schedule_type, "cron")) blk: {
            const expr = timers_mod.CronExpr.parse(req.schedule_value) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid cron expression");
            break :blk .{ .cron = expr };
        } else
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid schedule type");

        // Build schedule_str for display
        const schedule_str = if (std.mem.eql(u8, req.schedule_type, "interval"))
            try timers_mod.formatIntervalStr(self.allocator, schedule.interval)
        else
            try self.allocator.dupe(u8, req.schedule_value);
        defer self.allocator.free(schedule_str);

        tt.add(req.name, schedule, schedule_str, req.topic, req.payload, req.persistent) catch |err| return switch (err) {
            error.AlreadyExists => sendError(self.allocator, fd, protocol.ErrorCode.conflict, "timer name already exists"),
            else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "add timer failed"),
        };

        try protocol.writeFrame(fd, .add_timer_ok, "{}");
    }

    fn handleRemoveTimer(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const tt = self.timer_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "timers not enabled");

        const parsed = protocol.decodeBody(protocol.RemoveTimerRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid remove timer request");
        defer parsed.deinit();

        tt.remove(parsed.value.name) catch |err| return switch (err) {
            error.NotFound => sendError(self.allocator, fd, protocol.ErrorCode.not_found, "timer not found"),
            else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "remove timer failed"),
        };

        try protocol.writeFrame(fd, .remove_timer_ok, "{}");
    }

    fn handleListTimers(self: *Server, fd: std.posix.fd_t) !void {
        const tt = self.timer_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "timers not enabled");

        const timers = tt.list();
        const timer_infos = try self.allocator.alloc(protocol.TimerInfoData, timers.len);
        defer self.allocator.free(timer_infos);

        for (timers, 0..) |timer, i| {
            timer_infos[i] = .{
                .name = timer.name,
                .schedule = timer.schedule_str,
                .topic = timer.topic,
                .payload = timer.payload,
                .last_fired_at = timer.last_fired_at,
                .fire_count = timer.fire_count,
                .persistent = timer.persistent,
                .created_at = timer.created_at,
            };
        }

        const resp_body = try protocol.encodeBody(self.allocator, protocol.ListTimersResponse{ .timers = timer_infos });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .list_timers_ok, resp_body);
    }

    fn handleTimerInfo(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const tt = self.timer_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "timers not enabled");

        const parsed = protocol.decodeBody(protocol.TimerInfoRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid timer info request");
        defer parsed.deinit();

        const timer = tt.find(parsed.value.name) orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.not_found, "timer not found");

        const timer_info = protocol.TimerInfoData{
            .name = timer.name,
            .schedule = timer.schedule_str,
            .topic = timer.topic,
            .payload = timer.payload,
            .last_fired_at = timer.last_fired_at,
            .fire_count = timer.fire_count,
            .persistent = timer.persistent,
            .created_at = timer.created_at,
        };

        const resp_body = try protocol.encodeBody(self.allocator, protocol.TimerInfoResponse{ .timer = timer_info });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .timer_info_ok, resp_body);
    }
};

fn signalHandler(_: std.os.linux.SIG) callconv(.c) void {
    if (global_server) |s| {
        s.shutdown_requested.store(true, .release);
        // Close the listener to unblock accept(). The full drain
        // happens in shutdown() which the main thread will reach
        // after run() returns.
        if (s.net_server) |*ns| { ns.deinit(s.io); s.net_server = null; }
    }
}

fn sendError(allocator: Allocator, fd: std.posix.fd_t, code: u16, message: []const u8) !void {
    const body = try protocol.encodeBody(allocator, protocol.ErrorResponse{ .code = code, .message = message });
    defer allocator.free(body);
    try protocol.writeFrame(fd, .error_response, body);
}

test "Server init and deinit" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    var server = try Server.init(std.testing.allocator, io, &tm, .{});
    defer server.deinit();
    try std.testing.expect(!server.shutdown_requested.load(.acquire));
}
