//! TCP server for the Ever store.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;
const protocol = @import("../protocol/message.zig");
const topic_mod = @import("../store/topic.zig");
const TopicManager = topic_mod.TopicManager;
const store = @import("../store/store.zig");
const hooks_mod = @import("../store/hooks.zig");
const HookTable = hooks_mod.HookTable;
const HookDaemon = hooks_mod.HookDaemon;
const timers_mod = @import("../store/timers.zig");
const TimerTable = timers_mod.TimerTable;

pub const Config = struct {
    address: []const u8 = "127.0.0.1",
    /// TCP port to bind. 0 means "any free port" — ask `boundPort()` once
    /// `run()` has bound to learn which one the OS assigned.
    port: u16 = 7890,
    max_connections: u32 = 1024,
    /// Data directory the store is serving from. Reported verbatim in the
    /// status response so clients can see which directory the *server* uses.
    data_dir: []const u8 = "./data",
};

/// Global server pointer for signal handler access.
var global_server: ?*Server = null;

/// Test-only count of times a blocking fetch has looked for events. One look
/// per request means the waiter was woken; N looks per second means it was
/// polling. Counting looks rather than CPU keeps the assertion deterministic
/// — a CPU-time threshold cannot separate a cheap poll from a parked wait
/// without a topic big enough to make each poll expensive.
/// See air/v0.1/subscribe-notify-on-append.org.
pub var test_fetch_attempts: if (builtin.is_test) std.atomic.Value(u64) else void =
    if (builtin.is_test) .init(0) else {};

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    topic_manager: *TopicManager,
    hook_table: ?*HookTable,
    hook_daemon: ?*HookDaemon,
    timer_table: ?*TimerTable,
    config: Config,
    net_server: ?net.Server,
    /// Port actually bound, published by `run()` and read via `boundPort()`.
    /// Atomic because the accept loop writes it while other threads (a test
    /// harness, `shutdown()`, the signal handler) read it.
    bound_port: std.atomic.Value(u16),
    /// Address the self-connect in `unblockAccept` dials. Taken from the
    /// listener itself, with the wildcard address mapped to loopback.
    /// Written before `bound_port` is published and read after, so the
    /// release store on `bound_port` is what makes it visible.
    self_connect_ip: [4]u8,
    shutdown_requested: std.atomic.Value(bool),
    /// True from just before the accept loop starts until `run()` returns.
    /// `shutdown()` waits on this before closing the listener: the loop
    /// dereferences `net_server` on every iteration, so closing it under a
    /// running loop is a use-after-free rather than a cancellation.
    accept_loop_active: std.atomic.Value(bool),
    active_connections: std.atomic.Value(u32),
    /// Milliseconds since epoch at `init` — used to compute status uptime.
    start_time: i64,

    /// Create a new server. Call `run()` to start accepting connections.
    pub fn init(allocator: Allocator, io: Io, topic_manager: *TopicManager, config: Config) !Server {
        return .{
            .allocator = allocator,
            .io = io,
            .topic_manager = topic_manager,
            .hook_table = null,
            .hook_daemon = null,
            .timer_table = null,
            .config = config,
            .net_server = null,
            .bound_port = std.atomic.Value(u16).init(0),
            .self_connect_ip = .{ 127, 0, 0, 1 },
            .shutdown_requested = std.atomic.Value(bool).init(false),
            .accept_loop_active = std.atomic.Value(bool).init(false),
            .active_connections = std.atomic.Value(u32).init(0),
            .start_time = getMilliTimestamp(),
        };
    }

    /// Attach a hook table for server-side hook management.
    pub fn setHookTable(self: *Server, ht: *HookTable) void {
        self.hook_table = ht;
    }

    /// Attach a hook daemon for process table access (hook ps/logs).
    pub fn setHookDaemon(self: *Server, hd: *HookDaemon) void {
        self.hook_daemon = hd;
    }

    /// Attach a timer table for server-side timer management.
    pub fn setTimerTable(self: *Server, tt: *TimerTable) void {
        self.timer_table = tt;
    }

    /// Release server resources (closes the listener socket).
    pub fn deinit(self: *Server) void {
        if (self.net_server) |*s| { s.deinit(self.io); self.net_server = null; }
        self.bound_port.store(0, .release);
    }

    /// The port this server is listening on. Equals `config.port` unless that
    /// was 0, in which case it is the port the OS assigned. Valid only after
    /// `run()` has bound; 0 before that.
    pub fn boundPort(self: *const Server) u16 {
        return self.bound_port.load(.acquire);
    }

    /// Read back the address the listener actually got and publish it, so a
    /// caller that asked for port 0 can find the server and `unblockAccept`
    /// has something to dial. Called once, immediately after `listen`.
    fn publishBoundAddress(self: *Server) void {
        var sa: std.os.linux.sockaddr.in = undefined;
        var sa_len: std.os.linux.socklen_t = @sizeOf(std.os.linux.sockaddr.in);
        const rc = std.os.linux.getsockname(self.net_server.?.socket.handle, @ptrCast(&sa), &sa_len);
        if (std.os.linux.errno(rc) != .SUCCESS) {
            // Only an ephemeral bind actually needs the syscall; for an
            // explicit port the config value is already the answer.
            self.bound_port.store(self.config.port, .release);
            return;
        }
        // A server bound to the wildcard address is reachable on loopback,
        // and 0.0.0.0 is not a sensible connect() target.
        if (sa.addr != 0) self.self_connect_ip = @bitCast(sa.addr);
        self.bound_port.store(std.mem.bigToNative(u16, sa.port), .release);
    }

    /// Start the accept loop. Blocks until `shutdown()` is called.
    pub fn run(self: *Server) !void {
        const ip4 = try net.Ip4Address.parse(self.config.address, self.config.port);
        const address: net.IpAddress = .{ .ip4 = ip4 };
        self.net_server = try address.listen(self.io, .{ .reuse_address = false });
        self.publishBoundAddress();

        self.accept_loop_active.store(true, .release);
        defer self.accept_loop_active.store(false, .release);

        while (!self.shutdown_requested.load(.acquire)) {
            const stream = self.net_server.?.accept(self.io) catch |err| switch (err) {
                error.ConnectionAborted => continue,
                else => { if (self.shutdown_requested.load(.acquire)) break; return err; },
            };

            // Drop, do not serve, anything that arrives once shutdown has
            // begun. The wake-up self-connect is an ordinary connection: hand
            // it to a detached handler and that handler outlives the loop,
            // still writing to `active_connections` after an owner that
            // joined this thread has freed the server. Joining is what makes
            // that use-after-free reachable, so the drop is part of being
            // joinable rather than a separate policy.
            if (self.shutdown_requested.load(.acquire)) {
                var s = stream;
                s.close(self.io);
                break;
            }

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

    /// Wake a thread blocked in `accept()` by connecting to our own listener
    /// and dropping the connection immediately. The accept loop returns with
    /// that throwaway stream, re-tests `shutdown_requested` and breaks.
    ///
    /// This is the only mechanism that reliably returns a blocked `accept()`:
    /// closing the listening socket out from under the accepting thread does
    /// not wake it on Linux. It lives here — rather than in `signalHandler`,
    /// which used to hold the only copy — so that *every* caller of
    /// `shutdown()` gets a stoppable server, not only one stopped by a signal.
    ///
    /// Raw syscalls only, no allocation and no locks: `socket()`, `connect()`
    /// and `close()` are async-signal-safe, so the signal handler calls this
    /// directly.
    fn unblockAccept(self: *const Server) void {
        // The bound port, not the configured one: with `config.port = 0` the
        // configured value names no listener at all, so reading the config
        // here would silently stop waking `accept()` for exactly the
        // ephemeral-port callers this exists to serve. Zero here means the
        // listener is not up yet, and a loop that has not entered `accept()`
        // sees the flag on its next iteration anyway.
        const port = self.boundPort();
        if (port == 0) return;
        const ip = self.self_connect_ip;

        const fd_rc = std.os.linux.socket(2, 1, 0); // AF_INET, SOCK_STREAM
        const fd_i: isize = @bitCast(fd_rc);
        if (fd_i < 0) return;
        const fd: i32 = @intCast(fd_rc);

        var addr: [16]u8 = undefined;
        @memset(&addr, 0);
        addr[0] = 2; // AF_INET
        addr[1] = 0;
        addr[2] = @intCast(port >> 8);
        addr[3] = @intCast(port & 0xFF);
        addr[4] = ip[0];
        addr[5] = ip[1];
        addr[6] = ip[2];
        addr[7] = ip[3];
        _ = std.os.linux.connect(fd, @ptrCast(&addr), 16);
        _ = std.os.linux.close(fd);
    }

    /// Signal the server to stop accepting connections, close the listener,
    /// and wait for in-flight connections to drain (up to 5 seconds).
    ///
    /// Safe to call from any thread, and the accept loop is guaranteed to
    /// return — so the caller may join the thread running `run()`.
    pub fn shutdown(self: *Server) void {
        self.shutdown_requested.store(true, .release);
        // Blocking fetches are parked on the publication epoch, not on a
        // 100ms timer, so nothing will notice the flag unless we say so.
        // Without this every subscriber waits out its full block_ms and the
        // drain below gives up on it.
        self.topic_manager.wakeAllWaiters();

        // Order matters. The self-connect needs a listener to connect to, so
        // it has to happen *before* the listener is closed; closing first (as
        // this used to) leaves a blocked `accept()` with nothing to wake it.
        self.unblockAccept();

        // And the listener cannot be closed until the loop is out of it: the
        // loop reads `net_server` each iteration, so freeing it from here is a
        // race, not a cancellation. Bounded, because a wedged accept loop must
        // not turn shutdown into a deadlock — if the wait expires the close
        // below is the last resort it always was.
        var accept_wait_ms: u32 = 0;
        while (self.accept_loop_active.load(.acquire) and accept_wait_ms < 2000) {
            _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 1_000_000 }, null); // 1ms
            accept_wait_ms += 1;
        }

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
            .register_hook => try self.handleRegisterHook(frame.body, fd),
            .unregister_hook => try self.handleUnregisterHook(frame.body, fd),
            .list_hooks => try self.handleListHooks(fd),
            .add_timer => try self.handleAddTimer(frame.body, fd),
            .remove_timer => try self.handleRemoveTimer(frame.body, fd),
            .list_timers => try self.handleListTimers(fd),
            .timer_info => try self.handleTimerInfo(frame.body, fd),
            .hook_ps => try self.handleHookPs(fd),
            .hook_logs => try self.handleHookLogs(frame.body, fd),
            .status => try self.handleStatus(fd),
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
            error.ReservedKey => sendError(self.allocator, fd, protocol.ErrorCode.bad_request, topic_mod.reserved_key_message),
            error.EmptyValue => sendError(self.allocator, fd, protocol.ErrorCode.bad_request, topic_mod.empty_value_message),
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

        // `offset` is a per-topic skip count. Applied to every topic a
        // pattern matches ("skip the first N of each") it is a number no
        // client can compute a next value for -- the response interleaves
        // topics and the client only knows how many events it received in
        // total. Refuse the combination rather than answer something no
        // cursor can be built from. (air/v0.1/pattern-fetch-rejects-offset.org)
        if (req.pattern != null and req.after_offset == null and req.offset != 0)
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "offset is a per-topic skip count and is undefined for a pattern; resume a pattern with after_offset");

        // Blocking fetch: wait for an append, not for a timer.
        //
        // The epoch is read *before* each look for events, so an append that
        // lands between the look and the wait is not slept through -- see
        // TopicManager.waitForAppend and air/v0.1/subscribe-notify-on-append.org.
        const block_ms = req.block_ms;
        const deadline_ms = monotonicMillis() + @as(i64, block_ms);
        var epoch = self.topic_manager.appendEpoch();

        while (true) {
            if (builtin.is_test) _ = test_fetch_attempts.fetchAdd(1, .monotonic);
            // Resolve events — either by pattern or single topic. When
            // `after_offset` is set it takes precedence over `offset`
            // (global-offset cursor vs. topic-local skip count).
            const events = if (req.pattern) |pattern|
                (if (req.after_offset) |after|
                    self.topic_manager.fetchPatternByOffset(self.allocator, pattern, after +| 1, req.max_count)
                else
                    self.topic_manager.fetchPattern(self.allocator, pattern, req.offset, req.max_count)) catch
                    return sendError(self.allocator, fd, protocol.ErrorCode.internal, "fetch failed")
            else if (req.topic) |topic_name|
                (if (req.after_offset) |after|
                    self.topic_manager.fetchAfterOffset(self.allocator, topic_name, after, req.max_count)
                else
                    self.topic_manager.fetch(self.allocator, topic_name, req.offset, req.max_count)) catch |err| return switch (err) {
                    error.NotFound => sendError(self.allocator, fd, protocol.ErrorCode.not_found, "topic not found"),
                    else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "fetch failed"),
                }
            else
                return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "missing topic or pattern");

            // If we have events or not blocking, send response
            const now_ms = monotonicMillis();
            if (events.len > 0 or block_ms == 0 or now_ms >= deadline_ms or self.shutdown_requested.load(.acquire)) {
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

                // Exact-topic requests carry the topic's non-marker event
                // count so clients can tell "start beyond end" from "empty".
                const topic_events: ?u64 = if (req.pattern == null)
                    (if (req.topic) |topic_name| self.topic_manager.topicEventCount(topic_name) else null)
                else
                    null;

                const resp_body = try protocol.encodeBody(self.allocator, protocol.FetchResponse{ .events = event_data, .topic_events = topic_events });
                defer self.allocator.free(resp_body);
                try protocol.writeFrame(fd, .fetch_ok, resp_body);
                return;
            }

            // No events yet. Park until something is published or the
            // client's block window runs out. Costs nothing while waiting.
            self.allocator.free(events); // empty slice

            const remaining_ms: u32 = @intCast(@max(0, deadline_ms - now_ms));
            epoch = self.topic_manager.waitForAppend(epoch, remaining_ms);
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

        if (req.create_topic) {
            // Server-side validation mirrors the client's — the server must
            // not trust the client.
            if (req.start_cursor != null)
                return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "create_topic cannot be combined with an explicit start cursor (a fresh topic has no history to replay)");
            if (topic_mod.isPatternShape(req.pattern))
                return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "create_topic requires an exact topic name (no trailing-dot prefix, no '*' wildcard)");

            const id = createTopicAndRegisterHook(self.topic_manager, ht, req.pattern, req.command, req.cwd, req.once, req.env, req.name) catch |err| return switch (err) {
                error.AlreadyExists => sendError(self.allocator, fd, protocol.ErrorCode.conflict, "topic already exists"),
                error.InvalidName => sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid topic name"),
                else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "failed to create topic and register hook"),
            };

            const resp_body = try protocol.encodeBody(self.allocator, protocol.RegisterHookResponse{ .id = id });
            defer self.allocator.free(resp_body);
            try protocol.writeFrame(fd, .register_hook_ok, resp_body);
            return;
        }

        // Resolve start cursor. `null` means "current tip" — computed while
        // holding the TopicManager's publish lock so no event can slip in
        // between tip-read and hook insertion.
        const id = if (req.start_cursor) |explicit|
            ht.addWithCursor(req.pattern, req.command, req.cwd, req.once, req.env, req.name, explicit) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.internal, "failed to register hook")
        else blk: {
            self.topic_manager.lockForHookRegistration();
            defer self.topic_manager.unlockForHookRegistration();
            const tip = self.topic_manager.tipForPatternLocked(req.pattern);
            break :blk ht.addWithCursor(req.pattern, req.command, req.cwd, req.once, req.env, req.name, tip) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.internal, "failed to register hook");
        };

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

        // Kill running children for this hook before removing it
        if (self.hook_daemon) |hd| {
            hd.killChildrenForHook(parsed.value.id);
        }

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
            const derived = computeHookPending(self.topic_manager, self.allocator, hook.pattern, hook.cursor) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.internal, "pending computation failed");
            hook_infos[i] = .{
                .id = hook.id,
                .name = hook.name,
                .pattern = hook.pattern,
                .command = hook.command,
                .cwd = hook.cwd,
                .cursor = hook.cursor,
                .once = hook.once,
                .env = hook.env,
                .fired_count = hook.fired_count,
                .failure_count = hook.failure_count,
                .last_exit_status = hook.last_exit_status,
                .last_failed_offset = hook.last_failed_offset,
                .pending = derived.pending,
                .cursor_kind = derived.cursor_kind,
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
        const is_one_shot = std.mem.eql(u8, req.schedule_type, "one_shot");
        const schedule: timers_mod.Schedule = if (std.mem.eql(u8, req.schedule_type, "interval") or is_one_shot) blk: {
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
        const schedule_str = if (is_one_shot)
            try timers_mod.formatOneShotStr(self.allocator, schedule.interval)
        else if (std.mem.eql(u8, req.schedule_type, "interval"))
            try timers_mod.formatIntervalStr(self.allocator, schedule.interval)
        else
            try self.allocator.dupe(u8, req.schedule_value);
        defer self.allocator.free(schedule_str);

        if (is_one_shot) {
            tt.addOneShot(req.name, schedule, schedule_str, req.topic, req.payload) catch |err| return switch (err) {
                error.AlreadyExists => sendError(self.allocator, fd, protocol.ErrorCode.conflict, "timer name already exists"),
                else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "add timer failed"),
            };
        } else {
            tt.add(req.name, schedule, schedule_str, req.topic, req.payload, req.persistent) catch |err| return switch (err) {
                error.AlreadyExists => sendError(self.allocator, fd, protocol.ErrorCode.conflict, "timer name already exists"),
                else => sendError(self.allocator, fd, protocol.ErrorCode.internal, "add timer failed"),
            };
        }

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

        // Use snapshot() to get a deep copy — list() returns internal references
        // that can be invalidated by the daemon thread modifying the timer table.
        const timers = tt.snapshot(self.allocator) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "list timers failed");
        defer timers_mod.freeTimerSnapshot(self.allocator, timers);

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

    fn handleHookPs(self: *Server, fd: std.posix.fd_t) !void {
        const hd = self.hook_daemon orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "hooks not enabled");

        var buf: [hooks_mod.MAX_CONCURRENT_HOOKS]hooks_mod.ProcessEntry = undefined;
        const pt = hd.getProcessTable();
        const n = pt.snapshot(&buf);

        const infos = try self.allocator.alloc(protocol.HookProcessInfo, n);
        defer self.allocator.free(infos);

        for (buf[0..n], 0..) |entry, i| {
            infos[i] = .{
                .hook_id = entry.hook_id,
                .pid = entry.pid,
                .pattern = entry.pattern[0..entry.pattern_len],
                .command = entry.command_display[0..entry.command_display_len],
                .start_time = entry.start_time,
                .log_path = entry.log_path[0..entry.log_path_len],
            };
        }

        const resp_body = try protocol.encodeBody(self.allocator, protocol.HookPsResponse{ .processes = infos });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .hook_ps_ok, resp_body);
    }

    fn handleHookLogs(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        _ = self.hook_daemon orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "hooks not enabled");

        const ht = self.hook_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "hooks not enabled");

        const parsed = protocol.decodeBody(protocol.HookLogsRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid hook logs request");
        defer parsed.deinit();
        const req = parsed.value;

        // Distinguish "no such hook" (error) from "known hook, no executions"
        // (empty success): consult the hook table before scanning the log dir.
        // See air/v0.1/hook-logs-json.org.
        if (!ht.contains(req.hook_id)) {
            var msg_buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(&msg_buf, "no such hook: {d}", .{req.hook_id}) catch "no such hook";
            return sendError(self.allocator, fd, protocol.ErrorCode.not_found, msg);
        }

        // Find the most recent log file for this hook ID by scanning the hooks dir
        const hooks_dir_path = std.fmt.allocPrint(self.allocator, "{s}/hooks", .{ht.data_dir}) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "alloc failed");
        defer self.allocator.free(hooks_dir_path);

        const prefix = std.fmt.allocPrint(self.allocator, "{d}-", .{req.hook_id}) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "alloc failed");
        defer self.allocator.free(prefix);

        // Find most recent log by scanning filenames (they contain timestamps)
        var best_path: ?[]u8 = null;
        defer if (best_path) |p| self.allocator.free(p);

        const hooks_dir_z = self.allocator.allocSentinel(u8, hooks_dir_path.len, 0) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "alloc failed");
        defer self.allocator.free(hooks_dir_z);
        @memcpy(hooks_dir_z[0..hooks_dir_path.len], hooks_dir_path);

        // Use linux opendir via getdents
        const dir_fd_rc = std.os.linux.open(hooks_dir_z.ptr, .{ .ACCMODE = .RDONLY, .DIRECTORY = true }, 0);
        const dir_fd_i: isize = @bitCast(dir_fd_rc);
        if (dir_fd_i < 0)
            return self.sendHookLogsEmpty(fd, req.hook_id);
        const dir_fd: i32 = @intCast(dir_fd_rc);
        defer _ = std.os.linux.close(dir_fd);

        var dirent_buf: [4096]u8 = undefined;
        var best_ts: i64 = -1;

        outer: while (true) {
            const nread = std.os.linux.getdents64(dir_fd, &dirent_buf, dirent_buf.len);
            const nread_i: isize = @bitCast(nread);
            if (nread_i <= 0) break;

            var offset: usize = 0;
            while (offset < nread) {
                const dirent: *align(1) const std.os.linux.dirent64 = @ptrCast(@alignCast(&dirent_buf[offset]));
                offset += dirent.reclen;

                const name_ptr: [*:0]const u8 = @ptrCast(&dirent.name);
                const name = std.mem.sliceTo(name_ptr, 0);

                if (!std.mem.startsWith(u8, name, prefix)) continue;
                if (!std.mem.endsWith(u8, name, ".log")) continue;

                // Extract timestamp from filename: <id>-<timestamp>.log
                const after_prefix = name[prefix.len..];
                const dot_pos = std.mem.indexOfScalar(u8, after_prefix, '.') orelse continue;
                const ts = std.fmt.parseInt(i64, after_prefix[0..dot_pos], 10) catch continue;
                if (ts > best_ts) {
                    best_ts = ts;
                    if (best_path) |p| self.allocator.free(p);
                    best_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ hooks_dir_path, name }) catch
                        break :outer;
                }
            }
        }

        const log_file_path = best_path orelse
            return self.sendHookLogsEmpty(fd, req.hook_id);

        // Read the log file content
        const log_z = self.allocator.allocSentinel(u8, log_file_path.len, 0) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "alloc failed");
        defer self.allocator.free(log_z);
        @memcpy(log_z[0..log_file_path.len], log_file_path);

        const log_fd_rc = std.os.linux.open(log_z.ptr, .{ .ACCMODE = .RDONLY }, 0);
        const log_fd_i: isize = @bitCast(log_fd_rc);
        if (log_fd_i < 0)
            return sendError(self.allocator, fd, protocol.ErrorCode.not_found, "cannot open log file");
        const log_fd: i32 = @intCast(log_fd_rc);
        defer _ = std.os.linux.close(log_fd);

        const max_bytes = @min(req.max_bytes, 1024 * 1024); // cap at 1MB
        const content_buf = self.allocator.alloc(u8, max_bytes) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "alloc failed");
        defer self.allocator.free(content_buf);

        var total: usize = 0;
        while (total < content_buf.len) {
            const rc = std.os.linux.read(log_fd, content_buf[total..].ptr, content_buf[total..].len);
            const n_i: isize = @bitCast(rc);
            if (n_i <= 0) break;
            total += @intCast(n_i);
        }

        const resp_body = try protocol.encodeBody(self.allocator, protocol.HookLogsResponse{
            .hook_id = req.hook_id,
            .log_path = log_file_path,
            .content = content_buf[0..total],
        });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .hook_logs_ok, resp_body);
    }

    /// Success response for a known hook that has no recorded executions:
    /// empty log_path + empty content. An empty history is not an error.
    fn sendHookLogsEmpty(self: *Server, fd: std.posix.fd_t, hook_id: u64) !void {
        const resp_body = try protocol.encodeBody(self.allocator, protocol.HookLogsResponse{
            .hook_id = hook_id,
            .log_path = "",
            .content = "",
        });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .hook_logs_ok, resp_body);
    }

    fn handleStatus(self: *Server, fd: std.posix.fd_t) !void {
        const tm = self.topic_manager;

        // Topic snapshot — assembled under the TopicManager lock so the
        // per-topic counts and the deleted set are mutually consistent.
        var topic_infos: []protocol.StatusTopicInfo = &.{};
        var topics_initialized: usize = 0;
        defer {
            for (topic_infos[0..topics_initialized]) |t| self.allocator.free(t.name);
            self.allocator.free(topic_infos);
        }
        var total_events: u64 = 0;
        {
            tm.lockForHookRegistration();
            defer tm.unlockForHookRegistration();
            const keys = tm.topics.keys();
            const vals = tm.topics.values();
            topic_infos = self.allocator.alloc(protocol.StatusTopicInfo, keys.len) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.internal, "status failed");
            for (keys, vals) |k, v| {
                const name = self.allocator.dupe(u8, k) catch
                    return sendError(self.allocator, fd, protocol.ErrorCode.internal, "status failed");
                topic_infos[topics_initialized] = .{
                    .name = name,
                    .events = v.non_marker_count,
                    .deleted = tm.deleted_topics.contains(k),
                };
                topics_initialized += 1;
                total_events += v.non_marker_count;
            }
        }

        // Log stats — under the Log's own mutex (appends mutate segments).
        var segments: u64 = 0;
        var total_bytes: u64 = 0;
        {
            tm.log.mutex.lockUncancelable(tm.log.io);
            defer tm.log.mutex.unlock(tm.log.io);
            segments = tm.log.segments.items.len;
            for (tm.log.segments.items) |seg| total_bytes += seg.size;
        }

        // Hooks — same source list_hooks uses. Empty when hooks are disabled.
        const hooks_snap: []hooks_mod.Hook = if (self.hook_table) |ht|
            ht.snapshot(self.allocator) catch
                return sendError(self.allocator, fd, protocol.ErrorCode.internal, "status failed")
        else
            &.{};
        defer if (hooks_snap.len > 0) hooks_mod.freeHookSnapshot(self.allocator, hooks_snap);

        const hook_infos = self.allocator.alloc(protocol.StatusHookInfo, hooks_snap.len) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "status failed");
        defer self.allocator.free(hook_infos);
        for (hooks_snap, 0..) |hook, i| {
            hook_infos[i] = .{
                .id = hook.id,
                .pattern = hook.pattern,
                .command = hook.command,
                .cursor = hook.cursor,
            };
        }

        const timer_count: u64 = if (self.timer_table) |tt| tt.count() else 0;

        const now = getMilliTimestamp();
        const uptime_ms: u64 = if (now > self.start_time) @intCast(now - self.start_time) else 0;

        const resp_body = try protocol.encodeBody(self.allocator, protocol.StatusResponse{
            .data_dir = self.config.data_dir,
            .segments = segments,
            .total_bytes = total_bytes,
            .total_events = total_events,
            .topics = topic_infos[0..topics_initialized],
            .hooks = hook_infos,
            .timer_count = timer_count,
            .uptime_ms = uptime_ms,
        });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .status_ok, resp_body);
    }

    fn handleTimerInfo(self: *Server, body: []const u8, fd: std.posix.fd_t) !void {
        const tt = self.timer_table orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.internal, "timers not enabled");

        const parsed = protocol.decodeBody(protocol.TimerInfoRequest, self.allocator, body) catch
            return sendError(self.allocator, fd, protocol.ErrorCode.bad_request, "invalid timer info request");
        defer parsed.deinit();

        // findCopy returns a deep-copied Timer that is safe to use after
        // the mutex is released. find() returns internal string pointers
        // that can be invalidated by concurrent timer daemon mutations.
        const timer = tt.findCopy(self.allocator, parsed.value.name) orelse
            return sendError(self.allocator, fd, protocol.ErrorCode.not_found, "timer not found");
        defer timers_mod.freeTimerCopy(self.allocator, timer);

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

/// Atomically create the exact topic `topic` and register a hook on it,
/// all inside one critical section under the TopicManager registration
/// lock (the same mutex the publish path uses). Because the lock is held
/// for the whole operation, no publish can interleave anywhere inside it:
/// at the moment the topic becomes publishable, the hook is already armed
/// with a tip cursor of 0, so the first event ever published is the first
/// event delivered.
///
/// Ordering inside the lock: all fallible validation first — so the
/// common failure, AlreadyExists (including tombstoned names), creates
/// nothing — then hook registration, then the topic-creation marker
/// append. If the marker append fails the hook record is removed before
/// the error propagates, leaving neither half. The reverse order would be
/// worse: a created topic cannot be un-created (deletion writes a
/// permanent tombstone), while a hook record can be removed cleanly.
///
/// Public (module-level, not a Server method) so integration tests can
/// exercise the critical section without standing up TCP.
pub fn createTopicAndRegisterHook(
    tm: *TopicManager,
    ht: *HookTable,
    topic: []const u8,
    command: []const u8,
    cwd: []const u8,
    once: bool,
    env: ?[]const []const u8,
    name: ?[]const u8,
) !u64 {
    if (topic_mod.isPatternShape(topic)) return topic_mod.TopicError.InvalidName;

    tm.lockForHookRegistration();
    defer tm.unlockForHookRegistration();

    // Validate before registering anything.
    try topic_mod.validateTopicName(topic);
    if (tm.hasTopicLocked(topic)) return topic_mod.TopicError.AlreadyExists;

    // Fresh topic ⇒ tip is 0. Computed through the same helper the plain
    // registration path uses, for consistency.
    const tip = tm.tipForPatternLocked(topic);
    const id = try ht.addWithCursor(topic, command, cwd, once, env, name, tip);
    errdefer ht.remove(id) catch {};

    try tm.createTopicLocked(topic);
    return id;
}

/// Cap for the Pending backlog computation in `list_hooks`. A pattern hook
/// far behind a large log would otherwise trigger an unbounded scan; a value
/// equal to the cap renders as "1000+" in the CLI.
pub const hook_pending_cap: u32 = 1000;

pub const HookPendingInfo = struct {
    pending: u64,
    cursor_kind: []const u8,
};

/// Derive the uniform `Pending` value + `cursor_kind` tag for one hook,
/// dispatching by pattern shape exactly like `HookDaemon.processHook`
/// (see `v0.1/hook-registration-cursor.org`):
///
/// - exact topic → cursor is a topic-local skip count;
///   `pending = non_marker_count -| cursor` (0 if the topic doesn't exist).
/// - prefix/wildcard → cursor is a global log offset; pending is the count
///   of matching non-marker events at `offset >= cursor`, capped at
///   `hook_pending_cap`.
///
/// Each branch reads under the TopicManager lock, so the value is a
/// consistent point-in-time snapshot (racing live publishes — fine for a
/// status display). Public (module-level) so integration tests can exercise
/// it without standing up TCP.
pub fn computeHookPending(
    tm: *TopicManager,
    allocator: std.mem.Allocator,
    pattern: []const u8,
    cursor: u64,
) !HookPendingInfo {
    if (topic_mod.isPatternShape(pattern)) {
        return .{
            .pending = try tm.countPatternByOffset(allocator, pattern, cursor, hook_pending_cap),
            .cursor_kind = "global",
        };
    }
    const total = tm.topicEventCount(pattern) orelse 0;
    return .{ .pending = total -| cursor, .cursor_kind = "topic_local" };
}

fn signalHandler(_: std.os.linux.SIG) callconv(.c) void {
    if (global_server) |s| {
        s.shutdown_requested.store(true, .release);
        // Make a dummy connection to ourselves to unblock accept().
        // connect()+close() are async-signal-safe. The accept loop will
        // see shutdown_requested and break out cleanly. The handler no
        // longer owns that mechanism — it shares `shutdown()`'s — but it
        // still cannot call `shutdown()` itself, which sleeps and prints
        // while draining.
        s.unblockAccept();
    }
}

/// Milliseconds on a clock that cannot jump. Used for the blocking-fetch
/// deadline: the old loop accumulated a fixed sleep interval, which stops
/// being a measure of elapsed time once the waits are variable, and
/// `getMilliTimestamp` is wall-clock and can be stepped by NTP mid-wait.
fn monotonicMillis() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.BOOTTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

fn getMilliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.REALTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
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

test "Server reports no bound port before run" {
    // A caller that asked for an ephemeral port must be able to tell "not
    // bound yet" from a real port, otherwise it races the accept loop and
    // connects to whatever happens to own port 0's answer.
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    var server = try Server.init(std.testing.allocator, io, &tm, .{ .port = 0 });
    defer server.deinit();
    try std.testing.expectEqual(@as(u16, 0), server.boundPort());
}
