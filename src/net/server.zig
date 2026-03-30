//! TCP server for the Ever store.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;
const protocol = @import("../protocol/message.zig");
const topic_mod = @import("../store/topic.zig");
const TopicManager = topic_mod.TopicManager;
const store = @import("../store/store.zig");

pub const Config = struct {
    address: []const u8 = "127.0.0.1",
    port: u16 = 7890,
    max_connections: u32 = 1024,
};

pub const Server = struct {
    allocator: Allocator,
    io: Io,
    topic_manager: *TopicManager,
    config: Config,
    net_server: ?net.Server,
    shutdown_requested: std.atomic.Value(bool),

    pub fn init(allocator: Allocator, io: Io, topic_manager: *TopicManager, config: Config) !Server {
        return .{
            .allocator = allocator,
            .io = io,
            .topic_manager = topic_manager,
            .config = config,
            .net_server = null,
            .shutdown_requested = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Server) void {
        if (self.net_server) |*s| { s.deinit(self.io); self.net_server = null; }
    }

    pub fn run(self: *Server) !void {
        const ip4 = try net.Ip4Address.parse(self.config.address, self.config.port);
        const address: net.IpAddress = .{ .ip4 = ip4 };
        self.net_server = try address.listen(self.io, .{ .reuse_address = true });

        while (!self.shutdown_requested.load(.acquire)) {
            const stream = self.net_server.?.accept(self.io) catch |err| switch (err) {
                error.ConnectionAborted => continue,
                else => { if (self.shutdown_requested.load(.acquire)) break; return err; },
            };
            const thread = std.Thread.spawn(.{}, handleConnection, .{ self, stream }) catch {
                var s = stream; s.close(self.io); continue;
            };
            thread.detach();
        }
    }

    pub fn shutdown(self: *Server) void {
        self.shutdown_requested.store(true, .release);
        if (self.net_server) |*s| { s.deinit(self.io); self.net_server = null; }
    }

    fn handleConnection(self: *Server, stream: net.Stream) void {
        var s = stream;
        defer s.close(self.io);
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
        defer { for (topics) |t| self.allocator.free(t); self.allocator.free(topics); }
        const resp_body = try protocol.encodeBody(self.allocator, protocol.ListTopicsResponse{ .topics = topics });
        defer self.allocator.free(resp_body);
        try protocol.writeFrame(fd, .list_topics_ok, resp_body);
    }
};

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
