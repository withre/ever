//! HTTP/JSON API server for the Ever store.
//!
//! Provides a REST-style HTTP interface alongside the binary TCP protocol.
//! Uses `std.http.Server` with Io-based Reader/Writer.
//!
//! Endpoints:
//!   POST   /topics                Create topic
//!   DELETE /topics/{name}         Delete topic
//!   GET    /topics                List topics
//!   POST   /topics/{name}/events  Publish event
//!   GET    /topics/{name}/events  Fetch events (?offset=0&limit=100)

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
    port: u16 = 8890,
    max_connections: u32 = 1024,
};

// HTTP-specific request body for publishing (topic comes from URL path)
const HttpPublishRequest = struct {
    key: ?[]const u8 = null,
    value: []const u8,
};

pub const HttpServer = struct {
    allocator: Allocator,
    io: Io,
    topic_manager: *TopicManager,
    config: Config,
    net_server: ?net.Server,
    shutdown_requested: std.atomic.Value(bool),
    active_connections: std.atomic.Value(u32),

    pub fn init(allocator: Allocator, io: Io, topic_manager: *TopicManager, config: Config) HttpServer {
        return .{
            .allocator = allocator,
            .io = io,
            .topic_manager = topic_manager,
            .config = config,
            .net_server = null,
            .shutdown_requested = std.atomic.Value(bool).init(false),
            .active_connections = std.atomic.Value(u32).init(0),
        };
    }

    pub fn deinit(self: *HttpServer) void {
        if (self.net_server) |*s| {
            s.deinit(self.io);
            self.net_server = null;
        }
    }

    /// Start the accept loop. Blocks until `shutdown()` is called.
    pub fn run(self: *HttpServer) !void {
        const ip4 = try net.Ip4Address.parse(self.config.address, self.config.port);
        const address: net.IpAddress = .{ .ip4 = ip4 };
        self.net_server = try address.listen(self.io, .{ .reuse_address = false });

        while (!self.shutdown_requested.load(.acquire)) {
            const stream = self.net_server.?.accept(self.io) catch |err| switch (err) {
                error.ConnectionAborted => continue,
                else => {
                    if (self.shutdown_requested.load(.acquire)) break;
                    return err;
                },
            };

            const current = self.active_connections.load(.acquire);
            if (current >= self.config.max_connections) {
                var s = stream;
                s.close(self.io);
                continue;
            }

            _ = self.active_connections.fetchAdd(1, .acq_rel);
            const thread = std.Thread.spawn(.{}, handleConnection, .{ self, stream }) catch {
                _ = self.active_connections.fetchSub(1, .acq_rel);
                var s = stream;
                s.close(self.io);
                continue;
            };
            thread.detach();
        }
    }

    /// Signal the server to stop accepting connections.
    pub fn shutdown(self: *HttpServer) void {
        self.shutdown_requested.store(true, .release);
        if (self.net_server) |*s| {
            s.deinit(self.io);
            self.net_server = null;
        }
    }

    fn handleConnection(self: *HttpServer, stream: net.Stream) void {
        var s = stream;
        defer {
            s.close(self.io);
            _ = self.active_connections.fetchSub(1, .acq_rel);
        }

        var read_buf: [8192]u8 = undefined;
        var write_buf: [8192]u8 = undefined;
        var reader = s.reader(self.io, &read_buf);
        var writer = s.writer(self.io, &write_buf);
        var http_server = std.http.Server.init(&reader.interface, &writer.interface);

        while (!self.shutdown_requested.load(.acquire)) {
            var request = http_server.receiveHead() catch break;
            self.handleRequest(&request) catch break;
        }
    }

    fn handleRequest(self: *HttpServer, request: *std.http.Server.Request) !void {
        const path_and_query = request.head.target;

        // Split path from query string
        var path: []const u8 = path_and_query;
        var query: []const u8 = "";
        if (std.mem.indexOfScalar(u8, path_and_query, '?')) |qi| {
            path = path_and_query[0..qi];
            query = path_and_query[qi + 1 ..];
        }

        // Remove trailing slash (except root)
        if (path.len > 1 and path[path.len - 1] == '/') {
            path = path[0 .. path.len - 1];
        }

        const method = request.head.method;

        // Route: GET /topics
        if (method == .GET and std.mem.eql(u8, path, "/topics")) {
            return self.handleListTopics(request);
        }

        // Route: POST /topics
        if (method == .POST and std.mem.eql(u8, path, "/topics")) {
            return self.handleCreateTopic(request);
        }

        // Routes with topic name: /topics/{name}...
        if (std.mem.startsWith(u8, path, "/topics/")) {
            const rest = path["/topics/".len..];

            // Check for /topics/{name}/events
            if (std.mem.indexOf(u8, rest, "/events")) |ei| {
                const topic_name = rest[0..ei];
                if (topic_name.len == 0) {
                    return self.respondError(request, .bad_request, "missing topic name");
                }

                if (method == .POST) {
                    return self.handlePublish(request, topic_name);
                }
                if (method == .GET) {
                    return self.handleFetch(request, topic_name, query);
                }
                return self.respondError(request, .method_not_allowed, "method not allowed");
            }

            // /topics/{name} — DELETE
            const topic_name = rest;
            if (topic_name.len == 0) {
                return self.respondError(request, .bad_request, "missing topic name");
            }

            if (method == .DELETE) {
                return self.handleDeleteTopic(request, topic_name);
            }

            return self.respondError(request, .method_not_allowed, "method not allowed");
        }

        return self.respondError(request, .not_found, "not found");
    }

    fn handleCreateTopic(self: *HttpServer, request: *std.http.Server.Request) !void {
        const body = self.readBody(request) catch {
            return self.respondError(request, .bad_request, "failed to read request body");
        };
        defer if (body) |b| self.allocator.free(b);

        const body_slice = body orelse {
            return self.respondError(request, .bad_request, "missing request body");
        };

        const parsed = protocol.decodeBody(protocol.TopicRequest, self.allocator, body_slice) catch {
            return self.respondError(request, .bad_request, "invalid JSON body");
        };
        defer parsed.deinit();

        self.topic_manager.createTopic(parsed.value.topic) catch |err| return switch (err) {
            error.AlreadyExists => self.respondError(request, .conflict, "topic already exists"),
            error.InvalidName => self.respondError(request, .bad_request, "invalid topic name"),
            else => self.respondError(request, .internal_server_error, "create topic failed"),
        };

        try self.respondJson(request, .created, "{\"ok\":true}");
    }

    fn handleDeleteTopic(self: *HttpServer, request: *std.http.Server.Request, topic_name: []const u8) !void {
        // URL-decode the topic name (dots are valid in topic names)
        self.topic_manager.deleteTopic(topic_name) catch |err| return switch (err) {
            error.NotFound => self.respondError(request, .not_found, "topic not found"),
            else => self.respondError(request, .internal_server_error, "delete topic failed"),
        };

        try self.respondJson(request, .ok, "{\"ok\":true}");
    }

    fn handleListTopics(self: *HttpServer, request: *std.http.Server.Request) !void {
        const topics = self.topic_manager.listTopics(self.allocator) catch {
            return self.respondError(request, .internal_server_error, "list topics failed");
        };
        defer {
            for (topics) |t| self.allocator.free(t);
            self.allocator.free(topics);
        }

        const resp = protocol.ListTopicsResponse{ .topics = topics };
        const json = protocol.encodeBody(self.allocator, resp) catch {
            return self.respondError(request, .internal_server_error, "encoding failed");
        };
        defer self.allocator.free(json);

        try self.respondJson(request, .ok, json);
    }

    fn handlePublish(self: *HttpServer, request: *std.http.Server.Request, topic_name: []const u8) !void {
        const body = self.readBody(request) catch {
            return self.respondError(request, .bad_request, "failed to read request body");
        };
        defer if (body) |b| self.allocator.free(b);

        const body_slice = body orelse {
            return self.respondError(request, .bad_request, "missing request body");
        };

        const parsed = std.json.parseFromSlice(HttpPublishRequest, self.allocator, body_slice, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch {
            return self.respondError(request, .bad_request, "invalid JSON body");
        };
        defer parsed.deinit();

        const offset = self.topic_manager.publish(topic_name, parsed.value.key, parsed.value.value) catch |err| return switch (err) {
            error.NotFound => self.respondError(request, .not_found, "topic not found"),
            else => self.respondError(request, .internal_server_error, "publish failed"),
        };

        const resp = protocol.PublishResponse{ .offset = offset };
        const json = protocol.encodeBody(self.allocator, resp) catch {
            return self.respondError(request, .internal_server_error, "encoding failed");
        };
        defer self.allocator.free(json);

        try self.respondJson(request, .ok, json);
    }

    fn handleFetch(self: *HttpServer, request: *std.http.Server.Request, topic_name: []const u8, query: []const u8) !void {
        var offset: u64 = 0;
        var limit: u32 = 100;

        // Parse query parameters
        var params = std.mem.splitScalar(u8, query, '&');
        while (params.next()) |param| {
            if (param.len == 0) continue;
            if (std.mem.indexOfScalar(u8, param, '=')) |eq| {
                const key = param[0..eq];
                const val = param[eq + 1 ..];
                if (std.mem.eql(u8, key, "offset")) {
                    offset = std.fmt.parseInt(u64, val, 10) catch 0;
                } else if (std.mem.eql(u8, key, "limit")) {
                    limit = std.fmt.parseInt(u32, val, 10) catch 100;
                }
            }
        }

        const events = self.topic_manager.fetch(self.allocator, topic_name, offset, limit) catch |err| return switch (err) {
            error.NotFound => self.respondError(request, .not_found, "topic not found"),
            else => self.respondError(request, .internal_server_error, "fetch failed"),
        };
        defer {
            for (events) |evt| store.freeEvent(self.allocator, evt);
            self.allocator.free(events);
        }

        // Convert to protocol format
        const event_data = self.allocator.alloc(protocol.EventData, events.len) catch {
            return self.respondError(request, .internal_server_error, "allocation failed");
        };
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

        const resp = protocol.FetchResponse{ .events = event_data };
        const json = protocol.encodeBody(self.allocator, resp) catch {
            return self.respondError(request, .internal_server_error, "encoding failed");
        };
        defer self.allocator.free(json);

        try self.respondJson(request, .ok, json);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Read the full request body. Returns null if no body.
    fn readBody(self: *HttpServer, request: *std.http.Server.Request) !?[]u8 {
        // Check if there's a content-length; if 0 or missing, no body
        const content_length = request.head.content_length orelse return null;
        if (content_length == 0) return null;

        var body_reader_buf: [8192]u8 = undefined;
        const body_reader = request.readerExpectNone(&body_reader_buf);

        // Use allocRemaining to read the full body
        const body = body_reader.allocRemaining(self.allocator, std.Io.Limit.limited(16 * 1024 * 1024)) catch return null;
        if (body.len == 0) {
            self.allocator.free(body);
            return null;
        }
        return body;
    }

    /// Send a JSON response with the given status and body.
    fn respondJson(_: *HttpServer, request: *std.http.Server.Request, status: std.http.Status, json: []const u8) !void {
        try request.respond(json, .{
            .status = status,
            .extra_headers = &.{
                .{ .name = "content-type", .value = "application/json" },
            },
        });
    }

    /// Send a JSON error response.
    fn respondError(self: *HttpServer, request: *std.http.Server.Request, status: std.http.Status, message: []const u8) !void {
        const json = protocol.encodeBody(self.allocator, protocol.ErrorResponse{
            .code = @intFromEnum(status),
            .message = message,
        }) catch {
            // Fallback if encoding fails
            try request.respond("{\"error\":\"internal error\"}", .{
                .status = .internal_server_error,
                .extra_headers = &.{
                    .{ .name = "content-type", .value = "application/json" },
                },
            });
            return;
        };
        defer self.allocator.free(json);

        try request.respond(json, .{
            .status = status,
            .extra_headers = &.{
                .{ .name = "content-type", .value = "application/json" },
            },
        });
    }
};

test "HttpServer init and deinit" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(std.testing.allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    var server = HttpServer.init(std.testing.allocator, io, &tm, .{});
    defer server.deinit();
    try std.testing.expect(!server.shutdown_requested.load(.acquire));
}
