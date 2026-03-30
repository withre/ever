//! Client library for connecting to the Ever store.
//!
//! Provides Publisher and Subscriber functionality through a unified Client.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;
const protocol = @import("../protocol/message.zig");
const store = @import("../store/store.zig");

pub const FetchResult = struct {
    events: []Event,
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

/// Unified client for publishing and subscribing to the Ever store.
pub const Client = struct {
    allocator: Allocator,
    io: Io,
    stream: net.Stream,

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

    pub fn deinit(self: *Client) void {
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
        try protocol.writeFrame(self.fd(), .publish, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return error.ServerError;
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

    fn doFetch(self: *Client, req: protocol.FetchRequest) !FetchResult {
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try protocol.writeFrame(self.fd(), .fetch, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return error.ServerError;
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
            .allocator = self.allocator,
        };
    }

    /// Create a topic on the server.
    pub fn createTopic(self: *Client, name: []const u8) !void {
        const req = protocol.TopicRequest{ .topic = name };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try protocol.writeFrame(self.fd(), .create_topic, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return error.ServerError;
        if (frame.msg_type != .create_topic_ok) return error.UnexpectedResponse;
    }

    /// Delete a topic on the server.
    pub fn deleteTopic(self: *Client, name: []const u8) !void {
        const req = protocol.TopicRequest{ .topic = name };
        const body = try protocol.encodeBody(self.allocator, req);
        defer self.allocator.free(body);
        try protocol.writeFrame(self.fd(), .delete_topic, body);

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return error.ServerError;
        if (frame.msg_type != .delete_topic_ok) return error.UnexpectedResponse;
    }

    /// List all topics on the server.
    pub fn listTopics(self: *Client) ![][]const u8 {
        try protocol.writeFrame(self.fd(), .list_topics, "{}");

        const frame = (try protocol.readFrame(self.allocator, self.fd())) orelse
            return error.ConnectionClosed;
        defer self.allocator.free(frame.body);

        if (frame.msg_type == .error_response) return error.ServerError;
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
            result[i] = try self.allocator.dupe(u8, t);
            initialized = i + 1;
        }

        return result;
    }
};

test "Client struct compiles" {
    _ = Client;
    _ = FetchResult;
    _ = Event;
}
