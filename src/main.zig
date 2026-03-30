const std = @import("std");
const ever = @import("ever");

const Io = std.Io;
const Dir = Io.Dir;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Collect args into a slice
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_list: std.ArrayList([]const u8) = .empty;
    defer args_list.deinit(allocator);

    while (args_iter.next()) |arg| {
        try args_list.append(allocator, arg);
    }

    const args = args_list.items;

    if (args.len < 2) {
        printUsage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "version")) {
        std.debug.print("ever 0.1.0\n", .{});
    } else if (std.mem.eql(u8, command, "help")) {
        printUsage();
    } else if (std.mem.eql(u8, command, "store")) {
        try handleStore(allocator, io, args[2..]);
    } else if (std.mem.eql(u8, command, "topic")) {
        try handleTopic(allocator, io, args[2..]);
    } else if (std.mem.eql(u8, command, "pub")) {
        try handlePub(allocator, io, args[2..]);
    } else if (std.mem.eql(u8, command, "sub")) {
        try handleSub(allocator, io, args[2..]);
    } else {
        std.process.fatal("unknown command '{s}'. Run 'ever help' for usage.", .{command});
    }
}

fn handleStore(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 1) {
        std.process.fatal("missing subcommand. Usage: ever store <start|stop|status>", .{});
    }

    const sub = args[0];
    if (std.mem.eql(u8, sub, "start")) {
        try startServer(allocator, io, args[1..]);
    } else {
        std.process.fatal("unknown store subcommand '{s}'.", .{sub});
    }
}

fn startServer(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    var address: []const u8 = "127.0.0.1";
    var port: u16 = 7890;
    var data_dir: []const u8 = "./data";

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--address") and i + 1 < args.len) {
            i += 1;
            address = args[i];
        } else if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            i += 1;
            port = std.fmt.parseInt(u16, args[i], 10) catch {
                std.process.fatal("invalid port '{s}'.", .{args[i]});
            };
        } else if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            i += 1;
            data_dir = args[i];
        }
    }

    const dir = Dir.cwd().createDirPathOpen(io, data_dir, .{
        .open_options = .{ .iterate = true },
    }) catch {
        std.process.fatal("cannot open data directory '{s}'.", .{data_dir});
    };

    var topic_manager = try ever.topic.TopicManager.init(allocator, io, dir, .{});
    defer topic_manager.deinit();

    var server = try ever.net.Server.init(allocator, io, &topic_manager, .{
        .address = address,
        .port = port,
    });
    defer server.deinit();

    std.debug.print("Ever store listening on {s}:{d}\n", .{ address, port });
    std.debug.print("Data directory: {s}\n", .{data_dir});
    std.debug.print("Press Ctrl-C to stop.\n", .{});

    server.run() catch |err| {
        std.process.fatal("server failed: {}", .{err});
    };
}

fn handleTopic(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 1) {
        std.process.fatal("missing subcommand. Usage: ever topic <create|list|delete> [name]", .{});
    }

    const sub = args[0];
    const address = "127.0.0.1";
    const port: u16 = 7890;

    if (std.mem.eql(u8, sub, "create")) {
        if (args.len < 2) {
            std.process.fatal("missing topic name. Usage: ever topic create <name>", .{});
        }
        var client = try ever.client.Client.connect(allocator, io, address, port);
        defer client.deinit();
        try client.createTopic(args[1]);
        std.debug.print("Created topic: {s}\n", .{args[1]});
    } else if (std.mem.eql(u8, sub, "list")) {
        var client = try ever.client.Client.connect(allocator, io, address, port);
        defer client.deinit();
        const topics = try client.listTopics();
        defer {
            for (topics) |t| allocator.free(t);
            allocator.free(topics);
        }
        if (topics.len == 0) {
            std.debug.print("No topics.\n", .{});
        } else {
            for (topics) |t| {
                std.debug.print("{s}\n", .{t});
            }
        }
    } else if (std.mem.eql(u8, sub, "delete")) {
        if (args.len < 2) {
            std.process.fatal("missing topic name. Usage: ever topic delete <name>", .{});
        }
        var client = try ever.client.Client.connect(allocator, io, address, port);
        defer client.deinit();
        try client.deleteTopic(args[1]);
        std.debug.print("Deleted topic: {s}\n", .{args[1]});
    } else {
        std.process.fatal("unknown topic subcommand '{s}'.", .{sub});
    }
}

fn handlePub(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 2) {
        std.process.fatal("usage: ever pub <topic> <data>", .{});
    }

    const topic_name = args[0];
    const data = args[1];
    const address = "127.0.0.1";
    const port: u16 = 7890;

    var client = try ever.client.Client.connect(allocator, io, address, port);
    defer client.deinit();
    const offset = try client.publish(topic_name, null, data);
    std.debug.print("Published to {s} at offset {d}\n", .{ topic_name, offset });
}

fn handleSub(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 1) {
        std.process.fatal("usage: ever sub <topic> [--from <offset>] [--max <count>]", .{});
    }

    const topic_name = args[0];
    var from_offset: u64 = 0;
    var max_count: u32 = 100;
    const address = "127.0.0.1";
    const port: u16 = 7890;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--from") and i + 1 < args.len) {
            i += 1;
            from_offset = std.fmt.parseInt(u64, args[i], 10) catch {
                std.process.fatal("invalid offset '{s}'.", .{args[i]});
            };
        } else if (std.mem.eql(u8, args[i], "--max") and i + 1 < args.len) {
            i += 1;
            max_count = std.fmt.parseInt(u32, args[i], 10) catch {
                std.process.fatal("invalid count '{s}'.", .{args[i]});
            };
        }
    }

    var client = try ever.client.Client.connect(allocator, io, address, port);
    defer client.deinit();

    // Detect if the input is a pattern (trailing dot, contains *, or is ".")
    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    var result = if (is_pattern)
        try client.fetchPattern(topic_name, from_offset, max_count)
    else
        try client.fetch(topic_name, from_offset, max_count);
    defer result.deinit();

    if (result.events.len == 0) {
        std.debug.print("No events.\n", .{});
    } else {
        for (result.events) |event| {
            const prefix = if (event.topic) |t| t else "";
            const has_prefix = prefix.len > 0;
            if (event.key) |k| {
                if (has_prefix) {
                    std.debug.print("[{s}:{d}] key={s} {s}\n", .{ prefix, event.offset, k, event.value });
                } else {
                    std.debug.print("[{d}] key={s} {s}\n", .{ event.offset, k, event.value });
                }
            } else {
                if (has_prefix) {
                    std.debug.print("[{s}:{d}] {s}\n", .{ prefix, event.offset, event.value });
                } else {
                    std.debug.print("[{d}] {s}\n", .{ event.offset, event.value });
                }
            }
        }
    }
}

fn printUsage() void {
    const usage =
        \\Ever — lightweight event storage
        \\
        \\Usage: ever <command> [options]
        \\
        \\Commands:
        \\  store start    Start the storage server
        \\  topic create   Create a new topic
        \\  topic list     List all topics
        \\  topic delete   Delete a topic
        \\  pub            Publish an event
        \\  sub            Subscribe to events
        \\  version        Show version
        \\  help           Show this help
        \\
        \\Run 'ever <command> --help' for more information.
        \\
    ;
    std.debug.print("{s}", .{usage});
}

test "main module compiles" {
    _ = ever;
}
