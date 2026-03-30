const std = @import("std");
const ever = @import("ever");

const Io = std.Io;
const Dir = Io.Dir;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

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
        try handleStore(allocator, io, args[2..], init.minimal.environ.block.slice.ptr);
    } else if (std.mem.eql(u8, command, "topic")) {
        try handleTopic(allocator, io, args[2..]);
    } else if (std.mem.eql(u8, command, "pub")) {
        try handlePub(allocator, io, args[2..]);
    } else if (std.mem.eql(u8, command, "sub")) {
        try handleSub(allocator, io, args[2..]);
    } else if (std.mem.eql(u8, command, "on")) {
        try handleOn(allocator, io, args[2..], init.minimal.environ.block.slice.ptr);
    } else if (std.mem.eql(u8, command, "hook")) {
        try handleHook(allocator, io, args[2..]);
    } else {
        std.process.fatal("unknown command '{s}'. Run 'ever help' for usage.", .{command});
    }
}

fn handleStore(allocator: std.mem.Allocator, io: Io, args: []const []const u8, envp: [*:null]const ?[*:0]const u8) !void {
    if (args.len < 1) std.process.fatal("missing subcommand. Usage: ever store <start>", .{});
    if (std.mem.eql(u8, args[0], "start")) {
        try startServer(allocator, io, args[1..], envp);
    } else {
        std.process.fatal("unknown store subcommand '{s}'.", .{args[0]});
    }
}

fn startServer(allocator: std.mem.Allocator, io: Io, args: []const []const u8, envp: [*:null]const ?[*:0]const u8) !void {
    var address: []const u8 = "127.0.0.1";
    var port: u16 = 7890;
    var data_dir: []const u8 = "./data";

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--address") and i + 1 < args.len) {
            i += 1; address = args[i];
        } else if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            i += 1; port = std.fmt.parseInt(u16, args[i], 10) catch std.process.fatal("invalid port '{s}'.", .{args[i]});
        } else if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            i += 1; data_dir = args[i];
        }
    }

    const dir = Dir.cwd().createDirPathOpen(io, data_dir, .{ .open_options = .{ .iterate = true } }) catch
        std.process.fatal("cannot open data directory '{s}'.", .{data_dir});

    // Acquire exclusive lockfile to prevent multiple stores on same data dir
    const lock_file = dir.createFile(io, "ever.lock", .{ .read = true, .truncate = false }) catch
        std.process.fatal("cannot create lockfile in '{s}'.", .{data_dir});
    defer lock_file.close(io);

    const LOCK_EX = 2;
    const LOCK_NB = 4;
    const lock_result = std.os.linux.flock(lock_file.handle, LOCK_EX | LOCK_NB);
    if (lock_result != 0) {
        std.process.fatal("another Ever store is already running on '{s}'. Remove ever.lock if stale.", .{data_dir});
    }

    var topic_manager = try ever.topic.TopicManager.init(allocator, io, dir, .{});
    defer topic_manager.deinit();

    // Initialize hook table
    var hook_table = try ever.hooks.HookTable.init(allocator, data_dir);
    defer hook_table.deinit();

    var server = try ever.net.Server.init(allocator, io, &topic_manager, .{ .address = address, .port = port });
    defer server.deinit();
    server.setHookTable(&hook_table);

    // Start hook daemon
    var hook_daemon = ever.hooks.HookDaemon.init(allocator, &hook_table, &topic_manager, envp);
    hook_daemon.start() catch |err| {
        std.debug.print("Warning: failed to start hook daemon: {}\n", .{err});
    };
    defer hook_daemon.stop();

    std.debug.print("Ever store listening on {s}:{d}\n", .{ address, port });
    std.debug.print("Data directory: {s}\n", .{data_dir});
    std.debug.print("Hook daemon started.\n", .{});
    std.debug.print("Press Ctrl-C to stop.\n", .{});
    server.run() catch |err| std.process.fatal("server failed: {}", .{err});
}

fn handleTopic(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 1) std.process.fatal("usage: ever topic <create|list|delete> [name]", .{});
    const sub = args[0];
    const address = "127.0.0.1";
    const port: u16 = 7890;

    if (std.mem.eql(u8, sub, "create")) {
        if (args.len < 2) std.process.fatal("usage: ever topic create <name>", .{});
        var c = try ever.client.Client.connect(allocator, io, address, port);
        defer c.deinit();
        try c.createTopic(args[1]);
        std.debug.print("Created topic: {s}\n", .{args[1]});
    } else if (std.mem.eql(u8, sub, "list")) {
        var c = try ever.client.Client.connect(allocator, io, address, port);
        defer c.deinit();
        const topics = try c.listTopics();
        defer { for (topics) |t| allocator.free(t); allocator.free(topics); }
        if (topics.len == 0) std.debug.print("No topics.\n", .{}) else for (topics) |t| std.debug.print("{s}\n", .{t});
    } else if (std.mem.eql(u8, sub, "delete")) {
        if (args.len < 2) std.process.fatal("usage: ever topic delete <name>", .{});
        var c = try ever.client.Client.connect(allocator, io, address, port);
        defer c.deinit();
        try c.deleteTopic(args[1]);
        std.debug.print("Deleted topic: {s}\n", .{args[1]});
    } else std.process.fatal("unknown topic subcommand '{s}'.", .{sub});
}

fn handlePub(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 2) std.process.fatal("usage: ever pub <topic> <data>", .{});
    const address = "127.0.0.1";
    const port: u16 = 7890;
    var c = try ever.client.Client.connect(allocator, io, address, port);
    defer c.deinit();
    const offset = try c.publish(args[0], null, args[1]);
    std.debug.print("Published to {s} at offset {d}\n", .{ args[0], offset });
}

fn handleSub(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 1) std.process.fatal("usage: ever sub <topic> [--from <offset>] [--max <count>]", .{});
    const topic_name = args[0];
    var from_offset: u64 = 0;
    var max_count: u32 = 100;
    const address = "127.0.0.1";
    const port: u16 = 7890;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--from") and i + 1 < args.len) {
            i += 1; from_offset = std.fmt.parseInt(u64, args[i], 10) catch std.process.fatal("invalid offset.", .{});
        } else if (std.mem.eql(u8, args[i], "--max") and i + 1 < args.len) {
            i += 1; max_count = std.fmt.parseInt(u32, args[i], 10) catch std.process.fatal("invalid count.", .{});
        }
    }

    var client = try ever.client.Client.connect(allocator, io, address, port);
    defer client.deinit();

    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    var result = if (is_pattern) try client.fetchPattern(topic_name, from_offset, max_count) else try client.fetch(topic_name, from_offset, max_count);
    defer result.deinit();

    if (result.events.len == 0) {
        std.debug.print("No events.\n", .{});
    } else {
        for (result.events) |event| {
            const t = if (event.topic) |tp| tp else "";
            if (t.len > 0) {
                if (event.key) |k| std.debug.print("[{s}:{d}] key={s} {s}\n", .{ t, event.offset, k, event.value })
                else std.debug.print("[{s}:{d}] {s}\n", .{ t, event.offset, event.value });
            } else {
                if (event.key) |k| std.debug.print("[{d}] key={s} {s}\n", .{ event.offset, k, event.value })
                else std.debug.print("[{d}] {s}\n", .{ event.offset, event.value });
            }
        }
    }
}

// ── ever on ─────────────────────────────────────────────────────────────────

fn handleOn(allocator: std.mem.Allocator, io: Io, args: []const []const u8, envp: [*:null]const ?[*:0]const u8) !void {
    var pattern: ?[]const u8 = null;
    var once = false;
    const address = "127.0.0.1";
    const port: u16 = 7890;

    // Find "--" separator
    var cmd_start: ?usize = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--")) {
            cmd_start = i + 1;
            break;
        } else if (std.mem.eql(u8, args[i], "--once")) {
            once = true;
        } else if (pattern == null) {
            pattern = args[i];
        }
    }

    if (pattern == null)
        std.process.fatal("usage: ever on <pattern> [--once] -- <command> [args...]", .{});
    if (cmd_start == null or cmd_start.? >= args.len)
        std.process.fatal("missing command after '--'.", .{});

    const cmd_args = args[cmd_start.?..];
    const topic_pattern = pattern.?;

    const is_pattern = std.mem.indexOfScalar(u8, topic_pattern, '*') != null or
        (topic_pattern.len > 0 and topic_pattern[topic_pattern.len - 1] == '.');

    // Connect and probe for current offset (start from latest)
    var client = try ever.client.Client.connect(allocator, io, address, port);
    defer client.deinit();

    // Probe: fetch existing events to find the current end offset
    var probe = if (is_pattern)
        try client.fetchBlocking(null, topic_pattern, 0, 1_000_000, 0)
    else
        try client.fetchBlocking(topic_pattern, null, 0, 1_000_000, 0);
    const next_offset: u64 = probe.events.len;
    probe.deinit();

    std.debug.print("Watching '{s}' from offset {d}...\n", .{ topic_pattern, next_offset });

    // Main loop
    var offset = next_offset;
    while (true) {
        var result = if (is_pattern)
            try client.fetchBlocking(null, topic_pattern, offset, 100, 5000)
        else
            try client.fetchBlocking(topic_pattern, null, offset, 100, 5000);
        defer result.deinit();

        if (result.events.len == 0) {
            if (once) break;
            continue;
        }

        for (result.events) |event| {
            // Build JSON for stdin
            const json = try buildEventJson(allocator, event);
            defer allocator.free(json);

            // Build env strings
            var offset_buf: [20]u8 = undefined;
            const offset_str = std.fmt.bufPrint(&offset_buf, "{d}", .{event.offset}) catch "0";
            var ts_buf: [20]u8 = undefined;
            const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{event.timestamp}) catch "0";

            // Build shell command: env vars + user command
            var shell_cmd: std.ArrayList(u8) = .empty;
            defer shell_cmd.deinit(allocator);

            try shell_cmd.appendSlice(allocator, "EVER_TOPIC='");
            try shell_cmd.appendSlice(allocator, if (event.topic) |t| t else "");
            try shell_cmd.appendSlice(allocator, "' EVER_OFFSET='");
            try shell_cmd.appendSlice(allocator, offset_str);
            try shell_cmd.appendSlice(allocator, "' EVER_TIMESTAMP='");
            try shell_cmd.appendSlice(allocator, ts_str);
            try shell_cmd.appendSlice(allocator, "' EVER_KEY='");
            try shell_cmd.appendSlice(allocator, if (event.key) |k| k else "");
            try shell_cmd.appendSlice(allocator, "' exec ");
            for (cmd_args) |arg| {
                // Shell-escape each argument
                try shell_cmd.append(allocator, '\'');
                for (arg) |c| {
                    if (c == '\'') {
                        try shell_cmd.appendSlice(allocator, "'\\''");
                    } else {
                        try shell_cmd.append(allocator, c);
                    }
                }
                try shell_cmd.appendSlice(allocator, "' ");
            }

            // Write JSON to a PID-specific temp file for stdin (avoid race with other processes)
            const my_pid = std.os.linux.getpid();
            var stdin_path_buf: [64]u8 = undefined;
            const stdin_path = std.fmt.bufPrint(&stdin_path_buf, "/tmp/.ever-on-stdin-{d}", .{my_pid}) catch "/tmp/.ever-on-stdin";
            const stdin_path_z = allocator.allocSentinel(u8, stdin_path.len, 0) catch continue;
            defer allocator.free(stdin_path_z);
            @memcpy(stdin_path_z[0..stdin_path.len], stdin_path);
            {
                const fd_rc = std.os.linux.open(stdin_path_z.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o600);
                if (@as(isize, @bitCast(fd_rc)) >= 0) {
                    const fd: i32 = @intCast(fd_rc);
                    _ = std.os.linux.write(fd, json.ptr, json.len);
                    _ = std.os.linux.close(fd);
                }
            }

            // Execute: sh -c '<env> <cmd>' < /tmp/.ever-on-stdin-<pid>
            const full_cmd = try std.fmt.allocPrint(allocator, "{s}< {s}", .{ shell_cmd.items, stdin_path });
            defer allocator.free(full_cmd);

            // Use fork+exec /bin/sh -c
            const pid = std.os.linux.fork();
            const pid_i: isize = @bitCast(pid);

            if (pid_i < 0) {
                std.debug.print("fork failed\n", .{});
                continue;
            }

            if (pid_i == 0) {
                // Child: exec sh -c '<command>'
                const sh: [*:0]const u8 = "/bin/sh";
                const dash_c: [*:0]const u8 = "-c";

                // Null-terminate the command
                const cmd_z = allocator.allocSentinel(u8, full_cmd.len, 0) catch std.os.linux.exit(127);
                @memcpy(cmd_z[0..full_cmd.len], full_cmd);

                const argv = [_]?[*:0]const u8{ sh, dash_c, cmd_z.ptr, null };
                _ = std.os.linux.execve(sh, @ptrCast(&argv), envp);
                std.os.linux.exit(127);
            }

            // Parent: wait for child
            var status: u32 = 0;
            _ = std.os.linux.waitpid(@intCast(pid), &status, 0);
            const exit_code = (status >> 8) & 0xFF;
            if (exit_code != 0) {
                std.debug.print("Command exited with status {d} for event on '{s}'\n", .{ exit_code, if (event.topic) |t| t else topic_pattern });
            }
        }

        offset += result.events.len;
        if (once) break;
    }
}

fn buildEventJson(allocator: std.mem.Allocator, event: ever.client.Event) ![]u8 {
    var json: std.ArrayList(u8) = .empty;
    errdefer json.deinit(allocator);

    try json.appendSlice(allocator, "{\"topic\":\"");
    if (event.topic) |t| try appendJsonEscaped(&json, allocator, t);
    try json.appendSlice(allocator, "\",\"offset\":");
    {
        var buf: [20]u8 = undefined;
        try json.appendSlice(allocator, std.fmt.bufPrint(&buf, "{d}", .{event.offset}) catch "0");
    }
    try json.appendSlice(allocator, ",\"timestamp\":");
    {
        var buf: [20]u8 = undefined;
        try json.appendSlice(allocator, std.fmt.bufPrint(&buf, "{d}", .{event.timestamp}) catch "0");
    }
    if (event.key) |k| {
        try json.appendSlice(allocator, ",\"key\":\"");
        try appendJsonEscaped(&json, allocator, k);
        try json.append(allocator, '"');
    } else {
        try json.appendSlice(allocator, ",\"key\":null");
    }
    try json.appendSlice(allocator, ",\"value\":\"");
    try appendJsonEscaped(&json, allocator, event.value);
    try json.appendSlice(allocator, "\"}");
    return json.toOwnedSlice(allocator);
}

/// Append a string to the JSON buffer with proper escaping for all control characters.
fn appendJsonEscaped(json: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| switch (c) {
        '"' => try json.appendSlice(allocator, "\\\""),
        '\\' => try json.appendSlice(allocator, "\\\\"),
        '\n' => try json.appendSlice(allocator, "\\n"),
        '\t' => try json.appendSlice(allocator, "\\t"),
        '\r' => try json.appendSlice(allocator, "\\r"),
        0x08 => try json.appendSlice(allocator, "\\b"),
        0x0C => try json.appendSlice(allocator, "\\f"),
        else => {
            if (c < 0x20) {
                // Escape other control characters as \u00XX
                var buf: [6]u8 = undefined;
                _ = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
                try json.appendSlice(allocator, &buf);
            } else {
                try json.append(allocator, c);
            }
        },
    };
}

// ── ever hook ────────────────────────────────────────────────────────────────

fn handleHook(allocator: std.mem.Allocator, io: Io, args: []const []const u8) !void {
    if (args.len < 1) std.process.fatal("usage: ever hook <add|list|rm> [args]", .{});
    const sub = args[0];
    const address = "127.0.0.1";
    const port: u16 = 7890;

    if (std.mem.eql(u8, sub, "add")) {
        // ever hook add <pattern> -- <cmd> [args...]
        var pattern: ?[]const u8 = null;
        var cmd_start: ?usize = null;

        var i: usize = 1;
        while (i < args.len) : (i += 1) {
            if (std.mem.eql(u8, args[i], "--")) {
                cmd_start = i + 1;
                break;
            } else if (pattern == null) {
                pattern = args[i];
            }
        }

        if (pattern == null) std.process.fatal("usage: ever hook add <pattern> -- <command> [args...]", .{});
        if (cmd_start == null or cmd_start.? >= args.len)
            std.process.fatal("missing command after '--'.", .{});

        const cmd_args = args[cmd_start.?..];

        // Get current working directory
        var cwd_buf: [4096]u8 = undefined;
        const cwd_z: [*:0]const u8 = "/proc/self/cwd";
        const cwd_len = std.os.linux.readlink(cwd_z, &cwd_buf, cwd_buf.len);
        const cwd_i: isize = @bitCast(cwd_len);
        const cwd: []const u8 = if (cwd_i > 0) cwd_buf[0..@intCast(cwd_i)] else "/tmp";

        var c = try ever.client.Client.connect(allocator, io, address, port);
        defer c.deinit();
        const id = try c.registerHook(pattern.?, cmd_args, cwd);

        // Format command for display
        var cmd_display: std.ArrayList(u8) = .empty;
        defer cmd_display.deinit(allocator);
        for (cmd_args, 0..) |arg, j| {
            if (j > 0) try cmd_display.append(allocator, ' ');
            try cmd_display.appendSlice(allocator, arg);
        }
        std.debug.print("Hook #{d} registered: {s} → {s}\n", .{ id, pattern.?, cmd_display.items });
    } else if (std.mem.eql(u8, sub, "list")) {
        var c = try ever.client.Client.connect(allocator, io, address, port);
        defer c.deinit();
        var result = try c.listHooks();
        defer result.deinit();

        if (result.hooks.len == 0) {
            std.debug.print("No hooks registered.\n", .{});
        } else {
            std.debug.print("{s:<4} {s:<25} {s:<30} {s:<12}\n", .{ "ID", "Pattern", "Command", "Last Offset" });
            for (result.hooks) |hook| {
                var cmd_display: std.ArrayList(u8) = .empty;
                defer cmd_display.deinit(allocator);
                for (hook.command, 0..) |arg, j| {
                    if (j > 0) try cmd_display.append(allocator, ' ');
                    try cmd_display.appendSlice(allocator, arg);
                }

                var id_buf: [20]u8 = undefined;
                const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{hook.id}) catch "?";
                var cursor_buf: [20]u8 = undefined;
                const cursor_str = std.fmt.bufPrint(&cursor_buf, "{d}", .{hook.cursor}) catch "?";

                std.debug.print("{s:<4} {s:<25} {s:<30} {s:<12}\n", .{
                    id_str,
                    hook.pattern,
                    cmd_display.items,
                    cursor_str,
                });
            }
        }
    } else if (std.mem.eql(u8, sub, "rm")) {
        if (args.len < 2) std.process.fatal("usage: ever hook rm <id>", .{});
        const id = std.fmt.parseInt(u64, args[1], 10) catch
            std.process.fatal("invalid hook ID '{s}'.", .{args[1]});

        var c = try ever.client.Client.connect(allocator, io, address, port);
        defer c.deinit();
        try c.removeHook(id);
        std.debug.print("Hook #{d} removed.\n", .{id});
    } else {
        std.process.fatal("unknown hook subcommand '{s}'. Use add, list, or rm.", .{sub});
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
        \\  on             Watch events and run command
        \\  hook add       Register a persistent server-side hook
        \\  hook list      List registered hooks
        \\  hook rm        Remove a hook by ID
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
