const std = @import("std");
const ever = @import("ever");
const cli = @import("cli.zig");

const Io = std.Io;
const Dir = Io.Dir;

/// Resolve store address from flags, env var, or default
pub fn parseStoreAddress(ctx: *const cli.Context, env_block: [*:null]const ?[*:0]const u8) struct {
    address: []const u8,
    port: u16,
} {
    const default_addr = "127.0.0.1";
    const default_port: u16 = 7890;

    const flag_addr = ctx.flag("address");
    const flag_port = ctx.flag("port");

    if (flag_addr.len > 0 and flag_port.len > 0) {
        return .{
            .address = flag_addr,
            .port = std.fmt.parseInt(u16, flag_port, 10) catch default_port,
        };
    }

    // Check EVER_ADDR env var
    var i: usize = 0;
    while (env_block[i]) |env_str| : (i += 1) {
        const len = std.mem.indexOfSentinel(u8, 0, env_str);
        if (std.mem.startsWith(u8, env_str[0..len], "EVER_ADDR=")) {
            const value = env_str[10..len];
            if (std.mem.indexOfScalar(u8, value, ':')) |colon_pos| {
                const addr = value[0..colon_pos];
                const port_str = value[colon_pos + 1 ..];
                const port = std.fmt.parseInt(u16, port_str, 10) catch default_port;
                return .{ .address = addr, .port = port };
            }
        }
    }

    return .{
        .address = if (flag_addr.len > 0) flag_addr else default_addr,
        .port = if (flag_port.len > 0) std.fmt.parseInt(u16, flag_port, 10) catch default_port else default_port,
    };
}

fn handlePub(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const topic = ctx.arg("topic");
    const data = ctx.arg("data");

    if (topic.len == 0) {
        std.debug.print("error: topic is required\n", .{});
        std.process.exit(1);
    }
    if (data.len == 0) {
        std.debug.print("error: data is required\n", .{});
        std.process.exit(1);
    }

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);
    var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    const offset = try c.publish(topic, null, data);
    std.debug.print("Published to {s} at offset {d}\n", .{ topic, offset });
}

fn handleSub(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const topic_name = ctx.arg("topic");
    if (topic_name.len == 0) {
        std.debug.print("error: topic is required\n", .{});
        std.process.exit(1);
    }

    const from_offset = ctx.flagInt(u64, "from");
    const max_count = ctx.flagInt(u32, "max");
    const follow = ctx.flagBool("follow");
    const json_values = ctx.flagBool("json-values");

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);

    var client = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
    defer client.deinit();

    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    if (follow) {
        var offset = from_offset;
        var did_initial = false;
        while (true) {
            var result = if (!did_initial)
                (if (is_pattern) try client.fetchPattern(topic_name, offset, max_count) else try client.fetch(topic_name, offset, max_count))
            else
                try client.fetchBlocking(
                    if (!is_pattern) topic_name else null,
                    if (is_pattern) topic_name else null,
                    offset,
                    100,
                    5000,
                );
            defer result.deinit();
            for (result.events) |event| printEvent(event, json_values);
            if (result.events.len > 0) offset += result.events.len;
            did_initial = true;
        }
    } else {
        var result = if (is_pattern) try client.fetchPattern(topic_name, from_offset, max_count) else try client.fetch(topic_name, from_offset, max_count);
        defer result.deinit();
        if (result.events.len == 0) {
            std.debug.print("No events.\n", .{});
        } else {
            for (result.events) |event| printEvent(event, json_values);
        }
    }
}

fn handleWait(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const topic_name = ctx.arg("topic");
    if (topic_name.len == 0) {
        std.debug.print("error: topic is required\n", .{});
        std.process.exit(1);
    }

    const count = ctx.flagInt(u32, "count");
    const timeout_secs = ctx.flagInt(u32, "timeout");
    const from_offset = ctx.flagInt(u64, "from");
    const json_values = ctx.flagBool("json-values");

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);

    var client = ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port) catch
        std.process.fatal("cannot connect to store at {s}:{d}.", .{ addr_info.address, addr_info.port });
    defer client.deinit();

    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    var collected: u32 = 0;
    var offset = from_offset;
    var elapsed_ms: u64 = 0;
    const timeout_ms: u64 = @as(u64, timeout_secs) * 1000;
    const block_interval_ms: u32 = 2000;

    while (collected < count) {
        if (timeout_secs > 0 and elapsed_ms >= timeout_ms) std.process.exit(1);
        const remaining_ms: u32 = if (timeout_secs > 0)
            @intCast(@min(block_interval_ms, timeout_ms - elapsed_ms))
        else
            block_interval_ms;
        var result = client.fetchBlocking(
            if (!is_pattern) topic_name else null,
            if (is_pattern) topic_name else null,
            offset,
            @min(100, count - collected),
            remaining_ms,
        ) catch std.process.fatal("fetch failed.", .{});
        defer result.deinit();
        for (result.events) |event| {
            printEvent(event, json_values);
            collected += 1;
        }
        if (result.events.len > 0) offset += result.events.len;
        elapsed_ms += remaining_ms;
    }
}

fn handleOn(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const envp = ctx.envp;

    const pattern = ctx.arg("pattern");
    const once = ctx.flagBool("once");
    const cmd = ctx.flag("cmd");

    if (pattern.len == 0) {
        std.debug.print("error: pattern is required\n", .{});
        std.process.exit(1);
    }
    if (cmd.len == 0) {
        std.debug.print("error: command is required after --\n", .{});
        std.process.exit(1);
    }

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);
    const is_pattern = std.mem.indexOfScalar(u8, pattern, '*') != null or
        (pattern.len > 0 and pattern[pattern.len - 1] == '.');

    var client = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
    defer client.deinit();

    var probe = if (is_pattern)
        try client.fetchBlocking(null, pattern, 0, 1_000_000, 0)
    else
        try client.fetchBlocking(pattern, null, 0, 1_000_000, 0);
    const next_offset: u64 = probe.events.len;
    probe.deinit();

    std.debug.print("Watching '{s}' from offset {d}...\n", .{ pattern, next_offset });

    var offset = next_offset;
    while (true) {
        var result = if (is_pattern)
            try client.fetchBlocking(null, pattern, offset, 100, 5000)
        else
            try client.fetchBlocking(pattern, null, offset, 100, 5000);
        defer result.deinit();

        if (result.events.len == 0) {
            if (once) break;
            continue;
        }

        for (result.events) |event| {
            const json = try buildEventJson(allocator, event);
            defer allocator.free(json);

            var offset_buf: [20]u8 = undefined;
            const offset_str = std.fmt.bufPrint(&offset_buf, "{d}", .{event.offset}) catch "0";
            var ts_buf: [20]u8 = undefined;
            const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{event.timestamp}) catch "0";

            var shell_cmd: std.ArrayList(u8) = .empty;
            defer shell_cmd.deinit(allocator);

            try shell_cmd.appendSlice(allocator, "EVER_TOPIC='");
            if (event.topic) |t| try appendShellEscaped(&shell_cmd, allocator, t);
            try shell_cmd.appendSlice(allocator, "' EVER_OFFSET='");
            try shell_cmd.appendSlice(allocator, offset_str);
            try shell_cmd.appendSlice(allocator, "' EVER_TIMESTAMP='");
            try shell_cmd.appendSlice(allocator, ts_str);
            try shell_cmd.appendSlice(allocator, "' EVER_KEY='");
            if (event.key) |k| try appendShellEscaped(&shell_cmd, allocator, k);
            try shell_cmd.appendSlice(allocator, "' exec ");

            var cmd_list: std.ArrayList([]const u8) = .empty;
            defer cmd_list.deinit(allocator);
            var cmd_iter = std.mem.splitScalar(u8, cmd, ' ');
            while (cmd_iter.next()) |arg| {
                if (arg.len > 0) try cmd_list.append(allocator, arg);
            }

            for (cmd_list.items) |arg| {
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

            const cmd_z = allocator.allocSentinel(u8, shell_cmd.items.len, 0) catch continue;
            defer allocator.free(cmd_z);
            @memcpy(cmd_z[0..shell_cmd.items.len], shell_cmd.items);

            var pipe_fds: [2]i32 = undefined;
            const pipe_rc = std.os.linux.pipe2(&pipe_fds, .{ .CLOEXEC = true });
            if (@as(isize, @bitCast(pipe_rc)) < 0) {
                std.debug.print("pipe2 failed\n", .{});
                continue;
            }

            const pid = std.os.linux.fork();
            const pid_i: isize = @bitCast(pid);

            if (pid_i < 0) {
                _ = std.os.linux.close(pipe_fds[0]);
                _ = std.os.linux.close(pipe_fds[1]);
                std.debug.print("fork failed\n", .{});
                continue;
            }

            if (pid_i == 0) {
                _ = std.os.linux.close(pipe_fds[1]);
                _ = std.os.linux.dup2(pipe_fds[0], 0);
                _ = std.os.linux.close(pipe_fds[0]);

                const sh: [*:0]const u8 = "/bin/sh";
                const dash_c: [*:0]const u8 = "-c";
                const argv = [_]?[*:0]const u8{ sh, dash_c, cmd_z.ptr, null };
                _ = std.os.linux.execve(sh, @ptrCast(&argv), envp);
                std.os.linux.exit(127);
            }

            _ = std.os.linux.close(pipe_fds[0]);
            writeAllFd(pipe_fds[1], json);
            _ = std.os.linux.close(pipe_fds[1]);

            var status: u32 = 0;
            _ = std.os.linux.waitpid(@intCast(pid), &status, 0);
            const exit_code = (status >> 8) & 0xFF;
            if (exit_code != 0) {
                std.debug.print("Command exited with status {d} for event on '{s}'\n", .{ exit_code, if (event.topic) |t| t else pattern });
            }
        }

        offset += result.events.len;
        if (once) break;
    }
}

fn handleTopic(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const sub = ctx.arg("subcommand");
    const name = ctx.arg("name");

    if (sub.len == 0) {
        std.debug.print("error: subcommand is required (create|list|delete)\n", .{});
        std.process.exit(1);
    }

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);

    if (std.mem.eql(u8, sub, "create")) {
        if (name.len == 0) {
            std.debug.print("error: topic name is required\n", .{});
            std.process.exit(1);
        }
        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        try c.createTopic(name);
        std.debug.print("Created topic: {s}\n", .{name});
    } else if (std.mem.eql(u8, sub, "list")) {
        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        const topics = try c.listTopics();
        defer {
            for (topics) |t| allocator.free(t);
            allocator.free(topics);
        }
        if (topics.len == 0) std.debug.print("No topics.\n", .{}) else for (topics) |t| std.debug.print("{s}\n", .{t});
    } else if (std.mem.eql(u8, sub, "delete")) {
        if (name.len == 0) {
            std.debug.print("error: topic name is required\n", .{});
            std.process.exit(1);
        }
        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        try c.deleteTopic(name);
        std.debug.print("Deleted topic: {s}\n", .{name});
    } else {
        std.debug.print("error: unknown topic subcommand\n", .{});
        std.process.exit(1);
    }
}

fn handleHook(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const sub = ctx.arg("subcommand");

    if (sub.len == 0) {
        std.debug.print("error: subcommand is required (add|list|rm)\n", .{});
        std.process.exit(1);
    }

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);

    if (std.mem.eql(u8, sub, "add")) {
        const pattern = ctx.arg("pattern");
        const once = ctx.flagBool("once");
        const cmd = ctx.flag("cmd");

        if (pattern.len == 0) {
            std.debug.print("error: pattern is required\n", .{});
            std.process.exit(1);
        }
        if (cmd.len == 0) {
            std.debug.print("error: command is required after --\n", .{});
            std.process.exit(1);
        }

        var cmd_list: std.ArrayList([]const u8) = .empty;
        defer cmd_list.deinit(allocator);
        var cmd_iter = std.mem.splitScalar(u8, cmd, ' ');
        while (cmd_iter.next()) |arg| {
            if (arg.len > 0) try cmd_list.append(allocator, arg);
        }

        var cwd_buf: [4096]u8 = undefined;
        const cwd_z: [*:0]const u8 = "/proc/self/cwd";
        const cwd_len = std.os.linux.readlink(cwd_z, &cwd_buf, cwd_buf.len);
        const cwd_i: isize = @bitCast(cwd_len);
        const cwd: []const u8 = if (cwd_i > 0) cwd_buf[0..@intCast(cwd_i)] else "/tmp";

        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        const id = try c.registerHookFull(pattern, cmd_list.items, cwd, once, null);

        var cmd_display: std.ArrayList(u8) = .empty;
        defer cmd_display.deinit(allocator);
        for (cmd_list.items, 0..) |arg, j| {
            if (j > 0) try cmd_display.append(allocator, ' ');
            try cmd_display.appendSlice(allocator, arg);
        }
        if (once) {
            std.debug.print("Hook #{d} registered (once): {s} → {s}\n", .{ id, pattern, cmd_display.items });
        } else {
            std.debug.print("Hook #{d} registered: {s} → {s}\n", .{ id, pattern, cmd_display.items });
        }
    } else if (std.mem.eql(u8, sub, "list")) {
        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
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
        const id_str = ctx.arg("id");
        if (id_str.len == 0) {
            std.debug.print("error: hook ID is required\n", .{});
            std.process.exit(1);
        }
        const id = std.fmt.parseInt(u64, id_str, 10) catch
            std.process.fatal("invalid hook ID '{s}'.", .{id_str});

        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        try c.removeHook(id);
        std.debug.print("Hook #{d} removed.\n", .{id});
    } else {
        std.debug.print("error: unknown hook subcommand\n", .{});
        std.process.exit(1);
    }
}

fn handleTimer(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const sub = ctx.arg("subcommand");

    if (sub.len == 0) {
        std.debug.print("error: subcommand is required (add|list|rm|info)\n", .{});
        std.process.exit(1);
    }

    const env_block = ctx.envp;
    const addr_info = parseStoreAddress(ctx, env_block);

    if (std.mem.eql(u8, sub, "add")) {
        const name = ctx.arg("name");
        const every = ctx.flag("every");
        const cron = ctx.flag("cron");
        const topic = ctx.arg("topic");
        const payload = ctx.arg("payload");
        const persistent = !ctx.flagBool("no-persist");

        if (name.len == 0) {
            std.debug.print("error: timer name is required\n", .{});
            std.process.exit(1);
        }
        if (every.len == 0 and cron.len == 0) {
            std.debug.print("error: either --every or --cron is required\n", .{});
            std.process.exit(1);
        }
        if (every.len > 0 and cron.len > 0) {
            std.debug.print("error: cannot specify both --every and --cron\n", .{});
            std.process.exit(1);
        }
        if (topic.len == 0) {
            std.debug.print("error: topic is required\n", .{});
            std.process.exit(1);
        }

        const schedule_type = if (every.len > 0) "interval" else "cron";
        const schedule_value = if (every.len > 0) every else cron;
        const actual_payload = if (payload.len > 0) payload else "{}";

        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        try c.addTimer(name, schedule_type, schedule_value, topic, actual_payload, persistent);

        std.debug.print("Timer '{s}' registered: {s} {s} → {s}\n", .{ name, if (every.len > 0) "every" else "cron", schedule_value, topic });
    } else if (std.mem.eql(u8, sub, "list")) {
        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        var result = try c.listTimers();
        defer result.deinit();

        if (result.timers.len == 0) {
            std.debug.print("No timers registered.\n", .{});
        } else {
            std.debug.print("{s:<20} {s:<15} {s:<25} {s:<10}\n", .{ "NAME", "SCHEDULE", "TOPIC", "FIRES" });
            for (result.timers) |timer| {
                std.debug.print("{s:<20} {s:<15} {s:<25} {d:<10}\n", .{
                    timer.name,
                    timer.schedule,
                    timer.topic,
                    timer.fire_count,
                });
            }
        }
    } else if (std.mem.eql(u8, sub, "rm")) {
        const name = ctx.arg("name");
        if (name.len == 0) {
            std.debug.print("error: timer name is required\n", .{});
            std.process.exit(1);
        }

        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        try c.removeTimer(name);
        std.debug.print("Timer '{s}' removed.\n", .{name});
    } else if (std.mem.eql(u8, sub, "info")) {
        const name = ctx.arg("name");
        if (name.len == 0) {
            std.debug.print("error: timer name is required\n", .{});
            std.process.exit(1);
        }

        var c = try ever.client.Client.connect(allocator, io, addr_info.address, addr_info.port);
        defer c.deinit();
        const timer = try c.timerInfo(name);
        defer {
            allocator.free(timer.name);
            allocator.free(timer.schedule);
            allocator.free(timer.topic);
            allocator.free(timer.payload);
        }

        std.debug.print("Name:        {s}\n", .{timer.name});
        std.debug.print("Schedule:    {s}\n", .{timer.schedule});
        std.debug.print("Topic:       {s}\n", .{timer.topic});
        std.debug.print("Payload:     {s}\n", .{timer.payload});
        std.debug.print("Last fired:  ", .{});
        if (timer.last_fired_at > 0) {
            std.debug.print("{d}\n", .{timer.last_fired_at});
        } else {
            std.debug.print("never\n", .{});
        }
        std.debug.print("Next fire:   (calculated on server)\n", .{});
        std.debug.print("Fire count:  {d}\n", .{timer.fire_count});
        std.debug.print("Persistent:  {s}\n", .{if (timer.persistent) "yes" else "no"});
    } else {
        std.debug.print("error: unknown timer subcommand\n", .{});
        std.process.exit(1);
    }
}

fn handleStore(ctx: *const cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const envp = ctx.envp;

    // Extract subcommand from command_name (e.g., "store start" -> "start")
    const sub = if (std.mem.indexOfScalar(u8, ctx.command_name, ' ')) |space|
        ctx.command_name[space + 1 ..]
    else
        ctx.arg("subcommand");

    if (sub.len == 0) {
        std.debug.print("error: subcommand is required (start)\n", .{});
        std.process.exit(1);
    }

    if (std.mem.eql(u8, sub, "start")) {
        try startServer(allocator, io, ctx, envp);
    } else if (std.mem.eql(u8, sub, "status")) {
        const data_dir = ctx.flag("data-dir");
        const json_output = ctx.flagBool("json");
        const actual_dir = if (data_dir.len > 0) data_dir else "./data";

        var store_status = ever.status.getStatus(allocator, io, actual_dir) catch |err| {
            std.process.fatal("failed to get store status: {}", .{err});
        };
        defer store_status.deinit(allocator);

        if (json_output) {
            ever.status.printJson(&store_status, allocator);
        } else {
            ever.status.printHuman(&store_status);
        }
    } else {
        std.debug.print("error: unknown store subcommand\n", .{});
        std.process.exit(1);
    }
}

fn startServer(allocator: std.mem.Allocator, io: Io, ctx: *const cli.Context, envp: [*:null]const ?[*:0]const u8) !void {
    const address = ctx.flag("address");
    const port_str = ctx.flag("port");
    const data_dir = ctx.flag("data-dir");

    const port = std.fmt.parseInt(u16, port_str, 10) catch 7890;
    const actual_data_dir = if (data_dir.len > 0) data_dir else "./data";

    const dir = Dir.cwd().createDirPathOpen(io, actual_data_dir, .{ .open_options = .{ .iterate = true } }) catch
        std.process.fatal("cannot open data directory '{s}'.", .{actual_data_dir});

    const lock_file = dir.createFile(io, "ever.lock", .{ .read = true, .truncate = false }) catch
        std.process.fatal("cannot create lockfile in '{s}'.", .{actual_data_dir});
    defer lock_file.close(io);

    const LOCK_EX = 2;
    const LOCK_NB = 4;
    const lock_result = std.os.linux.flock(lock_file.handle, LOCK_EX | LOCK_NB);
    if (lock_result != 0) {
        std.process.fatal("another Ever store is already running on '{s}'. Remove ever.lock if stale.", .{actual_data_dir});
    }

    var topic_manager = try ever.topic.TopicManager.init(allocator, io, dir, .{});
    defer topic_manager.deinit();

    var hook_table = try ever.hooks.HookTable.init(allocator, actual_data_dir);
    defer hook_table.deinit();

    var timer_table = try ever.timers.TimerTable.init(allocator, actual_data_dir);
    defer timer_table.deinit();

    var server = try ever.net.Server.init(allocator, io, &topic_manager, .{ .address = if (address.len > 0) address else "127.0.0.1", .port = port });
    defer server.deinit();
    server.setHookTable(&hook_table);
    server.setTimerTable(&timer_table);

    var hook_daemon = ever.hooks.HookDaemon.init(allocator, &hook_table, &topic_manager, envp);
    hook_daemon.start() catch |err| {
        std.debug.print("Warning: failed to start hook daemon: {}\n", .{err});
    };
    defer hook_daemon.stop();

    var timer_daemon = ever.timers.TimerDaemon.init(allocator, &timer_table, &topic_manager);
    timer_daemon.start() catch |err| {
        std.debug.print("Warning: failed to start timer daemon: {}\n", .{err});
    };
    defer timer_daemon.stop();

    // Start HTTP server unless --no-http
    var http_server_inst: ?ever.http.HttpServer = null;
    defer if (http_server_inst) |*hs| hs.deinit();

    const http_port_str = ctx.flag("http-port");
    const no_http = ctx.flagBool("no-http");
    const http_port = std.fmt.parseInt(u16, http_port_str, 10) catch 8890;

    if (!no_http) {
        http_server_inst = ever.http.HttpServer.init(allocator, io, &topic_manager, .{
            .address = if (address.len > 0) address else "127.0.0.1",
            .port = http_port,
        });
        if (std.Thread.spawn(.{}, struct {
            fn run(hs: *ever.http.HttpServer) void {
                hs.run() catch |err| {
                    std.debug.print("HTTP server failed: {}\n", .{err});
                };
            }
        }.run, .{&http_server_inst.?})) |http_thread| {
            http_thread.detach();
        } else |_| {
            std.debug.print("Warning: failed to start HTTP server thread.\n", .{});
            http_server_inst = null;
        }
    }

    std.debug.print("Ever store listening on {s}:{d}\n", .{ if (address.len > 0) address else "127.0.0.1", port });
    if (!no_http) {
        std.debug.print("HTTP API listening on {s}:{d}\n", .{ if (address.len > 0) address else "127.0.0.1", http_port });
    }
    std.debug.print("Data directory: {s}\n", .{actual_data_dir});
    std.debug.print("Hook daemon started.\n", .{});
    std.debug.print("Timer daemon started.\n", .{});
    std.debug.print("Press Ctrl-C to stop.\n", .{});
    server.installSignalHandlers();
    server.run() catch |err| {
        if (!server.shutdown_requested.load(.acquire))
            std.process.fatal("server failed: {}", .{err});
    };
    server.shutdown();
    std.debug.print("Server shut down gracefully.\n", .{});
}

fn printEvent(event: ever.client.Event, json_values: bool) void {
    if (json_values) {
        std.debug.print("{s}\n", .{event.value});
    } else {
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

fn writeAllFd(fd: i32, data: []const u8) void {
    var written: usize = 0;
    while (written < data.len) {
        const rc = std.os.linux.write(fd, data[written..].ptr, data[written..].len);
        const n: isize = @bitCast(rc);
        if (n <= 0) break;
        written += @intCast(n);
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
                var buf: [6]u8 = undefined;
                _ = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
                try json.appendSlice(allocator, &buf);
            } else {
                try json.append(allocator, c);
            }
        },
    };
}

fn appendShellEscaped(list: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| {
        if (c == '\'') {
            try list.appendSlice(allocator, "'\\''");
        } else {
            try list.append(allocator, c);
        }
    }
}

const app = cli.App{
    .name = "ever",
    .description = "lightweight event storage",
    .version = "0.1.0",
    .help_sections = &.{
        .{ .title = "Core", .commands = &.{ "store start", "store status", "version", "help" } },
        .{ .title = "Topics", .commands = &.{ "topic create", "topic list", "topic delete" } },
        .{ .title = "Events", .commands = &.{ "pub", "sub", "wait", "on" } },
        .{ .title = "Hooks", .commands = &.{ "hook add", "hook list", "hook rm" } },
        .{ .title = "Timers", .commands = &.{ "timer add", "timer list", "timer rm", "timer info" } },
    },
    .commands = &.{
        .{
            .name = "pub",
            .description = "Publish an event",
            .args = &.{
                .{ .name = "topic", .required = true, .description = "Topic name" },
                .{ .name = "data", .required = true, .description = "JSON event data" },
            },
            .flags = &.{
                .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
            },
            .run = handlePub,
        },
        .{
            .name = "sub",
            .description = "Subscribe to events",
            .args = &.{
                .{ .name = "topic", .required = true, .description = "Topic name or pattern" },
            },
            .flags = &.{
                .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                .{ .name = "from", .default = "0", .description = "Start offset" },
                .{ .name = "max", .default = "100", .description = "Max events" },
                .{ .name = "follow", .description = "Follow new events" },
                .{ .name = "json-values", .description = "Print only JSON values" },
            },
            .run = handleSub,
        },
        .{
            .name = "wait",
            .description = "Block until events arrive",
            .args = &.{
                .{ .name = "topic", .required = true, .description = "Topic name or pattern" },
            },
            .flags = &.{
                .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                .{ .name = "count", .default = "1", .description = "Events to wait for" },
                .{ .name = "timeout", .default = "0", .description = "Timeout in seconds" },
                .{ .name = "from", .default = "0", .description = "Start offset" },
                .{ .name = "json-values", .description = "Print only JSON values" },
            },
            .run = handleWait,
        },
        .{
            .name = "on",
            .description = "Watch events and run command",
            .args = &.{
                .{ .name = "pattern", .required = true, .description = "Topic pattern" },
            },
            .flags = &.{
                .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                .{ .name = "once", .description = "Exit after first event" },
                .{ .name = "cmd", .description = "Command to execute" },
            },
            .run = handleOn,
        },
        .{
            .name = "topic",
            .description = "Manage topics",
            .subcommands = &.{
                .{
                    .name = "create",
                    .description = "Create a new topic",
                    .args = &.{.{ .name = "name", .required = true, .description = "Topic name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTopic,
                },
                .{
                    .name = "list",
                    .description = "List all topics",
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTopic,
                },
                .{
                    .name = "delete",
                    .description = "Delete a topic",
                    .args = &.{.{ .name = "name", .required = true, .description = "Topic name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTopic,
                },
            },
        },
        .{
            .name = "hook",
            .description = "Manage server-side hooks",
            .subcommands = &.{
                .{
                    .name = "add",
                    .description = "Register a server-side hook",
                    .args = &.{.{ .name = "pattern", .required = true, .description = "Topic pattern" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                        .{ .name = "once", .description = "Remove after first trigger" },
                        .{ .name = "cmd", .description = "Command to execute" },
                    },
                    .run = handleHook,
                },
                .{
                    .name = "list",
                    .description = "List registered hooks",
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleHook,
                },
                .{
                    .name = "rm",
                    .description = "Remove a hook by ID",
                    .args = &.{.{ .name = "id", .required = true, .description = "Hook ID" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleHook,
                },
            },
        },
        .{
            .name = "store",
            .description = "Manage the storage server",
            .subcommands = &.{
                .{
                    .name = "start",
                    .description = "Start the storage server",
                    .flags = &.{
                        .{ .name = "address", .default = "127.0.0.1", .description = "Bind address" },
                        .{ .name = "port", .default = "7890", .description = "Bind port" },
                        .{ .name = "data-dir", .default = "./data", .description = "Data directory" },
                        .{ .name = "http-port", .default = "8890", .description = "HTTP API port" },
                        .{ .name = "no-http", .description = "Disable HTTP server" },
                    },
                    .run = handleStore,
                },
                .{
                    .name = "status",
                    .description = "Show store statistics",
                    .flags = &.{
                        .{ .name = "data-dir", .default = "./data", .description = "Data directory" },
                        .{ .name = "json", .description = "Output as JSON" },
                    },
                    .run = handleStore,
                },
            },
        },
        .{
            .name = "timer",
            .description = "Manage recurring timers",
            .subcommands = &.{
                .{
                    .name = "add",
                    .description = "Add a recurring timer",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                        .{ .name = "every", .description = "Interval (e.g., 5s, 1m)" },
                        .{ .name = "cron", .description = "Cron expression" },
                        .{ .name = "cmd", .description = "Command to execute" },
                    },
                    .run = handleTimer,
                },
                .{
                    .name = "list",
                    .description = "List registered timers",
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTimer,
                },
                .{
                    .name = "rm",
                    .description = "Remove a timer by name",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTimer,
                },
                .{
                    .name = "info",
                    .description = "Show timer details",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTimer,
                },
            },
        },
        .{
            .name = "timer",
            .description = "Manage scheduled timers",
            .subcommands = &.{
                .{
                    .name = "add",
                    .description = "Add a new timer",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                        .{ .name = "every", .description = "Interval (e.g., 5m, 1h, 1d)" },
                        .{ .name = "cron", .description = "Cron expression (e.g., \"0 3 * * *\")" },
                        .{ .name = "topic", .description = "Topic to publish to" },
                        .{ .name = "payload", .description = "JSON payload" },
                        .{ .name = "no-persist", .description = "Don't persist timer" },
                    },
                    .run = handleTimer,
                },
                .{
                    .name = "list",
                    .description = "List all timers",
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTimer,
                },
                .{
                    .name = "rm",
                    .description = "Remove a timer by name",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTimer,
                },
                .{
                    .name = "info",
                    .description = "Show timer details",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .flags = &.{
                        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .description = "Store address" },
                        .{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" },
                    },
                    .run = handleTimer,
                },
            },
        },
    },
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_list: std.ArrayList([]const u8) = .empty;
    defer args_list.deinit(allocator);

    while (args_iter.next()) |arg| {
        try args_list.append(allocator, arg);
    }

    const env_block = init.minimal.environ.block.slice.ptr;
    try app.run(allocator, io, args_list.items, env_block);
}

test "main module compiles" {
    _ = ever;
    _ = cli;
}
