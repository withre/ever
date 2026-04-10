const std = @import("std");
const ever = @import("ever");
const cli = @import("cli.zig");
const readline = @import("readline.zig");

const Io = std.Io;
const Dir = Io.Dir;

/// Resolve store address from context flags.
/// With global flags + env binding, priority is already: flag > env > default.
pub fn parseStoreAddress(ctx: *const cli.Context) struct {
    address: []const u8,
    port: u16,
} {
    const addr = ctx.flag("address");
    const port_str = ctx.flag("port");
    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        std.debug.print("error: invalid port '{s}'\n", .{port_str});
        std.process.exit(1);
    };
    return .{
        .address = if (addr.len > 0) addr else "127.0.0.1",
        .port = port,
    };
}

/// Connect to the store, printing a clean error and exiting on failure.
fn connectToStore(allocator: std.mem.Allocator, io: std.Io, addr: []const u8, port: u16) ever.client.Client {
    return ever.client.Client.connect(allocator, io, addr, port) catch {
        std.debug.print("error: cannot connect to store at {s}:{d}\n", .{ addr, port });
        std.process.exit(1);
    };
}

fn handlePub(ctx: *cli.Context) !void {
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

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    const offset = c.publish(topic, null, data) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    std.debug.print("Published to {s} at offset {d}\n", .{ topic, offset });
}

fn handleSub(ctx: *cli.Context) !void {
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

    const addr_info = parseStoreAddress(ctx);

    var client = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer client.deinit();

    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    if (follow) {
        var offset = from_offset;
        var did_initial = false;
        var emitted: u64 = 0;
        while (true) {
            const remaining: u32 = if (max_count > emitted) @intCast(max_count - emitted) else 0;
            if (remaining == 0 and did_initial) break;
            const fetch_count: u32 = if (remaining > 0) remaining else max_count;
            var result = if (!did_initial)
                (if (is_pattern) client.fetchPattern(topic_name, offset, fetch_count) else client.fetch(topic_name, offset, fetch_count)) catch |err| switch (err) {
                    error.ServerError => std.process.exit(1),
                    else => return err,
                }
            else
                client.fetchBlocking(
                    if (!is_pattern) topic_name else null,
                    if (is_pattern) topic_name else null,
                    offset,
                    @min(100, fetch_count),
                    5000,
                ) catch |err| switch (err) {
                    error.ServerError => std.process.exit(1),
                    else => return err,
                };
            defer result.deinit();
            for (result.events) |event| {
                printEvent(event, json_values);
                emitted += 1;
                if (emitted >= max_count) break;
            }
            if (result.events.len > 0) offset += result.events.len;
            did_initial = true;
            if (emitted >= max_count) break;
        }
    } else {
        var result = (if (is_pattern) client.fetchPattern(topic_name, from_offset, max_count) else client.fetch(topic_name, from_offset, max_count)) catch |err| switch (err) {
            error.ServerError => std.process.exit(1),
            else => return err,
        };
        defer result.deinit();
        if (result.events.len == 0) {
            std.debug.print("No events.\n", .{});
        } else {
            for (result.events) |event| printEvent(event, json_values);
        }
    }
}

fn handleWait(ctx: *cli.Context) !void {
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

    const addr_info = parseStoreAddress(ctx);

    var client = connectToStore(allocator, io, addr_info.address, addr_info.port);
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

fn handleOn(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const envp = ctx.envp;

    const pattern = ctx.arg("pattern");
    const once = ctx.flagBool("once");
    const rest_args = ctx.rest();

    if (pattern.len == 0) {
        std.debug.print("error: pattern is required\n", .{});
        std.process.exit(1);
    }
    if (rest_args.len == 0) {
        std.debug.print("error: command is required after --\n", .{});
        std.process.exit(1);
    }

    const addr_info = parseStoreAddress(ctx);
    const is_pattern = std.mem.indexOfScalar(u8, pattern, '*') != null or
        (pattern.len > 0 and pattern[pattern.len - 1] == '.');

    var client = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer client.deinit();

    var probe = (if (is_pattern)
        client.fetchBlocking(null, pattern, 0, 1_000_000, 0)
    else
        client.fetchBlocking(pattern, null, 0, 1_000_000, 0)) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    const next_offset: u64 = probe.events.len;
    probe.deinit();

    std.debug.print("Watching '{s}' from offset {d}...\n", .{ pattern, next_offset });

    var offset = next_offset;
    while (true) {
        var result = (if (is_pattern)
            client.fetchBlocking(null, pattern, offset, 100, 5000)
        else
            client.fetchBlocking(pattern, null, offset, 100, 5000)) catch |err| switch (err) {
            error.ServerError => std.process.exit(1),
            else => return err,
        };
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

            for (rest_args) |arg| {
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

fn handleTopicCreate(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const name = ctx.arg("name");

    if (name.len == 0) {
        std.debug.print("error: topic name is required\n", .{});
        std.process.exit(1);
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    c.createTopic(name) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    std.debug.print("Created topic: {s}\n", .{name});
}

fn handleTopicList(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    const topics = c.listTopics() catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    defer {
        for (topics) |t| allocator.free(t);
        allocator.free(topics);
    }
    if (topics.len == 0) std.debug.print("No topics.\n", .{}) else for (topics) |t| std.debug.print("{s}\n", .{t});
}

fn handleTopicDelete(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const name = ctx.arg("name");

    if (name.len == 0) {
        std.debug.print("error: topic name is required\n", .{});
        std.process.exit(1);
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    c.deleteTopic(name) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    std.debug.print("Deleted topic: {s}\n", .{name});
}

fn handleHookAdd(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const pattern = ctx.arg("pattern");
    const once = ctx.flagBool("once");
    const rest_args = ctx.rest();

    if (pattern.len == 0) {
        std.debug.print("error: pattern is required\n", .{});
        std.process.exit(1);
    }
    if (rest_args.len == 0) {
        std.debug.print("error: command is required after --\n", .{});
        std.process.exit(1);
    }

    var cwd_buf: [4096]u8 = undefined;
    const cwd_z: [*:0]const u8 = "/proc/self/cwd";
    const cwd_len = std.os.linux.readlink(cwd_z, &cwd_buf, cwd_buf.len);
    const cwd_i: isize = @bitCast(cwd_len);
    const cwd: []const u8 = if (cwd_i > 0) cwd_buf[0..@intCast(cwd_i)] else "/tmp";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    const id = c.registerHookFull(pattern, rest_args, cwd, once, null) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };

    var cmd_display: std.ArrayList(u8) = .empty;
    defer cmd_display.deinit(allocator);
    for (rest_args, 0..) |arg, j| {
        if (j > 0) try cmd_display.append(allocator, ' ');
        try cmd_display.appendSlice(allocator, arg);
    }
    if (once) {
        std.debug.print("Hook #{d} registered (once): {s} → {s}\n", .{ id, pattern, cmd_display.items });
    } else {
        std.debug.print("Hook #{d} registered: {s} → {s}\n", .{ id, pattern, cmd_display.items });
    }
}

fn handleHookList(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.listHooks() catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
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
}

fn handleHookPs(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.hookPs() catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    defer result.deinit();

    if (result.processes.len == 0) {
        std.debug.print("No running hook processes.\n", .{});
    } else {
        std.debug.print("{s:<6} {s:<8} {s:<25} {s:<30} {s:<12}\n", .{ "HOOK", "PID", "PATTERN", "COMMAND", "ELAPSED" });
        const now = getMilliTimestamp();
        for (result.processes) |p| {
            var hook_buf: [20]u8 = undefined;
            const hook_str = std.fmt.bufPrint(&hook_buf, "#{d}", .{p.hook_id}) catch "?";
            var pid_buf: [20]u8 = undefined;
            const pid_str = std.fmt.bufPrint(&pid_buf, "{d}", .{p.pid}) catch "?";
            const elapsed_s = @divTrunc(now - p.start_time, 1000);
            var elapsed_buf: [20]u8 = undefined;
            const elapsed_str = std.fmt.bufPrint(&elapsed_buf, "{d}s", .{elapsed_s}) catch "?";
            std.debug.print("{s:<6} {s:<8} {s:<25} {s:<30} {s:<12}\n", .{
                hook_str, pid_str, p.pattern, p.command, elapsed_str,
            });
        }
    }
}

fn handleHookLogs(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const id_str = ctx.arg("id");

    if (id_str.len == 0) {
        std.debug.print("error: hook ID is required\n", .{});
        std.process.exit(1);
    }
    const id = std.fmt.parseInt(u64, id_str, 10) catch
        std.process.fatal("invalid hook ID '{s}'.", .{id_str});

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.hookLogs(id) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    defer result.deinit();

    std.debug.print("Log file: {s}\n---\n", .{result.log_path});
    if (result.content.len > 0) {
        std.debug.print("{s}", .{result.content});
        // Ensure trailing newline
        if (result.content[result.content.len - 1] != '\n') {
            std.debug.print("\n", .{});
        }
    } else {
        std.debug.print("(empty)\n", .{});
    }
}

fn handleHookRm(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const id_str = ctx.arg("id");

    if (id_str.len == 0) {
        std.debug.print("error: hook ID is required\n", .{});
        std.process.exit(1);
    }
    const id = std.fmt.parseInt(u64, id_str, 10) catch
        std.process.fatal("invalid hook ID '{s}'.", .{id_str});

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    c.removeHook(id) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    std.debug.print("Hook #{d} removed.\n", .{id});
}

fn handleTimerAdd(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const name = ctx.arg("name");
    const every = ctx.flag("every");
    const cron = ctx.flag("cron");
    const in_flag = ctx.flag("in");
    const topic = ctx.arg("topic");
    const payload = ctx.arg("payload");
    const persistent = !ctx.flagBool("no-persist");

    if (name.len == 0) {
        std.debug.print("error: timer name is required\n", .{});
        std.process.exit(1);
    }

    // Count how many schedule flags are set (use hasFlag to detect even empty values)
    const has_every = ctx.hasFlag("every") and every.len > 0;
    const has_cron = ctx.hasFlag("cron");
    const has_in = ctx.hasFlag("in") and in_flag.len > 0;
    const schedule_count = @as(u8, if (has_every) 1 else 0) + @as(u8, if (has_cron) 1 else 0) + @as(u8, if (has_in) 1 else 0);

    if (schedule_count == 0) {
        std.debug.print("error: one of --every, --cron, or --in is required\n", .{});
        std.process.exit(1);
    }
    if (schedule_count > 1) {
        std.debug.print("error: --every, --cron, and --in are mutually exclusive\n", .{});
        std.process.exit(1);
    }
    // Validate non-empty for cron if flag was provided
    if (has_cron and cron.len == 0) {
        std.debug.print("error: invalid cron expression\n", .{});
        std.process.exit(1);
    }
    if (topic.len == 0) {
        std.debug.print("error: topic is required\n", .{});
        std.process.exit(1);
    }

    const schedule_type: []const u8 = if (has_in) "one_shot" else if (has_every) "interval" else "cron";
    const schedule_value = if (has_in) in_flag else if (has_every) every else cron;
    const actual_payload = if (payload.len > 0) payload else "{}";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    c.addTimer(name, schedule_type, schedule_value, topic, actual_payload, if (has_in) false else persistent) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };

    if (has_in) {
        std.debug.print("Timer '{s}' registered (one-shot): in {s} → {s}\n", .{ name, in_flag, topic });
    } else {
        std.debug.print("Timer '{s}' registered: {s} {s} → {s}\n", .{ name, if (has_every) "every" else "cron", schedule_value, topic });
    }
}

fn handleTimerList(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.listTimers() catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
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
}

fn handleTimerRm(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const name = ctx.arg("name");

    if (name.len == 0) {
        std.debug.print("error: timer name is required\n", .{});
        std.process.exit(1);
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    c.removeTimer(name) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
    std.debug.print("Timer '{s}' removed.\n", .{name});
}

fn handleTimerInfo(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const name = ctx.arg("name");

    if (name.len == 0) {
        std.debug.print("error: timer name is required\n", .{});
        std.process.exit(1);
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    const timer = c.timerInfo(name) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };
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
}

fn handleTimerNew(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    // Prompt: Timer name
    std.debug.print("Timer name: ", .{});
    const name = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(name);
    if (name.len == 0) {
        std.debug.print("error: timer name is required\n", .{});
        std.process.exit(1);
    }

    // Prompt: Schedule type
    std.debug.print("Schedule type (every/cron/in): ", .{});
    const sched_type_input = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(sched_type_input);

    const is_every = std.mem.eql(u8, sched_type_input, "every");
    const is_cron = std.mem.eql(u8, sched_type_input, "cron");
    const is_in = std.mem.eql(u8, sched_type_input, "in");
    if (!is_every and !is_cron and !is_in) {
        std.debug.print("error: schedule type must be 'every', 'cron', or 'in'\n", .{});
        std.process.exit(1);
    }

    // Prompt: Schedule value
    if (is_every) {
        std.debug.print("Interval: ", .{});
    } else if (is_cron) {
        std.debug.print("Cron expression: ", .{});
    } else {
        std.debug.print("Delay: ", .{});
    }
    const sched_value = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(sched_value);
    if (sched_value.len == 0) {
        std.debug.print("error: schedule value is required\n", .{});
        std.process.exit(1);
    }

    // Prompt: Topic
    std.debug.print("Topic: ", .{});
    const topic = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(topic);
    if (topic.len == 0) {
        std.debug.print("error: topic is required\n", .{});
        std.process.exit(1);
    }

    // Prompt: Payload
    std.debug.print("Payload [{{}}]: ", .{});
    const payload_input = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(payload_input);
    const payload = if (payload_input.len > 0) payload_input else "{}";

    // Prompt: Persistent (only for non-one-shot)
    var persistent = true;
    if (!is_in) {
        std.debug.print("Persistent (y/n) [y]: ", .{});
        const persist_input = readLine(allocator) catch {
            std.debug.print("\nerror: failed to read input\n", .{});
            std.process.exit(1);
        };
        defer allocator.free(persist_input);
        if (std.mem.eql(u8, persist_input, "n") or std.mem.eql(u8, persist_input, "N")) {
            persistent = false;
        }
    }

    const schedule_type: []const u8 = if (is_in) "one_shot" else if (is_every) "interval" else "cron";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    c.addTimer(name, schedule_type, sched_value, topic, payload, if (is_in) false else persistent) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };

    std.debug.print("\n", .{});
    if (is_in) {
        std.debug.print("Timer '{s}' registered: in {s} \u{2192} {s}\n", .{ name, sched_value, topic });
    } else {
        std.debug.print("Timer '{s}' registered: {s} {s} \u{2192} {s}\n", .{ name, sched_type_input, sched_value, topic });
    }
}

fn handleHookNew(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;

    // Prompt: Topic pattern
    std.debug.print("Topic pattern: ", .{});
    const pattern = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(pattern);
    if (pattern.len == 0) {
        std.debug.print("error: topic pattern is required\n", .{});
        std.process.exit(1);
    }

    // Prompt: Command
    std.debug.print("Command: ", .{});
    const cmd_input = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(cmd_input);
    if (cmd_input.len == 0) {
        std.debug.print("error: command is required\n", .{});
        std.process.exit(1);
    }

    // Prompt: One-shot
    std.debug.print("One-shot (y/n) [n]: ", .{});
    const once_input = readLine(allocator) catch {
        std.debug.print("\nerror: failed to read input\n", .{});
        std.process.exit(1);
    };
    defer allocator.free(once_input);
    const once = std.mem.eql(u8, once_input, "y") or std.mem.eql(u8, once_input, "Y");

    // Parse command into args
    var cmd_list: std.ArrayList([]const u8) = .empty;
    defer cmd_list.deinit(allocator);
    var cmd_iter = std.mem.splitScalar(u8, cmd_input, ' ');
    while (cmd_iter.next()) |arg| {
        if (arg.len > 0) try cmd_list.append(allocator, arg);
    }

    // Get cwd
    var cwd_buf: [4096]u8 = undefined;
    const cwd_z: [*:0]const u8 = "/proc/self/cwd";
    const cwd_len = std.os.linux.readlink(cwd_z, &cwd_buf, cwd_buf.len);
    const cwd_i: isize = @bitCast(cwd_len);
    const cwd: []const u8 = if (cwd_i > 0) cwd_buf[0..@intCast(cwd_i)] else "/tmp";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, io, addr_info.address, addr_info.port);
    defer c.deinit();
    const id = c.registerHookFull(pattern, cmd_list.items, cwd, once, null) catch |err| switch (err) {
        error.ServerError => std.process.exit(1),
        else => return err,
    };

    std.debug.print("\n", .{});
    if (once) {
        std.debug.print("Hook #{d} registered (once): {s} \u{2192} {s}\n", .{ id, pattern, cmd_input });
    } else {
        std.debug.print("Hook #{d} registered: {s} \u{2192} {s}\n", .{ id, pattern, cmd_input });
    }
}

fn readLine(allocator: std.mem.Allocator) ![]u8 {
    return readline.readLine(allocator);
}

fn handleStart(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;
    const io = ctx.io;
    const envp = ctx.envp;
    try startServer(allocator, io, ctx, envp);
}

fn handleStatus(ctx: *cli.Context) !void {
    const allocator = ctx.allocator;

    const data_dir = ctx.flag("data-dir");
    const json_output = ctx.flagBool("json");
    const actual_dir = if (data_dir.len > 0) data_dir else "./data";

    // Validate directory looks like an Ever store
    {
        const dir = std.Io.Dir.cwd().openDir(ctx.io, actual_dir, .{ .iterate = true }) catch {
            std.debug.print("error: cannot open data directory '{s}'\n", .{actual_dir});
            std.process.exit(1);
        };
        defer dir.close(ctx.io);
        var found_store_marker = false;
        var iter = dir.iterate();
        while (iter.next(ctx.io) catch null) |entry| {
            if (entry.kind != .file) continue;
            if (std.mem.endsWith(u8, entry.name, ".log") or
                std.mem.eql(u8, entry.name, "ever.lock") or
                std.mem.eql(u8, entry.name, "hooks.json") or
                std.mem.eql(u8, entry.name, "timers.json"))
            {
                found_store_marker = true;
                break;
            }
        }
        if (!found_store_marker) {
            std.debug.print("error: '{s}' does not appear to be an Ever data directory\n", .{actual_dir});
            std.process.exit(1);
        }
    }

    var store_status = ever.status.getStatus(allocator, ctx.io, actual_dir) catch |err| {
        std.process.fatal("failed to get store status: {}", .{err});
    };
    defer store_status.deinit(allocator);

    if (json_output) {
        ever.status.printJson(&store_status, allocator);
    } else {
        ever.status.printHuman(&store_status);
    }
}

fn startServer(allocator: std.mem.Allocator, io: Io, ctx: *const cli.Context, envp: [*:null]const ?[*:0]const u8) !void {
    const address = ctx.flag("address");
    const port_str = ctx.flag("port");
    const data_dir = ctx.flag("data-dir");

    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        std.debug.print("error: invalid port '{s}'\n", .{port_str});
        std.process.exit(1);
    };
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
    server.setHookDaemon(&hook_daemon);
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
    const http_port = std.fmt.parseInt(u16, http_port_str, 10) catch {
        std.debug.print("error: invalid http-port '{s}'\n", .{http_port_str});
        std.process.exit(1);
    };

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

    const actual_addr = if (address.len > 0) address else "127.0.0.1";
    logTimestamped("Ever store starting...");
    logTimestampedFmt("TCP server listening on {s}:{d}", .{ actual_addr, port });
    if (!no_http) {
        logTimestampedFmt("HTTP API listening on {s}:{d}", .{ actual_addr, http_port });
    }
    logTimestampedFmt("Data directory: {s}", .{actual_data_dir});
    logTimestampedFmt("Hook daemon started ({d} hooks loaded)", .{hook_table.count()});
    logTimestampedFmt("Timer daemon started ({d} timers loaded)", .{timer_table.count()});
    logTimestamped("Ready. Press Ctrl-C to stop.");
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

// ── Timestamped logging ────────────────────────────────────────────────

fn getMilliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.REALTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

fn getTimestamp() struct { year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8 } {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.REALTIME, &ts);
    const secs: u64 = @intCast(ts.sec);

    const SECS_PER_DAY = 86400;
    const days = secs / SECS_PER_DAY;
    const day_secs = secs % SECS_PER_DAY;
    const hour: u8 = @intCast(day_secs / 3600);
    const minute: u8 = @intCast((day_secs % 3600) / 60);
    const second: u8 = @intCast(day_secs % 60);

    var y: u16 = 1970;
    var remaining = days;
    while (true) {
        const is_leap = (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0);
        const year_days: u64 = if (is_leap) 366 else 365;
        if (remaining < year_days) break;
        remaining -= year_days;
        y += 1;
    }
    const is_leap = (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0);
    const month_days = [12]u8{ 31, if (is_leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var m: u8 = 0;
    while (m < 12) : (m += 1) {
        if (remaining < month_days[m]) break;
        remaining -= month_days[m];
    }
    return .{ .year = y, .month = m + 1, .day = @intCast(remaining + 1), .hour = hour, .minute = minute, .second = second };
}

fn logTimestamped(msg: []const u8) void {
    const t = getTimestamp();
    std.debug.print("[{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}] {s}\n", .{ t.year, t.month, t.day, t.hour, t.minute, t.second, msg });
}

fn logTimestampedFmt(comptime fmt: []const u8, args: anytype) void {
    const t = getTimestamp();
    var buf: [256]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, fmt, args) catch "<fmt error>";
    std.debug.print("[{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}] {s}\n", .{ t.year, t.month, t.day, t.hour, t.minute, t.second, msg });
}

const app = cli.App{
    .name = "ever",
    .description = "lightweight event storage",
    .version = "0.1.0",
    .global_flags = &.{
        .{ .name = "address", .short = 'a', .default = "127.0.0.1", .env = "EVER_HOST", .description = "Store address" },
        .{ .name = "port", .short = 'p', .default = "7890", .env = "EVER_PORT", .description = "Store port" },
    },
    .help_sections = &.{
        .{
            .title = "Store Setup",
            .entries = &.{
                .{ .label = "start", .description = "Start the storage server" },
                .{ .label = "status", .description = "Show store statistics" },
            },
        },
        .{
            .title = "Topic",
            .entries = &.{
                .{ .label = "topic create", .description = "Create a new topic" },
                .{ .label = "topic list", .description = "List all topics" },
                .{ .label = "topic delete", .description = "Delete a topic" },
            },
        },
        .{
            .title = "Access",
            .entries = &.{
                .{ .label = "pub", .description = "Publish an event" },
                .{ .label = "sub", .description = "Subscribe to events" },
                .{ .label = "wait", .description = "Block until events arrive" },
                .{ .label = "on", .description = "Watch events and run command" },
            },
        },
        .{
            .title = "Other Subcommands",
            .entries = &.{
                .{ .label = "hook", .description = "Manage hooks triggered by events" },
                .{ .label = "timer", .description = "Manage timer-based event generation" },
            },
        },
    },
    .commands = &.{
        .{
            .name = "pub",
            .description = "Publish an event",
            .args = &.{
                .{ .name = "topic", .required = true, .description = "Topic name" },
                .{ .name = "data", .required = true, .description = "Event data" },
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
                .{ .name = "from", .default = "0", .description = "Start offset" },
                .{ .name = "max", .default = "100", .description = "Max events" },
                .{ .name = "follow", .takes_value = false, .description = "Follow new events" },
                .{ .name = "json-values", .takes_value = false, .description = "Print only JSON values" },
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
                .{ .name = "count", .default = "1", .description = "Events to wait for" },
                .{ .name = "timeout", .default = "0", .description = "Timeout in seconds" },
                .{ .name = "from", .default = "0", .description = "Start offset" },
                .{ .name = "json-values", .takes_value = false, .description = "Print only JSON values" },
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
                .{ .name = "once", .takes_value = false, .description = "Exit after first event" },
            },
            .takes_rest = true,
            .run = handleOn,
        },
        .{
            .name = "topic",
            .description = "Manage topics",
            .subcommands = &.{
                .{
                    .name = "create",
                    .aliases = &.{"add"},
                    .description = "Create a new topic",
                    .args = &.{.{ .name = "name", .required = true, .description = "Topic name" }},
                    .run = handleTopicCreate,
                },
                .{
                    .name = "list",
                    .description = "List all topics",
                    .run = handleTopicList,
                },
                .{
                    .name = "delete",
                    .aliases = &.{ "rm", "remove" },
                    .description = "Delete a topic",
                    .args = &.{.{ .name = "name", .required = true, .description = "Topic name" }},
                    .run = handleTopicDelete,
                },
            },
        },
        .{
            .name = "hook",
            .description = "Manage hooks triggered by events",
            .subcommands = &.{
                .{
                    .name = "new",
                    .description = "Interactively create a hook",
                    .run = handleHookNew,
                },
                .{
                    .name = "add",
                    .description = "Register a server-side hook",
                    .args = &.{.{ .name = "pattern", .required = true, .description = "Topic pattern" }},
                    .flags = &.{
                        .{ .name = "once", .takes_value = false, .description = "Remove after first trigger" },
                    },
                    .takes_rest = true,
                    .run = handleHookAdd,
                },
                .{
                    .name = "list",
                    .description = "List registered hooks",
                    .run = handleHookList,
                },
                .{
                    .name = "rm",
                    .aliases = &.{ "remove", "delete" },
                    .description = "Remove a hook by ID",
                    .args = &.{.{ .name = "id", .required = true, .description = "Hook ID" }},
                    .run = handleHookRm,
                },
                .{
                    .name = "ps",
                    .description = "Show running hook processes",
                    .run = handleHookPs,
                },
                .{
                    .name = "logs",
                    .description = "Show recent hook execution output",
                    .args = &.{.{ .name = "id", .required = true, .description = "Hook ID" }},
                    .run = handleHookLogs,
                },
            },
        },
        .{
            .name = "start",
            .description = "Start the storage server",
            .flags = &.{
                .{ .name = "data-dir", .default = "./data", .env = "EVER_DATA_DIR", .description = "Data directory" },
                .{ .name = "http-port", .default = "8890", .env = "EVER_HTTP_PORT", .description = "HTTP API port" },
                .{ .name = "no-http", .takes_value = false, .negatable = false, .description = "Disable HTTP server" },
            },
            .run = handleStart,
        },
        .{
            .name = "status",
            .description = "Show store statistics",
            .flags = &.{
                .{ .name = "data-dir", .default = "./data", .env = "EVER_DATA_DIR", .description = "Data directory" },
                .{ .name = "json", .takes_value = false, .description = "Output as JSON" },
            },
            .run = handleStatus,
        },
        .{
            .name = "timer",
            .description = "Manage timer-based event generation",
            .subcommands = &.{
                .{
                    .name = "new",
                    .description = "Interactively create a timer",
                    .run = handleTimerNew,
                },
                .{
                    .name = "add",
                    .description = "Add a recurring timer",
                    .args = &.{
                        .{ .name = "name", .required = true, .description = "Timer name" },
                        .{ .name = "topic", .required = true, .description = "Topic to publish to" },
                        .{ .name = "payload", .required = false, .description = "JSON payload (optional)" },
                    },
                    .flags = &.{
                        .{ .name = "every", .value_name = "INTERVAL", .description = "Interval (e.g. 5s, 1m, 2h)", .conflicts = &.{ "cron", "in" } },
                        .{ .name = "cron", .value_name = "EXPR", .description = "Cron expression", .conflicts = &.{ "every", "in" } },
                        .{ .name = "in", .value_name = "DURATION", .description = "Fire once after duration (e.g. 5s, 2m)", .conflicts = &.{ "every", "cron" } },
                        .{ .name = "no-persist", .takes_value = false, .description = "Don't catch up missed fires" },
                    },
                    .run = handleTimerAdd,
                },
                .{
                    .name = "list",
                    .description = "List registered timers",
                    .run = handleTimerList,
                },
                .{
                    .name = "rm",
                    .aliases = &.{ "remove", "delete" },
                    .description = "Remove a timer by name",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .run = handleTimerRm,
                },
                .{
                    .name = "info",
                    .description = "Show timer details",
                    .args = &.{.{ .name = "name", .required = true, .description = "Timer name" }},
                    .run = handleTimerInfo,
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
