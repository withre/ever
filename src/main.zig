const std = @import("std");
const ever = @import("ever");
const cli = @import("zig-cli-kit");
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
        fatal(ctx, "error: invalid port '{s}'\n", .{port_str});
    };
    return .{
        .address = if (addr.len > 0) addr else "127.0.0.1",
        .port = port,
    };
}

/// Print a diagnostic to stderr. Flushes stdout first so terminal ordering
/// matches the old unbuffered behaviour when data and diagnostics interleave,
/// and flushes stderr so diagnostics appear immediately even in long-running
/// commands (`sub --follow`, `on`, interactive prompts).
fn diag(ctx: *const cli.Context, comptime fmt: []const u8, args: anytype) void {
    ctx.stdout.flush() catch {};
    ctx.stderr.print(fmt, args) catch {};
    ctx.stderr.flush() catch {};
}

/// Print a diagnostic and exit with status 1. `std.process.exit` bypasses
/// `main`'s deferred flushes, so every early-exit path must go through here
/// (or `exitFlushed`) or buffered output is silently dropped.
fn fatal(ctx: *const cli.Context, comptime fmt: []const u8, args: anytype) noreturn {
    diag(ctx, fmt, args);
    std.process.exit(1);
}

/// Flush both writers, then exit with the given code.
fn exitFlushed(ctx: *const cli.Context, code: u8) noreturn {
    ctx.stdout.flush() catch {};
    ctx.stderr.flush() catch {};
    std.process.exit(code);
}

/// Connect to the store, printing a clean error and exiting on failure.
fn connectToStore(allocator: std.mem.Allocator, ctx: *const cli.Context, addr: []const u8, port: u16) ever.client.Client {
    return ever.client.Client.connect(allocator, ctx.io, addr, port) catch {
        fatal(ctx, "error: cannot connect to store at {s}:{d}\n", .{ addr, port });
    };
}

fn handlePub(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const topic = ctx.arg("topic");
    const data = ctx.arg("data");

    if (topic.len == 0) {
        fatal(ctx, "error: topic is required\n", .{});
    }
    if (data.len == 0) {
        fatal(ctx, "error: data is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    const offset = c.publish(topic, null, data) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    diag(ctx, "Published to {s} at offset {d}\n", .{ topic, offset });
}

fn handleSub(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const topic_name = ctx.arg("topic");
    if (topic_name.len == 0) {
        fatal(ctx, "error: topic is required\n", .{});
    }

    const from_offset = try ctx.flagInt(u64, "from");
    const after_offset = try ctx.flagIntOrNull(u64, "after-offset");
    const max_count = try ctx.flagInt(u32, "max");
    const follow = ctx.flagBool("follow");
    const json_values = ctx.flagBool("json-values");

    // The framework's `.conflicts` rejects both flags on argv; this guards
    // any path that bypasses it (mirrors the server-side precedence rule).
    if (after_offset != null and ctx.hasFlag("from")) {
        fatal(ctx, "error: --from and --after-offset are mutually exclusive\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);

    var client = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer client.deinit();

    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    if (follow) {
        // Two cursor modes: `--after-offset` tracks the last delivered
        // event's *global* offset (resume strictly after it); `--from` /
        // default keeps the topic-local skip-count arithmetic.
        var offset = from_offset;
        var after_cursor = after_offset;
        var did_initial = false;
        var emitted: u64 = 0;
        while (true) {
            const remaining: u32 = if (max_count > emitted) @intCast(max_count - emitted) else 0;
            if (remaining == 0 and did_initial) break;
            const fetch_count: u32 = if (remaining > 0) remaining else max_count;
            var result = if (after_cursor) |after|
                client.fetchAfter(
                    if (!is_pattern) topic_name else null,
                    if (is_pattern) topic_name else null,
                    after,
                    if (!did_initial) fetch_count else @min(100, fetch_count),
                    if (!did_initial) 0 else 5000,
                ) catch |err| switch (err) {
                    error.ServerError => exitFlushed(ctx, 1),
                    else => return err,
                }
            else if (!did_initial)
                (if (is_pattern) client.fetchPattern(topic_name, offset, fetch_count) else client.fetch(topic_name, offset, fetch_count)) catch |err| switch (err) {
                    error.ServerError => exitFlushed(ctx, 1),
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
                    error.ServerError => exitFlushed(ctx, 1),
                    else => return err,
                };
            defer result.deinit();
            for (result.events) |event| {
                try printEvent(ctx.stdout, event, json_values);
                try ctx.stdout.flush();
                emitted += 1;
                if (emitted >= max_count) break;
            }
            if (result.events.len > 0) {
                if (after_cursor != null) {
                    after_cursor = result.events[result.events.len - 1].offset;
                } else {
                    offset += result.events.len;
                }
            }
            did_initial = true;
            if (emitted >= max_count) break;
        }
    } else {
        var result = (if (after_offset) |after|
            client.fetchAfter(
                if (!is_pattern) topic_name else null,
                if (is_pattern) topic_name else null,
                after,
                max_count,
                0,
            )
        else if (is_pattern) client.fetchPattern(topic_name, from_offset, max_count) else client.fetch(topic_name, from_offset, max_count)) catch |err| switch (err) {
            error.ServerError => exitFlushed(ctx, 1),
            else => return err,
        };
        defer result.deinit();
        if (result.events.len == 0) {
            warnBeyondEnd(ctx, topic_name, after_offset, from_offset, result.topic_events);
            diag(ctx, "No events.\n", .{});
        } else {
            for (result.events) |event| try printEvent(ctx.stdout, event, json_values);
        }
    }
}

/// Emit the "start beyond end of topic" warning (stderr, diagnostic only —
/// exit codes unchanged) when an empty result came from a `--from N` skip
/// count that exceeds the topic's event count. `N == M` is a legitimate
/// at-the-tail read — no warning. `topic_events` is null for pattern
/// requests, so those never warn; `--after-offset` reads never warn either
/// (any global offset at/past the tip is a valid "nothing new yet" cursor).
fn warnBeyondEnd(ctx: *const cli.Context, topic_name: []const u8, after_offset: ?u64, from_offset: u64, topic_events: ?u64) void {
    if (after_offset != null or from_offset == 0) return;
    const m = topic_events orelse return;
    if (from_offset > m) {
        diag(ctx, "warning: start {d} is beyond the end of topic '{s}' ({d} events)\n", .{ from_offset, topic_name, m });
    }
}

fn handleWait(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const topic_name = ctx.arg("topic");
    if (topic_name.len == 0) {
        fatal(ctx, "error: topic is required\n", .{});
    }

    const count = try ctx.flagInt(u32, "count");
    const timeout_secs = try ctx.flagInt(u32, "timeout");
    const from_offset = try ctx.flagInt(u64, "from");
    const after_offset = try ctx.flagIntOrNull(u64, "after-offset");
    const json_values = ctx.flagBool("json-values");

    if (after_offset != null and ctx.hasFlag("from")) {
        fatal(ctx, "error: --from and --after-offset are mutually exclusive\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);

    var client = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer client.deinit();

    const is_pattern = std.mem.indexOfScalar(u8, topic_name, '*') != null or
        (topic_name.len > 0 and topic_name[topic_name.len - 1] == '.');

    var collected: u32 = 0;
    var offset = from_offset;
    var after_cursor = after_offset;
    var elapsed_ms: u64 = 0;
    const timeout_ms: u64 = @as(u64, timeout_secs) * 1000;
    const block_interval_ms: u32 = 2000;

    // A `--from N` beyond the topic's end would silently sit at the block
    // timeout — probe non-blocking first so the beyond-end warning lands
    // before we start blocking. Any events the probe returns count.
    var probe_pending = after_offset == null and from_offset > 0 and !is_pattern;

    while (collected < count) {
        if (timeout_secs > 0 and elapsed_ms >= timeout_ms) exitFlushed(ctx, 1);
        const remaining_ms: u32 = if (timeout_secs > 0)
            @intCast(@min(block_interval_ms, timeout_ms - elapsed_ms))
        else
            block_interval_ms;
        const block_ms: u32 = if (probe_pending) 0 else remaining_ms;
        var result = (if (after_cursor) |after|
            client.fetchAfter(
                if (!is_pattern) topic_name else null,
                if (is_pattern) topic_name else null,
                after,
                @min(100, count - collected),
                block_ms,
            )
        else
            client.fetchBlocking(
                if (!is_pattern) topic_name else null,
                if (is_pattern) topic_name else null,
                offset,
                @min(100, count - collected),
                block_ms,
            )) catch fatal(ctx, "error: fetch failed.\n", .{});
        defer result.deinit();
        if (probe_pending) {
            probe_pending = false;
            if (result.events.len == 0) {
                warnBeyondEnd(ctx, topic_name, after_offset, from_offset, result.topic_events);
            }
        }
        for (result.events) |event| {
            try printEvent(ctx.stdout, event, json_values);
            try ctx.stdout.flush();
            collected += 1;
        }
        if (result.events.len > 0) {
            if (after_cursor != null) {
                after_cursor = result.events[result.events.len - 1].offset;
            } else {
                offset += result.events.len;
            }
        }
        elapsed_ms += block_ms;
    }
}

fn handleOn(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const envp = ctx.envp;

    const pattern = ctx.arg("pattern");
    const once = ctx.flagBool("once");
    const rest_args = ctx.rest();

    if (pattern.len == 0) {
        fatal(ctx, "error: pattern is required\n", .{});
    }
    if (rest_args.len == 0) {
        fatal(ctx, "error: command is required after --\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    const is_pattern = std.mem.indexOfScalar(u8, pattern, '*') != null or
        (pattern.len > 0 and pattern[pattern.len - 1] == '.');

    var client = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer client.deinit();

    var probe = (if (is_pattern)
        client.fetchBlocking(null, pattern, 0, 1_000_000, 0)
    else
        client.fetchBlocking(pattern, null, 0, 1_000_000, 0)) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    const next_offset: u64 = probe.events.len;
    probe.deinit();

    diag(ctx, "Watching '{s}' from offset {d}...\n", .{ pattern, next_offset });

    var offset = next_offset;
    while (true) {
        var result = (if (is_pattern)
            client.fetchBlocking(null, pattern, offset, 100, 5000)
        else
            client.fetchBlocking(pattern, null, offset, 100, 5000)) catch |err| switch (err) {
            error.ServerError => exitFlushed(ctx, 1),
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
                diag(ctx, "pipe2 failed\n", .{});
                continue;
            }

            const pid = std.os.linux.fork();
            const pid_i: isize = @bitCast(pid);

            if (pid_i < 0) {
                _ = std.os.linux.close(pipe_fds[0]);
                _ = std.os.linux.close(pipe_fds[1]);
                diag(ctx, "fork failed\n", .{});
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
                diag(ctx, "Command exited with status {d} for event on '{s}'\n", .{ exit_code, if (event.topic) |t| t else pattern });
            }
        }

        offset += result.events.len;
        if (once) break;
    }
}

fn handleTopicCreate(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const name = ctx.arg("name");

    if (name.len == 0) {
        fatal(ctx, "error: topic name is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    c.createTopic(name) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    diag(ctx, "Created topic: {s}\n", .{name});
}

fn handleTopicList(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    const topics = c.listTopics() catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer {
        for (topics) |t| allocator.free(t);
        allocator.free(topics);
    }
    if (topics.len == 0) diag(ctx, "No topics.\n", .{}) else for (topics) |t| try ctx.stdout.print("{s}\n", .{t});
}

fn handleTopicDelete(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const name = ctx.arg("name");

    if (name.len == 0) {
        fatal(ctx, "error: topic name is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    c.deleteTopic(name) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    diag(ctx, "Deleted topic: {s}\n", .{name});
}

fn handleHookAdd(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const pattern = ctx.arg("pattern");
    const once = ctx.flagBool("once");
    const name_flag = ctx.flag("name");
    const from_beginning = ctx.flagBool("from-beginning");
    const from_flag = ctx.flag("from");
    const create_topic = ctx.flagBool("create-topic");
    const rest_args = ctx.rest();

    if (pattern.len == 0) {
        fatal(ctx, "error: pattern is required\n", .{});
    }
    if (rest_args.len == 0) {
        fatal(ctx, "error: command is required after --\n", .{});
    }
    if (from_beginning and from_flag.len > 0) {
        fatal(ctx, "error: --from-beginning and --from are mutually exclusive\n", .{});
    }
    // Client-side validation for --create-topic; mirrored server-side (the
    // server must not trust the client).
    if (create_topic and (from_beginning or from_flag.len > 0)) {
        fatal(ctx, "error: --create-topic cannot be combined with --from-beginning/--from (a fresh topic has no history to replay)\n", .{});
    }
    if (create_topic and ever.topic.isPatternShape(pattern)) {
        fatal(ctx, "error: --create-topic requires an exact topic name (no trailing-dot prefix, no '*' wildcard)\n", .{});
    }

    // Resolve start cursor: null = tip (server resolves), 0 = full replay,
    // explicit N = start at topic-local offset N.
    const start_cursor: ?u64 = blk: {
        if (from_beginning) break :blk 0;
        if (from_flag.len > 0) {
            const n = std.fmt.parseInt(u64, from_flag, 10) catch {
                fatal(ctx, "error: --from must be a non-negative integer, got '{s}'\n", .{from_flag});
            };
            break :blk n;
        }
        break :blk null;
    };

    // Join rest args into a single command string
    var cmd_str: std.ArrayList(u8) = .empty;
    defer cmd_str.deinit(allocator);
    for (rest_args, 0..) |arg, j| {
        if (j > 0) try cmd_str.append(allocator, ' ');
        try cmd_str.appendSlice(allocator, arg);
    }

    var cwd_buf: [4096]u8 = undefined;
    const cwd_z: [*:0]const u8 = "/proc/self/cwd";
    const cwd_len = std.os.linux.readlink(cwd_z, &cwd_buf, cwd_buf.len);
    const cwd_i: isize = @bitCast(cwd_len);
    const cwd: []const u8 = if (cwd_i > 0) cwd_buf[0..@intCast(cwd_i)] else "/tmp";

    const name: ?[]const u8 = if (name_flag.len > 0) name_flag else null;

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    const id = if (create_topic)
        c.registerHookCreateTopic(pattern, cmd_str.items, cwd, once, null, name) catch |err| switch (err) {
            error.ServerError => exitFlushed(ctx, 1),
            else => return err,
        }
    else
        c.registerHookFullNamedCursor(pattern, cmd_str.items, cwd, once, null, name, start_cursor) catch |err| switch (err) {
            error.ServerError => exitFlushed(ctx, 1),
            else => return err,
        };

    const mode_suffix: []const u8 = if (create_topic)
        " (topic created)"
    else if (from_beginning)
        " [replaying from beginning]"
    else if (from_flag.len > 0)
        " [replaying from offset]"
    else
        "";
    if (once) {
        diag(ctx, "Hook #{d} registered (once): {s} \u{2192} {s}{s}\n", .{ id, pattern, cmd_str.items, mode_suffix });
    } else {
        diag(ctx, "Hook #{d} registered: {s} \u{2192} {s}{s}\n", .{ id, pattern, cmd_str.items, mode_suffix });
    }
}

fn handleHookList(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.listHooks() catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer result.deinit();

    if (result.hooks.len == 0) {
        diag(ctx, "No hooks registered.\n", .{});
    } else {
        try ctx.stdout.print("{s:<4} {s:<20} {s:<25} {s:<30} {s:<12}\n", .{ "ID", "NAME", "Pattern", "Command", "Last Offset" });
        for (result.hooks) |hook| {
            var id_buf: [20]u8 = undefined;
            const id_str = std.fmt.bufPrint(&id_buf, "#{d}", .{hook.id}) catch "?";
            var cursor_buf: [20]u8 = undefined;
            const cursor_str = std.fmt.bufPrint(&cursor_buf, "{d}", .{hook.cursor}) catch "?";
            const name_str = if (hook.name) |n| n else "-";

            try ctx.stdout.print("{s:<4} {s:<20} {s:<25} {s:<30} {s:<12}\n", .{
                id_str,
                name_str,
                hook.pattern,
                hook.command,
                cursor_str,
            });
        }
    }
}

fn handleHookPs(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.hookPs() catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer result.deinit();

    if (result.processes.len == 0) {
        diag(ctx, "No running hook processes.\n", .{});
    } else {
        try ctx.stdout.print("{s:<6} {s:<8} {s:<25} {s:<30} {s:<12}\n", .{ "HOOK", "PID", "PATTERN", "COMMAND", "ELAPSED" });
        const now = getMilliTimestamp();
        for (result.processes) |p| {
            var hook_buf: [20]u8 = undefined;
            const hook_str = std.fmt.bufPrint(&hook_buf, "#{d}", .{p.hook_id}) catch "?";
            var pid_buf: [20]u8 = undefined;
            const pid_str = std.fmt.bufPrint(&pid_buf, "{d}", .{p.pid}) catch "?";
            const elapsed_s = @divTrunc(now - p.start_time, 1000);
            var elapsed_buf: [20]u8 = undefined;
            const elapsed_str = std.fmt.bufPrint(&elapsed_buf, "{d}s", .{elapsed_s}) catch "?";
            try ctx.stdout.print("{s:<6} {s:<8} {s:<25} {s:<30} {s:<12}\n", .{
                hook_str, pid_str, p.pattern, p.command, elapsed_str,
            });
        }
    }
}

/// Result of resolving a CLI hook argument to a numeric hook ID.
const ResolvedHook = struct {
    id: u64,
    /// True when the argument was resolved via name lookup rather than
    /// parsed as an integer. Callers use this to pick the right message.
    by_name: bool,
};

/// Resolve a CLI hook argument to a numeric hook ID.
/// Strategy (identical to the original `hook rm`): try integer first; on
/// parse failure, list hooks and return the first whose name matches.
/// Unknown name: prints "error: hook '<name>' not found" and exits 1.
/// The name lookup consumes its own store connection (`listHooks()` eats
/// the connection), so callers must open a fresh connection for any
/// follow-up request.
fn resolveHookId(
    allocator: std.mem.Allocator,
    ctx: *cli.Context,
    address: []const u8,
    port: u16,
    id_or_name: []const u8,
) !ResolvedHook {
    // Integer parse wins: a hook literally named "42" is unreachable by name.
    if (std.fmt.parseInt(u64, id_or_name, 10)) |id| {
        return .{ .id = id, .by_name = false };
    } else |_| {}

    // Treat as name — list hooks to find the ID (first match wins).
    var c = connectToStore(allocator, ctx, address, port);
    defer c.deinit();
    var result = c.listHooks() catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer result.deinit();

    for (result.hooks) |hook| {
        if (hook.name) |name| {
            if (std.mem.eql(u8, name, id_or_name)) {
                return .{ .id = hook.id, .by_name = true };
            }
        }
    }

    fatal(ctx, "error: hook '{s}' not found\n", .{id_or_name});
}

fn handleHookLogs(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const id_or_name = ctx.arg("id");

    if (id_or_name.len == 0) {
        fatal(ctx, "error: hook ID or name is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    const resolved = try resolveHookId(allocator, ctx, addr_info.address, addr_info.port, id_or_name);

    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.hookLogs(resolved.id) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer result.deinit();

    try ctx.stdout.print("Log file: {s}\n---\n", .{result.log_path});
    if (result.content.len > 0) {
        try ctx.stdout.print("{s}", .{result.content});
        // Ensure trailing newline
        if (result.content[result.content.len - 1] != '\n') {
            try ctx.stdout.print("\n", .{});
        }
    } else {
        diag(ctx, "(empty)\n", .{});
    }
}

fn handleHookRm(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const id_or_name = ctx.arg("id");

    if (id_or_name.len == 0) {
        fatal(ctx, "error: hook ID or name is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    const resolved = try resolveHookId(allocator, ctx, addr_info.address, addr_info.port, id_or_name);

    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    c.removeHook(resolved.id) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };

    if (resolved.by_name) {
        diag(ctx, "Hook '{s}' (#{d}) removed.\n", .{ id_or_name, resolved.id });
    } else {
        diag(ctx, "Hook #{d} removed.\n", .{resolved.id});
    }
}

fn handleTimerAdd(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const name_arg = ctx.arg("name");
    const name_flag = ctx.flag("name");
    const every = ctx.flag("every");
    const cron = ctx.flag("cron");
    const in_flag = ctx.flag("in");
    const topic_arg = ctx.arg("topic");
    const payload_arg = ctx.arg("payload");
    const persistent = !ctx.flagBool("no-persist");

    // Resolve name and topic: support both old and new syntax
    // Old: ever timer add <name> --every 5s <topic> [payload]
    // New: ever timer add --every 5s <topic> [payload]  (auto-name)
    // New: ever timer add --name foo --every 5s <topic> [payload]
    var name: []const u8 = undefined;
    var topic: []const u8 = undefined;
    var payload: []const u8 = undefined;

    if (name_flag.len > 0) {
        // --name flag takes priority
        name = name_flag;
        topic = if (name_arg.len > 0) name_arg else topic_arg;
        payload = if (name_arg.len > 0) topic_arg else payload_arg;
    } else if (topic_arg.len > 0) {
        // Old syntax: name and topic both provided as positional
        name = name_arg;
        topic = topic_arg;
        payload = payload_arg;
    } else if (name_arg.len > 0) {
        // Only one positional arg — treat as topic, auto-generate name
        topic = name_arg;
        payload = topic_arg;
        name = "";
    } else {
        name = name_arg;
        topic = topic_arg;
        payload = payload_arg;
    }

    // Count how many schedule flags are set (use hasFlag to detect even empty values)
    const has_every = ctx.hasFlag("every");
    const has_cron = ctx.hasFlag("cron");
    const has_in = ctx.hasFlag("in");
    const schedule_count = @as(u8, if (has_every) 1 else 0) + @as(u8, if (has_cron) 1 else 0) + @as(u8, if (has_in) 1 else 0);

    if (schedule_count == 0) {
        fatal(ctx, "error: one of --every, --cron, or --in is required\n", .{});
    }
    if (schedule_count > 1) {
        fatal(ctx, "error: --every, --cron, and --in are mutually exclusive\n", .{});
    }
    // Validate non-empty values
    if (has_every and every.len == 0) {
        fatal(ctx, "error: invalid duration format\n", .{});
    }
    if (has_cron and cron.len == 0) {
        fatal(ctx, "error: invalid cron expression\n", .{});
    }
    if (has_in and in_flag.len == 0) {
        fatal(ctx, "error: invalid duration format\n", .{});
    }
    if (topic.len == 0) {
        fatal(ctx, "error: topic is required\n", .{});
    }

    const schedule_type: []const u8 = if (has_in) "one_shot" else if (has_every) "interval" else "cron";
    const schedule_value = if (has_in) in_flag else if (has_every) every else cron;
    const actual_payload = if (payload.len > 0) payload else "{}";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    // Auto-generate name if not provided
    const actual_name = if (name.len > 0) name else blk: {
        const auto = std.fmt.allocPrint(allocator, "{s}-{s}", .{ topic, schedule_value }) catch {
            fatal(ctx, "error: failed to generate timer name\n", .{});
        };
        break :blk auto;
    };

    c.addTimer(actual_name, schedule_type, schedule_value, topic, actual_payload, if (has_in) false else persistent) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };

    if (has_in) {
        diag(ctx, "Timer '{s}' registered (one-shot): in {s} → {s}\n", .{ actual_name, in_flag, topic });
    } else {
        diag(ctx, "Timer '{s}' registered: {s} {s} → {s}\n", .{ actual_name, if (has_every) "every" else "cron", schedule_value, topic });
    }
}

fn handleTimerList(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    var result = c.listTimers() catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer result.deinit();

    if (result.timers.len == 0) {
        diag(ctx, "No timers registered.\n", .{});
    } else {
        try ctx.stdout.print("{s:<20} {s:<15} {s:<25} {s:<10}\n", .{ "NAME", "SCHEDULE", "TOPIC", "FIRES" });
        for (result.timers) |timer| {
            try ctx.stdout.print("{s:<20} {s:<15} {s:<25} {d:<10}\n", .{
                timer.name,
                timer.schedule,
                timer.topic,
                timer.fire_count,
            });
        }
    }
}

fn handleTimerRm(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const name = ctx.arg("name");

    if (name.len == 0) {
        fatal(ctx, "error: timer name is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    c.removeTimer(name) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    diag(ctx, "Timer '{s}' removed.\n", .{name});
}

fn handleTimerInfo(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const name = ctx.arg("name");

    if (name.len == 0) {
        fatal(ctx, "error: timer name is required\n", .{});
    }

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    const timer = c.timerInfo(name) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };
    defer {
        allocator.free(timer.name);
        allocator.free(timer.schedule);
        allocator.free(timer.topic);
        allocator.free(timer.payload);
    }

    try ctx.stdout.print("Name:        {s}\n", .{timer.name});
    try ctx.stdout.print("Schedule:    {s}\n", .{timer.schedule});
    try ctx.stdout.print("Topic:       {s}\n", .{timer.topic});
    try ctx.stdout.print("Payload:     {s}\n", .{timer.payload});
    try ctx.stdout.print("Last fired:  ", .{});
    if (timer.last_fired_at > 0) {
        try ctx.stdout.print("{d}\n", .{timer.last_fired_at});
    } else {
        try ctx.stdout.print("never\n", .{});
    }
    try ctx.stdout.print("Next fire:   (calculated on server)\n", .{});
    try ctx.stdout.print("Fire count:  {d}\n", .{timer.fire_count});
    try ctx.stdout.print("Persistent:  {s}\n", .{if (timer.persistent) "yes" else "no"});
}

fn handleTimerNew(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    // Prompt: Timer name
    diag(ctx, "Timer name: ", .{});
    const name = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(name);
    if (name.len == 0) {
        fatal(ctx, "error: timer name is required\n", .{});
    }

    // Prompt: Schedule type
    diag(ctx, "Schedule type (every/cron/in): ", .{});
    const sched_type_input = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(sched_type_input);

    const is_every = std.mem.eql(u8, sched_type_input, "every");
    const is_cron = std.mem.eql(u8, sched_type_input, "cron");
    const is_in = std.mem.eql(u8, sched_type_input, "in");
    if (!is_every and !is_cron and !is_in) {
        fatal(ctx, "error: schedule type must be 'every', 'cron', or 'in'\n", .{});
    }

    // Prompt: Schedule value
    if (is_every) {
        diag(ctx, "Interval: ", .{});
    } else if (is_cron) {
        diag(ctx, "Cron expression: ", .{});
    } else {
        diag(ctx, "Delay: ", .{});
    }
    const sched_value = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(sched_value);
    if (sched_value.len == 0) {
        fatal(ctx, "error: schedule value is required\n", .{});
    }

    // Prompt: Topic
    diag(ctx, "Topic: ", .{});
    const topic = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(topic);
    if (topic.len == 0) {
        fatal(ctx, "error: topic is required\n", .{});
    }

    // Prompt: Payload
    diag(ctx, "Payload [{{}}]: ", .{});
    const payload_input = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(payload_input);
    const payload = if (payload_input.len > 0) payload_input else "{}";

    // Prompt: Persistent (only for non-one-shot)
    var persistent = true;
    if (!is_in) {
        diag(ctx, "Persistent (y/n) [y]: ", .{});
        const persist_input = readLine(allocator) catch {
            fatal(ctx, "\nerror: failed to read input\n", .{});
        };
        defer allocator.free(persist_input);
        if (std.mem.eql(u8, persist_input, "n") or std.mem.eql(u8, persist_input, "N")) {
            persistent = false;
        }
    }

    const schedule_type: []const u8 = if (is_in) "one_shot" else if (is_every) "interval" else "cron";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    c.addTimer(name, schedule_type, sched_value, topic, payload, if (is_in) false else persistent) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };

    diag(ctx, "\n", .{});
    if (is_in) {
        diag(ctx, "Timer '{s}' registered: in {s} \u{2192} {s}\n", .{ name, sched_value, topic });
    } else {
        diag(ctx, "Timer '{s}' registered: {s} {s} \u{2192} {s}\n", .{ name, sched_type_input, sched_value, topic });
    }
}

fn handleHookNew(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    // Prompt: Topic pattern
    diag(ctx, "Topic pattern: ", .{});
    const pattern = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(pattern);
    if (pattern.len == 0) {
        fatal(ctx, "error: topic pattern is required\n", .{});
    }

    // Prompt: Command
    diag(ctx, "Command: ", .{});
    const cmd_input = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(cmd_input);
    if (cmd_input.len == 0) {
        fatal(ctx, "error: command is required\n", .{});
    }

    // Prompt: One-shot
    diag(ctx, "One-shot (y/n) [n]: ", .{});
    const once_input = readLine(allocator) catch {
        fatal(ctx, "\nerror: failed to read input\n", .{});
    };
    defer allocator.free(once_input);
    const once = std.mem.eql(u8, once_input, "y") or std.mem.eql(u8, once_input, "Y");

    // Get cwd
    var cwd_buf: [4096]u8 = undefined;
    const cwd_z: [*:0]const u8 = "/proc/self/cwd";
    const cwd_len = std.os.linux.readlink(cwd_z, &cwd_buf, cwd_buf.len);
    const cwd_i: isize = @bitCast(cwd_len);
    const cwd: []const u8 = if (cwd_i > 0) cwd_buf[0..@intCast(cwd_i)] else "/tmp";

    const addr_info = parseStoreAddress(ctx);
    var c = connectToStore(allocator, ctx, addr_info.address, addr_info.port);
    defer c.deinit();
    const id = c.registerHookFull(pattern, cmd_input, cwd, once, null) catch |err| switch (err) {
        error.ServerError => exitFlushed(ctx, 1),
        else => return err,
    };

    diag(ctx, "\n", .{});
    if (once) {
        diag(ctx, "Hook #{d} registered (once): {s} \u{2192} {s}\n", .{ id, pattern, cmd_input });
    } else {
        diag(ctx, "Hook #{d} registered: {s} \u{2192} {s}\n", .{ id, pattern, cmd_input });
    }
}

fn readLine(allocator: std.mem.Allocator) ![]u8 {
    return readline.readLine(allocator);
}

fn handleStart(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    try startServer(allocator, ctx.io, ctx, ctx.envp);
}

/// True when `name` is present in the process environment (any value).
fn envHasVar(envp: [*:null]const ?[*:0]const u8, name: []const u8) bool {
    var i: usize = 0;
    while (envp[i]) |entry| : (i += 1) {
        const s = std.mem.sliceTo(entry, 0);
        if (s.len > name.len and s[name.len] == '=' and std.mem.eql(u8, s[0..name.len], name)) return true;
    }
    return false;
}

fn handleStatus(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const data_dir = ctx.flag("data-dir");
    const json_output = ctx.flagBool("json");

    // Default: query the running server over TCP — no local filesystem
    // access. The offline directory scan only runs when a data dir was
    // explicitly requested (--data-dir flag or EVER_DATA_DIR env).
    const explicit_data_dir = ctx.hasFlag("data-dir") or envHasVar(ctx.envp, "EVER_DATA_DIR");
    if (!explicit_data_dir) {
        const addr_info = parseStoreAddress(ctx);
        var c = ever.client.Client.connect(allocator, ctx.io, addr_info.address, addr_info.port) catch {
            fatal(ctx, "error: cannot reach Ever server at {s}:{d}. Is the store running? For offline inspection use --data-dir <path>.\n", .{ addr_info.address, addr_info.port });
        };
        defer c.deinit();

        var result = c.status() catch |err| switch (err) {
            error.ServerError => exitFlushed(ctx, 1),
            else => return err,
        };
        defer result.deinit();

        // We just spoke the protocol — a strictly stronger signal than the
        // offline path's bare TCP connect probe.
        result.status.server = .{
            .address = addr_info.address,
            .port = addr_info.port,
            .reachable = true,
        };

        if (json_output) {
            try ever.status.printJson(&result.status, allocator, ctx.stdout);
        } else {
            try ever.status.printHuman(&result.status, allocator, ctx.stdout);
        }
        ctx.stdout.flush() catch {};
        return;
    }

    // Offline/forensic inspection of a local data directory.
    const actual_dir = if (data_dir.len > 0) data_dir else "./data";

    // Validate directory looks like an Ever store
    {
        const dir = std.Io.Dir.cwd().openDir(ctx.io, actual_dir, .{ .iterate = true }) catch {
            fatal(ctx, "error: cannot open data directory '{s}'\n", .{actual_dir});
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
            fatal(ctx, "error: '{s}' does not appear to be an Ever data directory\n", .{actual_dir});
        }
    }

    var store_status = ever.status.getStatus(allocator, ctx.io, actual_dir) catch |err| {
        fatal(ctx, "error: failed to get store status: {}\n", .{err});
    };
    defer store_status.deinit(allocator);

    const addr_info = parseStoreAddress(ctx);
    store_status.server = .{
        .address = addr_info.address,
        .port = addr_info.port,
        .reachable = ever.status.probeServer(ctx.io, addr_info.address, addr_info.port, 500),
    };

    if (json_output) {
        try ever.status.printJson(&store_status, allocator, ctx.stdout);
    } else {
        try ever.status.printHuman(&store_status, allocator, ctx.stdout);
    }
    ctx.stdout.flush() catch {};
}

fn startServer(allocator: std.mem.Allocator, io: Io, ctx: *const cli.Context, envp: [*:null]const ?[*:0]const u8) !void {
    const address = ctx.flag("address");
    const port_str = ctx.flag("port");
    const data_dir = ctx.flag("data-dir");

    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        fatal(ctx, "error: invalid port '{s}'\n", .{port_str});
    };
    const actual_data_dir = if (data_dir.len > 0) data_dir else "./data";

    const dir = Dir.cwd().createDirPathOpen(io, actual_data_dir, .{ .open_options = .{ .iterate = true } }) catch
        std.process.fatal("cannot open data directory '{s}'.", .{actual_data_dir});
    defer dir.close(io);

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

    var server = try ever.net.Server.init(allocator, io, &topic_manager, .{ .address = if (address.len > 0) address else "127.0.0.1", .port = port, .data_dir = actual_data_dir });
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
        fatal(ctx, "error: invalid http-port '{s}'\n", .{http_port_str});
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

fn printEvent(out: *std.Io.Writer, event: ever.client.Event, json_values: bool) !void {
    if (json_values) {
        try out.print("{s}\n", .{event.value});
    } else {
        const t = if (event.topic) |tp| tp else "";
        if (t.len > 0) {
            if (event.key) |k| try out.print("[{s}:{d}] key={s} {s}\n", .{ t, event.offset, k, event.value }) else try out.print("[{s}:{d}] {s}\n", .{ t, event.offset, event.value });
        } else {
            if (event.key) |k| try out.print("[{d}] key={s} {s}\n", .{ event.offset, k, event.value }) else try out.print("[{d}] {s}\n", .{ event.offset, event.value });
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
                .{ .name = "from", .default = "0", .description = "Skip the first N events of the topic (topic-local count, not a log offset)", .conflicts = &.{"after-offset"} },
                .{ .name = "after-offset", .value_name = "OFFSET", .description = "Resume strictly after this global log offset (as printed in [topic:offset])", .conflicts = &.{"from"} },
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
                .{ .name = "from", .default = "0", .description = "Skip the first N events of the topic (topic-local count, not a log offset)", .conflicts = &.{"after-offset"} },
                .{ .name = "after-offset", .value_name = "OFFSET", .description = "Resume strictly after this global log offset (as printed in [topic:offset])", .conflicts = &.{"from"} },
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
                        .{ .name = "name", .description = "Hook name (auto-generated if omitted)" },
                        .{ .name = "from-beginning", .takes_value = false, .description = "Replay all historical events before following new ones", .conflicts = &.{ "from", "create-topic" } },
                        .{ .name = "from", .description = "Start at an explicit topic-local offset (replay from there)", .conflicts = &.{ "from-beginning", "create-topic" } },
                        .{ .name = "create-topic", .takes_value = false, .description = "Create the (exact-name) topic and register the hook atomically", .conflicts = &.{ "from", "from-beginning" } },
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
                    .description = "Remove a hook by ID or name",
                    .args = &.{.{ .name = "id", .required = true, .description = "Hook ID or name" }},
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
            .description = "Show store statistics (queries the running server; --data-dir for offline scan)",
            .flags = &.{
                .{ .name = "data-dir", .default = "./data", .env = "EVER_DATA_DIR", .description = "Inspect a local data directory offline instead of querying the server" },
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
                        .{ .name = "name", .required = true, .description = "Timer name or topic (if --name used)" },
                        .{ .name = "topic", .required = false, .description = "Topic to publish to" },
                        .{ .name = "payload", .required = false, .description = "JSON payload (optional)" },
                    },
                    .flags = &.{
                        .{ .name = "name", .description = "Timer name (auto-generated if omitted)" },
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

    var stdout_buf: [4096]u8 = undefined;
    var stderr_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buf);
    var stderr = std.Io.File.stderr().writer(io, &stderr_buf);
    defer stdout.interface.flush() catch {};
    defer stderr.interface.flush() catch {};

    app.run(
        allocator,
        io,
        &stdout.interface,
        &stderr.interface,
        env_block,
        args_list.items,
    ) catch |err| {
        // Deliver whatever output was produced before the failure; the
        // deferred flushes above never run past std.process.exit.
        stdout.interface.flush() catch {};
        stderr.interface.flush() catch {};
        switch (err) {
            // Usage errors: the CLI framework already wrote a human-readable
            // message to stderr before returning — just exit non-zero
            // instead of dumping a stack trace.
            error.UnknownFlag,
            error.MissingFlagValue,
            error.MissingRequiredFlag,
            error.ConflictingFlags,
            error.UnknownCommand,
            error.UnknownSubcommand,
            error.UnexpectedArgument,
            error.InvalidIntValue,
            => std.process.exit(1),
            // A closed stdout (e.g. `ever sub t --follow | head -1`) is
            // normal termination per Unix convention: exit 0, no message.
            error.WriteFailed => {
                const stdout_broken = if (stdout.err) |e| e == error.BrokenPipe else false;
                const stderr_broken = if (stderr.err) |e| e == error.BrokenPipe else false;
                if (stdout_broken or stderr_broken) std.process.exit(0);
                std.debug.print("error: failed to write output\n", .{});
                std.process.exit(1);
            },
            error.UnsupportedVersion => {
                std.debug.print("error: protocol version mismatch (is the server an Ever store?)\n", .{});
                std.process.exit(1);
            },
            error.BrokenPipe, error.ConnectionResetByPeer => {
                std.debug.print("error: connection lost\n", .{});
                std.process.exit(1);
            },
            error.IncompleteHeader, error.IncompleteBody => {
                std.debug.print("error: incomplete response from server\n", .{});
                std.process.exit(1);
            },
            error.MessageTooLarge => {
                std.debug.print("error: server response too large\n", .{});
                std.process.exit(1);
            },
            else => return err,
        }
    };
}

test "main module compiles" {
    _ = ever;
    _ = cli;
}

// Regression guard from air/v0.1/stdout-stderr-split.org: after the
// stdout/stderr migration, `std.debug` printing may only appear where no
// cli.Context is in scope and stderr is the correct stream — the server
// daemon path (`ever start` warnings + lifecycle logs, logTimestamped*)
// and main's post-run catch arms (writers may themselves be broken).
// Client output must go through ctx.stdout / diag / fatal instead.
// The needle is split so this test doesn't count itself.
fn countDebugPrints(source: []const u8) usize {
    const needle = "std.debug" ++ ".print";
    var count: usize = 0;
    var idx: usize = 0;
    while (std.mem.indexOfPos(u8, source, idx, needle)) |pos| {
        count += 1;
        idx = pos + needle.len;
    }
    return count;
}

test "stdout/stderr split: debug printing confined to the stderr allow-list" {
    // main.zig: 7 server-daemon sites + 5 post-run catch arms.
    try std.testing.expectEqual(@as(usize, 12), countDebugPrints(@embedFile("main.zig")));
    // status printers write data; they must use the threaded writer.
    try std.testing.expectEqual(@as(usize, 0), countDebugPrints(@embedFile("store/status.zig")));
    // client.zig has no Context; its 2 sites are server-error diagnostics.
    try std.testing.expectEqual(@as(usize, 2), countDebugPrints(@embedFile("net/client.zig")));
}
