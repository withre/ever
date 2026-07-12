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
    const json_full = ctx.flagBool("json");

    // The framework's `.conflicts` rejects both flags on argv; this guards
    // any path that bypasses it (mirrors the server-side precedence rule).
    if (after_offset != null and ctx.hasFlag("from")) {
        fatal(ctx, "error: --from and --after-offset are mutually exclusive\n", .{});
    }
    if (json_full and json_values) {
        fatal(ctx, "error: --json and --json-values are mutually exclusive\n", .{});
    }
    const mode: PrintMode = if (json_full) .json else if (json_values) .json_values else .default;

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
                try printEvent(allocator, ctx.stdout, event, mode, topic_name);
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
            for (result.events) |event| {
                try printEvent(allocator, ctx.stdout, event, mode, topic_name);
                try ctx.stdout.flush();
            }
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
            try printEvent(allocator, ctx.stdout, event, if (json_values) PrintMode.json_values else PrintMode.default, topic_name);
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

/// Row format shared by the `hook list` header and data rows. Every column
/// has a fixed width except `Command` — the only unbounded-width field —
/// which is rendered last and un-padded (ps-style), so the table stays
/// positionally parseable no matter how long the command is. No truncation.
const hook_list_row_fmt = "{s:<4} {s:<20} {s:<25} {s:<9} {s:<9} {s}\n";

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
        try ctx.stdout.print(hook_list_row_fmt, .{ "ID", "NAME", "Pattern", "Pending", "Fired", "Command" });
        for (result.hooks) |hook| {
            var id_buf: [20]u8 = undefined;
            const id_str = std.fmt.bufPrint(&id_buf, "#{d}", .{hook.id}) catch "?";
            const name_str = if (hook.name) |n| n else "-";

            // Pending column: matching events not yet processed. Uniform
            // meaning for both cursor kinds (derived server-side); the
            // server caps the pattern-scan at 1000 — render the cap as
            // "1000+".
            var pending_buf: [20]u8 = undefined;
            const pending_str = if (hook.pending >= 1000)
                "1000+"
            else
                std.fmt.bufPrint(&pending_buf, "{d}", .{hook.pending}) catch "?";

            // Fired column: "12" for a healthy hook, "12 (2 failed)" or
            // "12 (2 failed, last exit 127)" when deliveries have failed.
            var fired_buf: [64]u8 = undefined;
            const fired_str = blk: {
                if (hook.failure_count > 0) {
                    if (hook.last_exit_status) |les| {
                        if (les != 0) {
                            break :blk std.fmt.bufPrint(&fired_buf, "{d} ({d} failed, last exit {d})", .{
                                hook.fired_count, hook.failure_count, les,
                            }) catch "?";
                        }
                    }
                    break :blk std.fmt.bufPrint(&fired_buf, "{d} ({d} failed)", .{
                        hook.fired_count, hook.failure_count,
                    }) catch "?";
                }
                break :blk std.fmt.bufPrint(&fired_buf, "{d}", .{hook.fired_count}) catch "?";
            };

            try ctx.stdout.print(hook_list_row_fmt, .{
                id_str,
                name_str,
                hook.pattern,
                pending_str,
                fired_str,
                hook.command,
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

/// One `hook logs --json` execution record parsed from the structured
/// metadata header the hook daemon writes to every per-execution log file
/// (see `executeHookCommand` in src/store/hooks.zig).
const ParsedHookLog = struct {
    hook_id: u64,
    hook_name: ?[]const u8,
    pattern: []const u8,
    command: []const u8,
    topic: []const u8,
    offset: u64,
    /// Raw bytes of the header's single-line `Payload:` JSON object.
    payload: []const u8,
    output: []const u8,
};

/// Strip a `Label:` prefix plus its column-alignment padding from a header
/// line. Returns null when the line does not start with the label.
fn stripHeaderLabel(line: []const u8, label: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, line, label)) return null;
    return std.mem.trimStart(u8, line[label.len..], " ");
}

/// Parse a hook execution log against the fixed header grammar written by
/// `executeHookCommand`: sentinel first line, labelled fields in fixed
/// order, `===` terminator plus blank line, remainder = command output.
/// Returns null for content that does not conform (pre-change or
/// hand-edited files) — callers degrade instead of failing.
fn parseHookLogContent(content: []const u8) ?ParsedHookLog {
    var lines = std.mem.splitScalar(u8, content, '\n');
    if (!std.mem.eql(u8, lines.next() orelse return null, "=== Hook Execution ===")) return null;

    // `Hook:      #<id>` (unnamed) or `Hook:      #<id> (<name>)`
    const hook_val = stripHeaderLabel(lines.next() orelse return null, "Hook:") orelse return null;
    if (hook_val.len < 2 or hook_val[0] != '#') return null;
    var id_end: usize = 1;
    while (id_end < hook_val.len and std.ascii.isDigit(hook_val[id_end])) : (id_end += 1) {}
    const hook_id = std.fmt.parseInt(u64, hook_val[1..id_end], 10) catch return null;
    var hook_name: ?[]const u8 = null;
    const name_part = std.mem.trim(u8, hook_val[id_end..], " ");
    if (name_part.len > 0) {
        if (name_part.len < 2 or name_part[0] != '(' or name_part[name_part.len - 1] != ')') return null;
        hook_name = name_part[1 .. name_part.len - 1];
    }

    const pattern = stripHeaderLabel(lines.next() orelse return null, "Pattern:") orelse return null;
    const command = stripHeaderLabel(lines.next() orelse return null, "Command:") orelse return null;
    const topic = stripHeaderLabel(lines.next() orelse return null, "Topic:") orelse return null;
    const offset_str = stripHeaderLabel(lines.next() orelse return null, "Offset:") orelse return null;
    const offset = std.fmt.parseInt(u64, offset_str, 10) catch return null;
    _ = stripHeaderLabel(lines.next() orelse return null, "Timestamp:") orelse return null;
    const payload = stripHeaderLabel(lines.next() orelse return null, "Payload:") orelse return null;

    if (!std.mem.eql(u8, lines.next() orelse return null, "===")) return null;
    if ((lines.next() orelse return null).len != 0) return null;

    return .{
        .hook_id = hook_id,
        .hook_name = hook_name,
        .pattern = pattern,
        .command = command,
        .topic = topic,
        .offset = offset,
        .payload = payload,
        .output = lines.rest(),
    };
}

/// Extract the epoch-ms execution timestamp from a hook log path's
/// `<hook-id>-<timestamp-ms>.log` filename component — the instant the
/// daemon opened the log.
fn executedAtFromLogPath(log_path: []const u8) ?i64 {
    const base = if (std.mem.lastIndexOfScalar(u8, log_path, '/')) |i| log_path[i + 1 ..] else log_path;
    if (!std.mem.endsWith(u8, base, ".log")) return null;
    const stem = base[0 .. base.len - ".log".len];
    const dash = std.mem.indexOfScalar(u8, stem, '-') orelse return null;
    return std.fmt.parseInt(i64, stem[dash + 1 ..], 10) catch null;
}

/// Emit the `hook logs --json` record array on `out`: one record per
/// execution returned by the server (currently the most recent — see the
/// spec's Future Work). Content that fails the header grammar, mismatches
/// the wire-echoed hook ID, or carries a payload that is not a JSON object
/// degrades to a record with null header-derived fields and the full
/// content in `output` — never a parse failure.
fn emitHookLogsJson(
    allocator: std.mem.Allocator,
    out: *std.Io.Writer,
    wire_hook_id: u64,
    log_path: []const u8,
    content: []const u8,
) !void {
    var record: ?ParsedHookLog = parseHookLogContent(content);
    if (record) |p| {
        // The wire response echoes the requested hook ID — cross-check it.
        if (p.hook_id != wire_hook_id) record = null;
    }
    if (record) |p| {
        // The payload must be the single-line JSON object written by
        // buildEventJsonFromStore; validate before embedding it verbatim
        // so `--json` output is always valid JSON.
        if (std.json.parseFromSlice(std.json.Value, allocator, p.payload, .{})) |pv| {
            defer pv.deinit();
            if (pv.value != .object) record = null;
        } else |_| record = null;
    }

    var js: std.json.Stringify = .{ .writer = out };
    try js.beginArray();
    try js.beginObject();
    if (record) |p| {
        try js.objectField("hook_id");
        try js.write(p.hook_id);
        try js.objectField("hook_name");
        try js.write(p.hook_name);
        try js.objectField("pattern");
        try js.write(p.pattern);
        try js.objectField("command");
        try js.write(p.command);
        try js.objectField("topic");
        try js.write(p.topic);
        try js.objectField("offset");
        try js.write(p.offset);
        try js.objectField("executed_at");
        try js.write(executedAtFromLogPath(log_path));
        try js.objectField("payload");
        // Embedded verbatim: the header line is valid single-line JSON, so
        // raw embedding avoids double-escaping.
        try js.print("{s}", .{p.payload});
        try js.objectField("output");
        try js.write(p.output);
    } else {
        // Degraded record: header-derived fields null, full content in
        // output. executed_at is filename-derived, so it survives.
        try js.objectField("hook_id");
        try js.write(null);
        try js.objectField("hook_name");
        try js.write(null);
        try js.objectField("pattern");
        try js.write(null);
        try js.objectField("command");
        try js.write(null);
        try js.objectField("topic");
        try js.write(null);
        try js.objectField("offset");
        try js.write(null);
        try js.objectField("executed_at");
        try js.write(executedAtFromLogPath(log_path));
        try js.objectField("payload");
        try js.write(null);
        try js.objectField("output");
        try js.write(content);
    }
    try js.objectField("log_path");
    try js.write(log_path);
    try js.endObject();
    try js.endArray();
    try out.print("\n", .{});
}

fn handleHookLogs(allocator: std.mem.Allocator, ctx: *cli.Context) !void {
    const id_or_name = ctx.arg("id");
    const json_output = ctx.flagBool("json");

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

    // Empty-success wire shape (empty log_path + empty content): known
    // hook, no recorded executions. An empty history is not an error.
    const no_executions = result.log_path.len == 0 and result.content.len == 0;

    if (json_output) {
        if (no_executions) {
            try ctx.stdout.print("[]\n", .{});
        } else {
            try emitHookLogsJson(allocator, ctx.stdout, result.hook_id, result.log_path, result.content);
        }
        return;
    }

    if (no_executions) {
        diag(ctx, "(no executions)\n", .{});
        return;
    }

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

/// Output mode for `printEvent`. `default` is the human `[topic:offset]`
/// line, `json_values` the bare payload per line, `json` the full NDJSON
/// envelope (byte-compatible with the hook-stdin envelope built by
/// `buildEventJson`). See air/v0.1/sub-json-output.org.
const PrintMode = enum { default, json_values, json };

fn printEvent(allocator: std.mem.Allocator, out: *std.Io.Writer, event: ever.client.Event, mode: PrintMode, fallback_topic: []const u8) !void {
    switch (mode) {
        .json => {
            // Exact-topic fetches may omit the per-event topic; populate it
            // from the subscribed topic name rather than emitting "".
            var ev = event;
            if (ev.topic == null or ev.topic.?.len == 0) ev.topic = fallback_topic;
            const json = try buildEventJson(allocator, ev);
            defer allocator.free(json);
            try out.print("{s}\n", .{json});
        },
        .json_values => try out.print("{s}\n", .{event.value}),
        .default => {
            const t = if (event.topic) |tp| tp else "";
            if (t.len > 0) {
                if (event.key) |k| try out.print("[{s}:{d}] key={s} {s}\n", .{ t, event.offset, k, event.value }) else try out.print("[{s}:{d}] {s}\n", .{ t, event.offset, event.value });
            } else {
                if (event.key) |k| try out.print("[{d}] key={s} {s}\n", .{ event.offset, k, event.value }) else try out.print("[{d}] {s}\n", .{ event.offset, event.value });
            }
        },
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
                .{ .name = "json-values", .takes_value = false, .description = "Print only JSON values", .conflicts = &.{"json"} },
                .{ .name = "json", .takes_value = false, .description = "Output full event objects as NDJSON", .conflicts = &.{"json-values"} },
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
                    .flags = &.{
                        .{ .name = "json", .takes_value = false, .description = "Output execution records as a JSON array" },
                    },
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

// --- sub --json (air/v0.1/sub-json-output.org) ------------------------------

fn renderEvent(buf: []u8, event: ever.client.Event, mode: PrintMode, fallback_topic: []const u8) ![]const u8 {
    var w = std.Io.Writer.fixed(buf);
    try printEvent(std.testing.allocator, &w, event, mode, fallback_topic);
    return w.buffered();
}

test "sub --json: envelope round-trips through a JSON parser" {
    var buf: [512]u8 = undefined;
    const keyed: ever.client.Event = .{
        .offset = 161,
        .timestamp = 1751500002789,
        .key = "worker-3",
        .value = "{\"n\":3}",
        .topic = "wutest.wever.subtest",
    };
    const line = try renderEvent(&buf, keyed, .json, "fallback.unused");
    try std.testing.expect(line.len > 0 and line[line.len - 1] == '\n');

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, line[0 .. line.len - 1], .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    // Per-event topic from the server wins over the fallback.
    try std.testing.expectEqualStrings("wutest.wever.subtest", obj.get("topic").?.string);
    try std.testing.expectEqual(@as(i64, 161), obj.get("offset").?.integer);
    try std.testing.expectEqual(@as(i64, 1751500002789), obj.get("timestamp").?.integer);
    try std.testing.expectEqualStrings("worker-3", obj.get("key").?.string);
    // value is ALWAYS an escaped JSON string; a JSON payload parses from it.
    const value = obj.get("value").?.string;
    try std.testing.expectEqualStrings("{\"n\":3}", value);
    var inner = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, value, .{});
    defer inner.deinit();
    try std.testing.expectEqual(@as(i64, 3), inner.value.object.get("n").?.integer);
}

test "sub --json: hostile payloads escape; unkeyed emits key null; exact-topic fallback" {
    // Quotes, backslash, newline, tab, and a raw control character.
    const hostile = "he said \"hi\"\\\nline2\ttab\x01ctl";
    const ev: ever.client.Event = .{
        .offset = 0,
        .timestamp = 5,
        .key = null,
        .value = hostile,
        .topic = null, // exact-topic fetch: server omits per-event topic
    };
    var buf: [512]u8 = undefined;
    const line = try renderEvent(&buf, ev, .json, "exact.topic");

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, line[0 .. line.len - 1], .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    // Never "": populated from the subscribed topic name.
    try std.testing.expectEqualStrings("exact.topic", obj.get("topic").?.string);
    try std.testing.expect(obj.get("key").? == .null);
    // Non-JSON payload round-trips the exact bytes through the string form.
    try std.testing.expectEqualStrings(hostile, obj.get("value").?.string);
}

test "sub regression: default and --json-values output byte-identical to pre-change" {
    var buf: [256]u8 = undefined;
    const keyed: ever.client.Event = .{
        .offset = 42,
        .timestamp = 1751500000123,
        .key = "worker-3",
        .value = "{\"n\":3}",
        .topic = "a.b",
    };
    const unkeyed_no_topic: ever.client.Event = .{
        .offset = 7,
        .timestamp = 0,
        .key = null,
        .value = "plain payload",
        .topic = null,
    };
    // Default human format — unchanged (fallback topic must NOT leak in).
    try std.testing.expectEqualStrings("[a.b:42] key=worker-3 {\"n\":3}\n", try renderEvent(&buf, keyed, .default, "a.b"));
    try std.testing.expectEqualStrings("[7] plain payload\n", try renderEvent(&buf, unkeyed_no_topic, .default, "x.y"));
    // --json-values — bare payload per line, unchanged.
    try std.testing.expectEqualStrings("{\"n\":3}\n", try renderEvent(&buf, keyed, .json_values, "a.b"));
    try std.testing.expectEqualStrings("plain payload\n", try renderEvent(&buf, unkeyed_no_topic, .json_values, "x.y"));
}

// From air/v0.1/hook-list-legibility.org: `Command` is the only
// unbounded-width field; rendered last and un-padded, a long command must
// not shift any preceding column out of alignment with the header.
test "hook list: 60-char command keeps header and row columns aligned" {
    var header_buf: [256]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buf, hook_list_row_fmt, .{
        "ID", "NAME", "Pattern", "Pending", "Fired", "Command",
    });

    const long_cmd = "sh -c 'cat >> /tmp/wutest-wever-handler-output-captured.log'";
    try std.testing.expectEqual(@as(usize, 60), long_cmd.len); // 60-char command
    var row_buf: [256]u8 = undefined;
    const row = try std.fmt.bufPrint(&row_buf, hook_list_row_fmt, .{
        "#47", "wutest-wever-h1", "wutest.wever.subtest", "1000+", "0", long_cmd,
    });

    // Every fixed-width column starts at the same byte position in header
    // and row; the un-padded Command column starts where the header says.
    const col = struct {
        fn start(line: []const u8, needle: []const u8) usize {
            return std.mem.indexOf(u8, line, needle).?;
        }
    };
    try std.testing.expectEqual(col.start(header, "NAME"), col.start(row, "wutest-wever-h1"));
    try std.testing.expectEqual(col.start(header, "Pattern"), col.start(row, "wutest.wever.subtest"));
    try std.testing.expectEqual(col.start(header, "Pending"), col.start(row, "1000+"));
    try std.testing.expectEqual(col.start(header, "Fired"), col.start(row, "0 "));
    try std.testing.expectEqual(col.start(header, "Command"), col.start(row, long_cmd));
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

// ── hook logs --json (air/v0.1/hook-logs-json.org) ─────────────────────────

const sample_hook_log_named =
    "=== Hook Execution ===\n" ++
    "Hook:      #47 (wutest-h1)\n" ++
    "Pattern:   wutest.wever.subtest\n" ++
    "Command:   sh -c cat >> /tmp/a.log\n" ++
    "Topic:     wutest.wever.subtest\n" ++
    "Offset:    162\n" ++
    "Timestamp: 2026-06-30 09:14:03\n" ++
    "Payload:   {\"topic\":\"wutest.wever.subtest\",\"offset\":162,\"timestamp\":1751274843101,\"key\":null,\"value\":\"{\\\"n\\\":4}\"}\n" ++
    "===\n" ++
    "\n" ++
    "handler output line\nwith \"quotes\"\n";

test "hook logs header parse: named hook" {
    const p = parseHookLogContent(sample_hook_log_named).?;
    try std.testing.expectEqual(@as(u64, 47), p.hook_id);
    try std.testing.expectEqualStrings("wutest-h1", p.hook_name.?);
    try std.testing.expectEqualStrings("wutest.wever.subtest", p.pattern);
    try std.testing.expectEqualStrings("sh -c cat >> /tmp/a.log", p.command);
    try std.testing.expectEqualStrings("wutest.wever.subtest", p.topic);
    try std.testing.expectEqual(@as(u64, 162), p.offset);
    try std.testing.expect(std.mem.startsWith(u8, p.payload, "{\"topic\""));
    try std.testing.expectEqualStrings("handler output line\nwith \"quotes\"\n", p.output);
}

test "hook logs header parse: unnamed hook" {
    const content =
        "=== Hook Execution ===\n" ++
        "Hook:      #3\n" ++
        "Pattern:   a.b\n" ++
        "Command:   true\n" ++
        "Topic:     a.b\n" ++
        "Offset:    0\n" ++
        "Timestamp: 2026-06-30 09:14:03\n" ++
        "Payload:   {}\n" ++
        "===\n" ++
        "\n";
    const p = parseHookLogContent(content).?;
    try std.testing.expectEqual(@as(u64, 3), p.hook_id);
    try std.testing.expect(p.hook_name == null);
    try std.testing.expectEqual(@as(u64, 0), p.offset);
    try std.testing.expectEqualStrings("", p.output);
}

test "hook logs header parse: non-conforming content yields null" {
    try std.testing.expect(parseHookLogContent("") == null);
    try std.testing.expect(parseHookLogContent("plain old log output\n") == null);
    try std.testing.expect(parseHookLogContent("=== Hook Execution ===\ntruncated") == null);
}

test "executedAtFromLogPath extracts epoch-ms from the filename" {
    try std.testing.expectEqual(@as(?i64, 1751274843123), executedAtFromLogPath("/srv/ever/data/hooks/47-1751274843123.log"));
    try std.testing.expectEqual(@as(?i64, 5), executedAtFromLogPath("2-5.log"));
    try std.testing.expect(executedAtFromLogPath("not-a-log") == null);
    try std.testing.expect(executedAtFromLogPath("/x/47.log") == null);
}

fn emitHookLogsJsonToString(
    alloc: std.mem.Allocator,
    wire_hook_id: u64,
    log_path: []const u8,
    content: []const u8,
) ![]u8 {
    var aw = std.Io.Writer.Allocating.init(alloc);
    defer aw.deinit();
    try emitHookLogsJson(alloc, &aw.writer, wire_hook_id, log_path, content);
    return alloc.dupe(u8, aw.written());
}

test "hook logs --json emit: full record round-trips through std.json" {
    const alloc = std.testing.allocator;
    const out = try emitHookLogsJsonToString(alloc, 47, "/data/hooks/47-1751274843123.log", sample_hook_log_named);
    defer alloc.free(out);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out, .{});
    defer parsed.deinit();
    const arr = parsed.value.array;
    try std.testing.expectEqual(@as(usize, 1), arr.items.len);
    const rec = arr.items[0].object;
    try std.testing.expectEqual(@as(i64, 47), rec.get("hook_id").?.integer);
    try std.testing.expectEqualStrings("wutest-h1", rec.get("hook_name").?.string);
    try std.testing.expectEqualStrings("wutest.wever.subtest", rec.get("pattern").?.string);
    try std.testing.expectEqualStrings("sh -c cat >> /tmp/a.log", rec.get("command").?.string);
    try std.testing.expectEqualStrings("wutest.wever.subtest", rec.get("topic").?.string);
    try std.testing.expectEqual(@as(i64, 162), rec.get("offset").?.integer);
    try std.testing.expectEqual(@as(i64, 1751274843123), rec.get("executed_at").?.integer);
    // Payload embedded verbatim as an object; .payload.value is a string.
    const payload = rec.get("payload").?.object;
    try std.testing.expectEqualStrings("wutest.wever.subtest", payload.get("topic").?.string);
    try std.testing.expectEqual(@as(i64, 162), payload.get("offset").?.integer);
    try std.testing.expectEqualStrings("{\"n\":4}", payload.get("value").?.string);
    try std.testing.expectEqualStrings("handler output line\nwith \"quotes\"\n", rec.get("output").?.string);
    try std.testing.expectEqualStrings("/data/hooks/47-1751274843123.log", rec.get("log_path").?.string);
}

test "hook logs --json emit: degraded record for pre-change content" {
    const alloc = std.testing.allocator;
    const content = "old-style log content\nno header here\n";
    const out = try emitHookLogsJsonToString(alloc, 9, "/data/hooks/9-123.log", content);
    defer alloc.free(out);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out, .{});
    defer parsed.deinit();
    const rec = parsed.value.array.items[0].object;
    try std.testing.expect(rec.get("hook_id").? == .null);
    try std.testing.expect(rec.get("hook_name").? == .null);
    try std.testing.expect(rec.get("pattern").? == .null);
    try std.testing.expect(rec.get("command").? == .null);
    try std.testing.expect(rec.get("topic").? == .null);
    try std.testing.expect(rec.get("offset").? == .null);
    try std.testing.expect(rec.get("payload").? == .null);
    try std.testing.expectEqual(@as(i64, 123), rec.get("executed_at").?.integer);
    try std.testing.expectEqualStrings(content, rec.get("output").?.string);
    try std.testing.expectEqualStrings("/data/hooks/9-123.log", rec.get("log_path").?.string);
}

test "hook logs --json emit: wire hook ID mismatch degrades" {
    const alloc = std.testing.allocator;
    const out = try emitHookLogsJsonToString(alloc, 5, "/data/hooks/5-123.log", sample_hook_log_named);
    defer alloc.free(out);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out, .{});
    defer parsed.deinit();
    const rec = parsed.value.array.items[0].object;
    try std.testing.expect(rec.get("hook_id").? == .null);
    try std.testing.expectEqualStrings(sample_hook_log_named, rec.get("output").?.string);
}

test "hook logs --json emit: invalid payload line degrades" {
    const alloc = std.testing.allocator;
    const content =
        "=== Hook Execution ===\n" ++
        "Hook:      #3\n" ++
        "Pattern:   a.b\n" ++
        "Command:   true\n" ++
        "Topic:     a.b\n" ++
        "Offset:    0\n" ++
        "Timestamp: 2026-06-30 09:14:03\n" ++
        "Payload:   not json at all\n" ++
        "===\n" ++
        "\n";
    const out = try emitHookLogsJsonToString(alloc, 3, "/data/hooks/3-123.log", content);
    defer alloc.free(out);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out, .{});
    defer parsed.deinit();
    const rec = parsed.value.array.items[0].object;
    try std.testing.expect(rec.get("hook_id").? == .null);
    try std.testing.expect(rec.get("payload").? == .null);
    try std.testing.expectEqualStrings(content, rec.get("output").?.string);
}
