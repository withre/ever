//! Hook table — persistent storage for server-side hooks.
//!
//! Hooks are stored in hooks.json in the data directory.
//! Each hook has a pattern, command, cwd, and cursor tracking
//! the last processed offset.

const std = @import("std");
const Allocator = std.mem.Allocator;
const topic_mod = @import("topic.zig");
const TopicManager = topic_mod.TopicManager;
const store = @import("store.zig");

pub const Hook = struct {
    id: u64,
    pattern: []const u8,
    command: []const []const u8,
    cwd: []const u8,
    cursor: u64,
    created_at: i64,
};

/// JSON-serializable hook for persistence.
const HookJson = struct {
    id: u64,
    pattern: []const u8,
    command: []const []const u8,
    cwd: []const u8,
    cursor: u64,
    created_at: i64,
};

const HookFileJson = struct {
    hooks: []const HookJson,
    next_id: u64,
};

pub const HookTable = struct {
    allocator: Allocator,
    hooks: std.ArrayList(Hook),
    next_id: u64,
    data_dir: []const u8,
    mutex: std.atomic.Mutex,
    /// Flag to signal the daemon to reload hooks
    reload_signal: std.atomic.Value(bool),

    pub fn init(allocator: Allocator, data_dir: []const u8) !HookTable {
        var table = HookTable{
            .allocator = allocator,
            .hooks = .empty,
            .next_id = 1,
            .data_dir = try allocator.dupe(u8, data_dir),
            .mutex = .unlocked,
            .reload_signal = std.atomic.Value(bool).init(false),
        };
        try table.load();
        return table;
    }

    pub fn deinit(self: *HookTable) void {
        for (self.hooks.items) |hook| {
            self.freeHook(hook);
        }
        self.hooks.deinit(self.allocator);
        self.allocator.free(self.data_dir);
    }

    fn lock(self: *HookTable) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn freeHook(self: *HookTable, hook: Hook) void {
        self.allocator.free(hook.pattern);
        for (hook.command) |arg| self.allocator.free(arg);
        self.allocator.free(hook.command);
        self.allocator.free(hook.cwd);
    }

    /// Add a hook. Returns the assigned ID.
    pub fn add(self: *HookTable, pattern: []const u8, command: []const []const u8, cwd: []const u8) !u64 {
        self.lock();
        defer self.mutex.unlock();

        const id = self.next_id;
        self.next_id += 1;

        // Deep copy all fields
        const pattern_copy = try self.allocator.dupe(u8, pattern);
        errdefer self.allocator.free(pattern_copy);

        const cmd_copy = try self.allocator.alloc([]const u8, command.len);
        errdefer self.allocator.free(cmd_copy);
        var copied: usize = 0;
        errdefer for (cmd_copy[0..copied]) |c| self.allocator.free(c);
        for (command, 0..) |arg, i| {
            cmd_copy[i] = try self.allocator.dupe(u8, arg);
            copied = i + 1;
        }

        const cwd_copy = try self.allocator.dupe(u8, cwd);
        errdefer self.allocator.free(cwd_copy);

        try self.hooks.append(self.allocator, .{
            .id = id,
            .pattern = pattern_copy,
            .command = cmd_copy,
            .cwd = cwd_copy,
            .cursor = 0,
            .created_at = getMilliTimestamp(),
        });

        try self.saveLocked();
        self.reload_signal.store(true, .release);
        return id;
    }

    /// Remove a hook by ID.
    pub fn remove(self: *HookTable, id: u64) !void {
        self.lock();
        defer self.mutex.unlock();

        for (self.hooks.items, 0..) |hook, i| {
            if (hook.id == id) {
                self.freeHook(hook);
                _ = self.hooks.swapRemove(i);
                try self.saveLocked();
                return;
            }
        }
        return error.NotFound;
    }

    /// List all hooks (returns references, caller must not free).
    pub fn list(self: *HookTable) []const Hook {
        self.lock();
        defer self.mutex.unlock();
        return self.hooks.items;
    }

    /// Update a hook's cursor. Called by the daemon after processing events.
    pub fn updateCursor(self: *HookTable, id: u64, new_cursor: u64) void {
        self.lock();
        defer self.mutex.unlock();
        for (self.hooks.items) |*hook| {
            if (hook.id == id) {
                hook.cursor = new_cursor;
                self.saveLocked() catch {};
                return;
            }
        }
    }

    /// Get a snapshot of hooks for the daemon (caller owns returned slice).
    pub fn snapshot(self: *HookTable, allocator: Allocator) ![]Hook {
        self.lock();
        defer self.mutex.unlock();
        const result = try allocator.alloc(Hook, self.hooks.items.len);
        for (self.hooks.items, 0..) |hook, i| {
            result[i] = hook;
        }
        return result;
    }

    // ── Persistence ─────────────────────────────────────────────────────

    fn hooksFilePath(self: *HookTable) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/hooks.json", .{self.data_dir});
    }

    fn hooksTmpPath(self: *HookTable) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/hooks.json.tmp", .{self.data_dir});
    }

    fn load(self: *HookTable) !void {
        const path = try self.hooksFilePath();
        defer self.allocator.free(path);

        const path_z = try self.allocator.allocSentinel(u8, path.len, 0);
        defer self.allocator.free(path_z);
        @memcpy(path_z[0..path.len], path);

        // Try to read the file
        const fd_rc = std.os.linux.open(path_z.ptr, .{ .ACCMODE = .RDONLY }, 0);
        const fd_i: isize = @bitCast(fd_rc);
        if (fd_i < 0) return; // File doesn't exist, start fresh

        const fd: std.posix.fd_t = @intCast(fd_rc);
        defer _ = std.os.linux.close(fd);

        // Read up to 1MB
        const buf = try self.allocator.alloc(u8, 1024 * 1024);
        defer self.allocator.free(buf);

        var total: usize = 0;
        while (total < buf.len) {
            const rc = std.os.linux.read(fd, buf[total..].ptr, buf[total..].len);
            const n: isize = @bitCast(rc);
            if (n <= 0) break;
            total += @intCast(n);
        }

        if (total == 0) return;

        const parsed = std.json.parseFromSlice(HookFileJson, self.allocator, buf[0..total], .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return;
        defer parsed.deinit();

        self.next_id = parsed.value.next_id;

        for (parsed.value.hooks) |h| {
            const pattern_copy = try self.allocator.dupe(u8, h.pattern);
            errdefer self.allocator.free(pattern_copy);

            const cmd_copy = try self.allocator.alloc([]const u8, h.command.len);
            errdefer self.allocator.free(cmd_copy);
            var copied: usize = 0;
            errdefer for (cmd_copy[0..copied]) |c| self.allocator.free(c);
            for (h.command, 0..) |arg, i| {
                cmd_copy[i] = try self.allocator.dupe(u8, arg);
                copied = i + 1;
            }

            const cwd_copy = try self.allocator.dupe(u8, h.cwd);
            errdefer self.allocator.free(cwd_copy);

            try self.hooks.append(self.allocator, .{
                .id = h.id,
                .pattern = pattern_copy,
                .command = cmd_copy,
                .cwd = cwd_copy,
                .cursor = h.cursor,
                .created_at = h.created_at,
            });
        }
    }

    /// Save hooks to disk atomically. Must be called with lock held.
    fn saveLocked(self: *HookTable) !void {
        // Build JSON data
        const hook_jsons = try self.allocator.alloc(HookJson, self.hooks.items.len);
        defer self.allocator.free(hook_jsons);

        for (self.hooks.items, 0..) |hook, i| {
            hook_jsons[i] = .{
                .id = hook.id,
                .pattern = hook.pattern,
                .command = hook.command,
                .cwd = hook.cwd,
                .cursor = hook.cursor,
                .created_at = hook.created_at,
            };
        }

        const file_json = HookFileJson{
            .hooks = hook_jsons,
            .next_id = self.next_id,
        };

        const json_bytes = try std.json.Stringify.valueAlloc(self.allocator, file_json, .{});
        defer self.allocator.free(json_bytes);

        // Write to temp file, then rename
        const tmp_path = try self.hooksTmpPath();
        defer self.allocator.free(tmp_path);
        const final_path = try self.hooksFilePath();
        defer self.allocator.free(final_path);

        const tmp_z = try self.allocator.allocSentinel(u8, tmp_path.len, 0);
        defer self.allocator.free(tmp_z);
        @memcpy(tmp_z[0..tmp_path.len], tmp_path);

        const final_z = try self.allocator.allocSentinel(u8, final_path.len, 0);
        defer self.allocator.free(final_z);
        @memcpy(final_z[0..final_path.len], final_path);

        // Write temp file
        const fd_rc = std.os.linux.open(tmp_z.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o644);
        const fd_i: isize = @bitCast(fd_rc);
        if (fd_i < 0) return error.Unexpected;
        const fd: std.posix.fd_t = @intCast(fd_rc);

        var written: usize = 0;
        while (written < json_bytes.len) {
            const rc = std.os.linux.write(fd, json_bytes[written..].ptr, json_bytes[written..].len);
            const n: isize = @bitCast(rc);
            if (n <= 0) {
                _ = std.os.linux.close(fd);
                return error.Unexpected;
            }
            written += @intCast(n);
        }
        _ = std.os.linux.close(fd);

        // Atomic rename
        const rc = std.os.linux.rename(tmp_z.ptr, final_z.ptr);
        const rc_i: isize = @bitCast(rc);
        if (rc_i < 0) return error.Unexpected;
    }
};

// ── Hook Daemon ─────────────────────────────────────────────────────────────

/// The hook daemon runs as a thread alongside the store. It polls the
/// TopicManager for new events matching registered hooks and executes
/// their commands via fork/exec.
pub const HookDaemon = struct {
    allocator: Allocator,
    hook_table: *HookTable,
    topic_manager: *TopicManager,
    shutdown: std.atomic.Value(bool),
    envp: [*:null]const ?[*:0]const u8,
    thread: ?std.Thread,

    pub fn init(
        allocator: Allocator,
        hook_table: *HookTable,
        topic_manager: *TopicManager,
        envp: [*:null]const ?[*:0]const u8,
    ) HookDaemon {
        return .{
            .allocator = allocator,
            .hook_table = hook_table,
            .topic_manager = topic_manager,
            .shutdown = std.atomic.Value(bool).init(false),
            .envp = envp,
            .thread = null,
        };
    }

    pub fn start(self: *HookDaemon) !void {
        self.thread = try std.Thread.spawn(.{}, daemonLoop, .{self});
    }

    pub fn stop(self: *HookDaemon) void {
        self.shutdown.store(true, .release);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    fn daemonLoop(self: *HookDaemon) void {
        while (!self.shutdown.load(.acquire)) {
            self.pollAndExecute();

            // Sleep 500ms between polls
            const ts = std.os.linux.timespec{
                .sec = 0,
                .nsec = 500_000_000,
            };
            _ = std.os.linux.nanosleep(&ts, null);
        }
    }

    fn pollAndExecute(self: *HookDaemon) void {
        // Take a snapshot — copies id/pattern/cursor values so we don't hold the
        // hook_table mutex during fetch or fork/exec.
        const hooks = self.hook_table.snapshot(self.allocator) catch return;
        defer self.allocator.free(hooks);

        for (hooks) |hook| {
            self.processHook(hook) catch |err| {
                std.debug.print("Hook #{d} error: {}\n", .{ hook.id, err });
            };
        }
    }

    fn processHook(self: *HookDaemon, hook: Hook) !void {
        const pattern = hook.pattern;
        const is_pattern = std.mem.indexOfScalar(u8, pattern, '*') != null or
            (pattern.len > 0 and pattern[pattern.len - 1] == '.');

        // Fetch new events from the hook's cursor.
        // TopicManager mutex is acquired/released internally — not held across fork/exec.
        const events = if (is_pattern)
            try self.topic_manager.fetchPattern(self.allocator, pattern, hook.cursor, 100)
        else
            try self.topic_manager.fetch(self.allocator, pattern, hook.cursor, 100);
        defer {
            for (events) |e| store.freeEvent(self.allocator, e);
            self.allocator.free(events);
        }

        if (events.len == 0) return;

        // Execute commands and update cursor — no mutex held during fork/exec.
        // updateCursor briefly acquires hook_table mutex only for the cursor write.
        for (events, 0..) |event, i| {
            self.executeHookCommand(hook, event) catch |err| {
                std.debug.print("Hook #{d} command failed: {}\n", .{ hook.id, err });
            };
            self.hook_table.updateCursor(hook.id, hook.cursor + i + 1);
        }
    }

    fn executeHookCommand(self: *HookDaemon, hook: Hook, event: store.Event) !void {
        // Build event JSON for stdin
        const json = try buildEventJsonFromStore(self.allocator, event);
        defer self.allocator.free(json);

        // Build env vars + command string
        var offset_buf: [20]u8 = undefined;
        const offset_str = std.fmt.bufPrint(&offset_buf, "{d}", .{event.offset}) catch "0";
        var ts_buf: [20]u8 = undefined;
        const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{event.timestamp}) catch "0";

        var shell_cmd: std.ArrayList(u8) = .empty;
        defer shell_cmd.deinit(self.allocator);

        // cd to hook's cwd first
        try shell_cmd.appendSlice(self.allocator, "cd '");
        for (hook.cwd) |c| {
            if (c == '\'') {
                try shell_cmd.appendSlice(self.allocator, "'\\''");
            } else {
                try shell_cmd.append(self.allocator, c);
            }
        }
        try shell_cmd.appendSlice(self.allocator, "' && ");

        try shell_cmd.appendSlice(self.allocator, "EVER_TOPIC='");
        try shell_cmd.appendSlice(self.allocator, event.topic);
        try shell_cmd.appendSlice(self.allocator, "' EVER_OFFSET='");
        try shell_cmd.appendSlice(self.allocator, offset_str);
        try shell_cmd.appendSlice(self.allocator, "' EVER_TIMESTAMP='");
        try shell_cmd.appendSlice(self.allocator, ts_str);
        try shell_cmd.appendSlice(self.allocator, "' EVER_KEY='");
        if (event.key) |k| try shell_cmd.appendSlice(self.allocator, k);
        try shell_cmd.appendSlice(self.allocator, "' exec ");

        for (hook.command) |arg| {
            try shell_cmd.append(self.allocator, '\'');
            for (arg) |c| {
                if (c == '\'') {
                    try shell_cmd.appendSlice(self.allocator, "'\\''");
                } else {
                    try shell_cmd.append(self.allocator, c);
                }
            }
            try shell_cmd.appendSlice(self.allocator, "' ");
        }

        // Write JSON to PID-specific temp file for stdin (avoid race with other processes)
        const my_pid = std.os.linux.getpid();
        var stdin_path_buf: [64]u8 = undefined;
        const stdin_path = std.fmt.bufPrint(&stdin_path_buf, "/tmp/.ever-hook-stdin-{d}", .{my_pid}) catch "/tmp/.ever-hook-stdin";
        const stdin_path_z = try self.allocator.allocSentinel(u8, stdin_path.len, 0);
        defer self.allocator.free(stdin_path_z);
        @memcpy(stdin_path_z[0..stdin_path.len], stdin_path);
        {
            const fd_rc = std.os.linux.open(stdin_path_z.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o600);
            const fd_i: isize = @bitCast(fd_rc);
            if (fd_i >= 0) {
                const tfd: i32 = @intCast(fd_rc);
                _ = std.os.linux.write(tfd, json.ptr, json.len);
                _ = std.os.linux.close(tfd);
            }
        }

        const full_cmd = try std.fmt.allocPrint(self.allocator, "{s}< {s}", .{ shell_cmd.items, stdin_path });
        defer self.allocator.free(full_cmd);

        // fork + exec
        const pid = std.os.linux.fork();
        const pid_i: isize = @bitCast(pid);

        if (pid_i < 0) return error.ForkFailed;

        if (pid_i == 0) {
            // Child
            const sh: [*:0]const u8 = "/bin/sh";
            const dash_c: [*:0]const u8 = "-c";
            const cmd_z = self.allocator.allocSentinel(u8, full_cmd.len, 0) catch std.os.linux.exit(127);
            @memcpy(cmd_z[0..full_cmd.len], full_cmd);
            const argv = [_]?[*:0]const u8{ sh, dash_c, cmd_z.ptr, null };
            _ = std.os.linux.execve(sh, @ptrCast(&argv), self.envp);
            std.os.linux.exit(127);
        }

        // Parent: wait
        var status: u32 = 0;
        _ = std.os.linux.waitpid(@intCast(pid), &status, 0);
        const exit_code = (status >> 8) & 0xFF;
        if (exit_code != 0) {
            std.debug.print("Hook #{d} command exited with status {d}\n", .{ hook.id, exit_code });
        }
    }
};

fn buildEventJsonFromStore(allocator: Allocator, event: store.Event) ![]u8 {
    var json: std.ArrayList(u8) = .empty;
    errdefer json.deinit(allocator);

    try json.appendSlice(allocator, "{\"topic\":\"");
    try json.appendSlice(allocator, event.topic);
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
        try json.appendSlice(allocator, k);
        try json.append(allocator, '"');
    } else {
        try json.appendSlice(allocator, ",\"key\":null");
    }
    try json.appendSlice(allocator, ",\"value\":\"");
    for (event.value) |c| switch (c) {
        '"' => try json.appendSlice(allocator, "\\\""),
        '\\' => try json.appendSlice(allocator, "\\\\"),
        '\n' => try json.appendSlice(allocator, "\\n"),
        '\t' => try json.appendSlice(allocator, "\\t"),
        else => try json.append(allocator, c),
    };
    try json.appendSlice(allocator, "\"}");
    return json.toOwnedSlice(allocator);
}

fn getMilliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.REALTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "HookTable add, list, remove" {
    const allocator = std.testing.allocator;

    // Use a temp directory
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Get the path string for the temp dir
    const tmp_path = try std.fmt.allocPrint(allocator, "/tmp/.ever-hook-test-{d}", .{std.os.linux.getpid()});
    defer allocator.free(tmp_path);

    // Create the dir
    const tmp_z = try allocator.allocSentinel(u8, tmp_path.len, 0);
    defer allocator.free(tmp_z);
    @memcpy(tmp_z[0..tmp_path.len], tmp_path);
    _ = std.os.linux.mkdir(tmp_z.ptr, 0o755);

    var table = try HookTable.init(allocator, tmp_path);
    defer table.deinit();

    const cmd = &[_][]const u8{ "echo", "hello" };
    const id1 = try table.add("test.topic", cmd, "/tmp");
    const id2 = try table.add("agent.", cmd, "/home");

    try std.testing.expectEqual(@as(u64, 1), id1);
    try std.testing.expectEqual(@as(u64, 2), id2);

    const hooks = table.list();
    try std.testing.expectEqual(@as(usize, 2), hooks.len);

    try table.remove(id1);
    const hooks2 = table.list();
    try std.testing.expectEqual(@as(usize, 1), hooks2.len);
    try std.testing.expectEqual(@as(u64, 2), hooks2[0].id);

    // Cleanup temp dir
    const hooks_json_z = try allocator.allocSentinel(u8, tmp_path.len + 11, 0);
    defer allocator.free(hooks_json_z);
    @memcpy(hooks_json_z[0..tmp_path.len], tmp_path);
    @memcpy(hooks_json_z[tmp_path.len .. tmp_path.len + 11], "/hooks.json");
    _ = std.os.linux.unlink(hooks_json_z.ptr);
    _ = std.os.linux.rmdir(tmp_z.ptr);
}
