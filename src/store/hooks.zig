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
    once: bool = false,
    env: ?[]const []const u8 = null,
};

/// JSON-serializable hook for persistence.
const HookJson = struct {
    id: u64,
    pattern: []const u8,
    command: []const []const u8,
    cwd: []const u8,
    cursor: u64,
    created_at: i64,
    once: bool = false,
    env: ?[]const []const u8 = null,
};

const HookFileJson = struct {
    hooks: []const HookJson,
    next_id: u64,
};

/// Thread-safe registry of hooks. Persists to hooks.json in the data directory.
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
        if (hook.env) |env| {
            for (env) |e| self.allocator.free(e);
            self.allocator.free(env);
        }
    }

    /// Add a hook. Returns the assigned ID.
    pub fn add(self: *HookTable, pattern: []const u8, command: []const []const u8, cwd: []const u8) !u64 {
        return self.addFull(pattern, command, cwd, false, null);
    }

    /// Add a hook with full options (once, env). Returns the assigned ID.
    pub fn addFull(self: *HookTable, pattern: []const u8, command: []const []const u8, cwd: []const u8, once: bool, env: ?[]const []const u8) !u64 {
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

        // Deep copy env if present
        const env_copy: ?[]const []const u8 = if (env) |e| blk: {
            const ec = try self.allocator.alloc([]const u8, e.len);
            var env_copied: usize = 0;
            errdefer {
                for (ec[0..env_copied]) |ev| self.allocator.free(ev);
                self.allocator.free(ec);
            }
            for (e, 0..) |item, j| {
                ec[j] = try self.allocator.dupe(u8, item);
                env_copied = j + 1;
            }
            break :blk ec;
        } else null;

        try self.hooks.append(self.allocator, .{
            .id = id,
            .pattern = pattern_copy,
            .command = cmd_copy,
            .cwd = cwd_copy,
            .cursor = 0,
            .created_at = getMilliTimestamp(),
            .once = once,
            .env = env_copy,
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

    /// Get a deep-copied snapshot of hooks for the daemon.
    /// Caller owns the returned slice and all string data within it.
    /// Free with `freeHookSnapshot`.
    pub fn snapshot(self: *HookTable, allocator: Allocator) ![]Hook {
        self.lock();
        defer self.mutex.unlock();
        const result = try allocator.alloc(Hook, self.hooks.items.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |h| freeHookCopy(allocator, h);
            allocator.free(result);
        }
        for (self.hooks.items, 0..) |hook, i| {
            result[i] = try deepCopyHook(allocator, hook);
            initialized = i + 1;
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

            // Deep copy env if present
            const env_copy: ?[]const []const u8 = if (h.env) |e| blk: {
                const ec = try self.allocator.alloc([]const u8, e.len);
                var env_copied: usize = 0;
                errdefer {
                    for (ec[0..env_copied]) |ev| self.allocator.free(ev);
                    self.allocator.free(ec);
                }
                for (e, 0..) |item, j| {
                    ec[j] = try self.allocator.dupe(u8, item);
                    env_copied = j + 1;
                }
                break :blk ec;
            } else null;

            try self.hooks.append(self.allocator, .{
                .id = h.id,
                .pattern = pattern_copy,
                .command = cmd_copy,
                .cwd = cwd_copy,
                .cursor = h.cursor,
                .created_at = h.created_at,
                .once = h.once,
                .env = env_copy,
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
                .once = hook.once,
                .env = hook.env,
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

/// Free a snapshot returned by `HookTable.snapshot`. Frees all deep-copied
/// string data and the slice itself.
pub fn freeHookSnapshot(allocator: Allocator, hooks: []Hook) void {
    for (hooks) |hook| freeHookCopy(allocator, hook);
    allocator.free(hooks);
}

fn deepCopyHook(allocator: Allocator, hook: Hook) !Hook {
    const pattern = try allocator.dupe(u8, hook.pattern);
    errdefer allocator.free(pattern);

    const cmd = try allocator.alloc([]const u8, hook.command.len);
    var cmd_copied: usize = 0;
    errdefer {
        for (cmd[0..cmd_copied]) |c| allocator.free(c);
        allocator.free(cmd);
    }
    for (hook.command, 0..) |arg, i| {
        cmd[i] = try allocator.dupe(u8, arg);
        cmd_copied = i + 1;
    }

    const cwd = try allocator.dupe(u8, hook.cwd);
    errdefer allocator.free(cwd);

    const env_copy: ?[]const []const u8 = if (hook.env) |e| blk: {
        const ec = try allocator.alloc([]const u8, e.len);
        var env_copied: usize = 0;
        errdefer {
            for (ec[0..env_copied]) |ev| allocator.free(ev);
            allocator.free(ec);
        }
        for (e, 0..) |item, j| {
            ec[j] = try allocator.dupe(u8, item);
            env_copied = j + 1;
        }
        break :blk ec;
    } else null;

    return .{
        .id = hook.id,
        .pattern = pattern,
        .command = cmd,
        .cwd = cwd,
        .cursor = hook.cursor,
        .created_at = hook.created_at,
        .once = hook.once,
        .env = env_copy,
    };
}

fn freeHookCopy(allocator: Allocator, hook: Hook) void {
    allocator.free(hook.pattern);
    for (hook.command) |arg| allocator.free(arg);
    allocator.free(hook.command);
    allocator.free(hook.cwd);
    if (hook.env) |env| {
        for (env) |e| allocator.free(e);
        allocator.free(env);
    }
}

// ── Hook Daemon ─────────────────────────────────────────────────────────────

/// The hook daemon runs as a thread alongside the store. It polls the
/// TopicManager for new events matching registered hooks and executes
/// their commands via fork/exec.
/// Background daemon that polls for new events and executes matching hooks
/// via fork/exec. Runs on a dedicated thread.
pub const HookDaemon = struct {
    allocator: Allocator,
    hook_table: *HookTable,
    topic_manager: *TopicManager,
    shutdown: std.atomic.Value(bool),
    envp: [*:null]const ?[*:0]const u8,
    thread: ?std.Thread,

    /// Create a new HookDaemon. Call `start()` to begin polling.
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

    /// Spawn the daemon polling thread.
    pub fn start(self: *HookDaemon) !void {
        self.thread = try std.Thread.spawn(.{}, daemonLoop, .{self});
    }

    /// Signal shutdown and join the daemon thread.
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
        defer freeHookSnapshot(self.allocator, hooks);

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

            // One-shot hooks: remove after first event execution
            if (hook.once) {
                self.hook_table.remove(hook.id) catch {};
                std.debug.print("Hook #{d} (once) auto-removed after firing.\n", .{hook.id});
                return;
            }
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

        // Prepend custom env vars from hook config
        if (hook.env) |env| {
            for (env) |env_pair| {
                // env_pair is "KEY=VAL" — validate KEY, shell-escape the value
                if (std.mem.indexOfScalar(u8, env_pair, '=')) |eq_pos| {
                    const key = env_pair[0..eq_pos];
                    // Validate key: must be non-empty, alphanumeric + underscore
                    if (key.len == 0) continue;
                    var valid_key = true;
                    for (key) |kc| {
                        if (!((kc >= 'a' and kc <= 'z') or (kc >= 'A' and kc <= 'Z') or
                            (kc >= '0' and kc <= '9') or kc == '_'))
                        {
                            valid_key = false;
                            break;
                        }
                    }
                    if (!valid_key) continue;
                    // First char must not be a digit
                    if (key[0] >= '0' and key[0] <= '9') continue;

                    try shell_cmd.appendSlice(self.allocator, key);
                    try shell_cmd.appendSlice(self.allocator, "='");
                    for (env_pair[eq_pos + 1 ..]) |c| {
                        if (c == '\'') {
                            try shell_cmd.appendSlice(self.allocator, "'\\''");
                        } else {
                            try shell_cmd.append(self.allocator, c);
                        }
                    }
                    try shell_cmd.appendSlice(self.allocator, "' ");
                }
            }
        }

        try shell_cmd.appendSlice(self.allocator, "EVER_TOPIC='");
        try appendShellEscaped(&shell_cmd, self.allocator, event.topic);
        try shell_cmd.appendSlice(self.allocator, "' EVER_OFFSET='");
        try shell_cmd.appendSlice(self.allocator, offset_str);
        try shell_cmd.appendSlice(self.allocator, "' EVER_TIMESTAMP='");
        try shell_cmd.appendSlice(self.allocator, ts_str);
        try shell_cmd.appendSlice(self.allocator, "' EVER_KEY='");
        if (event.key) |k| try appendShellEscaped(&shell_cmd, self.allocator, k);
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

        // Null-terminate the shell command for execve
        const cmd_z = try self.allocator.allocSentinel(u8, shell_cmd.items.len, 0);
        defer self.allocator.free(cmd_z);
        @memcpy(cmd_z[0..shell_cmd.items.len], shell_cmd.items);

        // Create pipe for stdin delivery
        var pipe_fds: [2]i32 = undefined;
        const pipe_rc = std.os.linux.pipe2(&pipe_fds, .{ .CLOEXEC = true });
        if (@as(isize, @bitCast(pipe_rc)) < 0) return error.PipeFailed;

        // fork + exec
        const pid = std.os.linux.fork();
        const pid_i: isize = @bitCast(pid);

        if (pid_i < 0) {
            _ = std.os.linux.close(pipe_fds[0]);
            _ = std.os.linux.close(pipe_fds[1]);
            return error.ForkFailed;
        }

        if (pid_i == 0) {
            // Child: redirect stdin from pipe read end
            _ = std.os.linux.close(pipe_fds[1]); // close write end
            _ = std.os.linux.dup2(pipe_fds[0], 0); // stdin = pipe read end
            _ = std.os.linux.close(pipe_fds[0]);

            const sh: [*:0]const u8 = "/bin/sh";
            const dash_c: [*:0]const u8 = "-c";
            const argv = [_]?[*:0]const u8{ sh, dash_c, cmd_z.ptr, null };
            _ = std.os.linux.execve(sh, @ptrCast(&argv), self.envp);
            std.os.linux.exit(127);
        }

        // Parent: write JSON to pipe write end, then close for EOF
        _ = std.os.linux.close(pipe_fds[0]); // close read end
        writeAllFd(pipe_fds[1], json);
        _ = std.os.linux.close(pipe_fds[1]); // EOF for child's stdin

        // Wait for child
        var status: u32 = 0;
        _ = std.os.linux.waitpid(@intCast(pid), &status, 0);
        const exit_code = (status >> 8) & 0xFF;
        if (exit_code != 0) {
            std.debug.print("Hook #{d} command exited with status {d}\n", .{ hook.id, exit_code });
        }
    }
};

/// Write all bytes to a raw file descriptor, retrying on partial writes.
fn writeAllFd(fd: i32, data: []const u8) void {
    var written: usize = 0;
    while (written < data.len) {
        const rc = std.os.linux.write(fd, data[written..].ptr, data[written..].len);
        const n: isize = @bitCast(rc);
        if (n <= 0) break;
        written += @intCast(n);
    }
}

/// Append a string with single-quote escaping for safe shell interpolation.
/// Inside single quotes, the only character that needs escaping is the single quote itself.
fn appendShellEscaped(list: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    for (s) |c| {
        if (c == '\'') {
            try list.appendSlice(allocator, "'\\''");
        } else {
            try list.append(allocator, c);
        }
    }
}

fn buildEventJsonFromStore(allocator: Allocator, event: store.Event) ![]u8 {
    var json: std.ArrayList(u8) = .empty;
    errdefer json.deinit(allocator);

    try json.appendSlice(allocator, "{\"topic\":\"");
    try appendJsonEscaped(&json, allocator, event.topic);
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

/// Append a string with JSON escaping for all special and control characters.
fn appendJsonEscaped(json: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
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
