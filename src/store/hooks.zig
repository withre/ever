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

    pub fn init(allocator: Allocator, data_dir: []const u8) !HookTable {
        var table = HookTable{
            .allocator = allocator,
            .hooks = .empty,
            .next_id = 1,
            .data_dir = try allocator.dupe(u8, data_dir),
            .mutex = .unlocked,
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
        freeHookCopy(self.allocator, hook);
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

    /// Return the number of registered hooks. Safe for cross-thread display.
    pub fn count(self: *HookTable) usize {
        self.lock();
        defer self.mutex.unlock();
        return self.hooks.items.len;
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

/// Maximum number of concurrently running hook processes.
pub const MAX_CONCURRENT_HOOKS: usize = 16;
/// Default per-hook timeout in seconds. Processes running longer are killed.
const DEFAULT_HOOK_TIMEOUT_SECS: i64 = 300; // 5 minutes
/// Seconds to wait after SIGTERM before sending SIGKILL on shutdown.
const SHUTDOWN_GRACE_SECS: i64 = 5;

/// Tracks a running child process spawned by the hook daemon.
pub const ProcessEntry = struct {
    hook_id: u64,
    pid: i32,
    pgid: i32,
    start_time: i64,
    log_path: [256]u8,
    log_path_len: u8,
    pattern: [128]u8,
    pattern_len: u8,
    command_display: [128]u8,
    command_display_len: u8,
};

/// Thread-safe table of running hook processes.
/// The daemon owns this; server reads it via snapshot for `hook ps`.
pub const ProcessTable = struct {
    entries: [MAX_CONCURRENT_HOOKS]?ProcessEntry,
    count: usize,
    mutex: std.atomic.Mutex,

    pub fn init() ProcessTable {
        return .{
            .entries = [_]?ProcessEntry{null} ** MAX_CONCURRENT_HOOKS,
            .count = 0,
            .mutex = .unlocked,
        };
    }

    fn lock(self: *ProcessTable) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Register a running child. Returns false if table is full.
    pub fn add(self: *ProcessTable, entry: ProcessEntry) bool {
        self.lock();
        defer self.mutex.unlock();
        for (&self.entries) |*slot| {
            if (slot.* == null) {
                slot.* = entry;
                self.count += 1;
                return true;
            }
        }
        return false;
    }

    /// Remove the entry for a given PID. Returns the removed entry or null.
    pub fn removeByPid(self: *ProcessTable, pid: i32) ?ProcessEntry {
        self.lock();
        defer self.mutex.unlock();
        for (&self.entries) |*slot| {
            if (slot.*) |e| {
                if (e.pid == pid) {
                    const result = e;
                    slot.* = null;
                    self.count -= 1;
                    return result;
                }
            }
        }
        return null;
    }

    /// Return the number of active processes (lock-free snapshot for limit check).
    pub fn activeCount(self: *ProcessTable) usize {
        self.lock();
        defer self.mutex.unlock();
        return self.count;
    }

    /// Copy all active entries into caller-provided buffer.
    /// Returns the number copied.
    pub fn snapshot(self: *ProcessTable, out: []ProcessEntry) usize {
        self.lock();
        defer self.mutex.unlock();
        var n: usize = 0;
        for (self.entries) |slot| {
            if (slot) |e| {
                if (n >= out.len) break;
                out[n] = e;
                n += 1;
            }
        }
        return n;
    }

    /// Kill all tracked process groups with the given signal.
    fn killAll(self: *ProcessTable, sig: std.os.linux.SIG) void {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |slot| {
            if (slot) |e| {
                // Kill entire process group
                _ = std.os.linux.kill(-e.pgid, sig);
            }
        }
    }
};

/// The hook daemon runs as a thread alongside the store. It polls the
/// TopicManager for new events matching registered hooks and executes
/// their commands via fork/exec in the background (non-blocking).
///
/// Child processes run in their own process groups so the daemon can
/// signal an entire tree. stdout/stderr are redirected to log files
/// under `<data-dir>/hooks/`.
pub const HookDaemon = struct {
    allocator: Allocator,
    hook_table: *HookTable,
    topic_manager: *TopicManager,
    shutdown: std.atomic.Value(bool),
    envp: [*:null]const ?[*:0]const u8,
    thread: ?std.Thread,
    process_table: ProcessTable,

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
            .process_table = ProcessTable.init(),
        };
    }

    /// Spawn the daemon polling thread.
    pub fn start(self: *HookDaemon) !void {
        // Ensure hooks log directory exists
        self.ensureLogDir();
        self.thread = try std.Thread.spawn(.{}, daemonLoop, .{self});
    }

    /// Signal shutdown, terminate all running hooks, and join the daemon thread.
    pub fn stop(self: *HookDaemon) void {
        self.shutdown.store(true, .release);
        self.shutdownChildren();
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    /// Return a pointer to the process table so the server can read it.
    pub fn getProcessTable(self: *HookDaemon) *ProcessTable {
        return &self.process_table;
    }

    fn ensureLogDir(self: *HookDaemon) void {
        const path = std.fmt.allocPrint(self.allocator, "{s}/hooks", .{self.hook_table.data_dir}) catch return;
        defer self.allocator.free(path);
        const path_z = self.allocator.allocSentinel(u8, path.len, 0) catch return;
        defer self.allocator.free(path_z);
        @memcpy(path_z[0..path.len], path);
        _ = std.os.linux.mkdir(path_z.ptr, 0o755);
    }

    fn daemonLoop(self: *HookDaemon) void {
        while (!self.shutdown.load(.acquire)) {
            self.reapChildren();
            self.checkTimeouts();
            self.pollAndExecute();

            // Sleep 500ms between polls
            const ts = std.os.linux.timespec{
                .sec = 0,
                .nsec = 500_000_000,
            };
            _ = std.os.linux.nanosleep(&ts, null);
        }
    }

    /// Reap any terminated children with WNOHANG. Updates process table.
    fn reapChildren(self: *HookDaemon) void {
        while (true) {
            var status: u32 = 0;
            const WNOHANG: u32 = 1;
            const pid_raw = std.os.linux.waitpid(-1, &status, WNOHANG);
            const pid_i: isize = @bitCast(pid_raw);
            if (pid_i <= 0) break; // No more children to reap

            const pid: i32 = @intCast(pid_i);
            if (self.process_table.removeByPid(pid)) |entry| {
                const exit_code = (status >> 8) & 0xFF;
                if (exit_code != 0) {
                    logTimestampedFmt("Hook #{d} (pid {d}) exited with status {d}", .{ entry.hook_id, pid, exit_code });
                }
            }
        }
    }

    /// Kill processes that have exceeded the timeout.
    /// Sends SIGTERM to all timed-out processes first, then one grace sleep,
    /// then SIGKILL to any that remain — avoids N×1s blocking.
    fn checkTimeouts(self: *HookDaemon) void {
        const now = getMilliTimestamp();
        var buf: [MAX_CONCURRENT_HOOKS]ProcessEntry = undefined;
        const n = self.process_table.snapshot(&buf);

        // Phase 1: SIGTERM all timed-out processes
        var any_killed = false;
        for (buf[0..n]) |entry| {
            const elapsed_ms = now - entry.start_time;
            if (elapsed_ms > DEFAULT_HOOK_TIMEOUT_SECS * 1000) {
                logTimestampedFmt("Hook #{d} (pid {d}) timed out after {d}s, sending SIGTERM", .{
                    entry.hook_id, entry.pid, @divTrunc(elapsed_ms, 1000),
                });
                _ = std.os.linux.kill(-entry.pgid, .TERM);
                any_killed = true;
            }
        }
        if (!any_killed) return;

        // Phase 2: One grace period, then SIGKILL stragglers
        const ts = std.os.linux.timespec{ .sec = 1, .nsec = 0 };
        _ = std.os.linux.nanosleep(&ts, null);
        for (buf[0..n]) |entry| {
            const elapsed_ms = now - entry.start_time;
            if (elapsed_ms > DEFAULT_HOOK_TIMEOUT_SECS * 1000) {
                _ = std.os.linux.kill(-entry.pgid, .KILL);
            }
        }
    }

    /// SIGTERM all children → wait grace period → SIGKILL stragglers → reap.
    fn shutdownChildren(self: *HookDaemon) void {
        if (self.process_table.activeCount() == 0) return;

        logTimestampedFmt("Shutting down {d} running hook(s)...", .{self.process_table.activeCount()});
        self.process_table.killAll(.TERM);

        // Wait up to SHUTDOWN_GRACE_SECS for children to exit
        var waited: i64 = 0;
        while (waited < SHUTDOWN_GRACE_SECS * 1000) {
            self.reapChildren();
            if (self.process_table.activeCount() == 0) return;
            const ts = std.os.linux.timespec{ .sec = 0, .nsec = 100_000_000 };
            _ = std.os.linux.nanosleep(&ts, null);
            waited += 100;
        }

        // Force kill remaining
        if (self.process_table.activeCount() > 0) {
            logTimestampedFmt("Force-killing {d} remaining hook(s)", .{self.process_table.activeCount()});
            self.process_table.killAll(.KILL);
            const ts = std.os.linux.timespec{ .sec = 1, .nsec = 0 };
            _ = std.os.linux.nanosleep(&ts, null);
            self.reapChildren();
        }
    }

    fn pollAndExecute(self: *HookDaemon) void {
        const hooks = self.hook_table.snapshot(self.allocator) catch return;
        defer freeHookSnapshot(self.allocator, hooks);

        for (hooks) |hook| {
            self.processHook(hook) catch |err| {
                logTimestampedFmt("Hook #{d} error: {}", .{ hook.id, err });
            };
        }
    }

    fn processHook(self: *HookDaemon, hook: Hook) !void {
        const pattern = hook.pattern;
        const is_pattern = std.mem.indexOfScalar(u8, pattern, '*') != null or
            (pattern.len > 0 and pattern[pattern.len - 1] == '.');

        const events = if (is_pattern)
            try self.topic_manager.fetchPattern(self.allocator, pattern, hook.cursor, 100)
        else
            try self.topic_manager.fetch(self.allocator, pattern, hook.cursor, 100);
        defer {
            for (events) |e| store.freeEvent(self.allocator, e);
            self.allocator.free(events);
        }

        if (events.len == 0) return;

        for (events, 0..) |event, i| {
            self.executeHookCommand(hook, event) catch |err| switch (err) {
                error.ConcurrentLimitReached => {
                    // Don't advance cursor — retry these events on next poll cycle.
                    logTimestampedFmt("Hook #{d}: concurrent limit reached, will retry", .{hook.id});
                    return;
                },
                else => {
                    logTimestampedFmt("Hook #{d} command failed: {}", .{ hook.id, err });
                },
            };
            // Cursor is topic-local: fetch() interprets start as the number of
            // events to skip within the topic, so we advance by batch index.
            self.hook_table.updateCursor(hook.id, hook.cursor + i + 1);

            if (hook.once) {
                self.hook_table.remove(hook.id) catch {};
                logTimestampedFmt("Hook #{d} (once) auto-removed after firing.", .{hook.id});
                return;
            }
        }
    }

    fn executeHookCommand(self: *HookDaemon, hook: Hook, event: store.Event) !void {
        // Enforce concurrent limit — return typed error so caller can stop
        // advancing the cursor and retry on the next poll cycle.
        if (self.process_table.activeCount() >= MAX_CONCURRENT_HOOKS) {
            return error.ConcurrentLimitReached;
        }

        const json = try buildEventJsonFromStore(self.allocator, event);
        defer self.allocator.free(json);

        var offset_buf: [20]u8 = undefined;
        const offset_str = std.fmt.bufPrint(&offset_buf, "{d}", .{event.offset}) catch "0";
        var ts_buf: [20]u8 = undefined;
        const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{event.timestamp}) catch "0";

        var shell_cmd: std.ArrayList(u8) = .empty;
        defer shell_cmd.deinit(self.allocator);

        // cd to hook's cwd
        try shell_cmd.appendSlice(self.allocator, "cd '");
        try appendShellEscaped(&shell_cmd, self.allocator, hook.cwd);
        try shell_cmd.appendSlice(self.allocator, "' && ");

        // Custom env vars
        if (hook.env) |env| {
            for (env) |env_pair| {
                if (std.mem.indexOfScalar(u8, env_pair, '=')) |eq_pos| {
                    const key = env_pair[0..eq_pos];
                    if (key.len == 0) continue;
                    if (!isValidEnvKey(key)) continue;
                    try shell_cmd.appendSlice(self.allocator, key);
                    try shell_cmd.appendSlice(self.allocator, "='");
                    try appendShellEscaped(&shell_cmd, self.allocator, env_pair[eq_pos + 1 ..]);
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
            try appendShellEscaped(&shell_cmd, self.allocator, arg);
            try shell_cmd.appendSlice(self.allocator, "' ");
        }

        const cmd_z = try self.allocator.allocSentinel(u8, shell_cmd.items.len, 0);
        defer self.allocator.free(cmd_z);
        @memcpy(cmd_z[0..shell_cmd.items.len], shell_cmd.items);

        // Build log file path: <data-dir>/hooks/<hook-id>-<timestamp>.log
        const now_ms = getMilliTimestamp();
        var log_path_buf: [256]u8 = undefined;
        const log_path = std.fmt.bufPrint(&log_path_buf, "{s}/hooks/{d}-{d}.log", .{
            self.hook_table.data_dir, hook.id, now_ms,
        }) catch return error.PathTooLong;
        const log_path_z = try self.allocator.allocSentinel(u8, log_path.len, 0);
        defer self.allocator.free(log_path_z);
        @memcpy(log_path_z[0..log_path.len], log_path);

        // Open log file for child stdout/stderr
        const log_fd_rc = std.os.linux.open(
            log_path_z.ptr,
            .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true },
            0o644,
        );
        const log_fd_i: isize = @bitCast(log_fd_rc);
        if (log_fd_i < 0) return error.LogFileOpen;
        const log_fd: i32 = @intCast(log_fd_rc);

        // Create pipe for stdin delivery
        var pipe_fds: [2]i32 = undefined;
        const pipe_rc = std.os.linux.pipe2(&pipe_fds, .{ .CLOEXEC = true });
        if (@as(isize, @bitCast(pipe_rc)) < 0) {
            _ = std.os.linux.close(log_fd);
            return error.PipeFailed;
        }

        const pid = std.os.linux.fork();
        const pid_i: isize = @bitCast(pid);

        if (pid_i < 0) {
            _ = std.os.linux.close(pipe_fds[0]);
            _ = std.os.linux.close(pipe_fds[1]);
            _ = std.os.linux.close(log_fd);
            return error.ForkFailed;
        }

        if (pid_i == 0) {
            // ── Child process ──
            // Own process group so we can signal the whole tree.
            _ = std.os.linux.setpgid(0, 0);

            // Redirect stdin from pipe
            _ = std.os.linux.close(pipe_fds[1]);
            _ = std.os.linux.dup2(pipe_fds[0], 0);
            _ = std.os.linux.close(pipe_fds[0]);

            // Redirect stdout + stderr to log file
            _ = std.os.linux.dup2(log_fd, 1);
            _ = std.os.linux.dup2(log_fd, 2);
            _ = std.os.linux.close(log_fd);

            const sh: [*:0]const u8 = "/bin/sh";
            const dash_c: [*:0]const u8 = "-c";
            const argv = [_]?[*:0]const u8{ sh, dash_c, cmd_z.ptr, null };
            _ = std.os.linux.execve(sh, @ptrCast(&argv), self.envp);
            std.os.linux.exit(127);
        }

        // ── Parent process ──
        _ = std.os.linux.close(pipe_fds[0]);
        _ = std.os.linux.close(log_fd);

        // Write JSON to stdin pipe, then close for EOF
        writeAllFd(pipe_fds[1], json);
        _ = std.os.linux.close(pipe_fds[1]);

        const child_pid: i32 = @intCast(pid_i);

        // Build display string for `hook ps`
        var cmd_display: [128]u8 = undefined;
        var cmd_display_len: u8 = 0;
        for (hook.command) |arg| {
            if (cmd_display_len > 0 and cmd_display_len < 127) {
                cmd_display[cmd_display_len] = ' ';
                cmd_display_len += 1;
            }
            const remaining = 127 - cmd_display_len;
            const copy_len: u8 = @intCast(@min(arg.len, remaining));
            @memcpy(cmd_display[cmd_display_len .. cmd_display_len + copy_len], arg[0..copy_len]);
            cmd_display_len += copy_len;
            if (cmd_display_len >= 127) break;
        }

        // Copy pattern into fixed buffer
        var pat_buf: [128]u8 = undefined;
        const pat_len: u8 = @intCast(@min(hook.pattern.len, 128));
        @memcpy(pat_buf[0..pat_len], hook.pattern[0..pat_len]);

        // Copy log path into fixed buffer
        var lp_buf: [256]u8 = undefined;
        const lp_len: u8 = @intCast(@min(log_path.len, 255));
        @memcpy(lp_buf[0..lp_len], log_path[0..lp_len]);

        const entry = ProcessEntry{
            .hook_id = hook.id,
            .pid = child_pid,
            .pgid = child_pid, // setpgid(0,0) makes pgid == pid
            .start_time = now_ms,
            .log_path = lp_buf,
            .log_path_len = lp_len,
            .pattern = pat_buf,
            .pattern_len = pat_len,
            .command_display = cmd_display,
            .command_display_len = cmd_display_len,
        };

        if (!self.process_table.add(entry)) {
            // Table full — should not happen since we checked above, but be safe
            logTimestampedFmt("Hook #{d}: process table full, killing pid {d}", .{ hook.id, child_pid });
            _ = std.os.linux.kill(-child_pid, .KILL);
        } else {
            logTimestampedFmt("Hook #{d}: started pid {d}, log: {s}", .{ hook.id, child_pid, log_path });
        }
    }
};

fn isValidEnvKey(key: []const u8) bool {
    if (key[0] >= '0' and key[0] <= '9') return false;
    for (key) |c| {
        if (!((c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '_')) return false;
    }
    return true;
}

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

fn logTimestampedFmt(comptime fmt: []const u8, args: anytype) void {
    var ts = std.os.linux.timespec{ .sec = 0, .nsec = 0 };
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

    var buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, fmt, args) catch "<message truncated>";
    std.debug.print("[{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}] {s}\n", .{ y, m + 1, @as(u8, @intCast(remaining + 1)), hour, minute, second, msg });
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

    try std.testing.expectEqual(@as(usize, 2), table.count());

    try table.remove(id1);
    {
        const snap = try table.snapshot(allocator);
        defer freeHookSnapshot(allocator, snap);
        try std.testing.expectEqual(@as(usize, 1), snap.len);
        try std.testing.expectEqual(@as(u64, 2), snap[0].id);
    }

    // Cleanup temp dir
    const hooks_json_z = try allocator.allocSentinel(u8, tmp_path.len + 11, 0);
    defer allocator.free(hooks_json_z);
    @memcpy(hooks_json_z[0..tmp_path.len], tmp_path);
    @memcpy(hooks_json_z[tmp_path.len .. tmp_path.len + 11], "/hooks.json");
    _ = std.os.linux.unlink(hooks_json_z.ptr);
    _ = std.os.linux.rmdir(tmp_z.ptr);
}
