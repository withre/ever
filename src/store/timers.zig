//! Timer system — persistent scheduled event publishing.
//!
//! Timers publish JSON payloads to topics on a schedule (interval or cron).
//! Stored in timers.json in the data directory, same pattern as hooks.json.
//! Timer daemon runs as a thread alongside the hook daemon in `ever store start`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const topic_mod = @import("topic.zig");
const TopicManager = topic_mod.TopicManager;

// ── Cron Parser ─────────────────────────────────────────────────────────────

pub const CronField = struct {
    bits: u64, // bitmap of matching values

    pub fn matches(self: CronField, value: u6) bool {
        return (self.bits >> value) & 1 == 1;
    }

    /// Parse a single cron field. `min`..`max` is the valid range (inclusive).
    pub fn parse(field: []const u8, min: u6, max: u6) !CronField {
        if (field.len == 0) return error.InvalidCron;
        if (std.mem.eql(u8, field, "*")) {
            return allBits(min, max);
        }

        var bits: u64 = 0;
        // Split on comma for lists
        var list_iter = std.mem.splitScalar(u8, field, ',');
        while (list_iter.next()) |item| {
            if (item.len == 0) return error.InvalidCron;

            // Check for step: */N or N-M/N
            if (std.mem.indexOfScalar(u8, item, '/')) |slash_pos| {
                const base = item[0..slash_pos];
                const step_str = item[slash_pos + 1 ..];
                const step = std.fmt.parseInt(u6, step_str, 10) catch return error.InvalidCron;
                if (step == 0) return error.InvalidCron;

                var range_min = min;
                var range_max = max;

                if (std.mem.eql(u8, base, "*")) {
                    // */N — step over full range
                } else if (std.mem.indexOfScalar(u8, base, '-')) |dash_pos| {
                    range_min = std.fmt.parseInt(u6, base[0..dash_pos], 10) catch return error.InvalidCron;
                    range_max = std.fmt.parseInt(u6, base[dash_pos + 1 ..], 10) catch return error.InvalidCron;
                } else {
                    range_min = std.fmt.parseInt(u6, base, 10) catch return error.InvalidCron;
                    range_max = max;
                }

                if (range_min < min or range_max > max or range_min > range_max) return error.InvalidCron;

                var v: u6 = range_min;
                while (v <= range_max) {
                    bits |= @as(u64, 1) << v;
                    const next = @as(u7, v) + step;
                    if (next > range_max) break;
                    v = @intCast(next);
                }
            } else if (std.mem.indexOfScalar(u8, item, '-')) |dash_pos| {
                // Range: N-M
                const lo = std.fmt.parseInt(u6, item[0..dash_pos], 10) catch return error.InvalidCron;
                const hi = std.fmt.parseInt(u6, item[dash_pos + 1 ..], 10) catch return error.InvalidCron;
                if (lo < min or hi > max or lo > hi) return error.InvalidCron;
                var v: u6 = lo;
                while (v <= hi) : (v += 1) {
                    bits |= @as(u64, 1) << v;
                    if (v == hi) break;
                }
            } else {
                // Literal
                const v = std.fmt.parseInt(u6, item, 10) catch return error.InvalidCron;
                if (v < min or v > max) return error.InvalidCron;
                bits |= @as(u64, 1) << v;
            }
        }

        if (bits == 0) return error.InvalidCron;
        return .{ .bits = bits };
    }

    fn allBits(min: u6, max: u6) CronField {
        var bits: u64 = 0;
        var v: u6 = min;
        while (v <= max) : (v += 1) {
            bits |= @as(u64, 1) << v;
            if (v == max) break;
        }
        return .{ .bits = bits };
    }
};

pub const CronExpr = struct {
    minute: CronField,
    hour: CronField,
    day_of_month: CronField,
    month: CronField,
    day_of_week: CronField,

    /// Parse a 5-field cron expression or predefined macro.
    pub fn parse(expr: []const u8) !CronExpr {
        // Predefined macros
        if (std.mem.eql(u8, expr, "@hourly")) return parseFiveFields("0 * * * *");
        if (std.mem.eql(u8, expr, "@daily")) return parseFiveFields("0 0 * * *");
        if (std.mem.eql(u8, expr, "@weekly")) return parseFiveFields("0 0 * * 0");
        if (std.mem.eql(u8, expr, "@monthly")) return parseFiveFields("0 0 1 * *");

        return parseFiveFields(expr);
    }

    fn parseFiveFields(expr: []const u8) !CronExpr {
        var field_iter = std.mem.splitScalar(u8, expr, ' ');
        const f1 = field_iter.next() orelse return error.InvalidCron;
        const f2 = field_iter.next() orelse return error.InvalidCron;
        const f3 = field_iter.next() orelse return error.InvalidCron;
        const f4 = field_iter.next() orelse return error.InvalidCron;
        const f5 = field_iter.next() orelse return error.InvalidCron;
        // Should have no more fields
        if (field_iter.next() != null) return error.InvalidCron;

        return .{
            .minute = try CronField.parse(f1, 0, 59),
            .hour = try CronField.parse(f2, 0, 23),
            .day_of_month = try CronField.parse(f3, 1, 31),
            .month = try CronField.parse(f4, 1, 12),
            .day_of_week = try CronField.parse(f5, 0, 6),
        };
    }

    /// Find next fire time (epoch seconds) after `after` (epoch seconds).
    /// Searches minute-by-minute up to 1 year ahead.
    pub fn nextFire(self: CronExpr, after: i64) i64 {
        // Start from the next whole minute after `after`
        var t = after - @rem(after, 60) + 60;
        const limit = after + 366 * 24 * 3600; // 1 year ahead

        while (t < limit) {
            const dt = epochToDateTime(t);
            if (self.month.matches(dt.month) and
                self.day_of_month.matches(dt.day) and
                self.day_of_week.matches(dt.dow) and
                self.hour.matches(dt.hour) and
                self.minute.matches(dt.minute))
            {
                return t;
            }
            t += 60;
        }

        return limit; // fallback — should not happen for valid expressions
    }
};

const DateTime = struct {
    month: u6,
    day: u6,
    dow: u6,
    hour: u6,
    minute: u6,
};

/// Convert epoch seconds to date/time components (UTC).
fn epochToDateTime(epoch: i64) DateTime {
    // Days since 1970-01-01
    const secs_per_day: i64 = 86400;
    var days = @divFloor(epoch, secs_per_day);
    var remaining = @mod(epoch, secs_per_day);
    if (remaining < 0) {
        remaining += secs_per_day;
        days -= 1;
    }
    const hour: u6 = @intCast(@divFloor(remaining, 3600));
    const minute: u6 = @intCast(@divFloor(@mod(remaining, 3600), 60));

    // Day of week: 1970-01-01 was Thursday (4)
    var dow_i = @mod(days + 4, 7);
    if (dow_i < 0) dow_i += 7;
    const dow: u6 = @intCast(dow_i);

    // Civil date from days since epoch
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    const z = days + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097; // day of era [0, 146096]
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m_raw = if (mp < 10) mp + 3 else mp - 9;
    _ = y; // year not needed

    return .{
        .month = @intCast(m_raw),
        .day = @intCast(d),
        .dow = dow,
        .hour = hour,
        .minute = minute,
    };
}

// ── Schedule ────────────────────────────────────────────────────────────────

pub const ScheduleType = enum {
    interval,
    cron,
};

pub const Schedule = union(ScheduleType) {
    interval: u64, // seconds
    cron: CronExpr,

    /// Next fire time in epoch seconds after `after`.
    pub fn nextFire(self: Schedule, after: i64) i64 {
        return switch (self) {
            .interval => |secs| after + @as(i64, @intCast(secs)),
            .cron => |expr| expr.nextFire(after),
        };
    }
};

/// Parse a duration string like "5s", "10m", "2h", "1d" into seconds.
pub fn parseDuration(s: []const u8) !u64 {
    if (s.len < 2) return error.InvalidDuration;
    const unit = s[s.len - 1];
    const num = std.fmt.parseInt(u64, s[0 .. s.len - 1], 10) catch return error.InvalidDuration;
    if (num == 0) return error.InvalidDuration;
    return switch (unit) {
        's' => num,
        'm' => num * 60,
        'h' => num * 3600,
        'd' => num * 86400,
        else => error.InvalidDuration,
    };
}

// ── Timer JSON Persistence ──────────────────────────────────────────────────

/// JSON-serializable schedule.
const ScheduleJson = struct {
    type: []const u8, // "interval" or "cron"
    seconds: u64 = 0, // for interval
    expression: []const u8 = "", // for cron
};

const TimerJson = struct {
    name: []const u8,
    schedule: ScheduleJson,
    topic: []const u8,
    payload: []const u8,
    last_fired_at: i64,
    fire_count: u64,
    persistent: bool,
    one_shot: bool = false,
    created_at: i64,
};

const TimerFileJson = struct {
    timers: []const TimerJson,
};

pub const Timer = struct {
    name: []const u8,
    schedule: Schedule,
    schedule_str: []const u8, // original schedule string for display (e.g., "every 5m", "0 3 * * *")
    topic: []const u8,
    payload: []const u8,
    last_fired_at: i64, // epoch millis, 0 = never
    fire_count: u64,
    persistent: bool,
    one_shot: bool, // if true, auto-remove after first fire
    created_at: i64, // epoch millis
};

// ── Timer Table ─────────────────────────────────────────────────────────────

pub const TimerTable = struct {
    allocator: Allocator,
    timers: std.ArrayList(Timer),
    data_dir: []const u8,
    mutex: std.atomic.Mutex,

    pub fn init(allocator: Allocator, data_dir: []const u8) !TimerTable {
        var table = TimerTable{
            .allocator = allocator,
            .timers = .empty,
            .data_dir = try allocator.dupe(u8, data_dir),
            .mutex = .unlocked,
        };
        try table.load();
        return table;
    }

    pub fn deinit(self: *TimerTable) void {
        for (self.timers.items) |timer| {
            self.freeTimer(timer);
        }
        self.timers.deinit(self.allocator);
        self.allocator.free(self.data_dir);
    }

    fn lock(self: *TimerTable) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn freeTimer(self: *TimerTable, timer: Timer) void {
        self.allocator.free(timer.name);
        self.allocator.free(timer.schedule_str);
        self.allocator.free(timer.topic);
        self.allocator.free(timer.payload);
    }

    /// Add a timer. Returns error if name already exists.
    pub fn add(self: *TimerTable, name: []const u8, schedule: Schedule, schedule_str: []const u8, topic: []const u8, payload: []const u8, persistent: bool) !void {
        return self.addFull(name, schedule, schedule_str, topic, payload, persistent, false);
    }

    pub fn addOneShot(self: *TimerTable, name: []const u8, schedule: Schedule, schedule_str: []const u8, topic: []const u8, payload: []const u8) !void {
        return self.addFull(name, schedule, schedule_str, topic, payload, false, true);
    }

    fn addFull(self: *TimerTable, name: []const u8, schedule: Schedule, schedule_str: []const u8, topic: []const u8, payload: []const u8, persistent: bool, one_shot: bool) !void {
        self.lock();
        defer self.mutex.unlock();

        // Check for duplicate name
        for (self.timers.items) |t| {
            if (std.mem.eql(u8, t.name, name)) return error.AlreadyExists;
        }

        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        const sched_copy = try self.allocator.dupe(u8, schedule_str);
        errdefer self.allocator.free(sched_copy);
        const topic_copy = try self.allocator.dupe(u8, topic);
        errdefer self.allocator.free(topic_copy);
        const payload_copy = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(payload_copy);

        try self.timers.append(self.allocator, .{
            .name = name_copy,
            .schedule = schedule,
            .schedule_str = sched_copy,
            .topic = topic_copy,
            .payload = payload_copy,
            .last_fired_at = 0,
            .fire_count = 0,
            .persistent = persistent,
            .one_shot = one_shot,
            .created_at = getMilliTimestamp(),
        });

        try self.saveLocked();
    }

    /// Remove a timer by name.
    pub fn remove(self: *TimerTable, name: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();

        for (self.timers.items, 0..) |timer, i| {
            if (std.mem.eql(u8, timer.name, name)) {
                self.freeTimer(timer);
                _ = self.timers.swapRemove(i);
                try self.saveLocked();
                return;
            }
        }
        return error.NotFound;
    }

    /// List all timers (returns internal references, caller must not free).
    pub fn list(self: *TimerTable) []const Timer {
        self.lock();
        defer self.mutex.unlock();
        return self.timers.items;
    }

    /// Find a timer by name (returns null if not found).
    pub fn find(self: *TimerTable, name: []const u8) ?Timer {
        self.lock();
        defer self.mutex.unlock();
        for (self.timers.items) |timer| {
            if (std.mem.eql(u8, timer.name, name)) return timer;
        }
        return null;
    }

    /// Update a timer's fire state after publishing.
    pub fn updateFired(self: *TimerTable, name: []const u8, fired_at: i64) void {
        self.lock();
        defer self.mutex.unlock();
        for (self.timers.items) |*timer| {
            if (std.mem.eql(u8, timer.name, name)) {
                timer.last_fired_at = fired_at;
                timer.fire_count += 1;
                self.saveLocked() catch {};
                return;
            }
        }
    }

    /// Get a deep-copied snapshot of timers for the daemon.
    /// Caller owns returned slice. Free with `freeTimerSnapshot`.
    pub fn snapshot(self: *TimerTable, allocator: Allocator) ![]Timer {
        self.lock();
        defer self.mutex.unlock();
        const result = try allocator.alloc(Timer, self.timers.items.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |t| freeTimerCopy(allocator, t);
            allocator.free(result);
        }
        for (self.timers.items, 0..) |timer, i| {
            result[i] = try deepCopyTimer(allocator, timer);
            initialized = i + 1;
        }
        return result;
    }

    // ── Persistence ─────────────────────────────────────────────────────

    fn timersFilePath(self: *TimerTable) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/timers.json", .{self.data_dir});
    }

    fn timersTmpPath(self: *TimerTable) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/timers.json.tmp", .{self.data_dir});
    }

    fn load(self: *TimerTable) !void {
        const path = try self.timersFilePath();
        defer self.allocator.free(path);

        const path_z = try self.allocator.allocSentinel(u8, path.len, 0);
        defer self.allocator.free(path_z);
        @memcpy(path_z[0..path.len], path);

        const fd_rc = std.os.linux.open(path_z.ptr, .{ .ACCMODE = .RDONLY }, 0);
        const fd_i: isize = @bitCast(fd_rc);
        if (fd_i < 0) return; // File doesn't exist, start fresh

        const fd: std.posix.fd_t = @intCast(fd_rc);
        defer _ = std.os.linux.close(fd);

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

        const parsed = std.json.parseFromSlice(TimerFileJson, self.allocator, buf[0..total], .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return;
        defer parsed.deinit();

        for (parsed.value.timers) |t| {
            // Reconstruct schedule from JSON
            const schedule: Schedule = if (std.mem.eql(u8, t.schedule.type, "interval"))
                .{ .interval = t.schedule.seconds }
            else if (std.mem.eql(u8, t.schedule.type, "cron"))
                .{ .cron = CronExpr.parse(t.schedule.expression) catch continue }
            else
                continue;

            // Build schedule_str for display
            const sched_str = if (std.mem.eql(u8, t.schedule.type, "interval"))
                try formatIntervalStr(self.allocator, t.schedule.seconds)
            else
                try self.allocator.dupe(u8, t.schedule.expression);
            errdefer self.allocator.free(sched_str);

            const name_copy = try self.allocator.dupe(u8, t.name);
            errdefer self.allocator.free(name_copy);
            const topic_copy = try self.allocator.dupe(u8, t.topic);
            errdefer self.allocator.free(topic_copy);
            const payload_copy = try self.allocator.dupe(u8, t.payload);
            errdefer self.allocator.free(payload_copy);

            try self.timers.append(self.allocator, .{
                .name = name_copy,
                .schedule = schedule,
                .schedule_str = sched_str,
                .topic = topic_copy,
                .payload = payload_copy,
                .last_fired_at = t.last_fired_at,
                .fire_count = t.fire_count,
                .persistent = t.persistent,
                .one_shot = t.one_shot,
                .created_at = t.created_at,
            });
        }
    }

    fn saveLocked(self: *TimerTable) !void {
        const timer_jsons = try self.allocator.alloc(TimerJson, self.timers.items.len);
        defer self.allocator.free(timer_jsons);

        // We also need to allocate schedule JSON data
        const sched_jsons = try self.allocator.alloc(ScheduleJson, self.timers.items.len);
        defer self.allocator.free(sched_jsons);

        for (self.timers.items, 0..) |timer, i| {
            sched_jsons[i] = switch (timer.schedule) {
                .interval => |secs| ScheduleJson{
                    .type = "interval",
                    .seconds = secs,
                },
                .cron => ScheduleJson{
                    .type = "cron",
                    .expression = timer.schedule_str,
                },
            };
            timer_jsons[i] = .{
                .name = timer.name,
                .schedule = sched_jsons[i],
                .topic = timer.topic,
                .payload = timer.payload,
                .last_fired_at = timer.last_fired_at,
                .fire_count = timer.fire_count,
                .persistent = timer.persistent,
                .one_shot = timer.one_shot,
                .created_at = timer.created_at,
            };
        }

        const file_json = TimerFileJson{ .timers = timer_jsons };

        const json_bytes = try std.json.Stringify.valueAlloc(self.allocator, file_json, .{});
        defer self.allocator.free(json_bytes);

        // Atomic write: temp file + rename
        const tmp_path = try self.timersTmpPath();
        defer self.allocator.free(tmp_path);
        const final_path = try self.timersFilePath();
        defer self.allocator.free(final_path);

        const tmp_z = try self.allocator.allocSentinel(u8, tmp_path.len, 0);
        defer self.allocator.free(tmp_z);
        @memcpy(tmp_z[0..tmp_path.len], tmp_path);

        const final_z = try self.allocator.allocSentinel(u8, final_path.len, 0);
        defer self.allocator.free(final_z);
        @memcpy(final_z[0..final_path.len], final_path);

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

        const rc = std.os.linux.rename(tmp_z.ptr, final_z.ptr);
        const rc_i: isize = @bitCast(rc);
        if (rc_i < 0) return error.Unexpected;
    }
};

pub fn formatIntervalStr(allocator: Allocator, secs: u64) ![]u8 {
    if (secs % 86400 == 0 and secs >= 86400) {
        return std.fmt.allocPrint(allocator, "every {d}d", .{secs / 86400});
    } else if (secs % 3600 == 0 and secs >= 3600) {
        return std.fmt.allocPrint(allocator, "every {d}h", .{secs / 3600});
    } else if (secs % 60 == 0 and secs >= 60) {
        return std.fmt.allocPrint(allocator, "every {d}m", .{secs / 60});
    } else {
        return std.fmt.allocPrint(allocator, "every {d}s", .{secs});
    }
}

fn deepCopyTimer(allocator: Allocator, timer: Timer) !Timer {
    const name = try allocator.dupe(u8, timer.name);
    errdefer allocator.free(name);
    const sched_str = try allocator.dupe(u8, timer.schedule_str);
    errdefer allocator.free(sched_str);
    const topic = try allocator.dupe(u8, timer.topic);
    errdefer allocator.free(topic);
    const payload = try allocator.dupe(u8, timer.payload);
    errdefer allocator.free(payload);

    return .{
        .name = name,
        .schedule = timer.schedule,
        .schedule_str = sched_str,
        .topic = topic,
        .payload = payload,
        .last_fired_at = timer.last_fired_at,
        .fire_count = timer.fire_count,
        .persistent = timer.persistent,
        .one_shot = timer.one_shot,
        .created_at = timer.created_at,
    };
}

fn freeTimerCopy(allocator: Allocator, timer: Timer) void {
    allocator.free(timer.name);
    allocator.free(timer.schedule_str);
    allocator.free(timer.topic);
    allocator.free(timer.payload);
}

pub fn freeTimerSnapshot(allocator: Allocator, timers: []Timer) void {
    for (timers) |t| freeTimerCopy(allocator, t);
    allocator.free(timers);
}

// ── Timer Daemon ────────────────────────────────────────────────────────────

pub const TimerDaemon = struct {
    allocator: Allocator,
    timer_table: *TimerTable,
    topic_manager: *TopicManager,
    shutdown: std.atomic.Value(bool),
    thread: ?std.Thread,

    pub fn init(
        allocator: Allocator,
        timer_table: *TimerTable,
        topic_manager: *TopicManager,
    ) TimerDaemon {
        return .{
            .allocator = allocator,
            .timer_table = timer_table,
            .topic_manager = topic_manager,
            .shutdown = std.atomic.Value(bool).init(false),
            .thread = null,
        };
    }

    pub fn start(self: *TimerDaemon) !void {
        // Check for missed fires on startup (catch-up)
        self.catchUpMissedFires();
        self.thread = try std.Thread.spawn(.{}, daemonLoop, .{self});
    }

    pub fn stop(self: *TimerDaemon) void {
        self.shutdown.store(true, .release);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    fn daemonLoop(self: *TimerDaemon) void {
        while (!self.shutdown.load(.acquire)) {
            self.tick();
            // Sleep 1 second
            _ = std.os.linux.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);
        }
    }

    fn tick(self: *TimerDaemon) void {
        const now_ms = getMilliTimestamp();
        const now_secs = @divFloor(now_ms, 1000);

        const timers = self.timer_table.snapshot(self.allocator) catch return;
        defer freeTimerSnapshot(self.allocator, timers);

        for (timers) |timer| {
            const should_fire = self.shouldFire(timer, now_secs);
            if (should_fire) {
                self.fireTimer(timer, now_ms) catch |err| {
                    std.debug.print("Timer '{s}' fire error: {}\n", .{ timer.name, err });
                    continue;
                };
                // Auto-remove one-shot timers after firing
                if (timer.one_shot) {
                    self.timer_table.remove(timer.name) catch {};
                    std.debug.print("One-shot timer '{s}' removed after firing.\n", .{timer.name});
                }
            }
        }
    }

    fn shouldFire(self: *TimerDaemon, timer: Timer, now_secs: i64) bool {
        _ = self;
        const base_secs = if (timer.last_fired_at > 0)
            @divFloor(timer.last_fired_at, 1000)
        else
            // Never fired — use created_at as the base so first fire
            // happens created_at + interval seconds after creation.
            @divFloor(timer.created_at, 1000);
        const next = timer.schedule.nextFire(base_secs);
        return next <= now_secs;
    }

    fn catchUpMissedFires(self: *TimerDaemon) void {
        const now_ms = getMilliTimestamp();
        const now_secs = @divFloor(now_ms, 1000);

        const timers = self.timer_table.snapshot(self.allocator) catch return;
        defer freeTimerSnapshot(self.allocator, timers);

        for (timers) |timer| {
            if (!timer.persistent) continue;
            if (timer.last_fired_at == 0) continue;

            const last_secs = @divFloor(timer.last_fired_at, 1000);
            const next = timer.schedule.nextFire(last_secs);
            if (next < now_secs) {
                // Missed fire — catch up with one event
                std.debug.print("Timer '{s}' catching up missed fire.\n", .{timer.name});
                self.fireTimer(timer, now_ms) catch |err| {
                    std.debug.print("Timer '{s}' catch-up error: {}\n", .{ timer.name, err });
                };
            }
        }
    }

    fn fireTimer(self: *TimerDaemon, timer: Timer, now_ms: i64) !void {
        // Build the payload: user payload at top level, timer metadata in _timer
        const payload = try self.buildTimerPayload(timer);
        defer self.allocator.free(payload);

        // Auto-create topic if it doesn't exist
        if (!self.topic_manager.hasTopic(timer.topic)) {
            self.topic_manager.createTopic(timer.topic) catch |err| switch (err) {
                error.AlreadyExists => {}, // race condition — fine
                else => return err,
            };
        }

        _ = self.topic_manager.publish(timer.topic, null, payload) catch |err| {
            std.debug.print("Timer '{s}' publish failed: {}\n", .{ timer.name, err });
            return err;
        };

        self.timer_table.updateFired(timer.name, now_ms);
    }

    fn buildTimerPayload(self: *TimerDaemon, timer: Timer) ![]u8 {
        var json: std.ArrayList(u8) = .empty;
        errdefer json.deinit(self.allocator);

        if (timer.payload.len > 0 and timer.payload[0] == '{') {
            // Merge user payload with _timer metadata
            // User payload is a JSON object — inject _timer field
            // Remove trailing whitespace and '}', append _timer, close
            var trimmed = timer.payload;
            while (trimmed.len > 0 and isWhitespace(trimmed[trimmed.len - 1])) {
                trimmed = trimmed[0 .. trimmed.len - 1];
            }
            if (trimmed.len > 1 and trimmed[trimmed.len - 1] == '}') {
                const inner = trimmed[0 .. trimmed.len - 1];
                try json.appendSlice(self.allocator, inner);
                // Check if there's content (not just "{")
                var content = inner[1..];
                while (content.len > 0 and isWhitespace(content[0])) {
                    content = content[1..];
                }
                while (content.len > 0 and isWhitespace(content[content.len - 1])) {
                    content = content[0 .. content.len - 1];
                }
                if (content.len > 0) {
                    try json.appendSlice(self.allocator, ",\"_timer\":{\"name\":\"");
                } else {
                    try json.appendSlice(self.allocator, "\"_timer\":{\"name\":\"");
                }
                try appendJsonEscaped(&json, self.allocator, timer.name);
                try json.appendSlice(self.allocator, "\",\"fire_count\":");
                var buf: [20]u8 = undefined;
                const count_str = std.fmt.bufPrint(&buf, "{d}", .{timer.fire_count + 1}) catch "0";
                try json.appendSlice(self.allocator, count_str);
                try json.appendSlice(self.allocator, "}}");
                return json.toOwnedSlice(self.allocator);
            }
        }

        // Payload is not a JSON object — wrap it
        try json.appendSlice(self.allocator, "{\"_timer\":{\"name\":\"");
        try appendJsonEscaped(&json, self.allocator, timer.name);
        try json.appendSlice(self.allocator, "\",\"fire_count\":");
        var buf: [20]u8 = undefined;
        const count_str = std.fmt.bufPrint(&buf, "{d}", .{timer.fire_count + 1}) catch "0";
        try json.appendSlice(self.allocator, count_str);
        try json.appendSlice(self.allocator, "}}");
        return json.toOwnedSlice(self.allocator);
    }
};

/// Append a string with JSON escaping.
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
                var escape_buf: [6]u8 = undefined;
                _ = std.fmt.bufPrint(&escape_buf, "\\u{X:0>4}", .{c}) catch unreachable;
                try json.appendSlice(allocator, &escape_buf);
            } else {
                try json.append(allocator, c);
            }
        },
    };
}

fn isWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn getMilliTimestamp() i64 {
    var ts: std.os.linux.timespec = undefined;
    const rc = std.os.linux.clock_gettime(.REALTIME, &ts);
    if (rc != 0) return 0;
    return @as(i64, @intCast(ts.sec)) * 1000 + @divTrunc(@as(i64, @intCast(ts.nsec)), 1_000_000);
}

// ── Tests ───────────────────────────────────────────────────────────────────

test "CronField parse star" {
    const f = try CronField.parse("*", 0, 59);
    try std.testing.expect(f.matches(0));
    try std.testing.expect(f.matches(30));
    try std.testing.expect(f.matches(59));
}

test "CronField parse literal" {
    const f = try CronField.parse("5", 0, 59);
    try std.testing.expect(f.matches(5));
    try std.testing.expect(!f.matches(4));
    try std.testing.expect(!f.matches(6));
}

test "CronField parse range" {
    const f = try CronField.parse("1-5", 0, 59);
    try std.testing.expect(!f.matches(0));
    try std.testing.expect(f.matches(1));
    try std.testing.expect(f.matches(3));
    try std.testing.expect(f.matches(5));
    try std.testing.expect(!f.matches(6));
}

test "CronField parse step" {
    const f = try CronField.parse("*/15", 0, 59);
    try std.testing.expect(f.matches(0));
    try std.testing.expect(f.matches(15));
    try std.testing.expect(f.matches(30));
    try std.testing.expect(f.matches(45));
    try std.testing.expect(!f.matches(10));
}

test "CronField parse list" {
    const f = try CronField.parse("1,15,30", 0, 59);
    try std.testing.expect(f.matches(1));
    try std.testing.expect(f.matches(15));
    try std.testing.expect(f.matches(30));
    try std.testing.expect(!f.matches(0));
    try std.testing.expect(!f.matches(2));
}

test "CronExpr parse 5-field" {
    const expr = try CronExpr.parse("0 3 * * *");
    try std.testing.expect(expr.minute.matches(0));
    try std.testing.expect(!expr.minute.matches(1));
    try std.testing.expect(expr.hour.matches(3));
    try std.testing.expect(!expr.hour.matches(4));
}

test "CronExpr parse macros" {
    const hourly = try CronExpr.parse("@hourly");
    try std.testing.expect(hourly.minute.matches(0));
    try std.testing.expect(!hourly.minute.matches(1));

    const daily = try CronExpr.parse("@daily");
    try std.testing.expect(daily.minute.matches(0));
    try std.testing.expect(daily.hour.matches(0));
    try std.testing.expect(!daily.hour.matches(1));
}

test "CronExpr nextFire" {
    // "0 * * * *" = every hour at minute 0
    const expr = try CronExpr.parse("0 * * * *");
    // After 2026-04-05 10:30:00 UTC (epoch: 1775379000)
    const after: i64 = 1775379000;
    const next = expr.nextFire(after);
    // Should be 2026-04-05 11:00:00 UTC
    const dt = epochToDateTime(next);
    try std.testing.expectEqual(@as(u6, 0), dt.minute);
    try std.testing.expect(next > after);
    try std.testing.expect(next <= after + 3600);
}

test "parseDuration" {
    try std.testing.expectEqual(@as(u64, 5), try parseDuration("5s"));
    try std.testing.expectEqual(@as(u64, 300), try parseDuration("5m"));
    try std.testing.expectEqual(@as(u64, 7200), try parseDuration("2h"));
    try std.testing.expectEqual(@as(u64, 86400), try parseDuration("1d"));
    try std.testing.expectError(error.InvalidDuration, parseDuration("0s"));
    try std.testing.expectError(error.InvalidDuration, parseDuration("x"));
    try std.testing.expectError(error.InvalidDuration, parseDuration("5x"));
}

test "Schedule nextFire interval" {
    const sched: Schedule = .{ .interval = 300 }; // every 5m
    const after: i64 = 1000;
    try std.testing.expectEqual(@as(i64, 1300), sched.nextFire(after));
}

test "TimerTable add, find, remove" {
    const allocator = std.testing.allocator;

    const tmp_path = try std.fmt.allocPrint(allocator, "/tmp/.ever-timer-test-{d}", .{std.os.linux.getpid()});
    defer allocator.free(tmp_path);

    const tmp_z = try allocator.allocSentinel(u8, tmp_path.len, 0);
    defer allocator.free(tmp_z);
    @memcpy(tmp_z[0..tmp_path.len], tmp_path);
    _ = std.os.linux.mkdir(tmp_z.ptr, 0o755);

    var table = try TimerTable.init(allocator, tmp_path);
    defer table.deinit();

    try table.add("heartbeat", .{ .interval = 300 }, "every 5m", "health.check", "{\"ping\":true}", true);

    const found = table.find("heartbeat");
    try std.testing.expect(found != null);
    try std.testing.expectEqualStrings("health.check", found.?.topic);

    try std.testing.expectError(error.AlreadyExists, table.add("heartbeat", .{ .interval = 60 }, "every 1m", "x", "{}", true));

    try table.remove("heartbeat");
    try std.testing.expect(table.find("heartbeat") == null);
    try std.testing.expectError(error.NotFound, table.remove("nonexistent"));

    // Cleanup
    const timers_json_z = try allocator.allocSentinel(u8, tmp_path.len + 12, 0);
    defer allocator.free(timers_json_z);
    @memcpy(timers_json_z[0..tmp_path.len], tmp_path);
    @memcpy(timers_json_z[tmp_path.len .. tmp_path.len + 12], "/timers.json");
    _ = std.os.linux.unlink(timers_json_z.ptr);
    _ = std.os.linux.rmdir(tmp_z.ptr);
}

test "TimerTable snapshot" {
    const allocator = std.testing.allocator;

    const tmp_path = try std.fmt.allocPrint(allocator, "/tmp/.ever-timer-snap-{d}", .{std.os.linux.getpid()});
    defer allocator.free(tmp_path);

    const tmp_z = try allocator.allocSentinel(u8, tmp_path.len, 0);
    defer allocator.free(tmp_z);
    @memcpy(tmp_z[0..tmp_path.len], tmp_path);
    _ = std.os.linux.mkdir(tmp_z.ptr, 0o755);

    var table = try TimerTable.init(allocator, tmp_path);
    defer table.deinit();

    try table.add("t1", .{ .interval = 60 }, "every 1m", "topic1", "{}", true);
    try table.add("t2", .{ .interval = 300 }, "every 5m", "topic2", "{\"x\":1}", false);

    const snap = try table.snapshot(allocator);
    defer freeTimerSnapshot(allocator, snap);

    try std.testing.expectEqual(@as(usize, 2), snap.len);
    try std.testing.expectEqualStrings("t1", snap[0].name);
    try std.testing.expectEqualStrings("t2", snap[1].name);

    // Cleanup
    const timers_json_z = try allocator.allocSentinel(u8, tmp_path.len + 12, 0);
    defer allocator.free(timers_json_z);
    @memcpy(timers_json_z[0..tmp_path.len], tmp_path);
    @memcpy(timers_json_z[tmp_path.len .. tmp_path.len + 12], "/timers.json");
    _ = std.os.linux.unlink(timers_json_z.ptr);
    _ = std.os.linux.rmdir(tmp_z.ptr);
}
