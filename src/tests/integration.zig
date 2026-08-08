//! Integration tests for Ever — exercises the full publish-store-subscribe
//! pipeline through the TopicManager API (in-process, no TCP).
//!
//! All tests use `std.testing.allocator` for leak detection and
//! `std.testing.tmpDir` for isolated data directories.

const std = @import("std");
const ever = @import("ever");
const store = ever.store;
const topic_mod = ever.topic;
const TopicManager = topic_mod.TopicManager;
const Event = store.Event;

const io = std.testing.io;
const testing = std.testing;
const allocator = testing.allocator;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn runServerForTest(server: *ever.net.Server) void {
    server.run() catch {};
}

fn freeEvents(events: []Event) void {
    for (events) |e| store.freeEvent(allocator, e);
    allocator.free(events);
}

// ── Status Server Probe ─────────────────────────────────────────────────────

test "integration: status server probe flips true then false" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const pid_part: u16 = @intCast(@mod(std.os.linux.getpid(), 20000));
    const port: u16 = 30000 + pid_part;
    var server = try ever.net.Server.init(allocator, io, &tm, .{ .address = "127.0.0.1", .port = port });
    defer server.deinit();

    const thread = try std.Thread.spawn(.{}, runServerForTest, .{&server});

    var reachable = false;
    var attempts: u32 = 0;
    while (attempts < 50) : (attempts += 1) {
        if (ever.status.probeServer(io, "127.0.0.1", port, 50)) {
            reachable = true;
            break;
        }
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 20_000_000 }, null);
    }
    try testing.expect(reachable);

    var status = ever.status.StoreStatus{
        .data_dir = ".",
        .server = .{ .address = "127.0.0.1", .port = port, .reachable = reachable },
        .segments = 0,
        .total_bytes = 0,
        .total_events = 0,
        .topics = &.{},
        .hooks = &.{},
        .lock_held = false,
    };
    const json_running = try ever.status.formatJson(&status, allocator);
    defer allocator.free(json_running);
    try testing.expect(std.mem.indexOf(u8, json_running, "\"reachable\": true") != null);
    try testing.expect(std.mem.indexOf(u8, json_running, "\"lock_held\": false") != null);

    server.shutdown_requested.store(true, .release);
    _ = ever.status.probeServer(io, "127.0.0.1", port, 50); // wake accept loop
    thread.join();
    server.shutdown();

    const after_shutdown = ever.status.probeServer(io, "127.0.0.1", port, 50);
    try testing.expect(!after_shutdown);
}

// ── Status Over TCP ───────────────────────────────────────────────────────
//
// From air/v0.1/status-over-tcp.org: start a server on a real data dir,
// publish events, query status over the wire, and verify the server-sourced
// counters agree with the offline scan of the same directory.

fn makeStatusDir() ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "/tmp/.ever-integ-status-{d}", .{std.os.linux.getpid()});
    const path_z = try allocator.allocSentinel(u8, path.len, 0);
    defer allocator.free(path_z);
    @memcpy(path_z[0..path.len], path);
    _ = std.os.linux.mkdir(path_z.ptr, 0o755);
    return path;
}

fn cleanupStatusDir(path: []const u8) void {
    // Unlink every file in the dir, then remove it.
    blk: {
        const dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch break :blk;
        defer dir.close(io);
        var names: std.ArrayList([]u8) = .empty;
        defer {
            for (names.items) |n| allocator.free(n);
            names.deinit(allocator);
        }
        var iter = dir.iterate();
        while (iter.next(io) catch null) |entry| {
            if (entry.kind != .file) continue;
            names.append(allocator, allocator.dupe(u8, entry.name) catch break) catch break;
        }
        for (names.items) |name| {
            const full = std.fmt.allocPrint(allocator, "{s}/{s}", .{ path, name }) catch continue;
            defer allocator.free(full);
            const full_z = allocator.allocSentinel(u8, full.len, 0) catch continue;
            defer allocator.free(full_z);
            @memcpy(full_z[0..full.len], full);
            _ = std.os.linux.unlink(full_z.ptr);
        }
    }
    const p_z = allocator.allocSentinel(u8, path.len, 0) catch return;
    defer allocator.free(p_z);
    @memcpy(p_z[0..path.len], path);
    _ = std.os.linux.rmdir(p_z.ptr);
    allocator.free(path);
}

test "integration: status over TCP reports live stats and matches offline scan" {
    const data_dir = try makeStatusDir();
    defer cleanupStatusDir(data_dir);

    const dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{ .iterate = true });
    var tm = try TopicManager.init(allocator, io, dir, .{ .sync_on_append = false });
    defer {
        tm.deinit();
        dir.close(io);
    }

    try tm.createTopic("status.alpha");
    try tm.createTopic("status.beta");
    _ = try tm.publish("status.alpha", null, "a1");
    _ = try tm.publish("status.alpha", null, "a2");
    _ = try tm.publish("status.alpha", null, "a3");
    _ = try tm.publish("status.beta", null, "b1");

    const pid_part: u16 = @intCast(@mod(std.os.linux.getpid(), 20000));
    const port: u16 = 31000 + pid_part;
    var server = try ever.net.Server.init(allocator, io, &tm, .{
        .address = "127.0.0.1",
        .port = port,
        .data_dir = data_dir,
    });
    defer server.deinit();

    const thread = try std.Thread.spawn(.{}, runServerForTest, .{&server});

    var reachable = false;
    var attempts: u32 = 0;
    while (attempts < 50) : (attempts += 1) {
        if (ever.status.probeServer(io, "127.0.0.1", port, 50)) {
            reachable = true;
            break;
        }
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 20_000_000 }, null);
    }
    try testing.expect(reachable);

    // Query status over the wire — no local filesystem access on this path.
    var server_status: ever.client.StatusResult = undefined;
    {
        var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
        defer client.deinit();
        server_status = try client.status();
    }
    defer server_status.deinit();

    const st = &server_status.status;
    try testing.expectEqualStrings(data_dir, st.data_dir);
    try testing.expectEqual(ever.status.Source.server, st.source);
    try testing.expectEqual(true, st.lock_held);
    try testing.expectEqual(@as(u64, 4), st.total_events);
    try testing.expect(st.segments >= 1);
    try testing.expect(st.total_bytes > 0);
    try testing.expect(st.uptime_ms != null);
    try testing.expectEqual(@as(?u64, 0), st.timer_count); // no timer table attached
    try testing.expectEqual(@as(usize, 2), st.topics.len);
    var saw_alpha = false;
    var saw_beta = false;
    for (st.topics) |t| {
        if (std.mem.eql(u8, t.name, "status.alpha")) {
            try testing.expectEqual(@as(u64, 3), t.events);
            saw_alpha = true;
        }
        if (std.mem.eql(u8, t.name, "status.beta")) {
            try testing.expectEqual(@as(u64, 1), t.events);
            saw_beta = true;
        }
    }
    try testing.expect(saw_alpha);
    try testing.expect(saw_beta);
    try testing.expectEqual(@as(usize, 0), st.hooks.len); // no hook table attached

    // Stop the server.
    server.shutdown_requested.store(true, .release);
    _ = ever.status.probeServer(io, "127.0.0.1", port, 50); // wake accept loop
    thread.join();
    server.shutdown();

    // With the server stopped, connecting fails — the CLI maps this to the
    // endpoint-naming error and exit 1.
    if (ever.client.Client.connect(allocator, io, "127.0.0.1", port)) |c| {
        var cc = c;
        cc.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}

    // Offline scan of the same quiesced data dir agrees with the
    // server-sourced counters taken before shutdown.
    var offline = try ever.status.getStatus(allocator, io, data_dir);
    defer offline.deinit(allocator);

    try testing.expectEqual(ever.status.Source.local_scan, offline.source);
    try testing.expectEqual(st.total_events, offline.total_events);
    try testing.expectEqual(st.segments, offline.segments);
    try testing.expectEqual(st.total_bytes, offline.total_bytes);
    try testing.expectEqual(st.topics.len, offline.topics.len);
    for (st.topics) |server_topic| {
        var found = false;
        for (offline.topics) |offline_topic| {
            if (std.mem.eql(u8, server_topic.name, offline_topic.name)) {
                try testing.expectEqual(server_topic.events, offline_topic.events);
                try testing.expectEqual(server_topic.deleted, offline_topic.deleted);
                found = true;
                break;
            }
        }
        try testing.expect(found);
    }
    try testing.expectEqual(st.hooks.len, offline.hooks.len);
}

// ── Basic Publish-Subscribe ─────────────────────────────────────────────────

test "integration: basic publish-subscribe" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("test.events");

    // Publish N events with known data
    const N = 10;
    var offsets: [N]u64 = undefined;
    for (0..N) |i| {
        var buf: [64]u8 = undefined;
        const val = std.fmt.bufPrint(&buf, "{{\"seq\":{d}}}", .{i}) catch unreachable;
        offsets[i] = try tm.publish("test.events", null, val);
    }

    // Verify offsets are sequential (global offsets: 1..N because offset 0 is the createTopic marker)
    for (0..N) |i| {
        try testing.expectEqual(@as(u64, i + 1), offsets[i]);
    }

    // Fetch all events
    const events = try tm.fetch(allocator, "test.events", 0, N + 10);
    defer freeEvents(events);

    try testing.expectEqual(@as(usize, N), events.len);
    for (events, 0..) |evt, i| {
        var buf: [64]u8 = undefined;
        const expected = std.fmt.bufPrint(&buf, "{{\"seq\":{d}}}", .{i}) catch unreachable;
        try testing.expectEqualStrings(expected, evt.value);
        try testing.expectEqualStrings("test.events", evt.topic);
    }
}

// ── Topic Isolation ─────────────────────────────────────────────────────────

test "integration: topic isolation" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("alpha");
    try tm.createTopic("beta");

    _ = try tm.publish("alpha", "a1", "alpha-value-1");
    _ = try tm.publish("alpha", "a2", "alpha-value-2");
    _ = try tm.publish("beta", "b1", "beta-value-1");
    _ = try tm.publish("alpha", "a3", "alpha-value-3");
    _ = try tm.publish("beta", "b2", "beta-value-2");

    // Fetch alpha — should see exactly 3 events, all alpha
    const alpha_events = try tm.fetch(allocator, "alpha", 0, 100);
    defer freeEvents(alpha_events);
    try testing.expectEqual(@as(usize, 3), alpha_events.len);
    try testing.expectEqualStrings("alpha-value-1", alpha_events[0].value);
    try testing.expectEqualStrings("alpha-value-2", alpha_events[1].value);
    try testing.expectEqualStrings("alpha-value-3", alpha_events[2].value);

    // Fetch beta — should see exactly 2 events, all beta
    const beta_events = try tm.fetch(allocator, "beta", 0, 100);
    defer freeEvents(beta_events);
    try testing.expectEqual(@as(usize, 2), beta_events.len);
    try testing.expectEqualStrings("beta-value-1", beta_events[0].value);
    try testing.expectEqualStrings("beta-value-2", beta_events[1].value);

    // No cross-contamination: alpha events have no beta data and vice versa
    for (alpha_events) |e| {
        try testing.expectEqualStrings("alpha", e.topic);
        try testing.expect(!std.mem.startsWith(u8, e.value, "beta"));
    }
    for (beta_events) |e| {
        try testing.expectEqualStrings("beta", e.topic);
        try testing.expect(!std.mem.startsWith(u8, e.value, "alpha"));
    }
}

// ── Offset Tracking ─────────────────────────────────────────────────────────

test "integration: offset tracking with 100 events" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("offsets");

    // Publish 100 events
    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const val = std.fmt.bufPrint(&buf, "e{d}", .{i}) catch unreachable;
        _ = try tm.publish("offsets", null, val);
    }

    // Fetch first 50
    const first_half = try tm.fetch(allocator, "offsets", 0, 50);
    defer freeEvents(first_half);
    try testing.expectEqual(@as(usize, 50), first_half.len);

    // Fetch next 50
    const second_half = try tm.fetch(allocator, "offsets", 50, 50);
    defer freeEvents(second_half);
    try testing.expectEqual(@as(usize, 50), second_half.len);

    // Verify complete coverage — no gaps, no duplicates
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const expected = std.fmt.bufPrint(&buf, "e{d}", .{i}) catch unreachable;
        try testing.expectEqualStrings(expected, first_half[i].value);
    }
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const expected = std.fmt.bufPrint(&buf, "e{d}", .{i + 50}) catch unreachable;
        try testing.expectEqualStrings(expected, second_half[i].value);
    }

    // Verify no overlap: last of first half != first of second half
    try testing.expect(!std.mem.eql(u8, first_half[49].value, second_half[0].value));
}

// ── Persistence ─────────────────────────────────────────────────────────────

test "integration: persistence across close/reopen" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Phase 1: Create topics and publish events
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();

        try tm.createTopic("persist.a");
        try tm.createTopic("persist.b");
        _ = try tm.publish("persist.a", "key1", "value-a-1");
        _ = try tm.publish("persist.a", null, "value-a-2");
        _ = try tm.publish("persist.b", "key2", "value-b-1");
    }

    // Phase 2: Reopen and verify everything survived
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();

        // Topics must exist
        try testing.expect(tm.hasTopic("persist.a"));
        try testing.expect(tm.hasTopic("persist.b"));

        // Fetch persist.a — should see 2 events
        const events_a = try tm.fetch(allocator, "persist.a", 0, 100);
        defer freeEvents(events_a);
        try testing.expectEqual(@as(usize, 2), events_a.len);
        try testing.expectEqualStrings("value-a-1", events_a[0].value);
        try testing.expectEqualStrings("key1", events_a[0].key.?);
        try testing.expectEqualStrings("value-a-2", events_a[1].value);
        try testing.expectEqual(@as(?[]const u8, null), events_a[1].key);

        // Fetch persist.b — should see 1 event
        const events_b = try tm.fetch(allocator, "persist.b", 0, 100);
        defer freeEvents(events_b);
        try testing.expectEqual(@as(usize, 1), events_b.len);
        try testing.expectEqualStrings("value-b-1", events_b[0].value);

        // Verify next offset continues from where it left off
        const new_offset = try tm.publish("persist.a", null, "value-a-3");
        // Offsets: 0=persist.a marker, 1=persist.b marker, 2=a1, 3=a2, 4=b1, 5=a3
        try testing.expectEqual(@as(u64, 5), new_offset);
    }
}

// ── Error Cases ─────────────────────────────────────────────────────────────

test "integration: fetch non-existent topic returns error" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const result = tm.fetch(allocator, "ghost.topic", 0, 10);
    try testing.expectError(error.NotFound, result);
}

test "integration: create duplicate topic returns error" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("unique");
    try testing.expectError(error.AlreadyExists, tm.createTopic("unique"));
}

test "integration: publish to non-existent topic returns error" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const result = tm.publish("nope", null, "data");
    try testing.expectError(error.NotFound, result);
}

// ── Pattern Fetch Across Topics ─────────────────────────────────────────────

test "integration: pattern fetch across multiple topics" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("app.logs");
    try tm.createTopic("app.metrics");
    try tm.createTopic("system.health");

    _ = try tm.publish("app.logs", null, "log1");
    _ = try tm.publish("app.metrics", null, "metric1");
    _ = try tm.publish("system.health", null, "health1");
    _ = try tm.publish("app.logs", null, "log2");

    // Prefix pattern "app." should match app.logs and app.metrics
    const app_events = try tm.fetchPattern(allocator, "app.", 0, 100);
    defer freeEvents(app_events);
    try testing.expectEqual(@as(usize, 3), app_events.len);

    // "." should match everything
    const all_events = try tm.fetchPattern(allocator, ".", 0, 100);
    defer freeEvents(all_events);
    try testing.expectEqual(@as(usize, 4), all_events.len);

    // Wildcard "app.*" should match app.logs and app.metrics
    const wildcard_events = try tm.fetchPattern(allocator, "app.*", 0, 100);
    defer freeEvents(wildcard_events);
    try testing.expectEqual(@as(usize, 3), wildcard_events.len);
}

// ── Segment Rotation ────────────────────────────────────────────────────────

test "integration: data survives segment rotation" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Very small segments to force rotation
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{
        .max_segment_size = 100,
        .sync_on_append = false,
    });
    defer tm.deinit();

    try tm.createTopic("rotate");

    // Publish enough events to trigger multiple segment rotations
    for (0..20) |i| {
        var buf: [64]u8 = undefined;
        const val = std.fmt.bufPrint(&buf, "rotation-test-{d}", .{i}) catch unreachable;
        _ = try tm.publish("rotate", null, val);
    }

    // Verify all events readable
    const events = try tm.fetch(allocator, "rotate", 0, 100);
    defer freeEvents(events);
    try testing.expectEqual(@as(usize, 20), events.len);

    for (events, 0..) |evt, i| {
        var buf: [64]u8 = undefined;
        const expected = std.fmt.bufPrint(&buf, "rotation-test-{d}", .{i}) catch unreachable;
        try testing.expectEqualStrings(expected, evt.value);
    }

    // Verify multiple segments were created
    try testing.expect(tm.log.segments.items.len > 1);
}

// ── Empty Topic Persistence ─────────────────────────────────────────────────

test "integration: empty topic persists across restart" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Create topic but don't publish anything
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("empty.topic");
        try testing.expect(tm.hasTopic("empty.topic"));
    }

    // Reopen — topic should still exist (marker event in log)
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try testing.expect(tm.hasTopic("empty.topic"));

        // Fetch should return 0 events (marker filtered out)
        const events = try tm.fetch(allocator, "empty.topic", 0, 100);
        defer freeEvents(events);
        try testing.expectEqual(@as(usize, 0), events.len);
    }
}

// ── Hook Registration Cursor Semantics ──────────────────────────────────────
//
// Verifies the fix from air/v0.1/hook-registration-cursor.org: newly-registered
// hooks observe only events published after registration, not historical ones.

const HookTable = ever.hooks.HookTable;
const freeHookSnapshot = ever.hooks.freeHookSnapshot;

fn makeHookDir(suffix: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "/tmp/.ever-integ-hook-{d}-{s}", .{ std.os.linux.getpid(), suffix });
    const path_z = try allocator.allocSentinel(u8, path.len, 0);
    defer allocator.free(path_z);
    @memcpy(path_z[0..path.len], path);
    _ = std.os.linux.mkdir(path_z.ptr, 0o755);
    return path;
}

fn cleanupHookDir(path: []const u8) void {
    const hj = std.fmt.allocPrint(allocator, "{s}/hooks.json", .{path}) catch return;
    defer allocator.free(hj);
    const hj_z = allocator.allocSentinel(u8, hj.len, 0) catch return;
    defer allocator.free(hj_z);
    @memcpy(hj_z[0..hj.len], hj);
    _ = std.os.linux.unlink(hj_z.ptr);
    const p_z = allocator.allocSentinel(u8, path.len, 0) catch return;
    defer allocator.free(p_z);
    @memcpy(p_z[0..path.len], path);
    _ = std.os.linux.rmdir(p_z.ptr);
    allocator.free(path);
}

test "integration: hook registered at tip skips pre-existing events" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t.cursor");
    _ = try tm.publish("t.cursor", null, "pre-1");
    _ = try tm.publish("t.cursor", null, "pre-2");

    // Simulate the server registration path: compute tip under the same
    // lock used by publish, then insert the hook with that cursor.
    const hook_dir = try makeHookDir("tip");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    tm.lockForHookRegistration();
    const tip = tm.tipForPatternLocked("t.cursor");
    const hook_id = try ht.addWithCursor("t.cursor", "echo", "/tmp", false, null, null, tip);
    tm.unlockForHookRegistration();

    try testing.expectEqual(@as(u64, 2), tip);

    // Simulate the daemon's fetch: starting from hook.cursor, no historical
    // events should be visible.
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    const events_before = try tm.fetch(allocator, "t.cursor", snap[0].cursor, 100);
    defer freeEvents(events_before);
    try testing.expectEqual(@as(usize, 0), events_before.len);

    // Publish two more events — these should appear.
    _ = try tm.publish("t.cursor", null, "post-1");
    _ = try tm.publish("t.cursor", null, "post-2");

    const events_after = try tm.fetch(allocator, "t.cursor", snap[0].cursor, 100);
    defer freeEvents(events_after);
    try testing.expectEqual(@as(usize, 2), events_after.len);
    try testing.expectEqualStrings("post-1", events_after[0].value);
    try testing.expectEqualStrings("post-2", events_after[1].value);
    _ = hook_id;
}

test "integration: hook with from-beginning (cursor=0) replays history" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t.replay");
    _ = try tm.publish("t.replay", null, "a");
    _ = try tm.publish("t.replay", null, "b");
    _ = try tm.publish("t.replay", null, "c");

    const hook_dir = try makeHookDir("replay");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    // Explicit cursor=0 (from-beginning): historical events are visible.
    _ = try ht.addWithCursor("t.replay", "echo", "/tmp", false, null, null, 0);
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);

    const events = try tm.fetch(allocator, "t.replay", snap[0].cursor, 100);
    defer freeEvents(events);
    try testing.expectEqual(@as(usize, 3), events.len);
}

test "integration: hook with --from N starts at explicit offset" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t.fromn");
    _ = try tm.publish("t.fromn", null, "0");
    _ = try tm.publish("t.fromn", null, "1");
    _ = try tm.publish("t.fromn", null, "2");
    _ = try tm.publish("t.fromn", null, "3");

    const hook_dir = try makeHookDir("fromn");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    _ = try ht.addWithCursor("t.fromn", "echo", "/tmp", false, null, null, 2);
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);

    const events = try tm.fetch(allocator, "t.fromn", snap[0].cursor, 100);
    defer freeEvents(events);
    // Skip first 2 real events → see "2" and "3".
    try testing.expectEqual(@as(usize, 2), events.len);
    try testing.expectEqualStrings("2", events[0].value);
    try testing.expectEqualStrings("3", events[1].value);
}

test "integration: B1 wildcard hook delivers events on low-count and new topics" {
    // Reproducer for the B1 blocker on commit fd61e99: a wildcard/prefix hook
    // registered with a single global "tip" cursor silently drops events on
    //   (a) topics that exist but have a non_marker_count below `tip`, and
    //   (b) topics that don't exist yet at registration time.
    //
    // The fix routes wildcard hooks through `fetchPatternByOffset`, which uses
    // a global log-offset cursor instead of a per-topic skip count.
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("agent.busy");
    try tm.createTopic("agent.quiet");
    _ = try tm.publish("agent.busy", null, "b1");
    _ = try tm.publish("agent.busy", null, "b2");
    _ = try tm.publish("agent.busy", null, "b3");
    _ = try tm.publish("agent.quiet", null, "q1");

    // "Tip" semantics for a wildcard hook: only events published from now on
    // (on any matching topic, including topics that don't exist yet) should
    // be delivered. With the fixed semantics, this is the current global
    // log offset.
    const tip = tm.tipForPattern("agent.");

    // (a) Publish into a low-count existing topic. Pre-fix, the global
    // skip-count cursor (tip == 3) swallows this event because
    // agent.quiet has only 1+1=2 non-marker events, both below 3.
    _ = try tm.publish("agent.quiet", null, "q2-after");

    // (b) A topic created AFTER registration with a future event must fire.
    try tm.createTopic("agent.late");
    _ = try tm.publish("agent.late", null, "late-1");

    // (c) Sanity: an event on a high-count topic still fires.
    _ = try tm.publish("agent.busy", null, "b4-after");

    const events = try tm.fetchPatternByOffset(allocator, "agent.", tip, 100);
    defer freeEvents(events);

    var saw_q2 = false;
    var saw_late = false;
    var saw_b4 = false;
    for (events) |e| {
        if (std.mem.eql(u8, e.value, "q2-after")) saw_q2 = true;
        if (std.mem.eql(u8, e.value, "late-1")) saw_late = true;
        if (std.mem.eql(u8, e.value, "b4-after")) saw_b4 = true;
    }
    try testing.expect(saw_q2);
    try testing.expect(saw_late);
    try testing.expect(saw_b4);
    try testing.expectEqual(@as(usize, 3), events.len);
}

test "integration: --once hook with no prior events waits for next" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("t.once");
    _ = try tm.publish("t.once", null, "stale"); // pre-existing event
    const tip = tm.tipForPattern("t.once");

    const hook_dir = try makeHookDir("once");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    _ = try ht.addWithCursor("t.once", "echo", "/tmp", true, null, null, tip);
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);

    // Immediately after registration: no events pending for the --once hook.
    const none = try tm.fetch(allocator, "t.once", snap[0].cursor, 10);
    defer freeEvents(none);
    try testing.expectEqual(@as(usize, 0), none.len);

    // Publish the awaited event; hook should now see exactly this one.
    _ = try tm.publish("t.once", null, "fresh");
    const pending = try tm.fetch(allocator, "t.once", snap[0].cursor, 10);
    defer freeEvents(pending);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expectEqualStrings("fresh", pending[0].value);
}

// ── Atomic Topic + Hook Creation (P6) ───────────────────────────────────────
//
// Exercises `ever.net.createTopicAndRegisterHook` — the server-side critical
// section behind `ever hook add --create-topic` — without standing up TCP.
// See air/v0.1/topic-hook-atomic-create.org.

const createTopicAndRegisterHook = ever.net.createTopicAndRegisterHook;

test "integration: atomic create-topic-plus-hook arms hook before any publish" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const hook_dir = try makeHookDir("atomic");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    // One call: topic exists AND hook is armed at the fresh topic's tip (0).
    const id = try createTopicAndRegisterHook(&tm, &ht, "t.atomic", "echo", "/tmp", false, null, null);
    try testing.expect(tm.hasTopic("t.atomic"));

    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    try testing.expectEqual(@as(usize, 1), snap.len);
    try testing.expectEqual(id, snap[0].id);
    try testing.expectEqualStrings("t.atomic", snap[0].pattern);
    try testing.expectEqual(@as(u64, 0), snap[0].cursor);

    // The very next publish is visible from the hook's cursor — exactly once.
    _ = try tm.publish("t.atomic", null, "first");
    const pending = try tm.fetch(allocator, "t.atomic", snap[0].cursor, 10);
    defer freeEvents(pending);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expectEqualStrings("first", pending[0].value);

    // After the daemon would advance the cursor past it, nothing remains.
    const drained = try tm.fetch(allocator, "t.atomic", snap[0].cursor + 1, 10);
    defer freeEvents(drained);
    try testing.expectEqual(@as(usize, 0), drained.len);
}

test "integration: create-topic on existing or tombstoned topic registers no hook" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const hook_dir = try makeHookDir("exists");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    // Existing topic → AlreadyExists, no hook.
    try tm.createTopic("t.exists");
    try testing.expectError(error.AlreadyExists, createTopicAndRegisterHook(&tm, &ht, "t.exists", "echo", "/tmp", false, null, null));

    // Tombstoned topic → also AlreadyExists (soft-deleted names stay taken).
    try tm.createTopic("t.dead");
    try tm.deleteTopic("t.dead");
    try testing.expectError(error.AlreadyExists, createTopicAndRegisterHook(&tm, &ht, "t.dead", "echo", "/tmp", false, null, null));

    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    try testing.expectEqual(@as(usize, 0), snap.len);
}

test "integration: create-topic rejects pattern shapes, nothing created" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const hook_dir = try makeHookDir("shape");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    // Prefix, all-topics, and wildcard shapes are all rejected up front.
    try testing.expectError(error.InvalidName, createTopicAndRegisterHook(&tm, &ht, "agent.", "echo", "/tmp", false, null, null));
    try testing.expectError(error.InvalidName, createTopicAndRegisterHook(&tm, &ht, ".", "echo", "/tmp", false, null, null));
    try testing.expectError(error.InvalidName, createTopicAndRegisterHook(&tm, &ht, "agent.*", "echo", "/tmp", false, null, null));

    try testing.expect(!tm.hasTopic("agent."));
    try testing.expect(!tm.hasTopic("agent.*"));
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    try testing.expectEqual(@as(usize, 0), snap.len);
}

test "integration: marker-append failure removes the hook — neither half survives" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const hook_dir = try makeHookDir("inject");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    // Inject a failure in the topic-creation half. The hook was registered
    // first (it can be removed cleanly; a topic cannot be un-created), so
    // the error path must remove it.
    tm.test_fail_marker_append = true;
    try testing.expectError(error.InjectedMarkerAppendFailure, createTopicAndRegisterHook(&tm, &ht, "t.inject", "echo", "/tmp", false, null, null));
    tm.test_fail_marker_append = false;

    try testing.expect(!tm.hasTopic("t.inject"));
    {
        const snap = try ht.snapshot(allocator);
        defer freeHookSnapshot(allocator, snap);
        try testing.expectEqual(@as(usize, 0), snap.len);
    }

    // The caller can simply retry once the fault clears.
    _ = try createTopicAndRegisterHook(&tm, &ht, "t.inject", "echo", "/tmp", false, null, null);
    try testing.expect(tm.hasTopic("t.inject"));
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    try testing.expectEqual(@as(usize, 1), snap.len);
}

test "integration: two-step topic-create then hook-add flow is unchanged" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const hook_dir = try makeHookDir("twostep");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    // Step 1: create the topic and let history accumulate.
    try tm.createTopic("t.twostep");
    _ = try tm.publish("t.twostep", null, "pre-1");
    _ = try tm.publish("t.twostep", null, "pre-2");

    // Step 2: plain hook add — same registration path as before this change:
    // tip computed under the lock, hook inserted with that cursor.
    tm.lockForHookRegistration();
    const tip = tm.tipForPatternLocked("t.twostep");
    _ = try ht.addWithCursor("t.twostep", "echo", "/tmp", false, null, null, tip);
    tm.unlockForHookRegistration();

    // Cursor semantics unchanged: history skipped, next event delivered.
    try testing.expectEqual(@as(u64, 2), tip);
    _ = try tm.publish("t.twostep", null, "post-1");
    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    const pending = try tm.fetch(allocator, "t.twostep", snap[0].cursor, 10);
    defer freeEvents(pending);
    try testing.expectEqual(@as(usize, 1), pending.len);
    try testing.expectEqualStrings("post-1", pending[0].value);
}

// ── Offset Coherence (air/v0.1/offset-coherence.org) ───────────────────────
//
// `after_offset` is a global-offset cursor: "resume strictly after this
// event". Round-trip it over the wire and verify `topic_events` is carried
// for exact-topic requests (powering the beyond-end warning) and absent
// for pattern requests.

test "integration: after_offset cursor over TCP round-trips displayed offsets" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("coher.t");
    try tm.createTopic("coher.other");
    _ = try tm.publish("coher.other", null, "noise"); // interleave global offsets
    const g1 = try tm.publish("coher.t", null, "{\"n\":1}");
    const g2 = try tm.publish("coher.t", null, "{\"n\":2}");
    const g3 = try tm.publish("coher.t", null, "{\"n\":3}");

    const pid_part: u16 = @intCast(@mod(std.os.linux.getpid(), 20000));
    const port: u16 = 32000 + pid_part;
    var server = try ever.net.Server.init(allocator, io, &tm, .{ .address = "127.0.0.1", .port = port });
    defer server.deinit();
    const thread = try std.Thread.spawn(.{}, runServerForTest, .{&server});
    defer {
        server.shutdown_requested.store(true, .release);
        _ = ever.status.probeServer(io, "127.0.0.1", port, 50); // wake accept loop
        thread.join();
        server.shutdown();
    }

    var attempts: u32 = 0;
    var reachable = false;
    while (attempts < 50) : (attempts += 1) {
        if (ever.status.probeServer(io, "127.0.0.1", port, 50)) {
            reachable = true;
            break;
        }
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 20_000_000 }, null);
    }
    try testing.expect(reachable);

    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
    defer client.deinit();

    // A plain fetch tags events with global offsets; feed the 2nd one back.
    {
        var all = try client.fetch("coher.t", 0, 10);
        defer all.deinit();
        try testing.expectEqual(@as(usize, 3), all.events.len);
        try testing.expectEqual(g1, all.events[0].offset);
        try testing.expectEqual(@as(?u64, 3), all.topic_events);

        var after2 = try client.fetchAfter("coher.t", null, all.events[1].offset, 10, 0);
        defer after2.deinit();
        try testing.expectEqual(@as(usize, 1), after2.events.len);
        try testing.expectEqual(g3, after2.events[0].offset);
        try testing.expectEqualStrings("{\"n\":3}", after2.events[0].value);
    }

    // Strictly after the tip → nothing, and topic_events still says 3.
    {
        var at_tip = try client.fetchAfter("coher.t", null, g3, 10, 0);
        defer at_tip.deinit();
        try testing.expectEqual(@as(usize, 0), at_tip.events.len);
        try testing.expectEqual(@as(?u64, 3), at_tip.topic_events);
    }

    // Skip-count fetch beyond the end: empty, but topic_events lets the
    // client distinguish this from a genuinely empty topic.
    {
        var beyond = try client.fetch("coher.t", 160, 10);
        defer beyond.deinit();
        try testing.expectEqual(@as(usize, 0), beyond.events.len);
        try testing.expectEqual(@as(?u64, 3), beyond.topic_events);
    }

    // Pattern fetch with after_offset reuses the global-offset path and
    // carries no topic_events (patterns aggregate several topics).
    {
        var pat = try client.fetchAfter(null, "coher.", g2, 10, 0);
        defer pat.deinit();
        try testing.expectEqual(@as(usize, 1), pat.events.len);
        try testing.expectEqual(g3, pat.events[0].offset);
        try testing.expectEqual(@as(?u64, null), pat.topic_events);
    }

    // Blocking fetch with after_offset: publish the next event, then wait.
    {
        const g4 = try tm.publish("coher.t", null, "{\"n\":4}");
        var next = try client.fetchAfter("coher.t", null, g3, 10, 2000);
        defer next.deinit();
        try testing.expectEqual(@as(usize, 1), next.events.len);
        try testing.expectEqual(g4, next.events[0].offset);
    }
}

// ── Hook Logs over TCP ──────────────────────────────────────────────────────
//
// Server-side split from air/v0.1/hook-logs-json.org: an unknown hook ID is
// a not_found error, while a known hook with no recorded executions answers
// hook_logs_ok with empty log_path + content. A typo'd ID must not read as
// "no executions", and an empty history is not an error.

test "integration: hook logs distinguishes no-such-hook from no-executions" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const hook_dir = try makeHookDir("hooklogs");
    defer cleanupHookDir(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    const hook_id = try ht.addFull("hl.topic", "true", "/tmp", false, null, "hl-hook");

    // The daemon is attached but never started — handleHookLogs only
    // requires its presence, not a polling thread.
    const empty_envp: [0:null]?[*:0]const u8 = .{};
    var hd = ever.hooks.HookDaemon.init(allocator, &ht, &tm, &empty_envp);

    const pid_part: u16 = @intCast(@mod(std.os.linux.getpid(), 20000));
    const port: u16 = 33000 + pid_part;
    var server = try ever.net.Server.init(allocator, io, &tm, .{ .address = "127.0.0.1", .port = port });
    defer server.deinit();
    server.setHookTable(&ht);
    server.setHookDaemon(&hd);

    const thread = try std.Thread.spawn(.{}, runServerForTest, .{&server});

    var reachable = false;
    var attempts: u32 = 0;
    while (attempts < 50) : (attempts += 1) {
        if (ever.status.probeServer(io, "127.0.0.1", port, 50)) {
            reachable = true;
            break;
        }
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 20_000_000 }, null);
    }
    try testing.expect(reachable);

    // Known hook, no executions → empty-success, not an error.
    {
        var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
        defer client.deinit();
        var res = try client.hookLogs(hook_id);
        defer res.deinit();
        try testing.expectEqual(hook_id, res.hook_id);
        try testing.expectEqual(@as(usize, 0), res.log_path.len);
        try testing.expectEqual(@as(usize, 0), res.content.len);
    }

    // Unknown hook ID → not_found error ("no such hook"). Spoken raw so
    // the assertion covers the wire shape (code + message) and the client's
    // stderr diagnostic doesn't fire inside the test runner.
    {
        const ip4 = try std.Io.net.Ip4Address.parse("127.0.0.1", port);
        const ip_address: std.Io.net.IpAddress = .{ .ip4 = ip4 };
        const stream = try ip_address.connect(io, .{ .mode = .stream });
        defer stream.close(io);
        const cfd = stream.socket.handle;

        const req_body = try ever.protocol.encodeBody(allocator, ever.protocol.HookLogsRequest{ .hook_id = hook_id + 999 });
        defer allocator.free(req_body);
        try ever.protocol.writeFrame(cfd, .hook_logs, req_body);

        const frame = (try ever.protocol.readFrame(allocator, cfd)) orelse return error.TestUnexpectedResult;
        defer allocator.free(frame.body);
        try testing.expectEqual(ever.protocol.MessageType.error_response, frame.msg_type);
        const parsed = try ever.protocol.decodeBody(ever.protocol.ErrorResponse, allocator, frame.body);
        defer parsed.deinit();
        try testing.expectEqual(ever.protocol.ErrorCode.not_found, parsed.value.code);
        try testing.expect(std.mem.indexOf(u8, parsed.value.message, "no such hook") != null);
    }

    // With a recorded execution log on disk, content and path round-trip.
    const hooks_subdir = try std.fmt.allocPrint(allocator, "{s}/hooks", .{hook_dir});
    defer allocator.free(hooks_subdir);
    const log_file = try std.fmt.allocPrint(allocator, "{s}/{d}-1751274843123.log", .{ hooks_subdir, hook_id });
    defer allocator.free(log_file);
    {
        const subdir_z = try allocator.allocSentinel(u8, hooks_subdir.len, 0);
        defer allocator.free(subdir_z);
        @memcpy(subdir_z[0..hooks_subdir.len], hooks_subdir);
        _ = std.os.linux.mkdir(subdir_z.ptr, 0o755);

        const log_z = try allocator.allocSentinel(u8, log_file.len, 0);
        defer allocator.free(log_z);
        @memcpy(log_z[0..log_file.len], log_file);
        const fd_rc = std.os.linux.open(log_z.ptr, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, 0o644);
        try testing.expect(@as(isize, @bitCast(fd_rc)) >= 0);
        const fd: i32 = @intCast(fd_rc);
        const content = "=== Hook Execution ===\nHook:      #1 (hl-hook)\n===\n\nout\n";
        var written: usize = 0;
        while (written < content.len) {
            const w = std.os.linux.write(fd, content[written..].ptr, content.len - written);
            if (@as(isize, @bitCast(w)) <= 0) break;
            written += @intCast(w);
        }
        _ = std.os.linux.close(fd);

        var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
        defer client.deinit();
        var res = try client.hookLogs(hook_id);
        defer res.deinit();
        try testing.expectEqualStrings(log_file, res.log_path);
        try testing.expectEqualStrings(content, res.content);

        _ = std.os.linux.unlink(log_z.ptr);
        _ = std.os.linux.rmdir(subdir_z.ptr);
    }

    server.shutdown_requested.store(true, .release);
    _ = ever.status.probeServer(io, "127.0.0.1", port, 50); // wake accept loop
    thread.join();
    server.shutdown();
}

// ── Hook list legibility tests (air/v0.1/hook-list-legibility.org) ──────────
//
// Presentation-side derivation of the uniform Pending / Fired listing values
// (counter persistence and increment tests live with hook-failure-visibility).

const computeHookPending = ever.net.computeHookPending;
const hook_pending_cap = ever.net.hook_pending_cap;
const HookDaemon = ever.hooks.HookDaemon;

const legibility_test_envp: [*:null]const ?[*:0]const u8 = &[_:null]?[*:0]const u8{};

/// Scratch dir cleanup that also removes the daemon's hooks/ log tree.
fn cleanupHookTree(path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(io, path) catch {};
    allocator.free(path);
}

/// Register a hook at the current tip for `pattern`, mirroring the server
/// registration path (tip computed under the publish lock).
fn registerHookAtTip(tm: *TopicManager, ht: *HookTable, pattern: []const u8, command: []const u8) !u64 {
    tm.lockForHookRegistration();
    defer tm.unlockForHookRegistration();
    const tip = tm.tipForPatternLocked(pattern);
    return ht.addWithCursor(pattern, command, "/tmp", false, null, null, tip);
}

test "integration: fresh exact and prefix hooks both derive Pending 0 / Fired 0" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    // Prior events on the log — the raw cursors of the two hooks will differ
    // wildly (topic-local skip count vs global log offset), the derived
    // Pending/Fired must not.
    try tm.createTopic("wutest.wever.subtest");
    try tm.createTopic("wutest.wever.other");
    _ = try tm.publish("wutest.wever.other", null, "noise-1");
    _ = try tm.publish("wutest.wever.subtest", null, "pre-1");
    _ = try tm.publish("wutest.wever.subtest", null, "pre-2");
    _ = try tm.publish("wutest.wever.other", null, "noise-2");

    const hook_dir = try makeHookDir("legib-fresh");
    defer cleanupHookTree(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    _ = try registerHookAtTip(&tm, &ht, "wutest.wever.subtest", "/bin/sh -c 'exit 0'");
    _ = try registerHookAtTip(&tm, &ht, "wutest.wever.", "/bin/sh -c 'exit 0'");

    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    try testing.expectEqual(@as(usize, 2), snap.len);

    // Raw cursors diverge by construction (the 4-vs-164 trap)…
    try testing.expect(snap[0].cursor != snap[1].cursor);

    // …but the derived listing values are uniform: Pending 0 / Fired 0.
    const exact = try computeHookPending(&tm, allocator, snap[0].pattern, snap[0].cursor);
    try testing.expectEqual(@as(u64, 0), exact.pending);
    try testing.expectEqualStrings("topic_local", exact.cursor_kind);
    try testing.expectEqual(@as(u64, 0), snap[0].fired_count);

    const prefix = try computeHookPending(&tm, allocator, snap[1].pattern, snap[1].cursor);
    try testing.expectEqual(@as(u64, 0), prefix.pending);
    try testing.expectEqualStrings("global", prefix.cursor_kind);
    try testing.expectEqual(@as(u64, 0), snap[1].fired_count);

    // An exact hook on a topic that does not exist yet also derives 0.
    const ghost = try computeHookPending(&tm, allocator, "wutest.wever.ghost", 0);
    try testing.expectEqual(@as(u64, 0), ghost.pending);
    try testing.expectEqualStrings("topic_local", ghost.cursor_kind);
}

test "integration: daemon consumption drives Fired to N and Pending back to 0" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("t.legib.fire");

    const hook_dir = try makeHookDir("legib-fire");
    defer cleanupHookTree(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    _ = try registerHookAtTip(&tm, &ht, "t.legib.fire", "/bin/sh -c 'exit 0'");
    _ = try registerHookAtTip(&tm, &ht, "t.legib.", "/bin/sh -c 'exit 0'");

    var daemon = HookDaemon.init(allocator, &ht, &tm, legibility_test_envp);
    try daemon.start();
    defer daemon.stop();

    const n = 3;
    var i: usize = 0;
    while (i < n) : (i += 1) _ = try tm.publish("t.legib.fire", null, "evt");

    // Wait for the daemon (500ms poll cadence) to deliver all N to both hooks.
    var waited_ms: usize = 0;
    while (waited_ms < 15_000) : (waited_ms += 50) {
        const snap = try ht.snapshot(allocator);
        defer freeHookSnapshot(allocator, snap);
        if (snap[0].fired_count >= n and snap[1].fired_count >= n) break;
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);
    }

    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);
    for (snap) |hook| {
        try testing.expectEqual(@as(u64, n), hook.fired_count);
        const derived = try computeHookPending(&tm, allocator, hook.pattern, hook.cursor);
        try testing.expectEqual(@as(u64, 0), derived.pending);
    }
}

test "integration: backlog while the daemon is stopped grows for both cursor kinds" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("t.legib.stopped");

    const hook_dir = try makeHookDir("legib-stop");
    defer cleanupHookTree(hook_dir);
    var ht = try HookTable.init(allocator, hook_dir);
    defer ht.deinit();

    _ = try registerHookAtTip(&tm, &ht, "t.legib.stopped", "/bin/sh -c 'exit 0'");
    _ = try registerHookAtTip(&tm, &ht, "t.legib.", "/bin/sh -c 'exit 0'");

    // No daemon runs — publish a backlog.
    _ = try tm.publish("t.legib.stopped", null, "b1");
    _ = try tm.publish("t.legib.stopped", null, "b2");

    const snap = try ht.snapshot(allocator);
    defer freeHookSnapshot(allocator, snap);

    // Identical backlog for both cursor kinds.
    for (snap) |hook| {
        try testing.expectEqual(@as(u64, 0), hook.fired_count);
        const derived = try computeHookPending(&tm, allocator, hook.pattern, hook.cursor);
        try testing.expectEqual(@as(u64, 2), derived.pending);
    }

    // Marker events don't inflate the backlog, and a matching event on a
    // sibling topic is pending only for the prefix hook.
    try tm.createTopic("t.legib.sibling");
    _ = try tm.publish("t.legib.sibling", null, "s1");

    const exact = try computeHookPending(&tm, allocator, snap[0].pattern, snap[0].cursor);
    try testing.expectEqual(@as(u64, 2), exact.pending);
    const prefix = try computeHookPending(&tm, allocator, snap[1].pattern, snap[1].cursor);
    try testing.expectEqual(@as(u64, 3), prefix.pending);

    // The cap bounds the scan: a cap below the true backlog is honoured
    // (rendered as "1000+" by the CLI when it equals the cap).
    const capped = try tm.countPatternByOffset(allocator, "t.legib.", 0, 2);
    try testing.expectEqual(@as(u64, 2), capped);
}

// ── Publish Input Validation ────────────────────────────────────────────────
//
// From air/v0.1/publish-input-validation.org: a client publish of the
// reserved tombstone key with an empty value used to be accepted, returned
// an offset, was invisible to every reader, and then deleted the topic at
// the next restart — the damage separated from its cause by a restart.
//
// The regression is written against `TopicManager.publish` rather than
// through live sockets: both wire protocols call that one function, and it
// is the restart, not the transport, that the original bug hid behind.

test "integration: injected tombstone cannot delete a topic across a rebuild" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();

        try tm.createTopic("victim");
        _ = try tm.publish("victim", null, "before");
        const before = tm.log.nextOffset();

        // The exploit, as reported: the exact key/value pair `deleteTopic`
        // writes, from a client publish.
        try testing.expectError(error.ReservedKey, tm.publish("victim", "__ever_tombstone__", ""));
        // And the empty value on its own, which is what made it silent.
        try testing.expectError(error.EmptyValue, tm.publish("victim", null, ""));

        // Nothing was appended. If a refused publish still reached the log,
        // the tombstone would be on disk and the rebuild below would find it.
        try testing.expectEqual(before, tm.log.nextOffset());
        try testing.expect(!tm.isTopicDeleted("victim"));
    }

    // The rebuild is the whole point: this is where the injected tombstone
    // used to take effect.
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();

        try testing.expect(tm.hasTopic("victim"));
        try testing.expect(!tm.isTopicDeleted("victim"));

        // Alive *and* writable — the original bug left the topic readable but
        // permanently unpublishable, so existence alone is not enough.
        _ = try tm.publish("victim", null, "after-restart");
        try testing.expectError(error.AlreadyExists, tm.createTopic("victim"));

        const events = try tm.fetch(allocator, "victim", 0, 10);
        defer freeEvents(events);
        try testing.expectEqual(@as(usize, 2), events.len);
        try testing.expectEqualStrings("before", events[0].value);
        try testing.expectEqualStrings("after-restart", events[1].value);
    }
}

test "integration: a real deleteTopic still deletes across the same rebuild" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // The control for the test above: the guard must refuse the client's
    // tombstone without disarming the store's own. Same rebuild, opposite
    // expected outcome — so "topic survived" cannot pass by the tombstone
    // mechanism having quietly stopped working.
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();

        try tm.createTopic("doomed");
        _ = try tm.publish("doomed", null, "data");
        try tm.deleteTopic("doomed");
    }
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();

        try testing.expect(tm.hasTopic("doomed"));
        try testing.expect(tm.isTopicDeleted("doomed"));
        try testing.expectError(error.TopicDeleted, tm.publish("doomed", null, "nope"));

        // Historical events stay readable — soft delete, as before.
        const events = try tm.fetch(allocator, "doomed", 0, 10);
        defer freeEvents(events);
        try testing.expectEqual(@as(usize, 1), events.len);
    }
}

// ── Unknown opcode totality (air/v0.1/protocol-unknown-opcode.org) ─────────
//
// The request opcode arrives as an untrusted byte. Every value of that byte
// must produce an answer on the wire; none may end the process. The request
// side of these tests is written as raw bytes rather than through
// `writeFrame`, so the test compiles identically against a tree without the
// fix — that is what makes it a negative control rather than decoration.

/// Write a frame with an arbitrary opcode byte, bypassing `MessageType`.
fn writeRawFrame(fd: std.posix.fd_t, msg_type_byte: u8, body: []const u8) !void {
    var header: [ever.protocol.HEADER_SIZE]u8 = undefined;
    header[0] = ever.protocol.PROTOCOL_VERSION;
    header[1] = msg_type_byte;
    std.mem.writeInt(u32, header[2..6], @intCast(body.len), .little);
    try rawWriteAll(fd, &header);
    try rawWriteAll(fd, body);
}

fn rawWriteAll(fd: std.posix.fd_t, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const rc = std.os.linux.write(fd, data[written..].ptr, data.len - written);
        const signed: isize = @bitCast(rc);
        if (signed < 0) return error.WriteFailed;
        if (signed == 0) return error.WriteFailed;
        written += @intCast(signed);
    }
}

/// Send `msg_type_byte` and assert the reply is `400 unknown request type`.
fn expectUnknownRequestType(fd: std.posix.fd_t, msg_type_byte: u8, body: []const u8) !void {
    try writeRawFrame(fd, msg_type_byte, body);
    const frame = (try ever.protocol.readFrame(allocator, fd)) orelse return error.TestUnexpectedResult;
    defer allocator.free(frame.body);
    try testing.expectEqual(ever.protocol.MessageType.error_response, frame.msg_type);
    const parsed = try ever.protocol.decodeBody(ever.protocol.ErrorResponse, allocator, frame.body);
    defer parsed.deinit();
    try testing.expectEqual(ever.protocol.ErrorCode.bad_request, parsed.value.code);
    try testing.expectEqualStrings("unknown request type", parsed.value.message);
}

test "integration: every opcode byte is answered, not fatal" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("opcode.survivor");

    const pid_part: u16 = @intCast(@mod(std.os.linux.getpid(), 20000));
    const port: u16 = 34000 + pid_part;
    var server = try ever.net.Server.init(allocator, io, &tm, .{ .address = "127.0.0.1", .port = port });
    defer server.deinit();

    const thread = try std.Thread.spawn(.{}, runServerForTest, .{&server});

    var reachable = false;
    var attempts: u32 = 0;
    while (attempts < 50) : (attempts += 1) {
        if (ever.status.probeServer(io, "127.0.0.1", port, 50)) {
            reachable = true;
            break;
        }
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 20_000_000 }, null);
    }
    try testing.expect(reachable);

    {
        const ip4 = try std.Io.net.Ip4Address.parse("127.0.0.1", port);
        const ip_address: std.Io.net.IpAddress = .{ .ip4 = ip4 };
        const stream = try ip_address.connect(io, .{ .mode = .stream });
        defer stream.close(io);
        const cfd = stream.socket.handle;

        // Unassigned request byte: the request range ends at status = 0x10.
        // On an exhaustive MessageType this is the byte that killed the store.
        try expectUnknownRequestType(cfd, 0x11, "{}");

        // Retired byte: 0x06 was `ack`, which answered ack_ok to anything --
        // including this body, whose topic does not exist. It must now be
        // refused like any other opcode the server does not implement.
        // (air/v0.1/ack-opcode-removal.org)
        try expectUnknownRequestType(cfd, 0x06, "{\"topic\":\"no.such.topic\",\"group\":\"g\",\"offset\":7}");

        // Assigned but not a request: a response opcode replayed inbound.
        // This case always worked; assert the fix did not reroute it.
        try expectUnknownRequestType(cfd, 0x81, "{}");

        // The high end of the byte range, either side of the one assigned
        // value there (error_response = 0xFF).
        try expectUnknownRequestType(cfd, 0x91, "{}");
        try expectUnknownRequestType(cfd, 0xFE, "{}");

        // Same connection, still serving: the server neither died nor
        // dropped the client that spoke nonsense to it five times.
        try writeRawFrame(cfd, @intFromEnum(ever.protocol.MessageType.list_topics), "{}");

        const frame = (try ever.protocol.readFrame(allocator, cfd)) orelse return error.TestUnexpectedResult;
        defer allocator.free(frame.body);
        try testing.expectEqual(ever.protocol.MessageType.list_topics_ok, frame.msg_type);
        try testing.expect(std.mem.indexOf(u8, frame.body, "opcode.survivor") != null);
    }

    server.shutdown_requested.store(true, .release);
    _ = ever.status.probeServer(io, "127.0.0.1", port, 50); // wake accept loop
    thread.join();
    server.shutdown();
}
