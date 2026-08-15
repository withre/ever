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

// ── Server harness (air/v0.1/testable-server-lifecycle.org) ─────────────────
//
// One way to stand up a server in a test: ephemeral port, wait for genuine
// readiness, stop by joining. Every hand-rolled variant of this got at least
// one of the three wrong — a pid-derived port that collides across the three
// test binaries, a readiness poll whose ceiling was picked per test, and a
// thread that was never joined, so the listener outlived the test that
// created it.

/// A running server owned by a test.
///
/// Holds the `Server` *by pointer*, heap-allocated: the accept-loop thread
/// holds a `*Server`, so keeping it inline would move the server out from
/// under a running thread the moment the harness value itself is moved
/// (returned from `start`, stored in an array, captured by a closure).
const TestServer = struct {
    server: *ever.net.Server,
    thread: std.Thread,
    port: u16,

    /// Bind an ephemeral port, start accepting, and return once the server is
    /// genuinely reachable — not once it has been spawned.
    fn start(tm: *TopicManager) !TestServer {
        return startWithConfig(tm, .{});
    }

    /// As `start`, for a test that needs a non-default `data_dir` or
    /// connection limit. `address` and `port` are overridden: the whole point
    /// is that a test never names a port.
    fn startWithConfig(tm: *TopicManager, config: ever.net.Config) !TestServer {
        return startOwned(try create(tm, config));
    }

    /// Allocate and initialise a server without starting it — the hook and
    /// timer tests attach their tables before the accept loop may observe
    /// them. Hand the result straight to `startOwned`.
    fn create(tm: *TopicManager, config: ever.net.Config) !*ever.net.Server {
        var cfg = config;
        cfg.address = "127.0.0.1";
        cfg.port = 0; // ask the OS; `boundPort()` reports what we got
        const server = try allocator.create(ever.net.Server);
        errdefer allocator.destroy(server);
        server.* = try ever.net.Server.init(allocator, io, tm, cfg);
        return server;
    }

    /// Take ownership of a prepared server and start it. On failure the
    /// server is released, so a caller that got here never has to unwind it.
    fn startOwned(server: *ever.net.Server) !TestServer {
        errdefer {
            server.deinit();
            allocator.destroy(server);
        }
        const thread = try std.Thread.spawn(.{}, runServerForTest, .{server});

        // Two phases, reported separately: a server that never binds and a
        // server that binds but never answers are different failures, and a
        // test author reading "timed out" deserves to know which one.
        var port: u16 = 0;
        var waited_ms: u32 = 0;
        while (waited_ms < 2000) : (waited_ms += 5) {
            port = server.boundPort();
            if (port != 0) break;
            _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 5_000_000 }, null);
        }
        if (port == 0) {
            std.debug.print("TestServer.start: listener never bound within 2s\n", .{});
            server.shutdown();
            thread.join();
            return error.ServerNeverBound;
        }

        waited_ms = 0;
        while (waited_ms < 2000) : (waited_ms += 5) {
            if (ever.status.probeServer(io, "127.0.0.1", port, 50)) {
                return .{ .server = server, .thread = thread, .port = port };
            }
            _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 5_000_000 }, null);
        }
        std.debug.print("TestServer.start: bound port {d} never accepted within 2s\n", .{port});
        server.shutdown();
        thread.join();
        return error.ServerNeverAccepted;
    }

    /// Stop, join, and release. `defer` it; safe to call once.
    ///
    /// Close any client the test opened *first*: a connection handler writes
    /// to the server until its socket goes away, and the 5s drain gives up on
    /// an idle one rather than reading it out from under the handler.
    ///
    /// The join is deliberately unbounded and deliberately not optional: it is
    /// the assertion that `shutdown()` actually returned the accept loop. An
    /// unjoined thread can still hold the port when the next test binds, so a
    /// regression here has to hang this suite rather than leak quietly into a
    /// consumer's. `std.Thread` has no timed join, and bounding it would mean
    /// not joining.
    fn stop(self: *TestServer) void {
        self.server.shutdown();
        self.thread.join();
        self.release();
    }

    /// Free the server after the accept thread has been joined. Split out for
    /// the one test that stops the server by another route (the signal path)
    /// and still has to release it.
    fn release(self: *TestServer) void {
        self.server.deinit();
        allocator.destroy(self.server);
    }
};

/// Wait until a freshly-probed server's accept loop is genuinely parked in
/// `accept()`.
///
/// Readiness does not establish that on its own: the probe's connection can
/// still be sitting in the listen backlog, and then a `stop()` is returned by
/// that queued connection rather than by the wake-up self-connect — so the
/// test passes whether or not `shutdown()` can wake a blocked `accept()`.
/// Measured, not assumed: against an unfixed tree "start and stop twice"
/// passed until this settle was added, and hung afterwards.
fn settleIntoAccept() void {
    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 250_000_000 }, null);
}

// ── Status Server Probe ─────────────────────────────────────────────────────

test "integration: status server probe flips true then false" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    var ts = try TestServer.start(&tm);
    const port = ts.port;
    const reachable = ever.status.probeServer(io, "127.0.0.1", port, 200);
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

    ts.stop();

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

    var ts = try TestServer.startWithConfig(&tm, .{ .data_dir = data_dir });
    const port = ts.port;

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
    ts.stop();

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

    const events = (try tm.fetchPatternByOffset(allocator, "agent.", tip, 100)).events;
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

    var ts = try TestServer.start(&tm);
    defer ts.stop();
    const port = ts.port;

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

// ── Exclusive opener (air/v0.1/embedded-store-marker.org) ───────────────────

test "integration: exclusive TopicManager refuses a second opener" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true });
    defer tm.deinit();
    try tm.createTopic("excl.topic");
    _ = try tm.publish("excl.topic", null, "e1");

    // A second exclusive opener is refused — before it has read any
    // segment state it cannot trust.
    try testing.expectError(error.StoreLocked, TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false, .exclusive = true }));

    // A non-exclusive opener still succeeds: default false keeps today's
    // library behaviour reachable (multi-process-access.org's future
    // shared mode), and keeps the exclusion something you ask for.
    var tm2 = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    tm2.deinit();
}

// ── Pattern fetch refuses topic-local offsets ───────────────────────────────
//
// (air/v0.1/pattern-fetch-rejects-offset.org) `FetchRequest.offset` is a
// per-topic skip count; applied to every topic a pattern matches it is a
// number no client can compute a next value for, because the response
// interleaves topics. The server now refuses the combination outright
// rather than answering something no cursor can be built from.

test "integration: pattern fetch with a topic-local offset is refused" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("pat.a");
    try tm.createTopic("pat.b");
    _ = try tm.publish("pat.a", null, "a1");
    const g_b1 = try tm.publish("pat.b", null, "b1");
    const g_a2 = try tm.publish("pat.a", null, "a2");

    var ts = try TestServer.start(&tm);
    const port = ts.port;

    {
        var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
        defer client.deinit();

        const ip4 = try std.Io.net.Ip4Address.parse("127.0.0.1", port);
        const ip_address: std.Io.net.IpAddress = .{ .ip4 = ip4 };
        const stream = try ip_address.connect(io, .{ .mode = .stream });
        defer stream.close(io);
        const cfd = stream.socket.handle;

        // pattern + offset != 0, no after_offset: refused, with the reply
        // naming the cursor that does work.
        {
            try writeRawFrame(cfd, @intFromEnum(ever.protocol.MessageType.fetch), "{\"pattern\":\"pat.\",\"offset\":1}");
            const frame = (try ever.protocol.readFrame(allocator, cfd)) orelse return error.TestUnexpectedResult;
            defer allocator.free(frame.body);
            try testing.expectEqual(ever.protocol.MessageType.error_response, frame.msg_type);
            const parsed = try ever.protocol.decodeBody(ever.protocol.ErrorResponse, allocator, frame.body);
            defer parsed.deinit();
            try testing.expectEqual(ever.protocol.ErrorCode.bad_request, parsed.value.code);
            try testing.expectEqualStrings(
                "offset is a per-topic skip count and is undefined for a pattern; resume a pattern with after_offset",
                parsed.value.message,
            );
        }

        // pattern + offset == 0 keeps working: "from the beginning of every
        // matching topic" is well-defined and is what `sub <prefix>` and a
        // follow's first fetch rely on.
        {
            var all = try client.fetchPattern("pat.", 0, 10);
            defer all.deinit();
            try testing.expectEqual(@as(usize, 3), all.events.len);
        }

        // pattern + after_offset + offset != 0: after_offset wins, as its
        // doc-comment has always said. Sent raw because the client enforces
        // exclusivity and cannot produce this frame.
        {
            var buf: [128]u8 = undefined;
            const body = try std.fmt.bufPrint(&buf, "{{\"pattern\":\"pat.\",\"offset\":5,\"after_offset\":{d}}}", .{g_b1});
            try writeRawFrame(cfd, @intFromEnum(ever.protocol.MessageType.fetch), body);
            const frame = (try ever.protocol.readFrame(allocator, cfd)) orelse return error.TestUnexpectedResult;
            defer allocator.free(frame.body);
            try testing.expectEqual(ever.protocol.MessageType.fetch_ok, frame.msg_type);
            const parsed = try ever.protocol.decodeBody(ever.protocol.FetchResponse, allocator, frame.body);
            defer parsed.deinit();
            try testing.expectEqual(@as(usize, 1), parsed.value.events.len);
            try testing.expectEqual(g_a2, parsed.value.events[0].offset);
        }

        // exact topic + offset != 0 is untouched: topic-local skip counts on
        // a single topic are meaningful and used.
        {
            var skipped = try client.fetch("pat.a", 1, 10);
            defer skipped.deinit();
            try testing.expectEqual(@as(usize, 1), skipped.events.len);
            try testing.expectEqualStrings("a2", skipped.events[0].value);
        }
    }

    ts.stop();
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

    // Tables attached before the accept loop can observe them, which is why
    // this one builds the server itself instead of calling `start`.
    const server = try TestServer.create(&tm, .{});
    server.setHookTable(&ht);
    server.setHookDaemon(&hd);
    var ts = try TestServer.startOwned(server);
    const port = ts.port;

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

    ts.stop();
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

    var ts = try TestServer.start(&tm);
    const port = ts.port;

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

    ts.stop();
}

// ── Fetch seeks to its cursor (air/v0.1/subscribe-fetch-seek.org) ───────────
//
// `fetch`/`fetchPattern` take a topic-local skip count over *non-marker*
// events while the index stores *records*, so seeking to the cursor requires
// knowing where the markers are. The risk of getting that translation wrong
// is returning the wrong events, which is worse than being slow — hence a
// differential test against a reference implementation over a deliberately
// awkward marker layout, before any assertion about cost.

/// Reference: every non-marker value of `topic`, in order, read the slow way.
fn referenceNonMarkerValues(tm: *TopicManager, topic: []const u8) ![][]u8 {
    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |v| allocator.free(v);
        out.deinit(allocator);
    }
    var offset: u64 = 0;
    while (offset < tm.log.nextOffset()) : (offset += 1) {
        const evt = (try tm.log.read(allocator, offset)) orelse continue;
        defer store.freeEvent(allocator, evt);
        if (evt.value.len == 0) continue;
        if (!std.mem.eql(u8, evt.topic, topic)) continue;
        try out.append(allocator, try allocator.dupe(u8, evt.value));
    }
    return out.toOwnedSlice(allocator);
}

test "integration: fetch seeks past markers and agrees with a full scan" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    // Build an awkward layout in the log directly, then let `rebuildIndex`
    // construct the index from it on reopen. Markers leading, interior,
    // adjacent, and trailing — the shapes a seek can get wrong.
    {
        var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
        defer tm.deinit();
        try tm.createTopic("seek.topic"); // record 0: creation marker
        _ = try tm.log.append("seek.topic", null, "a"); // 1
        _ = try tm.log.append("seek.topic", null, ""); // 2  marker
        _ = try tm.log.append("seek.topic", null, "b"); // 3
        _ = try tm.log.append("seek.topic", null, ""); // 4  marker
        _ = try tm.log.append("seek.topic", null, ""); // 5  marker, adjacent
        _ = try tm.log.append("seek.topic", null, "c"); // 6
        _ = try tm.log.append("seek.topic", null, "d"); // 7
        _ = try tm.log.append("seek.topic", null, ""); // 8  trailing marker
    }

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const expected = try referenceNonMarkerValues(&tm, "seek.topic");
    defer {
        for (expected) |v| allocator.free(v);
        allocator.free(expected);
    }
    try testing.expectEqual(@as(usize, 4), expected.len);

    // Every start position, including one past the end, against both an
    // exact-topic fetch and a pattern fetch.
    var start: u64 = 0;
    while (start <= expected.len + 1) : (start += 1) {
        for ([_]u32{ 1, 2, 100 }) |max_count| {
            const want_len = @min(
                @as(u64, max_count),
                if (start >= expected.len) 0 else expected.len - start,
            );

            const got = try tm.fetch(allocator, "seek.topic", start, max_count);
            defer freeEvents(got);
            try testing.expectEqual(want_len, got.len);
            for (got, 0..) |evt, i| try testing.expectEqualStrings(expected[@intCast(start + i)], evt.value);

            const got_pat = try tm.fetchPattern(allocator, "seek.", start, max_count);
            defer freeEvents(got_pat);
            try testing.expectEqual(want_len, got_pat.len);
            for (got_pat, 0..) |evt, i| try testing.expectEqualStrings(expected[@intCast(start + i)], evt.value);
        }
    }
}

test "integration: fetch at the tail does not re-read the topic" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("cost.topic");
    const n: u64 = 2000;
    var i: u64 = 0;
    while (i < n) : (i += 1) _ = try tm.publish("cost.topic", null, "x");

    // The subscriber's steady state: parked at the tail, nothing to deliver.
    // Before the seek this read all 2000 records off disk to discard them.
    ever.store.Log.test_read_count = 0;
    {
        const events = try tm.fetch(allocator, "cost.topic", n, 10);
        defer freeEvents(events);
        try testing.expectEqual(@as(usize, 0), events.len);
    }
    const tail_reads = ever.store.Log.test_read_count;
    try testing.expect(tail_reads <= 8);

    // And one event behind the tail costs one read, not n.
    ever.store.Log.test_read_count = 0;
    {
        const events = try tm.fetch(allocator, "cost.topic", n - 1, 10);
        defer freeEvents(events);
        try testing.expectEqual(@as(usize, 1), events.len);
    }
    try testing.expect(ever.store.Log.test_read_count <= 8);

    // A full read still reads everything — the seek must not have turned
    // into "skip some events".
    ever.store.Log.test_read_count = 0;
    {
        const events = try tm.fetch(allocator, "cost.topic", 0, @intCast(n));
        defer freeEvents(events);
        try testing.expectEqual(@as(usize, n), events.len);
    }
    try testing.expect(ever.store.Log.test_read_count >= n);
}

// ── A fetch says how far it looked (air/v0.1/fetch-watermark.org) ──────────

test "integration: quiet pattern subscriber is O(new events), not O(log)" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    // A quiet topic the subscriber cares about, and a busy foreign one.
    try tm.createTopic("quiet.stream");
    try tm.createTopic("noise.stream");
    const last_match = try tm.publish("quiet.stream", null, "m0");
    const n: u64 = 2000;
    var i: u64 = 0;
    while (i < n) : (i += 1) _ = try tm.publish("noise.stream", null, "x");

    var ts = try TestServer.start(&tm);
    defer ts.stop();
    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", ts.port);
    defer client.deinit();

    // The subscriber's steady state: it delivered m0 long ago and resumes
    // strictly after it. This scan examines all the noise, delivers nothing.
    var after: u64 = last_match;
    {
        var result = try client.fetchAfter(null, "quiet.", after, 100, 0);
        defer result.deinit();
        try testing.expectEqual(@as(usize, 0), result.events.len);
        // The resume rule: strictly after max(last delivered, watermark - 1).
        // Before the watermark existed the cursor could only advance by
        // delivery, so with nothing delivered the next scan was O(log).
        for (result.events) |evt| after = @max(after, evt.offset);
        if (result.scan_watermark) |wm| {
            if (wm > 0) after = @max(after, wm - 1);
        }
    }

    // The second fetch must not re-walk the range the first one ruled out.
    ever.store.Log.test_read_count = 0;
    {
        var result = try client.fetchAfter(null, "quiet.", after, 100, 0);
        defer result.deinit();
        try testing.expectEqual(@as(usize, 0), result.events.len);
    }
    try testing.expect(ever.store.Log.test_read_count <= 32);
}

test "integration: the watermark is a global exclusive offset a fetch can resume from" {
    // The unit round-trip offset-coherence.org wanted: the watermark is in
    // the same space as after_offset and exclusive, so the very next event
    // published lands exactly AT it, and resuming strictly after wm - 1
    // yields exactly the events past the examined range — no gap, no rewind.
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("quiet.stream");
    try tm.createTopic("noise.stream");
    const last_match = try tm.publish("quiet.stream", null, "m0");
    var i: u64 = 0;
    while (i < 50) : (i += 1) _ = try tm.publish("noise.stream", null, "x");

    var ts = try TestServer.start(&tm);
    defer ts.stop();
    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", ts.port);
    defer client.deinit();

    const wm = blk: {
        var result = try client.fetchAfter(null, "quiet.", last_match, 100, 0);
        defer result.deinit();
        try testing.expectEqual(@as(usize, 0), result.events.len);
        break :blk result.scan_watermark.?;
    };

    // Exclusive upper bound of the examined range == the next offset the log
    // will assign. Two fresh matches land at wm and (being adjacent) wm + 1.
    const g_new = try tm.publish("quiet.stream", null, "m1");
    try testing.expectEqual(wm, g_new);
    const g_new2 = try tm.publish("quiet.stream", null, "m2");

    var result = try client.fetchAfter(null, "quiet.", wm - 1, 100, 0);
    defer result.deinit();
    try testing.expectEqual(@as(usize, 2), result.events.len);
    try testing.expectEqual(g_new, result.events[0].offset);
    try testing.expectEqual(g_new2, result.events[1].offset);
    try testing.expectEqualStrings("m1", result.events[0].value);
}

/// A blocking pattern fetch on its own thread, capturing what the reply
/// carried. The fetchAfter sibling of `BlockedFetcher` below, for the
/// watermark tests — which need the pattern+after_offset path and the
/// response's scan_watermark, not just an event count.
const BlockedPatternFetcher = struct {
    port: u16,
    pattern: []const u8,
    after: u64,
    block_ms: u32,
    event_count: std.atomic.Value(u64) = .init(0),
    watermark: std.atomic.Value(u64) = .init(0),
    sent: std.atomic.Value(bool) = .init(false),

    fn run(self: *BlockedPatternFetcher) void {
        var client = ever.client.Client.connect(allocator, io, "127.0.0.1", self.port) catch return;
        defer client.deinit();
        self.sent.store(true, .release);
        var res = client.fetchAfter(null, self.pattern, self.after, 10, self.block_ms) catch return;
        defer res.deinit();
        self.event_count.store(res.events.len, .release);
        if (res.scan_watermark) |wm| self.watermark.store(wm, .release);
    }
};

/// Wait, bounded, until `cond` observes true. The bound exists so a broken
/// tree fails this test with a nameable error instead of hanging the suite.
fn waitBounded(comptime cond: anytype, args: anytype) !void {
    var waited_ms: u32 = 0;
    while (waited_ms < 10_000) : (waited_ms += 1) {
        if (@call(.auto, cond, args)) return;
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 1_000_000 }, null);
    }
    return error.SubscriberStalled;
}

fn readCountAtLeast(target: usize) bool {
    return @atomicLoad(usize, &ever.store.Log.test_read_count, .monotonic) >= target;
}

fn attemptsAtLeast(target: u64) bool {
    return ever.net.test_fetch_attempts.load(.monotonic) >= target;
}

test "integration: a parked fetch advances its watermark across wakes" {
    // The decision the spec makes explicitly: keep parking, and advance a
    // server-side watermark on each wake. Every foreign publish wakes the
    // parked fetch and its rescan must start where the previous one stopped
    // — otherwise the server pays O(log) per wake for the whole block
    // window even though the client eventually gets the right watermark.
    //
    // Coordination is by condition, not by time, and it is deliberately
    // tighter than the other blocking-fetch tests need to be: publishing
    // *during* a pattern scan trips the pre-existing readBatch/append race
    // (log-read-append-serialization.org — a Non-Goal of the watermark
    // work), which errors the fetch and fails this test for the wrong
    // reason. So each round publishes exactly one event and then proves the
    // rescan is over before the next publish: first the read counter covers
    // the new record (the scan examined it), then appendEpoch() — which
    // needs the manager mutex the scan holds end-to-end — returns, so the
    // scan has finished its reads entirely. Only then may the next append
    // land. Wakes themselves are not timed: a publish's epoch bump makes
    // the rescan inevitable, however late it runs.
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    try tm.createTopic("quiet.stream");
    try tm.createTopic("noise.stream");
    const last_match = try tm.publish("quiet.stream", null, "m0");

    var ts = try TestServer.start(&tm);
    defer ts.stop();

    ever.net.test_fetch_attempts.store(0, .monotonic);
    @atomicStore(usize, &ever.store.Log.test_read_count, 0, .monotonic);

    var f = BlockedPatternFetcher{ .port = ts.port, .pattern = "quiet.", .after = last_match, .block_ms = 30_000 };
    const ft = try std.Thread.spawn(.{}, BlockedPatternFetcher.run, .{&f});

    // The request has arrived and looked once — from here on, every scan is
    // a wake of the parked fetch, which is the path under test. The initial
    // scan's range is empty (nothing follows m0 yet), so it reads nothing
    // and the first publish cannot race it.
    try waitBounded(attemptsAtLeast, .{@as(u64, 1)});

    // One foreign publish per round. A rescan-from-request-start
    // implementation reads all k earlier records again on round k — at
    // least 1+2+…+120 = 7,260 reads in total; one that carries its cursor
    // across wakes reads each record once — about 120.
    const rounds: u64 = 120;
    var round: u64 = 0;
    while (round < rounds) : (round += 1) {
        _ = try tm.publish("noise.stream", null, "x");
        // The rescan this publish forces has examined the new record…
        try waitBounded(readCountAtLeast, .{@as(usize, @intCast(round + 1))});
        // …and has released the manager mutex, i.e. finished reading.
        _ = tm.appendEpoch();
    }

    // A match ends the block window deterministically — no timeout to wait
    // out — and the reply must carry the watermark the wakes accumulated.
    const g_match = try tm.publish("quiet.stream", null, "m1");
    ft.join();

    try testing.expectEqual(@as(u64, 1), f.event_count.load(.acquire));
    try testing.expectEqual(g_match + 1, f.watermark.load(.acquire));
    // The server-side half of the fix: the whole window cost O(new events).
    try testing.expect(@atomicLoad(usize, &ever.store.Log.test_read_count, .monotonic) <= 1_000);
}

// ── Store locks park, they do not spin (air/v0.1/store-blocking-locks.org) ──
//
// Correctness is unchanged by the spinlock -> Io.Mutex conversion, so the
// only assertion that distinguishes the two implementations is the one about
// what a *waiting* thread costs. This test fails on the spinlock.

const LockHog = struct {
    tm: *TopicManager,
    hold_ns: i64,
    started: std.atomic.Value(bool) = .init(false),

    fn run(self: *LockHog) void {
        self.tm.lockForHookRegistration();
        defer self.tm.unlockForHookRegistration();
        self.started.store(true, .release);
        _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = self.hold_ns }, null);
    }
};

fn waitForTopicCount(tm: *TopicManager) void {
    // Any call that takes the manager lock. Blocks until the hog releases.
    _ = tm.topicEventCount("lock.topic");
}

fn threadCpuNanos() i64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.PROCESS_CPUTIME_ID, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

test "integration: threads waiting on the store lock burn no CPU" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("lock.topic");

    const hold_ms = 300;
    var hog = LockHog{ .tm = &tm, .hold_ns = hold_ms * 1_000_000 };
    const hog_thread = try std.Thread.spawn(.{}, LockHog.run, .{&hog});

    // Wait until the hog definitely holds the lock, without holding it here.
    while (!hog.started.load(.acquire)) _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 1_000_000 }, null);

    const cpu_before = threadCpuNanos();
    var waiters: [4]std.Thread = undefined;
    for (&waiters) |*t| t.* = try std.Thread.spawn(.{}, waitForTopicCount, .{&tm});
    for (&waiters) |*t| t.join();
    const cpu_after = threadCpuNanos();
    hog_thread.join();

    // Four threads each blocked for ~300ms. Spinning would charge the process
    // ~1.2s of CPU; parking charges approximately nothing. The bound is loose
    // on purpose — the point is to separate 1,200,000,000 from ~0, not to
    // measure a scheduler.
    const spent_ns = cpu_after - cpu_before;
    const spin_would_cost_ns: i64 = waiters.len * hold_ms * 1_000_000;
    try testing.expect(spent_ns < @divTrunc(spin_would_cost_ns, 4));
}

// ── Notified delivery (air/v0.1/subscribe-notify-on-append.org) ─────────────
//
// A blocking fetch waits on the publication epoch instead of sleeping on a
// 100ms timer. These tests all fail on the polling tree, which is what makes
// them worth having: two on latency, one on idle cost, one on the race that
// is the only bug this design can actually have.

/// A blocking fetch run on its own thread, recording when it was served.
const BlockedFetcher = struct {
    port: u16,
    topic: []const u8,
    from: u64,
    block_ms: u32,
    served_at_ns: std.atomic.Value(i64) = .init(0),
    event_count: std.atomic.Value(u64) = .init(0),
    sent: std.atomic.Value(bool) = .init(false),

    fn run(self: *BlockedFetcher) void {
        var client = ever.client.Client.connect(allocator, io, "127.0.0.1", self.port) catch return;
        defer client.deinit();
        self.sent.store(true, .release);
        var res = client.fetchBlocking(self.topic, null, self.from, 10, self.block_ms) catch return;
        defer res.deinit();
        self.event_count.store(res.events.len, .release);
        self.served_at_ns.store(nowNanos(), .release);
    }
};

fn nowNanos() i64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.BOOTTIME, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

fn processCpuNanos() i64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.PROCESS_CPUTIME_ID, &ts);
    return ts.sec * 1_000_000_000 + ts.nsec;
}

test "integration: a publish wakes a blocked subscriber immediately" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("notify.topic");

    var h = try TestServer.start(&tm);
    const port = h.port;

    var f = BlockedFetcher{ .port = port, .topic = "notify.topic", .from = 0, .block_ms = 30_000 };
    const ft = try std.Thread.spawn(.{}, BlockedFetcher.run, .{&f});

    // Let it get all the way into the wait. Generous, so the test is about
    // the wakeup and not about a race to park.
    while (!f.sent.load(.acquire)) _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 1_000_000 }, null);
    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 200_000_000 }, null);

    const published_at = nowNanos();
    _ = try tm.publish("notify.topic", null, "wake up");
    ft.join();

    try testing.expectEqual(@as(u64, 1), f.event_count.load(.acquire));
    const latency_ms = @divTrunc(f.served_at_ns.load(.acquire) - published_at, 1_000_000);
    // The old floor was a 100ms grid; a notified wakeup is a scheduler hop.
    // 20ms separates the two without measuring the scheduler.
    try testing.expect(latency_ms < 20);

    h.stop();
}

test "integration: blocked subscribers look once, not ten times a second" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("idle.topic");

    var h = try TestServer.start(&tm);
    const port = h.port;

    var fetchers: [4]BlockedFetcher = undefined;
    var threads: [4]std.Thread = undefined;
    for (&fetchers, &threads) |*f, *t| {
        f.* = .{ .port = port, .topic = "idle.topic", .from = 0, .block_ms = 1_500 };
        t.* = try std.Thread.spawn(.{}, BlockedFetcher.run, .{f});
    }
    for (&fetchers) |*f| {
        while (!f.sent.load(.acquire)) _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 1_000_000 }, null);
    }
    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 100_000_000 }, null);

    // Deterministic, unlike a CPU threshold: with a 100ms poll each of the
    // four subscribers looks ~10 times a second, so ~40 looks over the
    // window. Parked waiters look once each and then not again.
    ever.net.test_fetch_attempts.store(0, .monotonic);
    const cpu_before = processCpuNanos();
    _ = std.os.linux.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);
    const spent_ns = processCpuNanos() - cpu_before;
    const looks = ever.net.test_fetch_attempts.load(.monotonic);
    for (&threads) |*t| t.join();

    try testing.expect(looks == 0);
    // And the CPU that used to buy those looks is gone too.
    try testing.expect(spent_ns < 50_000_000);

    h.stop();
}

test "unit: an append between the look and the wait is not slept through" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("window.topic");

    // The one bug this design can have, reproduced deterministically instead
    // of hoped for over a socket. A reader takes its baseline, looks and
    // finds nothing, and *then* the append lands. The wait must return at
    // once, because the epoch has already moved past the baseline.
    const baseline = tm.appendEpoch();
    _ = try tm.publish("window.topic", null, "landed in the window");

    const t0 = nowNanos();
    const woke_at = tm.waitForAppend(baseline, 10_000);
    const waited_ms = @divTrunc(nowNanos() - t0, 1_000_000);

    try testing.expect(woke_at != baseline);
    try testing.expect(waited_ms < 100);

    // And with no append, the same call does wait out its timeout rather
    // than spinning through it — otherwise the test above proves nothing.
    const t1 = nowNanos();
    _ = tm.waitForAppend(woke_at, 150);
    try testing.expect(@divTrunc(nowNanos() - t1, 1_000_000) >= 100);
}

test "integration: a publish racing the wait is never slept through" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("race.topic");

    var h = try TestServer.start(&tm);
    const port = h.port;

    // Same window as the unit test above, but end to end, so the assertion
    // covers the server reading its baseline *before* the fetch rather than
    // after. The fetch-attempt counter is what makes the timing aimable:
    // publish the instant the server starts looking, which is as close to
    // the window as this side of the socket can get.
    var round: u64 = 0;
    while (round < 40) : (round += 1) {
        ever.net.test_fetch_attempts.store(0, .monotonic);
        var f = BlockedFetcher{ .port = port, .topic = "race.topic", .from = round, .block_ms = 2_000 };
        const ft = try std.Thread.spawn(.{}, BlockedFetcher.run, .{&f});
        while (ever.net.test_fetch_attempts.load(.monotonic) == 0) {
            _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 50_000 }, null);
        }
        _ = try tm.publish("race.topic", null, "racy");
        ft.join();
        try testing.expectEqual(@as(u64, 1), f.event_count.load(.acquire));
    }

    h.stop();
}

test "integration: shutdown does not wait out a subscriber's block window" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("bye.topic");

    var h = try TestServer.start(&tm);
    const port = h.port;

    // A subscriber that asked to block for a minute. Parking made this the
    // one property replacing a poll with a wait takes away, so shutdown has
    // to give it back explicitly.
    var f = BlockedFetcher{ .port = port, .topic = "bye.topic", .from = 0, .block_ms = 60_000 };
    const ft = try std.Thread.spawn(.{}, BlockedFetcher.run, .{&f});
    while (!f.sent.load(.acquire)) _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 1_000_000 }, null);
    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 200_000_000 }, null);

    const t0 = nowNanos();
    h.stop();
    ft.join();
    const elapsed_ms = @divTrunc(nowNanos() - t0, 1_000_000);
    try testing.expect(elapsed_ms < 2_000);
}

// ── One frame, one write (air/v0.1/protocol-frame-single-write.org) ─────────

test "unit: frame resumption advances across the header/body boundary" {
    const header = "HDRHDR";
    const body = "0123456789";
    const remainder = ever.protocol.frameRemainder;

    // Nothing sent yet.
    {
        const h, const b = remainder(header, body, 0);
        try testing.expectEqualStrings("HDRHDR", h);
        try testing.expectEqualStrings("0123456789", b);
    }
    // Short *within* the header.
    {
        const h, const b = remainder(header, body, 2);
        try testing.expectEqualStrings("RHDR", h);
        try testing.expectEqualStrings("0123456789", b);
    }
    // Short *exactly at* the boundary — the case a naive resumption gets
    // wrong by restarting the body.
    {
        const h, const b = remainder(header, body, header.len);
        try testing.expectEqualStrings("", h);
        try testing.expectEqualStrings("0123456789", b);
    }
    // Short within the body.
    {
        const h, const b = remainder(header, body, header.len + 4);
        try testing.expectEqualStrings("", h);
        try testing.expectEqualStrings("456789", b);
    }
    // Everything sent.
    {
        const h, const b = remainder(header, body, header.len + body.len);
        try testing.expectEqualStrings("", h);
        try testing.expectEqualStrings("", b);
    }
    // Empty body, header partly sent.
    {
        const h, const b = remainder(header, "", 3);
        try testing.expectEqualStrings("HDR", h);
        try testing.expectEqualStrings("", b);
    }
}

test "integration: a response is not held for the peer's delayed ACK" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("nagle.topic");
    // A body big enough to cross the socket buffer on the way out, so the
    // resumption path runs for real and not only in the unit test above.
    var i: u64 = 0;
    while (i < 400) : (i += 1) _ = try tm.publish("nagle.topic", null, "0123456789012345678901234567890123456789");

    var h = try TestServer.start(&tm);
    const port = h.port;

    const ip4 = try std.Io.net.Ip4Address.parse("127.0.0.1", port);
    const ip_address: std.Io.net.IpAddress = .{ .ip4 = ip4 };
    const stream = try ip_address.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    const cfd = stream.socket.handle;

    // The first request on a connection is free either way — there is no
    // unacknowledged data for Nagle to wait on. It is the ones after it that
    // used to sit for the full delayed-ACK interval, so the assertion is on
    // the second and later.
    var round: usize = 0;
    var worst_ms: i64 = 0;
    while (round < 6) : (round += 1) {
        const req = try ever.protocol.encodeBody(allocator, ever.protocol.FetchRequest{
            .topic = "nagle.topic",
            .offset = 0,
            .max_count = 400,
        });
        defer allocator.free(req);

        const t0 = nowNanos();
        try ever.protocol.writeFrame(cfd, .fetch, req);
        const frame = (try ever.protocol.readFrame(allocator, cfd)) orelse return error.TestUnexpectedResult;
        defer allocator.free(frame.body);
        const elapsed_ms = @divTrunc(nowNanos() - t0, 1_000_000);

        try testing.expectEqual(ever.protocol.MessageType.fetch_ok, frame.msg_type);
        try testing.expect(frame.body.len > 4000); // multi-segment response
        if (round > 0 and elapsed_ms > worst_ms) worst_ms = elapsed_ms;
    }

    // Linux delayed ACK is 40ms. Anything under 15ms cannot have waited for
    // one; the gap being 0.3ms against 41ms means the bound needs no
    // precision.
    try testing.expect(worst_ms < 15);

    h.stop();
}

// ── Server lifecycle (air/v0.1/testable-server-lifecycle.org) ───────────────
//
// The defect these cover: `shutdown()` set a flag and closed the listener but
// never returned the thread blocked in `accept()`, so the only caller that
// could stop a server was the signal handler. Every test here stops a server
// and keeps going; before the fix each of them hangs at 0% CPU rather than
// failing.

test "integration: a server can be started and stopped twice in one test" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    var first = try TestServer.start(&tm);
    const first_port = first.port;
    try testing.expect(first_port != 0);
    settleIntoAccept();
    first.stop();

    // The second start is the assertion. A harness that skipped the join
    // would leave the accept thread holding the listener, and this would
    // either bind a fresh port while the old one leaked or, on the same
    // port, fail outright — `listen` here uses reuse_address = false.
    var second = try TestServer.start(&tm);
    defer second.stop();
    try testing.expect(second.port != 0);
    try testing.expect(ever.status.probeServer(io, "127.0.0.1", second.port, 200));
    settleIntoAccept(); // the deferred stop must face a blocked accept too
}

test "integration: two concurrent servers get different ephemeral ports" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    var a = try TestServer.start(&tm);
    defer a.stop();
    var b = try TestServer.start(&tm);
    defer b.stop();

    // Not merely different numbers: both are live listeners at the same time,
    // which is what a pid-derived port could not guarantee.
    try testing.expect(a.port != 0 and b.port != 0);
    try testing.expect(a.port != b.port);
    try testing.expect(ever.status.probeServer(io, "127.0.0.1", a.port, 200));
    try testing.expect(ever.status.probeServer(io, "127.0.0.1", b.port, 200));
    settleIntoAccept();
}

test "integration: a test that stops a server keeps running" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("lifecycle.topic");

    var ts = try TestServer.start(&tm);

    // Force the accept loop to be *genuinely blocked* before stopping, or
    // this test asserts nothing: if `stop()` lands before the loop reaches
    // `accept()`, the wake-up connection is queued in the backlog and the
    // loop never blocks — it passes with or without the fix.
    //
    // A completed request round-trip proves the loop finished an iteration
    // (it spawned the handler that answered), and the settle gives it time to
    // re-enter `accept()`, where there is nothing left for it to do.
    {
        var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", ts.port);
        defer client.deinit();
        const offset = try client.publish("lifecycle.topic", null, "before stop");
        try testing.expect(offset >= 0);
    }
    settleIntoAccept();

    ts.stop();

    // Everything below is the actual regression: unfixed, control never
    // arrives here — the suite hangs at 0% CPU inside `stop()`.
    try testing.expect(!ever.status.probeServer(io, "127.0.0.1", ts.port, 200));
    const offset = try tm.publish("lifecycle.topic", null, "after stop");
    try testing.expect(offset > 0);
}

test "integration: stop returns quickly with no client attached" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    var ts = try TestServer.start(&tm);
    settleIntoAccept();

    const t0 = nowNanos();
    ts.stop();
    const elapsed_ms = @divTrunc(nowNanos() - t0, 1_000_000);

    // Nothing to drain, so this is the wake-up round trip and a join. The
    // bound is loose because the assertion is "not the 5s drain, and not
    // forever", not a latency measurement.
    try testing.expect(elapsed_ms < 1000);
}

test "integration: stop returns within the drain bound with an idle client" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();
    try tm.createTopic("idle.client.topic");

    var ts = try TestServer.start(&tm);
    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", ts.port);
    // One completed request, so the connection is genuinely established and
    // its handler is parked in `readFrame` — an unread socket is exactly what
    // the drain cannot hurry.
    _ = try client.publish("idle.client.topic", null, "hello");
    settleIntoAccept();

    const t0 = nowNanos();
    ts.server.shutdown();
    const elapsed_ms = @divTrunc(nowNanos() - t0, 1_000_000);

    // An idle connection cannot be read out from under its handler, so this
    // waits out the 5s drain by design. The assertion is that `shutdown()`
    // *returns*, not that it returns fast.
    try testing.expect(elapsed_ms < 8000);

    // The expired drain is a real condition, and the library's whole report
    // of it is this value — recorded, never printed, because `Server` is
    // library API (air/v0.1/client-as-library-citizen.org, "Follow-on").
    try testing.expectEqual(@as(u32, 1), ts.server.undrainedConnections());

    // Close the client before the server is freed: its handler thread writes
    // to `active_connections` until its socket goes away, and closing alone
    // only *lets* it exit — the second `shutdown()` is what waits for it.
    // Without that wait this test reports "write after free" rather than a
    // hang, which is the same defect wearing different clothes.
    client.deinit();
    ts.thread.join();
    ts.server.shutdown();
    // Each shutdown records its own drain result: this one, with the client
    // gone, drains clean and overwrites the 1 above.
    try testing.expectEqual(@as(u32, 0), ts.server.undrainedConnections());
    ts.release();
}

test "integration: the signal path still stops the server" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    const server = try TestServer.create(&tm, .{});
    server.installSignalHandlers();
    defer {
        // Leave no handler pointing at a server this test is about to free.
        const dfl = std.os.linux.Sigaction{
            .handler = .{ .handler = std.os.linux.SIG.DFL },
            .mask = std.os.linux.sigemptyset(),
            .flags = 0,
        };
        _ = std.os.linux.sigaction(.INT, &dfl, null);
        _ = std.os.linux.sigaction(.TERM, &dfl, null);
    }

    var ts = try TestServer.startOwned(server);
    settleIntoAccept();

    // The CLI's stop sequence: the handler returns the accept loop, `run()`
    // exits, and only then does main call `shutdown()` to drain. Moving the
    // self-connect into `shutdown()` must not have cost the handler its own
    // route out — it cannot call `shutdown()`, which sleeps while draining.
    try std.posix.raise(.TERM);
    ts.thread.join();
    ts.server.shutdown();
    ts.release();
}

// ── Client as a library citizen (air/v0.1/client-as-library-citizen.org) ────
//
// `ever.client.Client` is library API: a server error_response must reach
// the caller as a code-mapped error tag with both wire fields recoverable
// via `lastError()`, connection loss must always be `error.ConnectionClosed`
// whichever syscall noticed first, and nothing is ever printed. The
// no-printing half is enforced by main.zig's stderr allow-list test, which
// pins client.zig at zero `std.debug.print` sites.

test "integration: a server error reaches the caller with code and message intact" {
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var tm = try TopicManager.init(allocator, io, tmp.dir, .{ .sync_on_append = false });
    defer tm.deinit();

    var ts = try TestServer.start(&tm);

    {
        var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", ts.port);
        defer client.deinit();

        // 404: the tag is the code, and both wire fields are retained.
        try testing.expectError(error.NotFound, client.publish("no.such.topic", null, "x"));
        try testing.expectEqual(@as(u16, 404), client.lastError().?.code);
        try testing.expectEqualStrings("topic not found", client.lastError().?.message);

        // A following successful request clears the retained error.
        try client.createTopic("lib.citizen.topic");
        try testing.expect(client.lastError() == null);

        // 409 arrives as its own tag, with its own words.
        try testing.expectError(error.Conflict, client.createTopic("lib.citizen.topic"));
        try testing.expectEqual(@as(u16, 409), client.lastError().?.code);
        try testing.expectEqualStrings("topic already exists", client.lastError().?.message);
    }

    ts.stop();
}

/// A bare TCP listener for the peers `TestServer` cannot play: one that
/// hangs up without a word, and one that answers with a frame the client
/// cannot parse.
const RawPeer = struct {
    server: std.Io.net.Server,
    port: u16,

    fn open() !RawPeer {
        const ip4 = try std.Io.net.Ip4Address.parse("127.0.0.1", 0);
        const address: std.Io.net.IpAddress = .{ .ip4 = ip4 };
        var server = try address.listen(io, .{});
        var sa: std.os.linux.sockaddr.in = undefined;
        var sa_len: std.os.linux.socklen_t = @sizeOf(std.os.linux.sockaddr.in);
        const rc = std.os.linux.getsockname(server.socket.handle, @ptrCast(&sa), &sa_len);
        if (std.os.linux.errno(rc) != .SUCCESS) {
            server.deinit(io);
            return error.GetSockNameFailed;
        }
        return .{ .server = server, .port = std.mem.bigToNative(u16, sa.port) };
    }

    fn deinit(self: *RawPeer) void {
        self.server.deinit(io);
    }
};

test "integration: connection loss is one tag on the read and the write side" {
    // The write-side half sends to a peer it already saw die; EPIPE raises
    // SIGPIPE, which would take down the binary instead of returning the
    // error under test.
    const ign = std.os.linux.Sigaction{
        .handler = .{ .handler = std.os.linux.SIG.IGN },
        .mask = std.os.linux.sigemptyset(),
        .flags = 0,
    };
    _ = std.os.linux.sigaction(.PIPE, &ign, null);

    var peer = try RawPeer.open();
    defer peer.deinit();

    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", peer.port);
    defer client.deinit();

    // The peer accepts and hangs up without a word.
    const conn = try peer.server.accept(io);
    conn.close(io);

    // Read side: the request lands in a kernel buffer; the reply is EOF.
    try testing.expectError(error.ConnectionClosed, client.publish("t", null, "x"));

    // Write side: the first request drew a reset, so this one dies inside
    // write() with EPIPE — same condition, and it must wear the same name,
    // not BrokenPipe.
    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);
    try testing.expectError(error.ConnectionClosed, client.publish("t", null, "x"));
}

/// Serves exactly one connection: swallows the request frame, answers with
/// an error_response whose body is not JSON, then holds the socket open
/// until the client hangs up (so the reply is never cut short by a reset).
/// Allocation-free on purpose — `testing.allocator` is not thread-safe and
/// this runs on its own thread.
fn serveUnparseableError(peer: *RawPeer) void {
    const conn = peer.server.accept(io) catch return;
    defer conn.close(io);
    const fd = conn.socket.handle;

    // Read past the request; its header names the body length.
    var header: [ever.protocol.HEADER_SIZE]u8 = undefined;
    var got: usize = 0;
    while (got < header.len) {
        const n = std.posix.read(fd, header[got..]) catch return;
        if (n == 0) return;
        got += n;
    }
    var remaining = std.mem.readInt(u32, header[2..6], .little);
    var buf: [512]u8 = undefined;
    while (remaining > 0) {
        const n = std.posix.read(fd, buf[0..@min(buf.len, remaining)]) catch return;
        if (n == 0) return;
        remaining -= @intCast(n);
    }

    ever.protocol.writeFrame(fd, .error_response, "not json") catch return;

    while (true) {
        const n = std.posix.read(fd, &buf) catch return;
        if (n == 0) return;
    }
}

test "integration: an unparseable error_response is ServerError with code 0" {
    var peer = try RawPeer.open();
    defer peer.deinit();

    const thread = try std.Thread.spawn(.{}, serveUnparseableError, .{&peer});

    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", peer.port);

    try testing.expectError(error.ServerError, client.publish("t", null, "x"));
    try testing.expectEqual(@as(u16, 0), client.lastError().?.code);
    try testing.expectEqualStrings("server returned an error (could not parse details)", client.lastError().?.message);

    client.deinit(); // hangs up: the peer's final read returns 0
    thread.join();
}
