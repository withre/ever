//! Integration tests for Ever — exercises the full publish-store-subscribe
//! pipeline through the TopicManager API (in-process, no TCP).
//!
//! All tests use `std.testing.allocator` for leak detection and
//! `std.testing.tmpDir` for isolated data directories.

const std = @import("std");
const store = @import("../store/store.zig");
const topic_mod = @import("../store/topic.zig");
const TopicManager = topic_mod.TopicManager;
const Event = store.Event;

const io = std.testing.io;
const testing = std.testing;
const allocator = testing.allocator;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn freeEvents(events: []Event) void {
    for (events) |e| store.freeEvent(allocator, e);
    allocator.free(events);
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
