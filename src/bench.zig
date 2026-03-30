//! Ever benchmarks — measures throughput and latency of core operations.
//!
//! Run with: zig build bench
//! Options:  zig build bench -- --filter append --json --iterations 50000

const std = @import("std");
const ever = @import("ever");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Dir = Io.Dir;

// ── Result types ────────────────────────────────────────────────────────────

const BenchResult = struct {
    name: []const u8,
    iterations: u64,
    total_ns: u64,
    bytes_processed: u64,
    timings: []u64, // per-iteration ns, caller owns

    fn avgNs(self: BenchResult) u64 {
        if (self.iterations == 0) return 0;
        return self.total_ns / self.iterations;
    }

    fn minNs(self: BenchResult) u64 {
        if (self.timings.len == 0) return 0;
        return self.timings[0]; // sorted
    }

    fn maxNs(self: BenchResult) u64 {
        if (self.timings.len == 0) return 0;
        return self.timings[self.timings.len - 1]; // sorted
    }

    fn p99Ns(self: BenchResult) u64 {
        if (self.timings.len == 0) return 0;
        const idx = (self.timings.len * 99) / 100;
        return self.timings[@min(idx, self.timings.len - 1)];
    }

    fn opsPerSec(self: BenchResult) u64 {
        if (self.total_ns == 0) return 0;
        return @intCast((@as(u128, self.iterations) * 1_000_000_000) / self.total_ns);
    }

    fn throughputMBps(self: BenchResult) f64 {
        if (self.total_ns == 0) return 0;
        const bytes_f: f64 = @floatFromInt(self.bytes_processed);
        const secs = @as(f64, @floatFromInt(self.total_ns)) / 1_000_000_000.0;
        return bytes_f / secs / (1024.0 * 1024.0);
    }
};

// ── Benchmark definitions ───────────────────────────────────────────────────

const BenchDef = struct {
    name: []const u8,
    func: *const fn (Allocator, Io, u64) anyerror!BenchResult,
    default_iters: u64,
};

const benchmarks = [_]BenchDef{
    .{ .name = "log_append_nosync", .func = benchLogAppendNoSync, .default_iters = 100_000 },
    .{ .name = "log_append_sync", .func = benchLogAppendSync, .default_iters = 5_000 },
    .{ .name = "log_read_sequential", .func = benchLogReadSequential, .default_iters = 10_000 },
    .{ .name = "log_read_batch_100", .func = benchLogReadBatch100, .default_iters = 100 },
    .{ .name = "topic_publish", .func = benchTopicPublish, .default_iters = 100_000 },
    // TCP benchmarks disabled: Zig v0.16 Io is not thread-safe across
    // server/client threads. Re-enable when Io threading model is resolved.
    // .{ .name = "tcp_publish", .func = benchTcpPublish, .default_iters = 5_000 },
    // .{ .name = "tcp_fetch", .func = benchTcpFetch, .default_iters = 5_000 },
};

// ── Sample payload ──────────────────────────────────────────────────────────

const sample_value = "{\"type\":\"agent.task.complete\",\"agent\":\"build-01\",\"task_id\":\"abc123\",\"status\":\"success\",\"duration_ms\":1234}";
const sample_key = "agent.build-01";

// ── Benchmark implementations ───────────────────────────────────────────────

fn benchLogAppendNoSync(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    return benchLogAppend(allocator, io, iterations, false);
}

fn benchLogAppendSync(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    return benchLogAppend(allocator, io, iterations, true);
}

fn benchLogAppend(allocator: Allocator, io: Io, iterations: u64, sync: bool) !BenchResult {
    const dir = try makeTempDir(io);
    defer cleanupTempDir(io, dir);

    var log = try ever.store.Log.init(allocator, io, dir, .{
        .max_segment_size = 256 * 1024 * 1024,
        .sync_on_append = sync,
    });
    defer log.deinit();

    const timings = try allocator.alloc(u64, @intCast(iterations));
    errdefer allocator.free(timings);

    var total_bytes: u64 = 0;
    var timer = try std.time.Timer.start();

    for (0..iterations) |i| {
        const lap_start = timer.read();
        _ = try log.append(sample_key, sample_value);
        timings[i] = timer.read() - lap_start;
        total_bytes += ever.store.Event.header_size + sample_key.len + sample_value.len;
    }

    const total_ns = timer.read();
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    return .{
        .name = if (sync) "log_append_sync" else "log_append_nosync",
        .iterations = iterations,
        .total_ns = total_ns,
        .bytes_processed = total_bytes,
        .timings = timings,
    };
}

fn benchLogReadSequential(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    const dir = try makeTempDir(io);
    defer cleanupTempDir(io, dir);

    var log = try ever.store.Log.init(allocator, io, dir, .{
        .max_segment_size = 256 * 1024 * 1024,
        .sync_on_append = false,
    });
    defer log.deinit();

    // Populate
    for (0..iterations) |_| {
        _ = try log.append(sample_key, sample_value);
    }

    const timings = try allocator.alloc(u64, @intCast(iterations));
    errdefer allocator.free(timings);

    var total_bytes: u64 = 0;
    var timer = try std.time.Timer.start();

    for (0..iterations) |i| {
        const lap_start = timer.read();
        const event = (try log.read(allocator, @intCast(i))).?;
        const elapsed = timer.read() - lap_start;
        timings[i] = elapsed;
        total_bytes += ever.store.Event.header_size + (if (event.key) |k| k.len else 0) + event.value.len;
        ever.store.freeEvent(allocator, event);
    }

    const total_ns = timer.read();
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    return .{
        .name = "log_read_sequential",
        .iterations = iterations,
        .total_ns = total_ns,
        .bytes_processed = total_bytes,
        .timings = timings,
    };
}

fn benchLogReadBatch100(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    const dir = try makeTempDir(io);
    defer cleanupTempDir(io, dir);

    var log = try ever.store.Log.init(allocator, io, dir, .{
        .max_segment_size = 256 * 1024 * 1024,
        .sync_on_append = false,
    });
    defer log.deinit();

    const total_events = iterations * 100;
    for (0..total_events) |_| {
        _ = try log.append(sample_key, sample_value);
    }

    const timings = try allocator.alloc(u64, @intCast(iterations));
    errdefer allocator.free(timings);

    var total_bytes: u64 = 0;
    var timer = try std.time.Timer.start();

    for (0..iterations) |i| {
        const lap_start = timer.read();
        const events = try log.readBatch(allocator, @as(u64, @intCast(i)) * 100, 100);
        const elapsed = timer.read() - lap_start;
        timings[i] = elapsed;
        for (events) |evt| {
            total_bytes += ever.store.Event.header_size + (if (evt.key) |k| k.len else 0) + evt.value.len;
            ever.store.freeEvent(allocator, evt);
        }
        allocator.free(events);
    }

    const total_ns = timer.read();
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    return .{
        .name = "log_read_batch_100",
        .iterations = iterations,
        .total_ns = total_ns,
        .bytes_processed = total_bytes,
        .timings = timings,
    };
}

fn benchTopicPublish(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    const dir = try makeTempDir(io);
    defer cleanupTempDir(io, dir);

    var tm = try ever.topic.TopicManager.init(allocator, io, dir, .{
        .max_segment_size = 256 * 1024 * 1024,
        .sync_on_append = false,
    });
    defer tm.deinit();

    try tm.createTopic("bench-topic");

    const timings = try allocator.alloc(u64, @intCast(iterations));
    errdefer allocator.free(timings);

    var total_bytes: u64 = 0;
    var timer = try std.time.Timer.start();

    for (0..iterations) |i| {
        const lap_start = timer.read();
        _ = try tm.publish("bench-topic", sample_key, sample_value);
        timings[i] = timer.read() - lap_start;
        total_bytes += ever.store.Event.header_size + sample_key.len + sample_value.len;
    }

    const total_ns = timer.read();
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    return .{
        .name = "topic_publish",
        .iterations = iterations,
        .total_ns = total_ns,
        .bytes_processed = total_bytes,
        .timings = timings,
    };
}

fn benchTcpPublish(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    const port: u16 = 17890;
    const dir = try makeTempDir(io);
    defer cleanupTempDir(io, dir);

    var tm = try ever.topic.TopicManager.init(allocator, io, dir, .{
        .sync_on_append = false,
    });
    defer tm.deinit();

    var server = try ever.net.Server.init(allocator, io, &tm, .{
        .address = "127.0.0.1",
        .port = port,
    });
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *ever.net.Server) void {
            s.run() catch {};
        }
    }.run, .{&server});

    // Give server a moment to start
    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 10_000_000 }, null);

    defer {
        server.shutdown();
        server_thread.join();
    }

    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
    defer client.deinit();

    try client.createTopic("bench-tcp");

    const timings = try allocator.alloc(u64, @intCast(iterations));
    errdefer allocator.free(timings);

    var total_bytes: u64 = 0;
    var timer = try std.time.Timer.start();

    for (0..iterations) |i| {
        const lap_start = timer.read();
        _ = try client.publish("bench-tcp", sample_key, sample_value);
        timings[i] = timer.read() - lap_start;
        total_bytes += sample_key.len + sample_value.len;
    }

    const total_ns = timer.read();
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    return .{
        .name = "tcp_publish",
        .iterations = iterations,
        .total_ns = total_ns,
        .bytes_processed = total_bytes,
        .timings = timings,
    };
}

fn benchTcpFetch(allocator: Allocator, io: Io, iterations: u64) !BenchResult {
    const port: u16 = 14223;
    const dir = try makeTempDir(io);
    defer cleanupTempDir(io, dir);

    var tm = try ever.topic.TopicManager.init(allocator, io, dir, .{
        .sync_on_append = false,
    });
    defer tm.deinit();

    var server = try ever.net.Server.init(allocator, io, &tm, .{
        .address = "127.0.0.1",
        .port = port,
    });
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *ever.net.Server) void {
            s.run() catch {};
        }
    }.run, .{&server});

    _ = std.os.linux.nanosleep(&.{ .sec = 0, .nsec = 10_000_000 }, null);

    defer {
        server.shutdown();
        server_thread.join();
    }

    // Pre-populate via client
    {
        var pub_client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
        defer pub_client.deinit();
        try pub_client.createTopic("bench-tcp-fetch");
        for (0..iterations) |_| {
            _ = try pub_client.publish("bench-tcp-fetch", sample_key, sample_value);
        }
    }

    var client = try ever.client.Client.connect(allocator, io, "127.0.0.1", port);
    defer client.deinit();

    const batch_size: u32 = 100;
    const fetch_iters = iterations / batch_size;
    const actual_iters = if (fetch_iters == 0) 1 else fetch_iters;

    const timings = try allocator.alloc(u64, @intCast(actual_iters));
    errdefer allocator.free(timings);

    var total_bytes: u64 = 0;
    var total_events: u64 = 0;
    var timer = try std.time.Timer.start();

    for (0..actual_iters) |i| {
        const lap_start = timer.read();
        var result = try client.fetch("bench-tcp-fetch", @as(u64, @intCast(i)) * batch_size, batch_size);
        const elapsed = timer.read() - lap_start;
        timings[i] = elapsed;
        total_events += result.events.len;
        for (result.events) |evt| {
            total_bytes += (if (evt.key) |k| k.len else 0) + evt.value.len;
        }
        result.deinit();
    }

    const total_ns = timer.read();
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));

    return .{
        .name = "tcp_fetch",
        .iterations = total_events,
        .total_ns = total_ns,
        .bytes_processed = total_bytes,
        .timings = timings,
    };
}

// ── Output formatting ───────────────────────────────────────────────────────

fn printResultsTable(results: []const BenchResult) void {
    std.debug.print("\n", .{});
    std.debug.print("Ever Benchmarks\n", .{});
    std.debug.print("══════════════════════════════════════════════════════════════════════════════════\n", .{});
    std.debug.print(" {s:<24} {s:>10} {s:>12} {s:>10} {s:>10} {s:>12}\n", .{
        "Benchmark", "Iters", "Ops/sec", "Avg", "p99", "Throughput",
    });
    std.debug.print("──────────────────────────────────────────────────────────────────────────────────\n", .{});

    for (results) |r| {
        var avg_buf: [16]u8 = undefined;
        var p99_buf: [16]u8 = undefined;
        var tp_buf: [16]u8 = undefined;

        const avg_str = fmtDuration(&avg_buf, r.avgNs());
        const p99_str = fmtDuration(&p99_buf, r.p99Ns());
        const tp_str = fmtThroughput(&tp_buf, r.throughputMBps());

        std.debug.print(" {s:<24} {d:>10} {d:>12} {s:>10} {s:>10} {s:>12}\n", .{
            r.name, r.iterations, r.opsPerSec(), avg_str, p99_str, tp_str,
        });
    }
    std.debug.print("\n", .{});
}

fn printResultsJson(allocator: Allocator, results: []const BenchResult) void {
    std.debug.print("{{\n  \"benchmarks\": [\n", .{});
    for (results, 0..) |r, i| {
        std.debug.print("    {{\n", .{});
        std.debug.print("      \"name\": \"{s}\",\n", .{r.name});
        std.debug.print("      \"iterations\": {d},\n", .{r.iterations});
        std.debug.print("      \"total_ns\": {d},\n", .{r.total_ns});
        std.debug.print("      \"avg_ns\": {d},\n", .{r.avgNs()});
        std.debug.print("      \"min_ns\": {d},\n", .{r.minNs()});
        std.debug.print("      \"max_ns\": {d},\n", .{r.maxNs()});
        std.debug.print("      \"p99_ns\": {d},\n", .{r.p99Ns()});
        std.debug.print("      \"ops_per_sec\": {d},\n", .{r.opsPerSec()});
        std.debug.print("      \"throughput_mbps\": {d:.1}\n", .{r.throughputMBps()});
        if (i < results.len - 1) {
            std.debug.print("    }},\n", .{});
        } else {
            std.debug.print("    }}\n", .{});
        }
    }
    std.debug.print("  ]\n}}\n", .{});
    _ = allocator;
}

fn fmtDuration(buf: []u8, ns: u64) []const u8 {
    if (ns < 1_000) {
        return std.fmt.bufPrint(buf, "{d}ns", .{ns}) catch "???";
    } else if (ns < 1_000_000) {
        const us = @as(f64, @floatFromInt(ns)) / 1_000.0;
        return std.fmt.bufPrint(buf, "{d:.1}us", .{us}) catch "???";
    } else if (ns < 1_000_000_000) {
        const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
        return std.fmt.bufPrint(buf, "{d:.1}ms", .{ms}) catch "???";
    } else {
        const s = @as(f64, @floatFromInt(ns)) / 1_000_000_000.0;
        return std.fmt.bufPrint(buf, "{d:.2}s", .{s}) catch "???";
    }
}

fn fmtThroughput(buf: []u8, mbps: f64) []const u8 {
    if (mbps >= 1024.0) {
        return std.fmt.bufPrint(buf, "{d:.1} GB/s", .{mbps / 1024.0}) catch "???";
    } else {
        return std.fmt.bufPrint(buf, "{d:.1} MB/s", .{mbps}) catch "???";
    }
}

// ── Temp directory helpers ──────────────────────────────────────────────────

var temp_counter: u32 = 0;

fn makeTempDir(io: Io) !Dir {
    temp_counter += 1;
    var buf: [64]u8 = undefined;
    const pid = std.os.linux.getpid();
    const name = std.fmt.bufPrint(&buf, "ever-bench-{d}-{d}", .{ pid, temp_counter }) catch "ever-bench-tmp";

    const cache_dir = Dir.cwd().createDirPathOpen(io, ".zig-cache/bench", .{}) catch |err| return err;
    defer cache_dir.close(io);

    const dir = cache_dir.createDirPathOpen(io, name, .{ .open_options = .{ .iterate = true } }) catch |err| return err;
    return dir;
}

fn cleanupTempDir(io: Io, dir: Dir) void {
    dir.close(io);
    // Best-effort cleanup
    const cache_dir = Dir.cwd().openDir(io, ".zig-cache/bench", .{}) catch return;
    defer @constCast(&cache_dir).close(io);
    var buf: [64]u8 = undefined;
    const pid = std.os.linux.getpid();
    const name = std.fmt.bufPrint(&buf, "ever-bench-{d}-{d}", .{ pid, temp_counter }) catch return;
    cache_dir.deleteTree(io, name) catch {};
}

// ── Main ────────────────────────────────────────────────────────────────────

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Parse args
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next(); // skip program name

    var filter: ?[]const u8 = null;
    var json_output = false;
    var iter_override: ?u64 = null;

    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--json")) {
            json_output = true;
        } else if (std.mem.eql(u8, arg, "--filter")) {
            filter = args_iter.next();
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            if (args_iter.next()) |val| {
                iter_override = std.fmt.parseInt(u64, val, 10) catch null;
            }
        }
    }

    // Run benchmarks
    var results: std.ArrayList(BenchResult) = .empty;
    defer {
        for (results.items) |r| allocator.free(r.timings);
        results.deinit(allocator);
    }

    for (&benchmarks) |*bench| {
        if (filter) |f| {
            if (std.mem.indexOf(u8, bench.name, f) == null) continue;
        }

        const iters = iter_override orelse bench.default_iters;
        std.debug.print("Running {s} ({d} iterations)...\n", .{ bench.name, iters });

        const result = bench.func(allocator, io, iters) catch |err| {
            std.debug.print("  FAILED: {}\n", .{err});
            continue;
        };
        try results.append(allocator, result);
    }

    if (results.items.len == 0) {
        std.debug.print("No benchmarks matched.\n", .{});
        return;
    }

    if (json_output) {
        printResultsJson(allocator, results.items);
    } else {
        printResultsTable(results.items);
    }
}
