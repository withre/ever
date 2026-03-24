---
title: Benchmarking Setup
state: complete
tags: [testing, core]
---

# Summary
Add a benchmarking harness to measure Ever's throughput and latency for core operations: append, read, batch read, and topic publish.

# Motivation
Performance is a core principle of Ever. Without benchmarks, we can't quantify throughput (events/sec, MB/sec), identify bottlenecks, or track regressions.

## Goals
- `zig build bench` step that runs all benchmarks in ReleaseFast
- In-process benchmarks: log append (sync/nosync), log read, batch read, topic publish
- Output: human-readable table with ops/sec, latency (avg/p99), throughput MB/sec
- JSON output (--json) for programmatic consumption
- Filter (--filter) and iteration override (--iterations) flags

## Non-Goals
- OpenTelemetry integration (deferred — built-in benchmarks cover immediate needs)
- TCP round-trip benchmarks (deferred — Zig v0.16 Io is not thread-safe across server/client threads)
- Continuous benchmark tracking / regression detection (future CI work)

# Proposal

## Build Integration
```
zig build bench                        # Run all benchmarks (ReleaseFast)
zig build bench -- --filter append     # Run only matching benchmarks
zig build bench -- --json              # JSON output
zig build bench -- --iterations 50000  # Override iteration count
```

## Benchmark Scenarios
1. **log_append_nosync** — Append 100k events without fsync
2. **log_append_sync** — Append 5k events with fsync per write
3. **log_read_sequential** — Read 10k events by offset (O(1) via position index)
4. **log_read_batch_100** — Read in batches of 100
5. **topic_publish** — Publish through TopicManager (includes topic lookup)

## Output
Table format with: Benchmark, Iters, Ops/sec, Avg latency, p99 latency, Throughput MB/s

# Design Details

## Architecture Fix: O(1) Reads
The original storage engine did linear scans per read — O(n) per lookup, O(n²) for sequential access. This was fixed as part of the benchmarking work:

- Each Segment now holds an in-memory `positions: ArrayList(u64)` mapping `(offset - base_offset) → file byte position`
- Appends record position before writing
- Recovery builds the index by scanning once at startup
- Reads are now O(1): direct lookup in positions array + single pread
- Segment lookup changed from linear scan to binary search
- readBatch walks segments contiguously instead of calling read() N times
- Event payload (key+value) is a single contiguous allocation — one alloc, one free

## Timing
- `std.time.Timer` for high-resolution monotonic measurement
- Per-iteration timings stored in pre-allocated array
- Sorted for p99 percentile calculation

# History
- 2026-03-23: Implemented benchmarking harness with 5 in-process benchmarks. Fixed O(n²) read path to O(1) with in-memory position index. TCP benchmarks deferred due to Zig v0.16 Io threading model.

## Design Divergence
- TCP benchmarks (tcp_publish, tcp_fetch) were planned but deferred. Zig v0.16's Io abstraction is not thread-safe across spawned threads, causing hangs when server and client run on different threads with the same Io instance.
- OpenTelemetry integration was considered but deferred — the built-in JSON output provides a sufficient foundation for now.
