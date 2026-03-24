# System Architecture

## Core Philosophy

Ever is built on the principle that event storage should be **simple, fast, and reliable** without the operational overhead of distributed systems. A single-node event log with proper durability guarantees covers the vast majority of use cases where events need to be tracked and consumed.

## Design Principles

### Append-Only Log as Foundation
- All events are immutable once written — append-only semantics
- Sequential disk writes for maximum throughput
- Segmented log files for efficient space reclamation
- Offset-based addressing for O(1) random access within segments

### Explicit Resource Management (Zig-First)
- No hidden memory allocations — all allocations go through `std.mem.Allocator`
- No hidden control flow — no exceptions, no async magic
- Explicit error handling using Zig's error unions
- Deterministic cleanup with `defer` and `errdefer`
- Leverage comptime for zero-cost abstractions

### Process-Based Architecture
- Distinct processes for distinct roles (store, publisher, subscriber, orchestrator, transformer)
- Communication via well-defined protocol over TCP or Unix domain sockets
- Each process is independently deployable and restartable
- Orchestrator manages lifecycle but processes can run standalone

### At-Least-Once Delivery
- Consumer offset tracking for reliable replay
- Acknowledgment-based consumption — offset advances only after consumer ACK
- Consumers can rewind and replay from any committed offset
- No exactly-once guarantees (simplifies implementation significantly)

## System Architecture

```
┌──────────────┐     ┌──────────────────────────────────┐     ┌──────────────┐
│  Publisher    │────▶│           Store                   │◀────│  Subscriber  │
│  (producer)  │     │  ┌────────────────────────────┐   │     │  (consumer)  │
└──────────────┘     │  │  Topic: "agent.tasks"      │   │     └──────────────┘
                     │  │  ┌─────┬─────┬─────┬────┐  │   │
┌──────────────┐     │  │  │seg0 │seg1 │seg2 │seg3│  │   │     ┌──────────────┐
│ Transformer  │◀───▶│  │  └─────┴─────┴─────┴────┘  │   │◀───▶│ Transformer  │
│ (pub + sub)  │     │  └────────────────────────────┘   │     │ (pub + sub)  │
└──────────────┘     │  ┌────────────────────────────┐   │     └──────────────┘
                     │  │  Topic: "file.changes"     │   │
                     │  │  ┌─────┬─────┐             │   │
                     │  │  │seg0 │seg1 │             │   │
                     │  │  └─────┴─────┘             │   │
                     │  └────────────────────────────┘   │
                     └──────────────────────────────────┘
                                    ▲
                                    │
                            ┌───────┴────────┐
                            │  Orchestrator   │
                            │  (lifecycle)    │
                            └────────────────┘
```

## Core Components

### 1. Store (Storage Engine)

The store is the heart of Ever. It manages topics, each backed by segmented append-only logs.

#### Log Structure
- **Segment**: Fixed-size file containing sequential events
- **Index**: Sparse offset-to-position mapping for fast lookups
- **Topic**: Named collection of segments forming a logical stream

#### Storage Layout on Disk
```
data/
├── topics/
│   ├── agent.tasks/
│   │   ├── 00000000000000000000.log    # Segment file (events)
│   │   ├── 00000000000000000000.idx    # Index file (offset → position)
│   │   ├── 00000000000000001024.log    # Next segment
│   │   └── 00000000000000001024.idx
│   └── file.changes/
│       ├── 00000000000000000000.log
│       └── 00000000000000000000.idx
└── meta/
    ├── topics.json                      # Topic registry
    └── consumers/                       # Consumer offset storage
        └── my-consumer-group.json
```

#### Segment File Format
Each event in a segment:
```
┌──────────┬──────────┬──────────┬──────────┬──────────────┐
│ offset   │ timestamp│ key_len  │ val_len  │ key | value  │
│ (u64)    │ (i64)    │ (u32)    │ (u32)    │ (bytes)      │
└──────────┴──────────┴──────────┴──────────┴──────────────┘
```

### 2. Protocol Layer

Binary protocol for communication between processes.

#### Message Types
- `Publish` — Write event(s) to a topic
- `Subscribe` — Start consuming from a topic at an offset
- `Fetch` — Pull a batch of events from current position
- `Ack` — Acknowledge consumption up to an offset
- `CreateTopic` — Create a new topic
- `DeleteTopic` — Remove a topic
- `Heartbeat` — Keep-alive for long-lived connections
- `Error` — Error response with code and message

#### Frame Format
```
┌──────────┬──────────┬──────────┬──────────────┐
│ msg_type │ flags    │ body_len │ body         │
│ (u8)     │ (u8)    │ (u32)    │ (bytes)      │
└──────────┴──────────┴──────────┴──────────────┘
```

### 3. Networking Layer

TCP and Unix domain socket server using Zig's `std.posix` and `std.net`.

#### Design Decisions
- Single-threaded event loop using `std.posix.poll` or `io_uring` (if available)
- Non-blocking I/O for handling many concurrent connections
- Connection-per-consumer model with optional multiplexing
- Graceful shutdown with drain period for in-flight messages

### 4. Publisher

Client library and standalone process for producing events.

#### Features
- Synchronous and batched publish modes
- Optional key-based partitioning (for ordered delivery within a key)
- Automatic reconnection on connection loss
- Back-pressure handling when store is overloaded

### 5. Subscriber

Client library and standalone process for consuming events.

#### Features
- Pull-based fetching with configurable batch size
- Push-based streaming mode for low-latency consumption
- Consumer group support — multiple consumers sharing a topic
- Offset management — automatic or manual commit

### 6. Orchestrator

Process manager for the Ever ecosystem.

#### Responsibilities
- Start/stop store, publisher, subscriber processes
- Configuration management and distribution
- Health monitoring and automatic restart
- Graceful shutdown coordination
- CLI interface for operational commands

### 7. Transformer

Combined publisher + subscriber for event pipeline processing.

#### Design
- Subscribes to input topic, applies transformation function, publishes to output topic
- User-defined transformation logic (compiled Zig function or external process via stdin/stdout)
- At-least-once processing guarantee inherited from subscriber semantics
- Lightweight — single process handles one transformation step

## Error Handling Strategy

### Error Types
- **StorageError**: Disk I/O failures, segment corruption, out of space
- **ProtocolError**: Malformed messages, version mismatch, unknown message types
- **NetworkError**: Connection refused, timeout, broken pipe
- **TopicError**: Topic not found, already exists, invalid name
- **ConsumerError**: Invalid offset, consumer group conflict

### Recovery Strategies
- **Store**: fsync on commit, segment checksums for corruption detection
- **Publisher**: Retry with backoff on transient failures
- **Subscriber**: Resume from last committed offset on restart
- **Orchestrator**: Automatic process restart with configurable backoff

## Performance Considerations

### Write Path
- Sequential append — no random I/O on write
- Batch writes to amortize fsync cost
- Memory-mapped segment files for efficient access
- Zero-copy where possible (direct buffer writes)

### Read Path
- Index for O(1) offset lookups within segments
- Sequential read for batch fetches
- OS page cache friendly access patterns
- Configurable read-ahead for streaming consumers

### Memory Management
- Arena allocators for request-scoped allocations
- Fixed-size buffer pools for I/O operations
- No garbage collection — deterministic memory behavior
- Configurable memory limits per component

## Future Considerations

### Compression
- Per-segment or per-batch compression (LZ4, zstd)
- Configurable per topic

### Retention Policies
- Time-based retention (delete segments older than X)
- Size-based retention (keep at most Y bytes per topic)
- Compaction (keep only latest event per key)

### Binary Event Format
- Move from JSON to a more compact binary encoding
- Schema registry for typed events
- Backward/forward compatibility

### Observability
- Metrics export (Prometheus format)
- Structured logging
- Health check endpoints
