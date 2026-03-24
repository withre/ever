# Project Overview

## Description
Ever is a lightweight, high-performance event storage system written in Zig. Inspired by systems like NATS and Kafka, Ever provides at-least-once delivery semantics for event-driven architectures. It is designed as a single-node storage interface — not a distributed cluster — where events (agent task completions, file edits, process exits, etc.) can be easily tracked, reported, and subscribed to by interested consumers.

## Core Principles
- **Performance First**: Zig's zero-overhead abstractions, explicit memory management, and no hidden allocations make it ideal for high-throughput event processing
- **Simplicity Over Distribution**: Single-node focus — no distributed consensus, no replication overhead. Clean, fast, reliable
- **At-Least-Once Semantics**: Kafka-like delivery guarantees without Kafka-like complexity
- **Planning-First Methodology**: Air documents serve as single source of truth for specifications
- **Composable Processes**: Distinct roles (store, publisher, subscriber, orchestrator, transformer) communicate via well-defined protocols

## Technology Stack
- **Language**: Zig v0.16 (latest release candidate)
- **Build System**: Zig build system (`build.zig`)
- **Dependencies**: Minimal — prefer Zig standard library
- **Testing**: Zig's built-in testing framework
- **Development Environment**: Nix flake for reproducible builds
- **Data Format**: JSON initially, with path to more efficient binary encoding

## Project Structure
```
ever/
├── src/
│   ├── main.zig              # Entry point
│   ├── store/                # Event storage engine
│   │   ├── log.zig           # Append-only event log
│   │   ├── index.zig         # Topic/offset indexing
│   │   └── segment.zig       # Log segment management
│   ├── protocol/             # Wire protocol definitions
│   │   ├── message.zig       # Message types and serialization
│   │   └── codec.zig         # Encode/decode logic
│   ├── net/                  # Networking layer
│   │   ├── server.zig        # TCP/Unix socket server
│   │   └── client.zig        # Client connection handling
│   ├── orchestrator/         # Process orchestration
│   ├── publisher.zig         # Event publishing logic
│   ├── subscriber.zig        # Event subscription logic
│   └── transformer.zig       # Pub+sub event transformation
├── build.zig                 # Build configuration
├── build.zig.zon             # Package manifest
├── air/                      # Air planning documents
│   ├── v0.1/                 # Version 0.1 specifications
│   ├── context/              # Context files
│   └── templates/            # Document templates
├── flake.nix                 # Nix development environment
└── devshell.nix              # Nix shell configuration
```

## Architecture
Ever is built around an append-only log as the core storage primitive. Events are organized into topics, each backed by segmented log files. Processes communicate over a simple binary or JSON-based protocol via TCP or Unix domain sockets.

### Core Components

#### Store
The storage engine manages append-only logs, segment rotation, and offset tracking. It is the single source of truth for event data.

#### Publisher
Sends events to the store. Can be a library call or a separate process. Supports batching for throughput.

#### Subscriber
Reads events from the store, tracking consumer offsets for at-least-once delivery. Supports both pull-based polling and push-based streaming.

#### Orchestrator
Manages lifecycle of store, publisher, and subscriber processes. Handles configuration, health checks, and graceful shutdown.

#### Transformer
A combined publisher + subscriber that consumes events from one topic, applies a transformation, and publishes results to another topic. Useful for event pipelines.

## Document States (Air Workflow)
- `draft` - Initial planning phase
- `ready` - Specification complete, ready for implementation
- `work-in-progress` - Currently being implemented
- `complete` - Implementation finished
- `dropped` - No longer needed
- `unknown` - State cannot be determined

## Getting Started
1. Review current status: `airctl status`
2. Check ready work: `airctl status --state ready`
3. Read relevant Air documents in `./air/` before implementing
4. Update document states as work progresses

## Current Focus
Use `airctl status --state work-in-progress,ready` to see current priorities and available work.
