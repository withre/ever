---
title: TCP Server
state: complete
tags: [core, protocol]
---

# Summary
Implement the TCP server that accepts client connections, reads protocol frames, dispatches them to the store, and sends responses.

# Motivation
The TCP server is the entry point for all remote interactions with the store. Publishers and subscribers connect here to send and receive events.

## Goals
- Accept TCP connections on a configurable address
- Read and write protocol frames (from wire-protocol spec)
- Dispatch incoming messages to the TopicManager
- Handle multiple concurrent connections
- Graceful shutdown with connection draining

## Non-Goals
- Unix domain socket support (future enhancement)
- TLS/encryption
- Authentication/authorization
- Connection multiplexing or pipelining

# Proposal

## Server Lifecycle
```
init → listen → accept loop → handle connections → shutdown
```

## Core API

```zig
pub const Server = struct {
    pub fn init(allocator: Allocator, topic_manager: *TopicManager, config: Config) !Server
    pub fn deinit(self: *Server) void

    /// Start listening and accepting connections. Blocks until shutdown.
    pub fn run(self: *Server) !void

    /// Signal the server to stop accepting new connections and drain existing ones.
    pub fn shutdown(self: *Server) void
};

pub const Config = struct {
    address: []const u8 = "127.0.0.1",
    port: u16 = 4222,
    max_connections: u32 = 1024,
    read_timeout_ms: u32 = 30_000,
};
```

## Connection Handling
Each accepted connection:
1. Read a frame from the socket
2. Parse the message type and body
3. Dispatch to the appropriate handler
4. Encode and send the response frame
5. Loop back to step 1 until connection closes or errors

## Message Dispatch

| Message Type | Handler | Description |
|---|---|---|
| `publish` | `topicManager.publish(topic, key, value)` | Write event, return offset |
| `fetch` | `topicManager.getTopic(topic).readBatch(offset, count)` | Return events |
| `create_topic` | `topicManager.createTopic(name)` | Create topic |
| `delete_topic` | `topicManager.deleteTopic(name)` | Delete topic |
| `list_topics` | `topicManager.listTopics()` | Return topic names |
| `ack` | Store consumer offset (if implemented) | Acknowledge consumption |

# Design Details

## Concurrency Model
For v0.1, use a simple thread-per-connection model:
- Main thread accepts connections via `std.net.Server`
- Each connection spawns a new thread via `std.Thread.spawn`
- Thread handles the connection read-dispatch-write loop
- This is simple and correct; can optimize to poll/epoll later

## Shutdown Sequence
1. Stop accepting new connections (close listen socket)
2. Signal all connection threads to finish current request
3. Wait for all threads to complete (with timeout)
4. Close remaining connections
5. Server.run() returns

## Error Handling in Connections
- Protocol errors → send error response, continue serving
- I/O errors (broken pipe, timeout) → close connection, log warning
- Internal errors (store failure) → send error response, continue serving
- Never crash the server due to a single bad connection

## Buffer Management
- Per-connection read buffer (configurable, default 64KB)
- Write responses directly to socket via frame encoding
- Connection-scoped arena allocator for request processing

# History
- 2026-03-23: TCP server using Io.net API with thread-per-connection. Dispatches all message types to TopicManager. Tests pass.
## Design Divergence
Used Io.net high-level API instead of raw posix sockets for listen/accept. Protocol read/write still uses raw posix fd for simplicity.
