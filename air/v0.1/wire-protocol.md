---
title: Wire Protocol
state: complete
tags: [protocol, core]
---

# Summary
Define and implement the binary wire protocol for communication between Ever processes (publisher ↔ store, subscriber ↔ store).

# Motivation
A well-defined protocol is essential for inter-process communication. It needs to be simple enough to implement quickly, efficient enough for high throughput, and extensible enough to evolve.

## Goals
- Binary frame format with length-prefixed messages
- Request-response message types for all core operations
- JSON message bodies initially (binary payloads are just bytes)
- Serialization and deserialization in Zig
- Protocol versioning for forward compatibility

## Non-Goals
- Encryption or authentication (future work)
- Compression at the protocol level
- Streaming protocol (use repeated fetch for now)

# Proposal

## Frame Format
Every message on the wire:

```
┌───────────┬──────────┬──────────┬──────────────────┐
│ version   │ msg_type │ body_len │ body             │
│ u8        │ u8       │ u32 LE   │ [body_len]u8     │
└───────────┴──────────┴──────────┴──────────────────┘
```

Total header size: 6 bytes.

- **version**: Protocol version (starts at 1)
- **msg_type**: Message type enum
- **body_len**: Length of body in bytes
- **body**: JSON-encoded message body

## Message Types

```zig
pub const MessageType = enum(u8) {
    // Requests
    publish = 0x01,
    fetch = 0x02,
    create_topic = 0x03,
    delete_topic = 0x04,
    list_topics = 0x05,
    ack = 0x06,

    // Responses
    publish_ok = 0x81,
    fetch_ok = 0x82,
    create_topic_ok = 0x83,
    delete_topic_ok = 0x84,
    list_topics_ok = 0x85,
    ack_ok = 0x86,

    // Error
    error_response = 0xFF,
};
```

## Message Bodies (JSON)

### Publish Request
```json
{ "topic": "agent.tasks", "key": "task-123", "value": "{\"status\":\"complete\"}" }
```

### Publish Response
```json
{ "offset": 42 }
```

### Fetch Request
```json
{ "topic": "agent.tasks", "offset": 0, "max_count": 100 }
```

### Fetch Response
```json
{
  "events": [
    { "offset": 0, "timestamp": 1711234567890, "key": "task-123", "value": "{...}" },
    { "offset": 1, "timestamp": 1711234567891, "key": null, "value": "{...}" }
  ]
}
```

### Create/Delete Topic Request
```json
{ "topic": "agent.tasks" }
```

### List Topics Response
```json
{ "topics": ["agent.tasks", "file.changes"] }
```

### Ack Request
```json
{ "topic": "agent.tasks", "group": "my-consumer", "offset": 42 }
```

### Error Response
```json
{ "code": 404, "message": "topic not found: agent.tasks" }
```

## Core API

```zig
pub const Frame = struct {
    version: u8,
    msg_type: MessageType,
    body: []const u8,
};

/// Encode a frame to a writer
pub fn encodeFrame(writer: anytype, frame: Frame) !void

/// Decode a frame from a reader. Returns null on EOF.
pub fn decodeFrame(allocator: Allocator, reader: anytype) !?Frame

/// Encode a typed message to a frame body (JSON)
pub fn encodeBody(allocator: Allocator, comptime T: type, value: T) ![]u8

/// Decode a frame body to a typed message (JSON)
pub fn decodeBody(comptime T: type, body: []const u8) !T
```

# Design Details

## JSON Encoding
- Use `std.json.stringify` for encoding
- Use `std.json.parseFromSlice` for decoding
- Bodies are always valid UTF-8 JSON
- Event values within JSON are strings (may contain nested JSON)

## Frame Reading
- Read exactly 6 bytes for the header
- Validate version (must be 1)
- Validate msg_type is a known enum value
- Validate body_len ≤ max_message_size (16MB default)
- Read exactly body_len bytes for the body
- Return `ProtocolError.MessageTooLarge` if body_len exceeds limit
- Return `null` on clean EOF (connection closed)

## Error Codes
- `400` — Bad request (malformed JSON, missing fields)
- `404` — Topic not found
- `409` — Topic already exists
- `413` — Message too large
- `500` — Internal server error

# History
- 2026-03-23: Binary frame format with JSON bodies. encodeFrame/readFrame/writeFrame, encodeBody/decodeBody for all message types. Tests pass.
