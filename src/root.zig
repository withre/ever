//! Ever — lightweight, high-performance event storage.
//!
//! This is the library root that re-exports all public API modules.

pub const store = @import("store/store.zig");
pub const topic = @import("store/topic.zig");
pub const protocol = @import("protocol/message.zig");
pub const net = @import("net/server.zig");
pub const client = @import("net/client.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
