//! Ever — lightweight, high-performance event storage.
//!
//! This is the library root that re-exports all public API modules.

pub const store = @import("store/store.zig");
pub const topic = @import("store/topic.zig");
pub const hooks = @import("store/hooks.zig");
pub const timers = @import("store/timers.zig");
pub const status = @import("store/status.zig");
pub const protocol = @import("protocol/message.zig");
pub const net = @import("net/server.zig");
pub const http = @import("net/http.zig");
pub const client = @import("net/client.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
