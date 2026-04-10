//! Core types for the CLI framework.
//!
//! Contains the structural definitions that callers use to declare their
//! CLI layout: `App`, `Command`, `FlagDef`, `ArgDef`, `HelpSection`, and
//! the runtime `Context` passed to command handlers.

const std = @import("std");
const validate = @import("validate.zig");

// ── Argument & Flag Definitions ────────────────────────────────────────

/// A positional argument expected by a command.
pub const ArgDef = struct {
    name: []const u8,
    required: bool = false,
    description: []const u8 = "",
};

/// A named flag (--name / -x) accepted by a command or globally.
pub const FlagDef = struct {
    name: []const u8,
    short: ?u8 = null,
    default: ?[]const u8 = null,
    env: ?[]const u8 = null,
    description: []const u8 = "",
    /// If true, flag takes a value argument; if false, it's boolean.
    takes_value: bool = true,
    /// Custom placeholder name for the value (e.g. "INTERVAL").
    value_name: []const u8 = "",
    /// Flag must be provided (or have env/default).
    required: bool = false,
    /// Names of flags that conflict with this one (mutually exclusive).
    conflicts: []const []const u8 = &.{},
    /// If true, --no-NAME is automatically accepted as the negation.
    negatable: bool = false,
};

// ── Command ────────────────────────────────────────────────────────────

/// A CLI command or subcommand with optional flags, args, and children.
pub const Command = struct {
    name: []const u8,
    description: []const u8 = "",
    aliases: []const []const u8 = &.{},
    args: []const ArgDef = &.{},
    flags: []const FlagDef = &.{},
    subcommands: []const Command = &.{},
    /// If true, everything after `--` is captured as rest args.
    takes_rest: bool = false,
    run: ?*const fn (*Context) anyerror!void = null,

    /// Check if this command matches the given name (primary or alias).
    pub fn matches(self: *const Command, input: []const u8) bool {
        if (std.mem.eql(u8, self.name, input)) return true;
        for (self.aliases) |alias| {
            if (std.mem.eql(u8, alias, input)) return true;
        }
        return false;
    }
};

// ── Help Layout ────────────────────────────────────────────────────────

/// A titled section in the root help output (e.g. "Access", "Topic").
pub const HelpSection = struct {
    title: []const u8,
    entries: []const HelpEntry,
};

/// A single row in a help section.
pub const HelpEntry = struct {
    label: []const u8,
    description: []const u8,
};

// ── Error Set ──────────────────────────────────────────────────────────

/// Errors that can occur during argument parsing.
pub const ParseError = error{
    UnknownFlag,
    MissingFlagValue,
    MissingRequiredFlag,
    ConflictingFlags,
    UnknownCommand,
    MissingRequiredArg,
    OutOfMemory,
};

// ── Runtime Context ────────────────────────────────────────────────────

/// Runtime state passed to command handlers after successful parsing.
pub const Context = struct {
    flags: std.StringHashMap([]const u8),
    positional: std.StringHashMap([]const u8),
    rest_args: std.ArrayList([]const u8),
    allocator: std.mem.Allocator,
    io: std.Io,
    envp: [*:null]const ?[*:0]const u8,
    /// Full command path, e.g. "timer add".
    command_path: []const u8,

    /// Create a new empty context.
    pub fn init(allocator: std.mem.Allocator, io: std.Io, envp: [*:null]const ?[*:0]const u8) Context {
        return .{
            .flags = std.StringHashMap([]const u8).init(allocator),
            .positional = std.StringHashMap([]const u8).init(allocator),
            .rest_args = .empty,
            .allocator = allocator,
            .io = io,
            .envp = envp,
            .command_path = "",
        };
    }

    /// Release all owned resources.
    pub fn deinit(self: *Context) void {
        self.rest_args.deinit(self.allocator);
        self.flags.deinit();
        self.positional.deinit();
    }

    /// Get a flag value, returning "" if absent.
    pub fn flag(self: *const Context, name: []const u8) []const u8 {
        return self.flags.get(name) orelse "";
    }

    /// Returns true if the flag was explicitly set (even to empty string).
    pub fn hasFlag(self: *const Context, name: []const u8) bool {
        return self.flags.contains(name);
    }

    /// Parse a flag value as an integer, returning 0 if absent or empty.
    pub fn flagInt(self: *const Context, comptime T: type, name: []const u8) T {
        const value = self.flags.get(name) orelse return 0;
        if (value.len == 0) return 0;
        return std.fmt.parseInt(T, value, 10) catch {
            std.debug.print("error: invalid value '{s}' for --{s}\n", .{ value, name });
            std.process.exit(1);
        };
    }

    /// Parse a flag value as a boolean ("true" / "1").
    pub fn flagBool(self: *const Context, name: []const u8) bool {
        const value = self.flags.get(name) orelse return false;
        return std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1");
    }

    /// Get a positional argument by name, returning "" if absent.
    pub fn arg(self: *const Context, name: []const u8) []const u8 {
        return self.positional.get(name) orelse "";
    }

    /// Returns everything after `--` when the command has `takes_rest = true`.
    pub fn rest(self: *const Context) []const []const u8 {
        return self.rest_args.items;
    }

    /// Look up an environment variable from the envp block.
    pub fn getEnv(self: *const Context, name: []const u8) ?[]const u8 {
        return validate.lookupEnv(self.envp, name);
    }
};

// ── Resolve Result ─────────────────────────────────────────────────────

/// Outcome of command resolution: the matched command plus index metadata.
pub const ResolveResult = struct {
    cmd: Command,
    parent_name: []const u8,
    args_start: usize,
    /// Index in raw args where the first command word was found.
    pre_cmd_start: usize,
};

// ── App ────────────────────────────────────────────────────────────────

/// Top-level application definition. Delegates to parse / help / validate
/// modules through `run`.
pub const App = struct {
    name: []const u8,
    description: []const u8 = "",
    version: []const u8 = "",
    global_flags: []const FlagDef = &.{},
    commands: []const Command = &.{},
    help_sections: ?[]const HelpSection = null,

    /// Parse arguments and dispatch to the matched command handler.
    pub fn run(
        self: *const App,
        allocator: std.mem.Allocator,
        io: std.Io,
        args: []const []const u8,
        env_block: [*:null]const ?[*:0]const u8,
    ) !void {
        const parse = @import("parse.zig");
        try parse.run(self, allocator, io, args, env_block);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "context basics" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try ctx.flags.put("port", "8080");
    try std.testing.expectEqualStrings("8080", ctx.flag("port"));
    try std.testing.expectEqual(@as(u16, 8080), ctx.flagInt(u16, "port"));
}

test "context flagBool" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try ctx.flags.put("verbose", "true");
    try std.testing.expect(ctx.flagBool("verbose"));
    try std.testing.expect(!ctx.flagBool("missing"));
}

test "context hasFlag distinguishes set vs absent" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try ctx.flags.put("present", "");
    try std.testing.expect(ctx.hasFlag("present"));
    try std.testing.expect(!ctx.hasFlag("absent"));
}

test "context flag returns empty for missing" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try std.testing.expectEqualStrings("", ctx.flag("nope"));
}

test "context flagInt returns 0 for missing" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try std.testing.expectEqual(@as(u16, 0), ctx.flagInt(u16, "nope"));
}

test "context flagInt returns 0 for empty" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try ctx.flags.put("empty", "");
    try std.testing.expectEqual(@as(u32, 0), ctx.flagInt(u32, "empty"));
}

test "context rest args" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try ctx.rest_args.append(allocator, "echo");
    try ctx.rest_args.append(allocator, "hello");
    const r = ctx.rest();
    try std.testing.expectEqual(@as(usize, 2), r.len);
    try std.testing.expectEqualStrings("echo", r[0]);
    try std.testing.expectEqualStrings("hello", r[1]);
}

test "context arg returns empty for missing" {
    const allocator = std.testing.allocator;
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try std.testing.expectEqualStrings("", ctx.arg("missing"));
}

test "command matches primary name" {
    const cmd = Command{ .name = "create", .aliases = &.{"add"} };
    try std.testing.expect(cmd.matches("create"));
    try std.testing.expect(cmd.matches("add"));
    try std.testing.expect(!cmd.matches("delete"));
}

test "command matches without aliases" {
    const cmd = Command{ .name = "list" };
    try std.testing.expect(cmd.matches("list"));
    try std.testing.expect(!cmd.matches("ls"));
}
