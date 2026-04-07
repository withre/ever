//! CLI Argument Parsing Library for Zig v0.16
//!
//! A clean, reusable CLI library with:
//! - Subcommand dispatch (nested: "timer add")
//! - Full command path tracking for help output
//! - --help works anywhere in the arg list
//! - Grouped help sections for the root command
//! - TTY-aware coloured output

const std = @import("std");

// ── Types ──────────────────────────────────────────────────────────────

pub const ArgDef = struct {
    name: []const u8,
    required: bool = false,
    description: []const u8 = "",
};

pub const FlagDef = struct {
    name: []const u8,
    short: ?u8 = null,
    default: ?[]const u8 = null,
    description: []const u8 = "",
    /// If true, flag takes a value argument; if false, it's boolean.
    takes_value: bool = true,
    /// Custom placeholder name for the value (e.g. "INTERVAL"). If empty, derived from flag name.
    value_name: []const u8 = "",
};

pub const Command = struct {
    name: []const u8,
    description: []const u8 = "",
    aliases: []const []const u8 = &.{},
    args: []const ArgDef = &.{},
    flags: []const FlagDef = &.{},
    subcommands: []const Command = &.{},
    run: ?*const fn (*Context) anyerror!void = null,

    /// Check if this command matches the given name (primary or alias).
    fn matches(self: *const Command, input: []const u8) bool {
        if (std.mem.eql(u8, self.name, input)) return true;
        for (self.aliases) |alias| {
            if (std.mem.eql(u8, alias, input)) return true;
        }
        return false;
    }
};

pub const HelpSection = struct {
    title: []const u8,
    entries: []const HelpEntry,
};

pub const HelpEntry = struct {
    label: []const u8,
    description: []const u8,
};

pub const Context = struct {
    flags: std.StringHashMap([]const u8),
    positional: std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,
    io: std.Io,
    envp: [*:null]const ?[*:0]const u8,
    /// Full command path, e.g. "timer add"
    command_path: []const u8,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, envp: [*:null]const ?[*:0]const u8) Context {
        return .{
            .flags = std.StringHashMap([]const u8).init(allocator),
            .positional = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
            .io = io,
            .envp = envp,
            .command_path = "",
        };
    }

    pub fn deinit(self: *Context) void {
        self.flags.deinit();
        self.positional.deinit();
    }

    pub fn flag(self: *const Context, name: []const u8) []const u8 {
        return self.flags.get(name) orelse "";
    }

    pub fn flagInt(self: *const Context, comptime T: type, name: []const u8) T {
        const value = self.flags.get(name) orelse return 0;
        return std.fmt.parseInt(T, value, 10) catch 0;
    }

    pub fn flagBool(self: *const Context, name: []const u8) bool {
        const value = self.flags.get(name) orelse return false;
        return std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1");
    }

    pub fn arg(self: *const Context, name: []const u8) []const u8 {
        return self.positional.get(name) orelse "";
    }
};

pub const App = struct {
    name: []const u8,
    description: []const u8 = "",
    version: []const u8 = "",
    commands: []const Command = &.{},
    help_sections: ?[]const HelpSection = null,

    pub fn run(self: *const App, allocator: std.mem.Allocator, io: std.Io, args: []const []const u8, env_block: [*:null]const ?[*:0]const u8) !void {
        // Skip argv[0]
        const raw = if (args.len > 1) args[1..] else args[0..0];

        // No args → root help
        if (raw.len == 0) {
            self.printRootHelp();
            return;
        }

        // Check for --help anywhere in args (before command resolution)
        // We'll handle it after resolving the command for proper context.

        const first = raw[0];

        // "help" as first word
        if (std.mem.eql(u8, first, "help") or std.mem.eql(u8, first, "--help") or std.mem.eql(u8, first, "-h")) {
            // "ever help" → root help
            // "ever help timer" → same as "ever timer --help"
            // "ever help timer add" → same as "ever timer add --help"
            if (raw.len == 1) {
                self.printRootHelp();
                return;
            }
            // Resolve remaining words as command path
            if (self.resolveCommand(raw[1..])) |res| {
                self.printCommandHelp(res.cmd, res.parent_name);
            } else {
                std.debug.print("unknown command. Run '{s} help'.\n", .{self.name});
                std.process.exit(1);
            }
            return;
        }

        if (std.mem.eql(u8, first, "version") or std.mem.eql(u8, first, "--version")) {
            std.debug.print("{s}\n", .{self.version});
            return;
        }

        // Resolve command (possibly with subcommand)
        const resolved = self.resolveCommand(raw) orelse {
            std.debug.print("unknown command '{s}'. Run '{s} help'.\n", .{ first, self.name });
            std.process.exit(1);
        };

        const cmd = resolved.cmd;
        const arg_start = resolved.args_start;

        // Check if --help appears anywhere in remaining args
        for (raw[arg_start..]) |a| {
            if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
                self.printCommandHelp(cmd, resolved.parent_name);
                return;
            }
        }

        // If this is a parent command (has subcommands, no run fn), show its help
        if (cmd.subcommands.len > 0 and cmd.run == null) {
            self.printCommandHelp(cmd, resolved.parent_name);
            return;
        }

        // Build context
        var ctx = Context.init(allocator, io, env_block);
        defer ctx.deinit();
        // Store parent + command name as path for handlers
        ctx.command_path = if (resolved.parent_name.len > 0) resolved.parent_name else cmd.name;

        // Initialize flag defaults
        for (cmd.flags) |f| {
            if (f.default) |def| {
                try ctx.flags.put(f.name, def);
            }
        }

        // Parse remaining args
        var i: usize = arg_start;
        var positional_idx: usize = 0;
        while (i < raw.len) : (i += 1) {
            const a = raw[i];
            if (std.mem.eql(u8, a, "--")) {
                // Everything after -- is positional
                i += 1;
                while (i < raw.len) : (i += 1) {
                    if (positional_idx < cmd.args.len) {
                        try ctx.positional.put(cmd.args[positional_idx].name, raw[i]);
                        positional_idx += 1;
                    }
                }
                break;
            } else if (std.mem.startsWith(u8, a, "--")) {
                const flag_name = a[2..];
                if (std.mem.indexOfScalar(u8, flag_name, '=')) |eq| {
                    try ctx.flags.put(flag_name[0..eq], flag_name[eq + 1 ..]);
                } else {
                    // Look up flag definition to check if it takes a value
                    const fdef = findFlagDef(cmd.flags, flag_name);
                    if (fdef != null and !fdef.?.takes_value) {
                        try ctx.flags.put(flag_name, "true");
                    } else if (i + 1 < raw.len and !std.mem.startsWith(u8, raw[i + 1], "-")) {
                        i += 1;
                        try ctx.flags.put(flag_name, raw[i]);
                    } else {
                        try ctx.flags.put(flag_name, "true");
                    }
                }
            } else if (std.mem.startsWith(u8, a, "-") and a.len == 2) {
                const short = a[1];
                if (findFlagByShort(cmd.flags, short)) |fdef| {
                    if (!fdef.takes_value) {
                        try ctx.flags.put(fdef.name, "true");
                    } else if (i + 1 < raw.len and !std.mem.startsWith(u8, raw[i + 1], "-")) {
                        i += 1;
                        try ctx.flags.put(fdef.name, raw[i]);
                    } else {
                        try ctx.flags.put(fdef.name, "true");
                    }
                }
            } else {
                // Positional argument
                if (positional_idx < cmd.args.len) {
                    try ctx.positional.put(cmd.args[positional_idx].name, a);
                    positional_idx += 1;
                }
            }
        }

        if (cmd.run) |run_fn| {
            try run_fn(&ctx);
        }
    }

    const ResolveResult = struct {
        cmd: Command,
        parent_name: []const u8, // "" for top-level commands
        args_start: usize, // index into raw (after argv[0])
    };

    fn resolveCommand(self: *const App, raw: []const []const u8) ?ResolveResult {
        if (raw.len == 0) return null;
        const first = raw[0];

        for (self.commands) |cmd| {
            if (cmd.matches(first)) {
                // Check for subcommand
                if (cmd.subcommands.len > 0 and raw.len >= 2) {
                    const second = raw[1];
                    for (cmd.subcommands) |sub| {
                        if (sub.matches(second)) {
                            return .{
                                .cmd = sub,
                                .parent_name = cmd.name,
                                .args_start = 2,
                            };
                        }
                    }
                }
                // Return parent command itself
                return .{
                    .cmd = cmd,
                    .parent_name = "",
                    .args_start = 1,
                };
            }
        }
        return null;
    }

    // ── Help Printing ──────────────────────────────────────────────────

    fn isTty() bool {
        return std.c.isatty(std.posix.STDERR_FILENO) != 0;
    }

    fn printRootHelp(self: *const App) void {
        const tty = isTty();
        const title_c = if (tty) "\x1b[38;2;140;170;210m" else "";
        const section_c = if (tty) "\x1b[38;2;150;165;100m\x1b[1m" else "";
        const cmd_c = if (tty) "\x1b[38;2;130;155;170m" else "";
        const desc_c = if (tty) "\x1b[38;2;130;135;140m" else "";
        const reset = if (tty) "\x1b[0m" else "";

        std.debug.print("{s}{s}{s} — {s}{s}{s}\n\n", .{ title_c, self.name, reset, desc_c, self.description, reset });
        std.debug.print("Usage: {s} <command> [options]\n", .{self.name});

        if (self.help_sections) |sections| {
            for (sections) |sec| {
                std.debug.print("\n{s}{s}:{s}\n", .{ section_c, sec.title, reset });
                for (sec.entries) |entry| {
                    std.debug.print("  {s}{s:<17}{s}{s}{s}{s}\n", .{ cmd_c, entry.label, reset, desc_c, entry.description, reset });
                }
            }
        }

        std.debug.print("\n{s}Run '{s} <command> --help' for more information.{s}\n", .{ desc_c, self.name, reset });
    }

    fn printCommandHelp(self: *const App, cmd: Command, parent_name: []const u8) void {
        const tty = isTty();
        const cmd_c = if (tty) "\x1b[38;2;130;155;170m" else "";
        const section_c = if (tty) "\x1b[38;2;150;165;100m\x1b[1m" else "";
        const desc_c = if (tty) "\x1b[38;2;130;135;140m" else "";
        const flag_c = if (tty) "\x1b[38;2;120;130;140m" else "";
        const flag_desc_c = if (tty) "\x1b[38;2;100;105;110m" else "";
        const reset = if (tty) "\x1b[0m" else "";

        // Description line with command name highlighted
        if (parent_name.len > 0) {
            std.debug.print("{s}{s} {s}{s} — {s}{s}{s}\n", .{ cmd_c, parent_name, cmd.name, reset, desc_c, cmd.description, reset });
        } else {
            std.debug.print("{s}{s}{s} — {s}{s}{s}\n", .{ cmd_c, cmd.name, reset, desc_c, cmd.description, reset });
        }

        // Usage line
        std.debug.print("\nUsage:\n", .{});
        if (cmd.subcommands.len > 0) {
            if (parent_name.len > 0) {
                std.debug.print("  {s} {s} {s} <subcommand> [OPTIONS]\n", .{ self.name, parent_name, cmd.name });
            } else {
                std.debug.print("  {s} {s} <subcommand> [OPTIONS]\n", .{ self.name, cmd.name });
            }
        } else {
            if (parent_name.len > 0) {
                std.debug.print("  {s} {s} {s}", .{ self.name, parent_name, cmd.name });
            } else {
                std.debug.print("  {s} {s}", .{ self.name, cmd.name });
            }
            if (cmd.flags.len > 0) std.debug.print(" [OPTIONS]", .{});
            for (cmd.args) |a| {
                if (a.required) {
                    std.debug.print(" {s}", .{cmd_c});
                    printArgUpper(a.name);
                    std.debug.print("{s}", .{reset});
                } else {
                    std.debug.print(" [{s}", .{cmd_c});
                    printArgUpperBare(a.name);
                    std.debug.print("{s}]", .{reset});
                }
            }
            std.debug.print("\n", .{});
        }

        // Subcommands
        if (cmd.subcommands.len > 0) {
            std.debug.print("\n{s}Subcommands:{s}\n", .{ section_c, reset });
            for (cmd.subcommands) |sub| {
                std.debug.print("  {s}{s:<17}{s}{s}{s}{s}\n", .{ cmd_c, sub.name, reset, desc_c, sub.description, reset });
            }
            if (parent_name.len > 0) {
                std.debug.print("\n{s}Run '{s} {s} {s} <subcommand> --help' for details.{s}\n", .{ desc_c, self.name, parent_name, cmd.name, reset });
            } else {
                std.debug.print("\n{s}Run '{s} {s} <subcommand> --help' for details.{s}\n", .{ desc_c, self.name, cmd.name, reset });
            }
            return;
        }

        // Arguments
        if (cmd.args.len > 0) {
            std.debug.print("\n{s}Arguments:{s}\n", .{ section_c, reset });
            for (cmd.args) |a| {
                var label_buf: [64]u8 = undefined;
                const label = fmtArgLabel(a, &label_buf);
                std.debug.print("  {s}{s:<28}{s}{s}{s}{s}\n", .{ cmd_c, label, reset, desc_c, a.description, reset });
            }
        }

        // Flags
        if (cmd.flags.len > 0) {
            std.debug.print("\n{s}Flags:{s}\n", .{ section_c, reset });
            for (cmd.flags) |f| {
                var left_buf: [64]u8 = undefined;
                const left = fmtFlagLeft(f, &left_buf);
                if (f.default) |def| {
                    std.debug.print("  {s}{s:<28}{s}{s}{s}{s} {s}(default: {s}){s}\n", .{ flag_c, left, reset, flag_desc_c, f.description, reset, flag_desc_c, def, reset });
                } else {
                    std.debug.print("  {s}{s:<28}{s}{s}{s}{s}\n", .{ flag_c, left, reset, flag_desc_c, f.description, reset });
                }
            }
        }
    }

    // ── Formatting helpers ─────────────────────────────────────────────

    fn printArgUpper(name: []const u8) void {
        std.debug.print(" <", .{});
        printArgUpperBare(name);
        std.debug.print(">", .{});
    }

    fn printArgUpperBare(name: []const u8) void {
        for (name) |c| {
            if (c >= 'a' and c <= 'z')
                std.debug.print("{c}", .{c - 32})
            else
                std.debug.print("{c}", .{c});
        }
    }

    fn fmtArgLabel(a: ArgDef, buf: []u8) []const u8 {
        var pos: usize = 0;
        if (a.required) {
            buf[pos] = '<';
            pos += 1;
        } else {
            buf[pos] = '[';
            pos += 1;
        }
        for (a.name) |c| {
            if (c >= 'a' and c <= 'z') {
                buf[pos] = c - 32;
            } else {
                buf[pos] = c;
            }
            pos += 1;
        }
        if (a.required) {
            buf[pos] = '>';
            pos += 1;
        } else {
            buf[pos] = ']';
            pos += 1;
        }
        return buf[0..pos];
    }

    fn fmtFlagLeft(f: FlagDef, buf: []u8) []const u8 {
        var pos: usize = 0;
        if (f.short) |s| {
            buf[pos] = '-';
            pos += 1;
            buf[pos] = s;
            pos += 1;
            buf[pos] = ',';
            pos += 1;
            buf[pos] = ' ';
            pos += 1;
        } else {
            // 4 spaces to align with "-X, "
            buf[pos] = ' ';
            pos += 1;
            buf[pos] = ' ';
            pos += 1;
            buf[pos] = ' ';
            pos += 1;
            buf[pos] = ' ';
            pos += 1;
        }
        buf[pos] = '-';
        pos += 1;
        buf[pos] = '-';
        pos += 1;
        for (f.name) |c| {
            buf[pos] = c;
            pos += 1;
        }
        // If flag takes a value, add " <UPPER_NAME>"
        if (f.takes_value) {
            buf[pos] = ' ';
            pos += 1;
            buf[pos] = '<';
            pos += 1;
            if (f.value_name.len > 0) {
                for (f.value_name) |c| {
                    buf[pos] = c;
                    pos += 1;
                }
            } else {
                for (f.name) |c| {
                    if (c >= 'a' and c <= 'z') {
                        buf[pos] = c - 32;
                    } else if (c == '-') {
                        buf[pos] = '_';
                    } else {
                        buf[pos] = c;
                    }
                    pos += 1;
                }
            }
            buf[pos] = '>';
            pos += 1;
        }
        return buf[0..pos];
    }

    fn findFlagDef(flags: []const FlagDef, name: []const u8) ?FlagDef {
        for (flags) |f| {
            if (std.mem.eql(u8, f.name, name)) return f;
        }
        return null;
    }

    fn findFlagByShort(flags: []const FlagDef, short: u8) ?FlagDef {
        for (flags) |f| {
            if (f.short == short) return f;
        }
        return null;
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

test "fmtArgLabel required" {
    var buf: [64]u8 = undefined;
    const label = App.fmtArgLabel(.{ .name = "topic", .required = true }, &buf);
    try std.testing.expectEqualStrings("<TOPIC>", label);
}

test "fmtArgLabel optional" {
    var buf: [64]u8 = undefined;
    const label = App.fmtArgLabel(.{ .name = "payload", .required = false }, &buf);
    try std.testing.expectEqualStrings("[PAYLOAD]", label);
}

test "fmtFlagLeft with short and default" {
    var buf: [64]u8 = undefined;
    const left = App.fmtFlagLeft(.{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" }, &buf);
    try std.testing.expectEqualStrings("-p, --port <PORT>", left);
}

test "fmtFlagLeft long only no value" {
    var buf: [64]u8 = undefined;
    const left = App.fmtFlagLeft(.{ .name = "no-persist", .takes_value = false, .description = "Don't persist" }, &buf);
    try std.testing.expectEqualStrings("    --no-persist", left);
}

test "command matches primary name" {
    const cmd = Command{ .name = "create", .aliases = &.{ "add" } };
    try std.testing.expect(cmd.matches("create"));
    try std.testing.expect(cmd.matches("add"));
    try std.testing.expect(!cmd.matches("delete"));
}

test "command matches without aliases" {
    const cmd = Command{ .name = "list" };
    try std.testing.expect(cmd.matches("list"));
    try std.testing.expect(!cmd.matches("ls"));
}
