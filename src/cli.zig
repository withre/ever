//! CLI Argument Parsing Library for Zig v0.16

const std = @import("std");

pub const Colour = struct {
    r: u8,
    g: u8,
    b: u8,

    pub fn hex(comptime s: []const u8) Colour {
        comptime {
            if (s.len != 7 or s[0] != '#') @compileError("Invalid hex");
        }
        return .{
            .r = std.fmt.parseInt(u8, s[1..3], 16) catch unreachable,
            .g = std.fmt.parseInt(u8, s[3..5], 16) catch unreachable,
            .b = std.fmt.parseInt(u8, s[5..7], 16) catch unreachable,
        };
    }
};

pub const Palette = struct {
    name: Colour,
    description: Colour,
    flag: Colour,
    separator: Colour,
    subtle: Colour,
    error_colour: Colour,

    pub const default_palette = Palette{
        .name = Colour.hex("#82AAFF"),
        .description = Colour.hex("#C0C0C0"),
        .flag = Colour.hex("#C3E88D"),
        .separator = Colour.hex("#546E7A"),
        .subtle = Colour.hex("#666666"),
        .error_colour = Colour.hex("#FF5370"),
    };
};

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
};

pub const Command = struct {
    name: []const u8,
    description: []const u8 = "",
    args: []const ArgDef = &.{},
    flags: []const FlagDef = &.{},
    subcommands: []const Command = &.{},
    run: ?*const fn (*Context) anyerror!void = null,
};

pub const HelpSection = struct {
    title: []const u8,
    commands: []const []const u8,
};

pub const Context = struct {
    flags: std.StringHashMap([]const u8),
    positional: std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,
    io: std.Io,

    pub fn init(allocator: std.mem.Allocator, io: std.Io) Context {
        return .{
            .flags = std.StringHashMap([]const u8).init(allocator),
            .positional = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
            .io = io,
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

    pub fn hasHelp(self: *const Context) bool {
        return self.flags.contains("help");
    }
};

pub const App = struct {
    name: []const u8,
    description: []const u8 = "",
    version: []const u8 = "",
    commands: []const Command = &.{},
    help_sections: ?[]const HelpSection = null,

    pub fn run(self: *const App, allocator: std.mem.Allocator, io: std.Io, args: []const []const u8, env_block: [*:null]const ?[*:0]const u8) !void {
        if (args.len < 2) {
            self.printHelp();
            return;
        }

        const command_name = args[1];

        if (std.mem.eql(u8, command_name, "help")) {
            self.printHelp();
            return;
        }

        if (std.mem.eql(u8, command_name, "version")) {
            std.debug.print("{s} {s}\n", .{ self.name, self.version });
            return;
        }

        const cmd = self.findCommand(command_name);
        if (cmd == null) {
            std.debug.print("unknown command '{s}'. Run '{s} help'.\n", .{ command_name, self.name });
            std.process.exit(1);
        }

        var ctx = Context.init(allocator, io);
        defer ctx.deinit();

        // Initialize defaults
        for (cmd.?.flags) |flag| {
            if (flag.default) |def| {
                try ctx.flags.put(flag.name, def);
            }
        }

        // Parse flags and positionals
        var i: usize = 2;
        var positional_idx: usize = 0;
        while (i < args.len) : (i += 1) {
            const arg = args[i];
            if (std.mem.startsWith(u8, arg, "--")) {
                const flag_name = arg[2..];
                if (std.mem.indexOfScalar(u8, flag_name, '=')) |eq| {
                    try ctx.flags.put(flag_name[0..eq], arg[eq + 1 ..]);
                } else if (i + 1 < args.len and !std.mem.startsWith(u8, args[i + 1], "-")) {
                    i += 1;
                    try ctx.flags.put(flag_name, args[i]);
                } else {
                    try ctx.flags.put(flag_name, "true");
                }
            } else if (std.mem.startsWith(u8, arg, "-") and arg.len == 2) {
                const short = arg[1];
                for (cmd.?.flags) |flag| {
                    if (flag.short == short) {
                        if (i + 1 < args.len and !std.mem.startsWith(u8, args[i + 1], "-")) {
                            i += 1;
                            try ctx.flags.put(flag.name, args[i]);
                        } else {
                            try ctx.flags.put(flag.name, "true");
                        }
                        break;
                    }
                }
            } else {
                if (positional_idx < cmd.?.args.len) {
                    try ctx.positional.put(cmd.?.args[positional_idx].name, arg);
                    positional_idx += 1;
                }
            }
        }

        if (ctx.hasHelp()) {
            self.printCommandHelp(cmd.?);
            return;
        }

        _ = env_block;
        if (cmd.?.run) |run_fn| {
            try run_fn(&ctx);
        }
    }

    fn findCommand(self: *const App, name: []const u8) ?Command {
        for (self.commands) |cmd| {
            if (std.mem.eql(u8, cmd.name, name)) return cmd;
            for (cmd.subcommands) |sub| {
                const full = std.fmt.allocPrint(std.heap.page_allocator, "{s} {s}", .{ cmd.name, sub.name }) catch continue;
                defer std.heap.page_allocator.free(full);
                if (std.mem.eql(u8, full, name)) return sub;
            }
        }
        return null;
    }

    fn printHelp(self: *const App) void {
        std.debug.print("╭ {s} — {s}", .{ self.name, self.description });
        var p: usize = 64 - self.name.len - self.description.len - 5;
        while (p > 0) : (p -= 1) std.debug.print("─", .{});
        std.debug.print(" ╮\n", .{});

        if (self.help_sections) |sections| {
            for (sections) |sec| {
                std.debug.print("│  {s}:\n", .{sec.title});
                for (sec.commands) |cmd_name| {
                    const cmd = self.findCommand(cmd_name);
                    if (cmd) |c| {
                        std.debug.print("│    {s:<16} {s}\n", .{ c.name, c.description });
                    }
                }
            }
        }

        std.debug.print("│  Flags:\n", .{});
        std.debug.print("│    -h, --help       Show help\n", .{});
        std.debug.print("╰", .{});
        var b: usize = 0;
        while (b < 66) : (b += 1) std.debug.print("─", .{});
        std.debug.print("╯\n", .{});
    }

    fn printCommandHelp(self: *const App, cmd: Command) void {
        std.debug.print("{s} {s}\n{s}\n\n", .{ self.name, cmd.name, cmd.description });
        std.debug.print("Usage:\n  {s} {s} [options]\n\n", .{ self.name, cmd.name });

        if (cmd.args.len > 0) {
            std.debug.print("Arguments:\n", .{});
            for (cmd.args) |arg| {
                std.debug.print("  {s:<12} {s}\n", .{ arg.name, arg.description });
            }
        }

        if (cmd.flags.len > 0) {
            std.debug.print("Flags:\n", .{});
            for (cmd.flags) |flag| {
                if (flag.short) |s| {
                    std.debug.print("  -{c}, --{s:<12}", .{ s, flag.name });
                } else {
                    std.debug.print("      --{s:<12}", .{flag.name});
                }
                if (flag.default) |def| {
                    std.debug.print("{s} (default: {s})\n", .{ flag.description, def });
                } else {
                    std.debug.print("{s}\n", .{flag.description});
                }
            }
        }
    }
};

test "cli basics" {
    const allocator = std.testing.allocator;
    var ctx = Context.init(allocator, std.testing.io);
    defer ctx.deinit();

    try ctx.flags.put("port", "8080");
    try std.testing.expectEqualStrings("8080", ctx.flag("port"));
    try std.testing.expectEqual(@as(u16, 8080), ctx.flagInt(u16, "port"));
}
