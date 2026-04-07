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
    envp: [*:null]const ?[*:0]const u8,
    command_name: []const u8,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, envp: [*:null]const ?[*:0]const u8) Context {
        return .{
            .flags = std.StringHashMap([]const u8).init(allocator),
            .positional = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
            .io = io,
            .envp = envp,
            .command_name = "",
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

        // Find the command - check for parent+subcommand pattern
        var parent_cmd: ?Command = null;
        var sub_cmd: ?Command = null;
        var full_cmd_name: []const u8 = command_name;

        // First try to find as a simple command
        const simple_cmd = self.findCommand(command_name);
        if (simple_cmd) |cmd_found| {
            if (cmd_found.subcommands.len > 0) {
                // This is a parent command - check if there's a subcommand
                if (args.len >= 3 and !std.mem.startsWith(u8, args[2], "-")) {
                    const sub_name = args[2];
                    for (cmd_found.subcommands) |sub| {
                        if (std.mem.eql(u8, sub.name, sub_name)) {
                            parent_cmd = cmd_found;
                            sub_cmd = sub;
                            full_cmd_name = std.fmt.allocPrint(std.heap.page_allocator, "{s} {s}", .{ command_name, sub_name }) catch sub_name;
                            break;
                        }
                    }
                }
                if (sub_cmd == null) {
                    // Parent command without valid subcommand
                    parent_cmd = cmd_found;
                }
            }
        }

        const cmd = sub_cmd orelse simple_cmd;
        if (cmd == null) {
            std.debug.print("unknown command '{s}'. Run '{s} help'.\n", .{ command_name, self.name });
            std.process.exit(1);
        }

        var ctx = Context.init(allocator, io, env_block);
        defer ctx.deinit();
        ctx.command_name = if (parent_cmd != null and sub_cmd != null) full_cmd_name else command_name;

        // Initialize defaults
        for (cmd.?.flags) |flag| {
            if (flag.default) |def| {
                try ctx.flags.put(flag.name, def);
            }
        }

        // Parse flags and positionals
        var i: usize = if (parent_cmd != null and sub_cmd != null) 3 else 2;
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
        const is_tty = std.c.isatty(std.posix.STDERR_FILENO) != 0;
        
        // Title line with color
        if (is_tty) {
            std.debug.print("\x1b[38;2;130;170;255m{s}\x1b[0m — {s}\n\n", .{ self.name, self.description });
        } else {
            std.debug.print("{s} — {s}\n\n", .{ self.name, self.description });
        }
        
        std.debug.print("Usage: {s} <command> [options]\n\n", .{self.name});
        
        // Color for section headers
        const header_color = "\x1b[38;2;130;170;255m";
        const reset = "\x1b[0m";
        
        if (is_tty) {
            std.debug.print("{s}Commands:{s}\n", .{ header_color, reset });
        } else {
            std.debug.print("Commands:\n", .{});
        }
        
        if (self.help_sections) |sections| {
            for (sections) |sec| {
                for (sec.commands) |cmd_name| {
                    const cmd = self.findCommand(cmd_name);
                    if (cmd) |c| {
                        // Build flags string
                        var flags_buf: [256]u8 = undefined;
                        var flags_len: usize = 0;
                        if (c.flags.len > 0) {
                            var first = true;
                            for (c.flags) |flag| {
                                if (flag.description.len > 0) {
                                    if (!first) {
                                        flags_buf[flags_len] = ',';
                                        flags_len += 1;
                                        flags_buf[flags_len] = ' ';
                                        flags_len += 1;
                                    }
                                    const result = std.fmt.bufPrint(flags_buf[flags_len..], "--{s}", .{flag.name}) catch break;
                                    flags_len += result.len;
                                    first = false;
                                    if (flags_len > 250) break;
                                }
                            }
                        }
                        
                        if (flags_len > 0) {
                            std.debug.print("  {s:<15} {s} ({s})\n", .{ cmd_name, c.description, flags_buf[0..flags_len] });
                        } else {
                            std.debug.print("  {s:<15} {s}\n", .{ cmd_name, c.description });
                        }
                    }
                }
            }
        }
        
        std.debug.print("\nRun '{s} <command> --help' for more information.\n", .{ self.name });
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
    const envp: [*:null]const ?[*:0]const u8 = &.{};
    var ctx = Context.init(allocator, std.testing.io, envp);
    defer ctx.deinit();

    try ctx.flags.put("port", "8080");
    try std.testing.expectEqualStrings("8080", ctx.flag("port"));
    try std.testing.expectEqual(@as(u16, 8080), ctx.flagInt(u16, "port"));
}
