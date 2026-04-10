//! Help output formatting: root help, per-command help, flag/arg
//! label rendering. Uses `colour.Palette` — never raw escape codes.

const std = @import("std");
const types = @import("types.zig");
const colour = @import("colour.zig");

const App = types.App;
const Command = types.Command;
const FlagDef = types.FlagDef;
const ArgDef = types.ArgDef;

// ── Root Help ──────────────────────────────────────────────────────────

/// Print the top-level usage / section listing for the whole app.
pub fn printRootHelp(app: *const App) void {
    const p = colour.detect();

    std.debug.print("{s}{s}{s} — {s}{s}{s}\n\n", .{
        p.title, app.name, p.reset, p.desc, app.description, p.reset,
    });
    std.debug.print("Usage: {s} [global options] <command> [options]\n", .{app.name});

    printGlobalFlags(app, p);
    printHelpSections(app, p);

    std.debug.print("\n{s}Run '{s} <command> --help' for more information.{s}\n", .{
        p.desc, app.name, p.reset,
    });
}

/// Print global flags block (shared by root and command help).
fn printGlobalFlags(app: *const App, p: colour.Palette) void {
    if (app.global_flags.len == 0) return;
    std.debug.print("\n{s}Global Flags:{s}\n", .{ p.section, p.reset });
    for (app.global_flags) |f| printFlagHelp(f, p);
}

/// Print custom help sections defined on the app.
fn printHelpSections(app: *const App, p: colour.Palette) void {
    const sections = app.help_sections orelse return;
    for (sections) |sec| {
        std.debug.print("\n{s}{s}:{s}\n", .{ p.section, sec.title, p.reset });
        for (sec.entries) |entry| {
            std.debug.print("  {s}{s:<17}{s}{s}{s}{s}\n", .{
                p.cmd, entry.label, p.reset, p.desc, entry.description, p.reset,
            });
        }
    }
}

// ── Command Help ───────────────────────────────────────────────────────

/// Print full help for a single command (description, usage, flags, etc.).
pub fn printCommandHelp(app: *const App, cmd: Command, parent_name: []const u8) void {
    const p = colour.detect();

    printCmdDescription(cmd, parent_name, p);
    printCmdUsage(app, cmd, parent_name, p);
    printCmdAliases(cmd, p);
    printCmdSubcommands(app, cmd, parent_name, p);

    if (cmd.subcommands.len > 0) return;

    printCmdArguments(cmd, p);
    printCmdFlags(cmd, p);

    if (app.global_flags.len > 0) {
        std.debug.print("\n{s}Global Flags:{s}\n", .{ p.section, p.reset });
        for (app.global_flags) |f| printFlagHelp(f, p);
    }
}

/// Print the one-line command description.
fn printCmdDescription(cmd: Command, parent: []const u8, p: colour.Palette) void {
    if (parent.len > 0) {
        std.debug.print("{s}{s} {s}{s} — {s}{s}{s}\n", .{
            p.cmd, parent, cmd.name, p.reset, p.desc, cmd.description, p.reset,
        });
    } else {
        std.debug.print("{s}{s}{s} — {s}{s}{s}\n", .{
            p.cmd, cmd.name, p.reset, p.desc, cmd.description, p.reset,
        });
    }
}

/// Print the "Usage:" block for a command.
fn printCmdUsage(app: *const App, cmd: Command, parent: []const u8, p: colour.Palette) void {
    std.debug.print("\nUsage:\n", .{});
    if (cmd.subcommands.len > 0) {
        printUsageWithSubcmds(app, cmd, parent);
        return;
    }
    printUsageLeaf(app, cmd, parent, p);
}

/// Usage line for a command that has subcommands.
fn printUsageWithSubcmds(app: *const App, cmd: Command, parent: []const u8) void {
    if (parent.len > 0) {
        std.debug.print("  {s} {s} {s} <subcommand> [OPTIONS]\n", .{ app.name, parent, cmd.name });
    } else {
        std.debug.print("  {s} {s} <subcommand> [OPTIONS]\n", .{ app.name, cmd.name });
    }
}

/// Usage line for a leaf command (no subcommands).
fn printUsageLeaf(app: *const App, cmd: Command, parent: []const u8, p: colour.Palette) void {
    if (parent.len > 0) {
        std.debug.print("  {s} {s} {s}", .{ app.name, parent, cmd.name });
    } else {
        std.debug.print("  {s} {s}", .{ app.name, cmd.name });
    }
    if (cmd.flags.len > 0 or app.global_flags.len > 0)
        std.debug.print(" [OPTIONS]", .{});

    for (cmd.args) |a| printArgInUsage(a, p);

    if (cmd.takes_rest) std.debug.print(" [-- ARGS...]", .{});
    std.debug.print("\n", .{});
}

/// Print a single positional arg token in the usage line.
fn printArgInUsage(a: ArgDef, p: colour.Palette) void {
    if (a.required) {
        std.debug.print(" {s}", .{p.cmd});
        printArgUpper(a.name);
        std.debug.print("{s}", .{p.reset});
    } else {
        std.debug.print(" [{s}", .{p.cmd});
        printUppercase(a.name);
        std.debug.print("{s}]", .{p.reset});
    }
}

/// Print aliases line.
fn printCmdAliases(cmd: Command, p: colour.Palette) void {
    if (cmd.aliases.len == 0) return;
    std.debug.print("\n{s}Aliases:{s} ", .{ p.section, p.reset });
    for (cmd.aliases, 0..) |alias, i| {
        if (i > 0) std.debug.print(", ", .{});
        std.debug.print("{s}{s}{s}", .{ p.cmd, alias, p.reset });
    }
    std.debug.print("\n", .{});
}

/// Print subcommands section (and "Run … --help" hint).
fn printCmdSubcommands(app: *const App, cmd: Command, parent: []const u8, p: colour.Palette) void {
    if (cmd.subcommands.len == 0) return;
    std.debug.print("\n{s}Subcommands:{s}\n", .{ p.section, p.reset });
    for (cmd.subcommands) |sub| {
        std.debug.print("  {s}{s:<17}{s}{s}{s}{s}\n", .{
            p.cmd, sub.name, p.reset, p.desc, sub.description, p.reset,
        });
    }
    if (parent.len > 0) {
        std.debug.print("\n{s}Run '{s} {s} {s} <subcommand> --help' for details.{s}\n", .{
            p.desc, app.name, parent, cmd.name, p.reset,
        });
    } else {
        std.debug.print("\n{s}Run '{s} {s} <subcommand> --help' for details.{s}\n", .{
            p.desc, app.name, cmd.name, p.reset,
        });
    }
}

/// Print the "Arguments:" section.
fn printCmdArguments(cmd: Command, p: colour.Palette) void {
    if (cmd.args.len == 0) return;
    std.debug.print("\n{s}Arguments:{s}\n", .{ p.section, p.reset });
    for (cmd.args) |a| {
        var buf: [64]u8 = undefined;
        const label = fmtArgLabel(a, &buf);
        std.debug.print("  {s}{s:<28}{s}{s}{s}{s}\n", .{
            p.cmd, label, p.reset, p.desc, a.description, p.reset,
        });
    }
}

/// Print the command-level "Flags:" section.
fn printCmdFlags(cmd: Command, p: colour.Palette) void {
    if (cmd.flags.len == 0) return;
    std.debug.print("\n{s}Flags:{s}\n", .{ p.section, p.reset });
    for (cmd.flags) |f| printFlagHelp(f, p);
}

// ── Single-Flag Formatting ─────────────────────────────────────────────

/// Print one flag row: left column + description + meta annotations.
fn printFlagHelp(f: FlagDef, p: colour.Palette) void {
    var left_buf: [64]u8 = undefined;
    const left = fmtFlagLeft(f, &left_buf);

    std.debug.print("  {s}{s:<28}{s}{s}{s}", .{
        p.flag, left, p.reset, p.flag_desc, f.description,
    });
    printFlagMeta(f, p);
    std.debug.print("{s}\n", .{p.reset});
}

/// Print the trailing meta annotations (required, default, env, conflicts, negatable).
fn printFlagMeta(f: FlagDef, p: colour.Palette) void {
    if (f.required)
        std.debug.print(" {s}(required){s}", .{ p.required, p.reset });
    if (f.default) |def|
        std.debug.print(" {s}(default: {s}){s}", .{ p.flag_desc, def, p.reset });
    if (f.env) |env_name|
        std.debug.print(" {s}[${s}]{s}", .{ p.env, env_name, p.reset });
    if (f.conflicts.len > 0) {
        std.debug.print(" {s}(conflicts: ", .{p.flag_desc});
        for (f.conflicts, 0..) |c, i| {
            if (i > 0) std.debug.print(", ", .{});
            std.debug.print("--{s}", .{c});
        }
        std.debug.print("){s}", .{p.reset});
    }
    if (f.negatable)
        std.debug.print(" {s}(--no-{s} to disable){s}", .{ p.flag_desc, f.name, p.reset });
}

// ── Label Formatting ───────────────────────────────────────────────────

/// Format an arg label like `<TOPIC>` or `[PAYLOAD]` into `buf`.
pub fn fmtArgLabel(a: ArgDef, buf: []u8) []const u8 {
    var pos: usize = 0;
    buf[pos] = if (a.required) '<' else '[';
    pos += 1;
    for (a.name) |c| {
        buf[pos] = toUpper(c);
        pos += 1;
    }
    buf[pos] = if (a.required) '>' else ']';
    pos += 1;
    return buf[0..pos];
}

/// Format the left column for a flag: `-p, --port <PORT>` or `    --verbose`.
pub fn fmtFlagLeft(f: FlagDef, buf: []u8) []const u8 {
    var pos: usize = 0;
    pos = writeShortPrefix(f, buf, pos);
    pos = writeLongName(f, buf, pos);
    pos = writeValuePlaceholder(f, buf, pos);
    return buf[0..pos];
}

/// Write `-X, ` or `    ` into buf.
fn writeShortPrefix(f: FlagDef, buf: []u8, start: usize) usize {
    if (f.short) |s| {
        buf[start] = '-';
        buf[start + 1] = s;
        buf[start + 2] = ',';
        buf[start + 3] = ' ';
    } else {
        buf[start] = ' ';
        buf[start + 1] = ' ';
        buf[start + 2] = ' ';
        buf[start + 3] = ' ';
    }
    return start + 4;
}

/// Write `--name` into buf.
fn writeLongName(f: FlagDef, buf: []u8, start: usize) usize {
    var pos = start;
    buf[pos] = '-';
    buf[pos + 1] = '-';
    pos += 2;
    for (f.name) |c| {
        buf[pos] = c;
        pos += 1;
    }
    return pos;
}

/// Write ` <VALUE>` into buf if the flag takes a value.
fn writeValuePlaceholder(f: FlagDef, buf: []u8, start: usize) usize {
    if (!f.takes_value) return start;
    var pos = start;
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
            buf[pos] = if (c == '-') '_' else toUpper(c);
            pos += 1;
        }
    }
    buf[pos] = '>';
    return pos + 1;
}

/// Print ` <NAME>` (with angle brackets and space).
fn printArgUpper(name: []const u8) void {
    std.debug.print(" <", .{});
    printUppercase(name);
    std.debug.print(">", .{});
}

/// Print a name in uppercase.
fn printUppercase(name: []const u8) void {
    for (name) |c| std.debug.print("{c}", .{toUpper(c)});
}

/// Convert a lowercase ASCII char to uppercase; pass others through.
fn toUpper(c: u8) u8 {
    return if (c >= 'a' and c <= 'z') c - 32 else c;
}

// ── Tests ──────────────────────────────────────────────────────────────

test "fmtArgLabel required" {
    var buf: [64]u8 = undefined;
    const label = fmtArgLabel(.{ .name = "topic", .required = true }, &buf);
    try std.testing.expectEqualStrings("<TOPIC>", label);
}

test "fmtArgLabel optional" {
    var buf: [64]u8 = undefined;
    const label = fmtArgLabel(.{ .name = "payload", .required = false }, &buf);
    try std.testing.expectEqualStrings("[PAYLOAD]", label);
}

test "fmtFlagLeft with short" {
    var buf: [64]u8 = undefined;
    const left = fmtFlagLeft(.{ .name = "port", .short = 'p', .default = "7890", .description = "Store port" }, &buf);
    try std.testing.expectEqualStrings("-p, --port <PORT>", left);
}

test "fmtFlagLeft long only no value" {
    var buf: [64]u8 = undefined;
    const left = fmtFlagLeft(.{ .name = "no-persist", .takes_value = false, .description = "Don't persist" }, &buf);
    try std.testing.expectEqualStrings("    --no-persist", left);
}

test "fmtFlagLeft with custom value_name" {
    var buf: [64]u8 = undefined;
    const left = fmtFlagLeft(.{ .name = "every", .value_name = "INTERVAL", .description = "Interval" }, &buf);
    try std.testing.expectEqualStrings("    --every <INTERVAL>", left);
}

test "fmtFlagLeft long only with value" {
    var buf: [64]u8 = undefined;
    const left = fmtFlagLeft(.{ .name = "data-dir", .description = "Data directory" }, &buf);
    try std.testing.expectEqualStrings("    --data-dir <DATA_DIR>", left);
}

test "toUpper" {
    try std.testing.expectEqual(@as(u8, 'A'), toUpper('a'));
    try std.testing.expectEqual(@as(u8, 'Z'), toUpper('z'));
    try std.testing.expectEqual(@as(u8, '-'), toUpper('-'));
    try std.testing.expectEqual(@as(u8, '0'), toUpper('0'));
}
