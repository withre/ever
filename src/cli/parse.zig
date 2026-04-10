//! Argument parsing and command dispatch.
//!
//! Resolves the command from the argument list (skipping interleaved
//! global flags), parses all flags and positional arguments, then
//! dispatches to the matched command's `run` handler.

const std = @import("std");
const types = @import("types.zig");
const help = @import("help.zig");
const validate = @import("validate.zig");

const App = types.App;
const Command = types.Command;
const FlagDef = types.FlagDef;
const Context = types.Context;
const ResolveResult = types.ResolveResult;

// ── Entry Point ────────────────────────────────────────────────────────

/// Top-level parse-and-dispatch.  Called by `App.run`.
pub fn run(
    app: *const App,
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
    env_block: [*:null]const ?[*:0]const u8,
) !void {
    const raw = if (args.len > 1) args[1..] else args[0..0];
    if (raw.len == 0) {
        help.printRootHelp(app);
        return;
    }

    if (handleSpecialFirstArg(app, raw)) return;

    const resolved = resolveOrExit(app, raw);
    if (helpRequestedInArgs(raw[resolved.args_start..])) {
        help.printCommandHelp(app, resolved.cmd, resolved.parent_name);
        return;
    }
    if (handleParentWithoutRun(app, raw, resolved)) return;

    var ctx = Context.init(allocator, io, env_block);
    defer ctx.deinit();
    ctx.command_path = if (resolved.parent_name.len > 0) resolved.parent_name else resolved.cmd.name;

    applyDefaults(&ctx, resolved.cmd.flags, app.global_flags) catch return;
    applyEnv(&ctx, resolved.cmd.flags, app.global_flags, env_block) catch return;
    parsePreCmdGlobals(&ctx, raw, resolved, app.global_flags) catch return;
    try parseCommandArgs(&ctx, app, resolved.cmd, raw[resolved.args_start..]);

    validate.validateRequired(resolved.cmd, &ctx);
    validate.validateConflicts(resolved.cmd, &ctx);

    if (resolved.cmd.run) |run_fn| try run_fn(&ctx);
}

// ── Special First-Arg Handling ─────────────────────────────────────────

/// Handle help / version as the first token.  Returns true if handled.
fn handleSpecialFirstArg(app: *const App, raw: []const []const u8) bool {
    const first = raw[0];
    if (isHelpToken(first)) return handleHelpArg(app, raw);
    if (isVersionToken(first)) {
        std.debug.print("{s}\n", .{app.version});
        return true;
    }
    return false;
}

/// Resolve `help <cmd>` or just `help`.
fn handleHelpArg(app: *const App, raw: []const []const u8) bool {
    if (raw.len == 1) {
        help.printRootHelp(app);
        return true;
    }
    if (resolveCommandSkippingGlobals(app, raw[1..])) |res| {
        help.printCommandHelp(app, res.cmd, res.parent_name);
    } else {
        printError("unknown command. Run '{s} help'.\n", .{app.name});
        std.process.exit(1);
    }
    return true;
}

fn isHelpToken(tok: []const u8) bool {
    return std.mem.eql(u8, tok, "help") or
        std.mem.eql(u8, tok, "--help") or
        std.mem.eql(u8, tok, "-h");
}

fn isVersionToken(tok: []const u8) bool {
    return std.mem.eql(u8, tok, "version") or std.mem.eql(u8, tok, "--version");
}

// ── Command Resolution ─────────────────────────────────────────────────

/// Resolve a command or exit with a helpful error.
fn resolveOrExit(app: *const App, raw: []const []const u8) ResolveResult {
    return resolveCommandSkippingGlobals(app, raw) orelse {
        const cmd_name = firstNonFlagArg(app, raw) orelse raw[0];
        if (validate.suggestCommand(cmd_name, app.commands)) |sug| {
            printError("unknown command '{s}'. Did you mean '{s}'?\n", .{ cmd_name, sug });
        } else {
            printError("unknown command '{s}'. Run '{s} help'.\n", .{ cmd_name, app.name });
        }
        std.process.exit(1);
    };
}

/// True when `--help` or `-h` appears anywhere in the slice.
fn helpRequestedInArgs(args_tail: []const []const u8) bool {
    for (args_tail) |a| {
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) return true;
    }
    return false;
}

/// If the command is a parent (has subcommands, no run fn), show help or
/// error on unknown subcommand.  Returns true if handled.
fn handleParentWithoutRun(app: *const App, raw: []const []const u8, resolved: ResolveResult) bool {
    const cmd = resolved.cmd;
    if (cmd.subcommands.len == 0 or cmd.run != null) return false;
    if (resolved.args_start < raw.len) {
        const token = raw[resolved.args_start];
        if (!std.mem.startsWith(u8, token, "-")) {
            printError("unknown subcommand '{s}' for '{s}'. Run '{s} {s} --help'.\n", .{
                token, cmd.name, app.name, cmd.name,
            });
            std.process.exit(1);
        }
    }
    help.printCommandHelp(app, cmd, resolved.parent_name);
    return true;
}

/// Resolve command from args, skipping any global flags before/between
/// command words.
pub fn resolveCommandSkippingGlobals(app: *const App, raw: []const []const u8) ?ResolveResult {
    var i: usize = 0;
    while (i < raw.len) : (i += 1) {
        const a = raw[i];
        if (std.mem.startsWith(u8, a, "-")) {
            i = skipGlobalFlag(app, raw, i);
            continue;
        }
        return matchCommandAt(app, raw, i);
    }
    return null;
}

/// Skip a global flag (and its value if applicable). Returns the updated index.
fn skipGlobalFlag(app: *const App, raw: []const []const u8, idx: usize) usize {
    const a = raw[idx];
    if (std.mem.startsWith(u8, a, "--")) {
        const name = a[2..];
        if (std.mem.indexOfScalar(u8, name, '=') != null) return idx;
        if (validate.findFlagDef(app.global_flags, name)) |fdef| {
            return if (fdef.takes_value) idx + 1 else idx;
        }
        if (validate.negatedName(app.global_flags, name) != null) return idx;
    } else if (a.len == 2) {
        if (validate.findFlagByShort(app.global_flags, a[1])) |fdef| {
            return if (fdef.takes_value and a.len <= 2) idx + 1 else idx;
        }
    }
    return idx;
}

/// Try to match a command at `raw[idx]`, then optionally a subcommand.
fn matchCommandAt(app: *const App, raw: []const []const u8, idx: usize) ?ResolveResult {
    const token = raw[idx];
    for (app.commands) |cmd| {
        if (!cmd.matches(token)) continue;
        if (cmd.subcommands.len > 0) {
            if (findSubcommand(app, cmd, raw, idx + 1)) |sub_res| return sub_res;
        }
        return .{ .cmd = cmd, .parent_name = "", .args_start = idx + 1, .pre_cmd_start = idx };
    }
    return null;
}

/// Look for a subcommand token after a parent command, skipping global flags.
fn findSubcommand(app: *const App, parent: Command, raw: []const []const u8, start: usize) ?ResolveResult {
    var j = start;
    while (j < raw.len) : (j += 1) {
        const b = raw[j];
        if (std.mem.startsWith(u8, b, "-")) {
            j = skipGlobalFlag(app, raw, j);
            continue;
        }
        for (parent.subcommands) |sub| {
            if (sub.matches(b)) {
                return .{ .cmd = sub, .parent_name = parent.name, .args_start = j + 1, .pre_cmd_start = start - 1 };
            }
        }
        break;
    }
    return null;
}

/// Return the first non-flag token (for error messages).
fn firstNonFlagArg(app: *const App, raw: []const []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < raw.len) : (i += 1) {
        const a = raw[i];
        if (!std.mem.startsWith(u8, a, "-")) return a;
        if (std.mem.startsWith(u8, a, "--")) {
            const name = a[2..];
            if (std.mem.indexOfScalar(u8, name, '=') == null) {
                if (validate.findFlagDef(app.global_flags, name)) |fdef| {
                    if (fdef.takes_value) i += 1;
                }
            }
        } else if (a.len == 2) {
            if (validate.findFlagByShort(app.global_flags, a[1])) |fdef| {
                if (fdef.takes_value) i += 1;
            }
        }
    }
    return null;
}

// ── Default / Env Application ──────────────────────────────────────────

/// Apply default values for both command and global flags.
fn applyDefaults(ctx: *Context, cmd_flags: []const FlagDef, global_flags: []const FlagDef) !void {
    for (cmd_flags) |f| {
        if (f.default) |def| try ctx.flags.put(f.name, def);
    }
    for (global_flags) |f| {
        if (f.default) |def| try ctx.flags.put(f.name, def);
    }
}

/// Apply environment-variable overrides for both command and global flags.
fn applyEnv(
    ctx: *Context,
    cmd_flags: []const FlagDef,
    global_flags: []const FlagDef,
    env_block: [*:null]const ?[*:0]const u8,
) !void {
    for (cmd_flags) |f| {
        if (f.env) |env_name| {
            if (validate.lookupEnv(env_block, env_name)) |val|
                try ctx.flags.put(f.name, val);
        }
    }
    for (global_flags) |f| {
        if (f.env) |env_name| {
            if (validate.lookupEnv(env_block, env_name)) |val|
                try ctx.flags.put(f.name, val);
        }
    }
}

// ── Pre-Command Global Flag Parsing ────────────────────────────────────

/// Parse global flags that appeared before the command word.
fn parsePreCmdGlobals(
    ctx: *Context,
    raw: []const []const u8,
    resolved: ResolveResult,
    global_flags: []const FlagDef,
) !void {
    var i: usize = 0;
    while (i < resolved.pre_cmd_start) : (i += 1) {
        const a = raw[i];
        if (std.mem.startsWith(u8, a, "--")) {
            i = try parseLongGlobalFlag(ctx, raw, i, resolved.pre_cmd_start, global_flags);
        } else if (std.mem.startsWith(u8, a, "-") and a.len > 1) {
            i = try parseShortGlobalFlag(ctx, raw, i, resolved.pre_cmd_start, global_flags);
        }
    }
}

/// Parse a single `--flag[=value]` global flag. Returns updated index.
fn parseLongGlobalFlag(
    ctx: *Context,
    raw: []const []const u8,
    idx: usize,
    limit: usize,
    global_flags: []const FlagDef,
) !usize {
    const flag_name = raw[idx][2..];
    if (std.mem.indexOfScalar(u8, flag_name, '=')) |eq| {
        if (validate.findFlagDef(global_flags, flag_name[0..eq]) != null)
            try ctx.flags.put(flag_name[0..eq], flag_name[eq + 1 ..]);
        return idx;
    }
    if (validate.findFlagDef(global_flags, flag_name)) |fdef| {
        if (!fdef.takes_value) {
            try ctx.flags.put(fdef.name, "true");
            return idx;
        }
        if (idx + 1 < limit) {
            try ctx.flags.put(fdef.name, raw[idx + 1]);
            return idx + 1;
        }
    } else if (validate.negatedName(global_flags, flag_name)) |orig| {
        try ctx.flags.put(orig, "false");
    }
    return idx;
}

/// Parse a single `-X[value]` global flag. Returns updated index.
fn parseShortGlobalFlag(
    ctx: *Context,
    raw: []const []const u8,
    idx: usize,
    limit: usize,
    global_flags: []const FlagDef,
) !usize {
    const a = raw[idx];
    const short = a[1];
    const fdef = validate.findFlagByShort(global_flags, short) orelse return idx;
    if (!fdef.takes_value) {
        try ctx.flags.put(fdef.name, "true");
        return idx;
    }
    if (a.len > 2) {
        const rest = a[2..];
        const val = if (rest[0] == '=') rest[1..] else rest;
        try ctx.flags.put(fdef.name, val);
        return idx;
    }
    if (idx + 1 < limit) {
        try ctx.flags.put(fdef.name, raw[idx + 1]);
        return idx + 1;
    }
    return idx;
}

// ── Main Argument Loop ─────────────────────────────────────────────────

/// Parse flags and positional args for the resolved command.
fn parseCommandArgs(
    ctx: *Context,
    app: *const App,
    cmd: Command,
    args: []const []const u8,
) !void {
    var i: usize = 0;
    var pos_idx: usize = 0;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "--")) {
            try parseRestOrPositional(ctx, cmd, args[i + 1 ..], &pos_idx);
            break;
        }
        if (std.mem.startsWith(u8, a, "--")) {
            i = try parseLongFlag(ctx, app, cmd, args, i);
        } else if (std.mem.startsWith(u8, a, "-") and a.len > 1) {
            i = try parseShortFlag(ctx, app, cmd, args, i);
        } else {
            try parsePositional(ctx, cmd, a, &pos_idx);
        }
    }
}

/// Handle tokens after `--`: rest args or overflow positional.
fn parseRestOrPositional(ctx: *Context, cmd: Command, rest: []const []const u8, pos_idx: *usize) !void {
    if (cmd.takes_rest) {
        for (rest) |r| try ctx.rest_args.append(ctx.allocator, r);
    } else {
        for (rest) |r| {
            if (pos_idx.* < cmd.args.len) {
                try ctx.positional.put(cmd.args[pos_idx.*].name, r);
                pos_idx.* += 1;
            }
        }
    }
}

/// Parse a `--flag` or `--flag=value` token. Returns updated index.
fn parseLongFlag(
    ctx: *Context,
    app: *const App,
    cmd: Command,
    args: []const []const u8,
    idx: usize,
) !usize {
    const flag_name = args[idx][2..];

    // --flag=value
    if (std.mem.indexOfScalar(u8, flag_name, '=')) |eq|
        return parseLongFlagEq(ctx, app, cmd, flag_name, eq, idx);

    // --no-X negation
    if (validate.negatedName(cmd.flags, flag_name)) |orig| {
        try ctx.flags.put(orig, "false");
        return idx;
    }
    if (validate.negatedName(app.global_flags, flag_name)) |orig| {
        try ctx.flags.put(orig, "false");
        return idx;
    }

    const fd = validate.findFlagDef(cmd.flags, flag_name) orelse
        validate.findFlagDef(app.global_flags, flag_name) orelse {
        printUnknownFlagError(flag_name, cmd, app);
        std.process.exit(1);
    };

    if (!fd.takes_value) {
        try ctx.flags.put(fd.name, "true");
        return idx;
    }
    if (idx + 1 < args.len and !validate.isKnownFlag(cmd, app.global_flags, args[idx + 1])) {
        try ctx.flags.put(fd.name, args[idx + 1]);
        return idx + 1;
    }
    printError("flag '--{s}' requires a value\n", .{fd.name});
    std.process.exit(1);
}

/// Handle `--name=value` form. Returns `idx` unchanged (single token).
fn parseLongFlagEq(
    ctx: *Context,
    app: *const App,
    cmd: Command,
    flag_name: []const u8,
    eq: usize,
    idx: usize,
) !usize {
    const name = flag_name[0..eq];
    const value = flag_name[eq + 1 ..];
    if (validate.findFlagDef(cmd.flags, name) != null or
        validate.findFlagDef(app.global_flags, name) != null)
    {
        try ctx.flags.put(name, value);
        return idx;
    }
    printUnknownFlagError(name, cmd, app);
    std.process.exit(1);
}

/// Parse a `-X` or `-Xvalue` short flag. Returns updated index.
fn parseShortFlag(
    ctx: *Context,
    app: *const App,
    cmd: Command,
    args: []const []const u8,
    idx: usize,
) !usize {
    const a = args[idx];
    const short = a[1];
    const fd = validate.findFlagByShort(cmd.flags, short) orelse
        validate.findFlagByShort(app.global_flags, short) orelse {
        printError("unknown flag '-{c}'\n", .{short});
        std.process.exit(1);
    };
    if (!fd.takes_value) {
        try ctx.flags.put(fd.name, "true");
        return idx;
    }
    if (a.len > 2) {
        const rest = a[2..];
        const val = if (rest[0] == '=') rest[1..] else rest;
        try ctx.flags.put(fd.name, val);
        return idx;
    }
    if (idx + 1 < args.len) {
        try ctx.flags.put(fd.name, args[idx + 1]);
        return idx + 1;
    }
    printError("flag '-{c}' (--{s}) requires a value\n", .{ short, fd.name });
    std.process.exit(1);
}

/// Parse a positional argument token.
fn parsePositional(ctx: *Context, cmd: Command, token: []const u8, pos_idx: *usize) !void {
    if (pos_idx.* < cmd.args.len) {
        try ctx.positional.put(cmd.args[pos_idx.*].name, token);
        pos_idx.* += 1;
        return;
    }
    printError("unexpected argument '{s}'\n", .{token});
    std.process.exit(1);
}

// ── Error Helpers ──────────────────────────────────────────────────────

/// Print a prefixed error message to stderr.
fn printError(comptime fmt: []const u8, args: anytype) void {
    std.debug.print("error: " ++ fmt, args);
}

/// Print an unknown-flag error with an optional "did you mean" suggestion.
fn printUnknownFlagError(flag_name: []const u8, cmd: Command, app: *const App) void {
    const suggestion = validate.suggestFlag(flag_name, cmd.flags) orelse
        validate.suggestFlag(flag_name, app.global_flags);
    if (suggestion) |sug| {
        std.debug.print("error: unknown flag '--{s}'. Did you mean '--{s}'?\n", .{ flag_name, sug });
    } else {
        std.debug.print("error: unknown flag '--{s}'\n", .{flag_name});
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

test "resolveCommandSkippingGlobals finds simple command" {
    const app = App{
        .name = "test",
        .commands = &.{.{ .name = "status" }},
    };
    const raw = &[_][]const u8{"status"};
    const res = resolveCommandSkippingGlobals(&app, raw).?;
    try std.testing.expectEqualStrings("status", res.cmd.name);
    try std.testing.expectEqual(@as(usize, 1), res.args_start);
}

test "resolveCommandSkippingGlobals skips global flag before command" {
    const app = App{
        .name = "test",
        .global_flags = &.{.{ .name = "port", .short = 'p', .description = "port" }},
        .commands = &.{.{ .name = "status" }},
    };
    const raw = &[_][]const u8{ "-p", "9999", "status" };
    const res = resolveCommandSkippingGlobals(&app, raw).?;
    try std.testing.expectEqualStrings("status", res.cmd.name);
}

test "resolveCommandSkippingGlobals resolves subcommand" {
    const app = App{
        .name = "test",
        .commands = &.{.{
            .name = "topic",
            .subcommands = &.{.{ .name = "list" }},
        }},
    };
    const raw = &[_][]const u8{ "topic", "list" };
    const res = resolveCommandSkippingGlobals(&app, raw).?;
    try std.testing.expectEqualStrings("list", res.cmd.name);
    try std.testing.expectEqualStrings("topic", res.parent_name);
}

test "resolveCommandSkippingGlobals returns null for unknown" {
    const app = App{ .name = "test", .commands = &.{.{ .name = "status" }} };
    const raw = &[_][]const u8{"bogus"};
    try std.testing.expect(resolveCommandSkippingGlobals(&app, raw) == null);
}

test "helpRequestedInArgs" {
    try std.testing.expect(helpRequestedInArgs(&.{ "foo", "--help" }));
    try std.testing.expect(helpRequestedInArgs(&.{"-h"}));
    try std.testing.expect(!helpRequestedInArgs(&.{ "foo", "bar" }));
    try std.testing.expect(!helpRequestedInArgs(&.{}));
}

test "isHelpToken" {
    try std.testing.expect(isHelpToken("help"));
    try std.testing.expect(isHelpToken("--help"));
    try std.testing.expect(isHelpToken("-h"));
    try std.testing.expect(!isHelpToken("status"));
}

test "isVersionToken" {
    try std.testing.expect(isVersionToken("version"));
    try std.testing.expect(isVersionToken("--version"));
    try std.testing.expect(!isVersionToken("help"));
}
