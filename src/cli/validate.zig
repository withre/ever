//! Validation helpers: env lookup, Levenshtein distance, flag / command
//! suggestions, required-flag checks, and conflict enforcement.

const std = @import("std");
const types = @import("types.zig");

const FlagDef = types.FlagDef;
const Command = types.Command;
const Context = types.Context;

// ── Environment Variable Lookup ────────────────────────────────────────

/// Look up an environment variable from a null-terminated envp block.
pub fn lookupEnv(envp: [*:null]const ?[*:0]const u8, name: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (envp[i]) |env_str| : (i += 1) {
        const len = std.mem.indexOfSentinel(u8, 0, env_str);
        const entry = env_str[0..len];
        if (entry.len <= name.len) continue;
        if (entry[name.len] != '=') continue;
        if (!std.mem.eql(u8, entry[0..name.len], name)) continue;
        return entry[name.len + 1 ..];
    }
    return null;
}

// ── Levenshtein Distance ───────────────────────────────────────────────

/// Compute the edit distance between two strings (max length 32).
pub fn levenshteinDistance(a: []const u8, b: []const u8) usize {
    if (a.len == 0) return b.len;
    if (b.len == 0) return a.len;
    if (a.len > 32 or b.len > 32) return @max(a.len, b.len);

    var prev: [33]usize = undefined;
    var curr: [33]usize = undefined;

    for (0..b.len + 1) |j| prev[j] = j;

    for (a, 0..) |ca, i| {
        curr[0] = i + 1;
        for (b, 0..) |cb, j| {
            const cost: usize = if (ca == cb) 0 else 1;
            curr[j + 1] = @min(@min(curr[j] + 1, prev[j + 1] + 1), prev[j] + cost);
        }
        @memcpy(prev[0 .. b.len + 1], curr[0 .. b.len + 1]);
    }
    return prev[b.len];
}

// ── Suggestion Helpers ─────────────────────────────────────────────────

/// Suggest the closest flag name (threshold < 4), or null.
pub fn suggestFlag(input: []const u8, flags: []const FlagDef) ?[]const u8 {
    var best: ?[]const u8 = null;
    var best_dist: usize = 4;
    for (flags) |f| {
        const dist = levenshteinDistance(input, f.name);
        if (dist < best_dist) {
            best_dist = dist;
            best = f.name;
        }
    }
    return best;
}

/// Suggest the closest command name or alias (threshold < 4), or null.
pub fn suggestCommand(input: []const u8, commands: []const types.Command) ?[]const u8 {
    var best: ?[]const u8 = null;
    var best_dist: usize = 4;
    for (commands) |cmd| {
        const dist = levenshteinDistance(input, cmd.name);
        if (dist < best_dist) {
            best_dist = dist;
            best = cmd.name;
        }
        for (cmd.aliases) |alias| {
            const adist = levenshteinDistance(input, alias);
            if (adist < best_dist) {
                best_dist = adist;
                best = alias;
            }
        }
    }
    return best;
}

// ── Flag Lookup ────────────────────────────────────────────────────────

/// Find a flag definition by long name.
pub fn findFlagDef(flags: []const FlagDef, name: []const u8) ?FlagDef {
    for (flags) |f| {
        if (std.mem.eql(u8, f.name, name)) return f;
    }
    return null;
}

/// Find a flag definition by short character.
pub fn findFlagByShort(flags: []const FlagDef, short: u8) ?FlagDef {
    for (flags) |f| {
        if (f.short == short) return f;
    }
    return null;
}

/// If `flag_name` is `"no-X"` and `X` is negatable, return `X`.
pub fn negatedName(flags: []const FlagDef, flag_name: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, flag_name, "no-")) return null;
    const orig = flag_name[3..];
    for (flags) |f| {
        if (f.negatable and std.mem.eql(u8, f.name, orig)) return f.name;
    }
    return null;
}

// ── Required / Conflict Validation ─────────────────────────────────────

/// Check all required flags are present; exit(1) on failure.
pub fn validateRequired(cmd: Command, ctx: *const Context) void {
    for (cmd.flags) |f| {
        if (!f.required) continue;
        const val = ctx.flags.get(f.name) orelse {
            printRequiredError(f);
            std.process.exit(1);
        };
        if (val.len == 0) {
            printRequiredError(f);
            std.process.exit(1);
        }
    }
}

/// Print a "missing required flag" error with optional env hint.
fn printRequiredError(f: FlagDef) void {
    std.debug.print("error: missing required flag --{s}", .{f.name});
    if (f.env) |env_name| {
        std.debug.print(" (or set ${s})", .{env_name});
    }
    std.debug.print("\n", .{});
}

/// Check no mutually-exclusive flags are set together; exit(1) on failure.
pub fn validateConflicts(cmd: Command, ctx: *const Context) void {
    for (cmd.flags) |f| {
        if (f.conflicts.len == 0) continue;
        const this_val = ctx.flags.get(f.name) orelse continue;
        if (this_val.len == 0) continue;
        if (isDefault(f, this_val)) continue;
        for (f.conflicts) |conflict_name| {
            const conflict_val = ctx.flags.get(conflict_name) orelse continue;
            if (conflict_val.len == 0) continue;
            if (findFlagDef(cmd.flags, conflict_name)) |cd| {
                if (isDefault(cd, conflict_val)) continue;
            }
            std.debug.print("error: --{s} and --{s} are mutually exclusive\n", .{ f.name, conflict_name });
            std.process.exit(1);
        }
    }
}

/// True when `val` equals the flag's default.
fn isDefault(f: FlagDef, val: []const u8) bool {
    const def = f.default orelse return false;
    return std.mem.eql(u8, val, def);
}

/// Check if a token is a recognised flag for the given command + globals.
pub fn isKnownFlag(cmd: Command, global_flags: []const FlagDef, token: []const u8) bool {
    if (!std.mem.startsWith(u8, token, "-")) return false;
    if (std.mem.startsWith(u8, token, "--")) {
        const name = token[2..];
        const bare = if (std.mem.indexOfScalar(u8, name, '=')) |eq| name[0..eq] else name;
        if (bare.len == 0) return true; // "--" separator
        return findFlagDef(cmd.flags, bare) != null or
            findFlagDef(global_flags, bare) != null or
            negatedName(cmd.flags, bare) != null or
            negatedName(global_flags, bare) != null or
            std.mem.eql(u8, bare, "help");
    }
    if (token.len >= 2) {
        const short = token[1];
        if (short == 'h') return true;
        return findFlagByShort(cmd.flags, short) != null or
            findFlagByShort(global_flags, short) != null;
    }
    return false;
}

// ── Tests ──────────────────────────────────────────────────────────────

test "lookupEnv finds variable" {
    const env1: [*:0]const u8 = "HOME=/home/test";
    const env2: [*:0]const u8 = "EVER_PORT=9999";
    const envp: [*:null]const ?[*:0]const u8 = &.{ env1, env2, null };

    try std.testing.expectEqualStrings("/home/test", lookupEnv(envp, "HOME").?);
    try std.testing.expectEqualStrings("9999", lookupEnv(envp, "EVER_PORT").?);
    try std.testing.expect(lookupEnv(envp, "MISSING") == null);
    try std.testing.expect(lookupEnv(envp, "HOM") == null);
}

test "lookupEnv handles empty envp" {
    const envp: [*:null]const ?[*:0]const u8 = &.{null};
    try std.testing.expect(lookupEnv(envp, "ANYTHING") == null);
}

test "levenshtein distance" {
    try std.testing.expectEqual(@as(usize, 0), levenshteinDistance("abc", "abc"));
    try std.testing.expectEqual(@as(usize, 1), levenshteinDistance("abc", "ab"));
    try std.testing.expectEqual(@as(usize, 1), levenshteinDistance("evry", "every"));
    try std.testing.expectEqual(@as(usize, 1), levenshteinDistance("crn", "cron"));
    try std.testing.expectEqual(@as(usize, 3), levenshteinDistance("abc", "xyz"));
}

test "levenshtein empty strings" {
    try std.testing.expectEqual(@as(usize, 0), levenshteinDistance("", ""));
    try std.testing.expectEqual(@as(usize, 3), levenshteinDistance("abc", ""));
    try std.testing.expectEqual(@as(usize, 3), levenshteinDistance("", "abc"));
}

test "suggestFlag finds close match" {
    const flags = &[_]FlagDef{
        .{ .name = "every", .description = "interval" },
        .{ .name = "cron", .description = "cron expr" },
        .{ .name = "port", .description = "port" },
    };
    try std.testing.expectEqualStrings("every", suggestFlag("evry", flags).?);
    try std.testing.expectEqualStrings("cron", suggestFlag("crn", flags).?);
    try std.testing.expect(suggestFlag("xyzxyz", flags) == null);
}

test "suggestFlag returns null for empty flags" {
    try std.testing.expect(suggestFlag("test", &.{}) == null);
}

test "suggestCommand finds commands and aliases" {
    const cmds = &[_]types.Command{
        .{ .name = "topic", .aliases = &.{"tp"} },
        .{ .name = "hook" },
    };
    try std.testing.expectEqualStrings("topic", suggestCommand("topi", cmds).?);
    try std.testing.expectEqualStrings("hook", suggestCommand("hok", cmds).?);
    try std.testing.expect(suggestCommand("zzzzzzz", cmds) == null);
}

test "negatedName" {
    const flags = &[_]FlagDef{
        .{ .name = "http", .takes_value = false, .negatable = true, .description = "HTTP" },
        .{ .name = "persist", .takes_value = false, .negatable = false, .description = "persist" },
    };
    try std.testing.expectEqualStrings("http", negatedName(flags, "no-http").?);
    try std.testing.expect(negatedName(flags, "no-persist") == null);
    try std.testing.expect(negatedName(flags, "http") == null);
}

test "findFlagDef" {
    const flags = &[_]FlagDef{
        .{ .name = "port", .short = 'p', .description = "port" },
        .{ .name = "host", .description = "host" },
    };
    const found = findFlagDef(flags, "port").?;
    try std.testing.expectEqualStrings("port", found.name);
    try std.testing.expectEqual(@as(?u8, 'p'), found.short);
    try std.testing.expect(findFlagDef(flags, "missing") == null);
}

test "findFlagByShort" {
    const flags = &[_]FlagDef{
        .{ .name = "port", .short = 'p', .description = "port" },
        .{ .name = "host", .description = "host" },
    };
    try std.testing.expectEqualStrings("port", findFlagByShort(flags, 'p').?.name);
    try std.testing.expect(findFlagByShort(flags, 'x') == null);
}

test "isKnownFlag long flag" {
    const cmd = Command{ .name = "test", .flags = &.{
        .{ .name = "verbose", .takes_value = false, .description = "v" },
    } };
    const globals = &[_]FlagDef{
        .{ .name = "port", .short = 'p', .description = "port" },
    };
    try std.testing.expect(isKnownFlag(cmd, globals, "--verbose"));
    try std.testing.expect(isKnownFlag(cmd, globals, "--port"));
    try std.testing.expect(isKnownFlag(cmd, globals, "--help"));
    try std.testing.expect(!isKnownFlag(cmd, globals, "--unknown"));
    try std.testing.expect(!isKnownFlag(cmd, globals, "notaflag"));
}

test "isKnownFlag short flag" {
    const cmd = Command{ .name = "test" };
    const globals = &[_]FlagDef{
        .{ .name = "port", .short = 'p', .description = "port" },
    };
    try std.testing.expect(isKnownFlag(cmd, globals, "-p"));
    try std.testing.expect(isKnownFlag(cmd, globals, "-h"));
    try std.testing.expect(!isKnownFlag(cmd, globals, "-z"));
}
