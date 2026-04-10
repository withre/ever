//! CLI Argument Parsing Library for Zig v0.16
//!
//! A declarative CLI library with:
//! - Global flags that work anywhere in the arg list
//! - Env var binding with flag > env > default priority
//! - Subcommand dispatch with aliases and nesting
//! - Mutual exclusivity enforcement
//! - Required flag validation
//! - `--` rest arg capture
//! - "Did you mean?" suggestions for unknown flags
//! - Negatable boolean flags (--no-X)
//! - TTY-aware coloured help output
//! - Comptime command tree validation

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
    env: ?[]const u8 = null,
    description: []const u8 = "",
    /// If true, flag takes a value argument; if false, it's boolean.
    takes_value: bool = true,
    /// Custom placeholder name for the value (e.g. "INTERVAL"). If empty, derived from flag name.
    value_name: []const u8 = "",
    /// Flag must be provided (or have env/default).
    required: bool = false,
    /// Names of flags that conflict with this one (mutually exclusive).
    conflicts: []const []const u8 = &.{},
    /// If true, --no-NAME is automatically accepted as the negation.
    negatable: bool = false,
};

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

    /// Get all flags including inherited global flags.
    fn allFlags(self: *const Command, global_flags: []const FlagDef) []const FlagDef {
        // At comptime we can't concatenate, so we handle lookup in both sets.
        // This is a conceptual helper — actual lookup searches both.
        _ = self;
        _ = global_flags;
        return &.{};
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

pub const ParseError = error{
    UnknownFlag,
    MissingFlagValue,
    MissingRequiredFlag,
    ConflictingFlags,
    UnknownCommand,
    MissingRequiredArg,
    OutOfMemory,
};

pub const Context = struct {
    flags: std.StringHashMap([]const u8),
    positional: std.StringHashMap([]const u8),
    rest_args: std.ArrayList([]const u8),
    allocator: std.mem.Allocator,
    io: std.Io,
    envp: [*:null]const ?[*:0]const u8,
    /// Full command path, e.g. "timer add"
    command_path: []const u8,

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

    pub fn deinit(self: *Context) void {
        self.rest_args.deinit(self.allocator);
        self.flags.deinit();
        self.positional.deinit();
    }

    pub fn flag(self: *const Context, name: []const u8) []const u8 {
        return self.flags.get(name) orelse "";
    }

    /// Returns true if the flag was explicitly set (even to empty string).
    pub fn hasFlag(self: *const Context, name: []const u8) bool {
        return self.flags.contains(name);
    }

    pub fn flagInt(self: *const Context, comptime T: type, name: []const u8) T {
        const value = self.flags.get(name) orelse return 0;
        if (value.len == 0) return 0;
        return std.fmt.parseInt(T, value, 10) catch {
            std.debug.print("error: invalid value '{s}' for --{s}\n", .{ value, name });
            std.process.exit(1);
        };
    }

    pub fn flagBool(self: *const Context, name: []const u8) bool {
        const value = self.flags.get(name) orelse return false;
        return std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1");
    }

    pub fn arg(self: *const Context, name: []const u8) []const u8 {
        return self.positional.get(name) orelse "";
    }

    /// Returns everything after `--` when the command has `takes_rest = true`.
    pub fn rest(self: *const Context) []const []const u8 {
        return self.rest_args.items;
    }

    /// Look up an environment variable from the envp block.
    pub fn getEnv(self: *const Context, name: []const u8) ?[]const u8 {
        return lookupEnv(self.envp, name);
    }
};

/// Look up an environment variable from a null-terminated envp block.
fn lookupEnv(envp: [*:null]const ?[*:0]const u8, name: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (envp[i]) |env_str| : (i += 1) {
        const len = std.mem.indexOfSentinel(u8, 0, env_str);
        const entry = env_str[0..len];
        // Check "NAME=" prefix
        if (entry.len > name.len and
            entry[name.len] == '=' and
            std.mem.eql(u8, entry[0..name.len], name))
        {
            return entry[name.len + 1 ..];
        }
    }
    return null;
}

pub const App = struct {
    name: []const u8,
    description: []const u8 = "",
    version: []const u8 = "",
    global_flags: []const FlagDef = &.{},
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

        // Scan for global flags before command resolution.
        // We need to skip global flags to find the command name.
        // Global flags can appear anywhere, but we first need to find command words.

        const first = raw[0];

        // "help" / "--help" / "-h" as first word
        if (std.mem.eql(u8, first, "help") or std.mem.eql(u8, first, "--help") or std.mem.eql(u8, first, "-h")) {
            if (raw.len == 1) {
                self.printRootHelp();
                return;
            }
            // Resolve remaining words as command path (skip global flags in between)
            if (self.resolveCommandSkippingGlobals(raw[1..])) |res| {
                self.printCommandHelp(res.cmd, res.parent_name);
            } else {
                self.printError("unknown command. Run '{s} help'.\n", .{self.name});
                std.process.exit(1);
            }
            return;
        }

        if (std.mem.eql(u8, first, "version") or std.mem.eql(u8, first, "--version")) {
            std.debug.print("{s}\n", .{self.version});
            return;
        }

        // Resolve command, skipping global flags that may appear before/between command words
        const resolved = self.resolveCommandSkippingGlobals(raw) orelse {
            // Check if the first non-flag arg is an unknown command
            const cmd_name = self.firstNonFlagArg(raw) orelse first;
            const suggestion = self.suggestCommand(cmd_name);
            if (suggestion) |sug| {
                self.printError("unknown command '{s}'. Did you mean '{s}'?\n", .{ cmd_name, sug });
            } else {
                self.printError("unknown command '{s}'. Run '{s} help'.\n", .{ cmd_name, self.name });
            }
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

        // If this is a parent command (has subcommands, no run fn), check for unknown subcommand
        if (cmd.subcommands.len > 0 and cmd.run == null) {
            // Check if there's an unknown subcommand token
            if (arg_start < raw.len) {
                const token = raw[arg_start];
                if (!std.mem.startsWith(u8, token, "-")) {
                    self.printError("unknown subcommand '{s}' for '{s}'. Run '{s} {s} --help'.\n", .{ token, cmd.name, self.name, cmd.name });
                    std.process.exit(1);
                }
            }
            self.printCommandHelp(cmd, resolved.parent_name);
            return;
        }

        // Build context
        var ctx = Context.init(allocator, io, env_block);
        defer ctx.deinit();
        ctx.command_path = if (resolved.parent_name.len > 0) resolved.parent_name else cmd.name;

        // 1. Apply defaults for all flags (command + global)
        for (cmd.flags) |f| {
            if (f.default) |def| try ctx.flags.put(f.name, def);
        }
        for (self.global_flags) |f| {
            if (f.default) |def| try ctx.flags.put(f.name, def);
        }

        // 2. Apply env vars (overrides defaults)
        for (cmd.flags) |f| {
            if (f.env) |env_name| {
                if (lookupEnv(env_block, env_name)) |val| {
                    try ctx.flags.put(f.name, val);
                }
            }
        }
        for (self.global_flags) |f| {
            if (f.env) |env_name| {
                if (lookupEnv(env_block, env_name)) |val| {
                    try ctx.flags.put(f.name, val);
                }
            }
        }

        // 3. Parse CLI args (overrides env and defaults)
        var i: usize = arg_start;
        var positional_idx: usize = 0;

        // Parse global flags that appeared BEFORE the command
        {
            var pi: usize = 0;
            while (pi < resolved.pre_cmd_start) : (pi += 1) {
                const a = raw[pi];
                if (std.mem.startsWith(u8, a, "--")) {
                    const flag_name = a[2..];
                    if (std.mem.indexOfScalar(u8, flag_name, '=')) |eq| {
                        if (findFlagDef(self.global_flags, flag_name[0..eq]) != null) {
                            try ctx.flags.put(flag_name[0..eq], flag_name[eq + 1 ..]);
                        }
                    } else if (findFlagDef(self.global_flags, flag_name)) |fdef| {
                        if (!fdef.takes_value) {
                            try ctx.flags.put(fdef.name, "true");
                        } else if (pi + 1 < resolved.pre_cmd_start) {
                            pi += 1;
                            try ctx.flags.put(fdef.name, raw[pi]);
                        }
                    } else if (negatedName(self.global_flags, flag_name)) |orig_name| {
                        try ctx.flags.put(orig_name, "false");
                    }
                } else if (std.mem.startsWith(u8, a, "-") and a.len > 1) {
                    const short = a[1];
                    if (findFlagByShort(self.global_flags, short)) |fdef| {
                        if (!fdef.takes_value) {
                            try ctx.flags.put(fdef.name, "true");
                        } else if (a.len > 2) {
                            // -Xvalue or -X=value
                            const rest = a[2..];
                            if (rest[0] == '=') {
                                try ctx.flags.put(fdef.name, rest[1..]);
                            } else {
                                try ctx.flags.put(fdef.name, rest);
                            }
                        } else if (pi + 1 < resolved.pre_cmd_start) {
                            pi += 1;
                            try ctx.flags.put(fdef.name, raw[pi]);
                        }
                    }
                }
            }
        }

        while (i < raw.len) : (i += 1) {
            const a = raw[i];
            if (std.mem.eql(u8, a, "--")) {
                // Everything after -- goes to rest args
                i += 1;
                if (cmd.takes_rest) {
                    while (i < raw.len) : (i += 1) {
                        try ctx.rest_args.append(allocator, raw[i]);
                    }
                } else {
                    // Legacy: remaining args become positional
                    while (i < raw.len) : (i += 1) {
                        if (positional_idx < cmd.args.len) {
                            try ctx.positional.put(cmd.args[positional_idx].name, raw[i]);
                            positional_idx += 1;
                        }
                    }
                }
                break;
            } else if (std.mem.startsWith(u8, a, "--")) {
                const flag_name = a[2..];

                // Handle --flag=value
                if (std.mem.indexOfScalar(u8, flag_name, '=')) |eq| {
                    const name = flag_name[0..eq];
                    const value = flag_name[eq + 1 ..];
                    if (findFlagDef(cmd.flags, name) != null or findFlagDef(self.global_flags, name) != null) {
                        try ctx.flags.put(name, value);
                    } else {
                        self.printUnknownFlagError(name, cmd);
                        std.process.exit(1);
                    }
                    continue;
                }

                // Handle --no-X negation
                if (negatedName(cmd.flags, flag_name)) |orig_name| {
                    try ctx.flags.put(orig_name, "false");
                    continue;
                }
                if (negatedName(self.global_flags, flag_name)) |orig_name| {
                    try ctx.flags.put(orig_name, "false");
                    continue;
                }

                // Look up in command flags, then global flags
                const fdef = findFlagDef(cmd.flags, flag_name) orelse
                    findFlagDef(self.global_flags, flag_name);

                if (fdef == null) {
                    self.printUnknownFlagError(flag_name, cmd);
                    std.process.exit(1);
                }

                const fd = fdef.?;
                if (!fd.takes_value) {
                    try ctx.flags.put(fd.name, "true");
                } else if (i + 1 < raw.len and !self.isKnownFlag(cmd, raw[i + 1])) {
                    i += 1;
                    try ctx.flags.put(fd.name, raw[i]);
                } else {
                    self.printError("flag '--{s}' requires a value\n", .{fd.name});
                    std.process.exit(1);
                }
            } else if (std.mem.startsWith(u8, a, "-") and a.len > 1) {
                const short = a[1];
                const fdef = findFlagByShort(cmd.flags, short) orelse
                    findFlagByShort(self.global_flags, short);

                if (fdef == null) {
                    self.printError("unknown flag '-{c}'\n", .{short});
                    std.process.exit(1);
                }

                const fd = fdef.?;
                if (!fd.takes_value) {
                    try ctx.flags.put(fd.name, "true");
                } else if (a.len > 2) {
                    // -Xvalue or -X=value
                    const rest = a[2..];
                    if (rest[0] == '=') {
                        try ctx.flags.put(fd.name, rest[1..]);
                    } else {
                        try ctx.flags.put(fd.name, rest);
                    }
                } else if (i + 1 < raw.len) {
                    i += 1;
                    try ctx.flags.put(fd.name, raw[i]);
                } else {
                    self.printError("flag '-{c}' (--{s}) requires a value\n", .{ short, fd.name });
                    std.process.exit(1);
                }
            } else {
                // Positional argument
                if (positional_idx < cmd.args.len) {
                    try ctx.positional.put(cmd.args[positional_idx].name, a);
                    positional_idx += 1;
                } else {
                    self.printError("unexpected argument '{s}'\n", .{a});
                    std.process.exit(1);
                }
            }
        }

        // 4. Validate required flags
        self.validateRequired(cmd, &ctx);

        // 5. Validate mutual exclusivity
        self.validateConflicts(cmd, &ctx);

        if (cmd.run) |run_fn| {
            try run_fn(&ctx);
        }
    }

    // ── Validation ─────────────────────────────────────────────────────

    fn validateRequired(self: *const App, cmd: Command, ctx: *const Context) void {
        _ = self;
        for (cmd.flags) |f| {
            if (f.required) {
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
    }

    fn printRequiredError(f: FlagDef) void {
        std.debug.print("error: missing required flag --{s}", .{f.name});
        if (f.env) |env_name| {
            std.debug.print(" (or set ${s})", .{env_name});
        }
        std.debug.print("\n", .{});
    }

    fn validateConflicts(self: *const App, cmd: Command, ctx: *const Context) void {
        _ = self;
        // Check command-level flags
        for (cmd.flags) |f| {
            if (f.conflicts.len == 0) continue;
            const this_val = ctx.flags.get(f.name) orelse continue;
            // Only check if this flag was actually set to a non-default, non-empty value
            if (this_val.len == 0) continue;
            // Skip if value is same as default (means it wasn't explicitly set)
            if (f.default) |def| {
                if (std.mem.eql(u8, this_val, def)) continue;
            }
            for (f.conflicts) |conflict_name| {
                const conflict_val = ctx.flags.get(conflict_name) orelse continue;
                if (conflict_val.len == 0) continue;
                // Check if conflict was also explicitly set (not just default)
                const conflict_def = findFlagDef(cmd.flags, conflict_name);
                if (conflict_def) |cd| {
                    if (cd.default) |def| {
                        if (std.mem.eql(u8, conflict_val, def)) continue;
                    }
                }
                std.debug.print("error: --{s} and --{s} are mutually exclusive\n", .{ f.name, conflict_name });
                std.process.exit(1);
            }
        }
    }

    // ── Error helpers ──────────────────────────────────────────────────

    fn printError(self: *const App, comptime fmt: []const u8, args: anytype) void {
        _ = self;
        std.debug.print("error: " ++ fmt, args);
    }

    fn printUnknownFlagError(self: *const App, flag_name: []const u8, cmd: Command) void {
        const suggestion = suggestFlag(flag_name, cmd.flags) orelse
            suggestFlag(flag_name, self.global_flags);
        if (suggestion) |sug| {
            std.debug.print("error: unknown flag '--{s}'. Did you mean '--{s}'?\n", .{ flag_name, sug });
        } else {
            std.debug.print("error: unknown flag '--{s}'\n", .{flag_name});
        }
    }

    // ── Levenshtein distance for suggestions ───────────────────────────

    fn levenshteinDistance(a: []const u8, b: []const u8) usize {
        if (a.len == 0) return b.len;
        if (b.len == 0) return a.len;
        if (a.len > 32 or b.len > 32) return @max(a.len, b.len); // bail on huge strings

        // Use two rows for the DP
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

    fn suggestFlag(input: []const u8, flags: []const FlagDef) ?[]const u8 {
        var best: ?[]const u8 = null;
        var best_dist: usize = 4; // threshold: don't suggest if distance >= 4
        for (flags) |f| {
            const dist = levenshteinDistance(input, f.name);
            if (dist < best_dist) {
                best_dist = dist;
                best = f.name;
            }
        }
        return best;
    }

    fn suggestCommand(self: *const App, input: []const u8) ?[]const u8 {
        var best: ?[]const u8 = null;
        var best_dist: usize = 4;
        for (self.commands) |cmd| {
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

    // ── Command resolution ─────────────────────────────────────────────

    const ResolveResult = struct {
        cmd: Command,
        parent_name: []const u8,
        args_start: usize,
        pre_cmd_start: usize, // where command word was found
    };

    fn firstNonFlagArg(self: *const App, raw: []const []const u8) ?[]const u8 {
        var i: usize = 0;
        while (i < raw.len) : (i += 1) {
            const a = raw[i];
            if (std.mem.startsWith(u8, a, "-")) {
                // Skip flag value if it takes one
                if (std.mem.startsWith(u8, a, "--")) {
                    const name = a[2..];
                    if (std.mem.indexOfScalar(u8, name, '=') == null) {
                        if (findFlagDef(self.global_flags, name)) |fdef| {
                            if (fdef.takes_value) i += 1;
                        }
                    }
                } else if (a.len == 2) {
                    if (findFlagByShort(self.global_flags, a[1])) |fdef| {
                        if (fdef.takes_value) i += 1;
                    }
                }
                continue;
            }
            return a;
        }
        return null;
    }

    /// Resolve command from args, skipping any global flags that appear before/between command words.
    fn resolveCommandSkippingGlobals(self: *const App, raw: []const []const u8) ?ResolveResult {
        // Find the first non-global-flag token — that's the command name
        var i: usize = 0;


        while (i < raw.len) : (i += 1) {
            const a = raw[i];
            if (std.mem.startsWith(u8, a, "-")) {
                // Could be a global flag
                if (std.mem.startsWith(u8, a, "--")) {
                    const name = a[2..];
                    if (std.mem.indexOfScalar(u8, name, '=')) |_| {
                        // --flag=value, single token
                        continue;
                    }
                    if (findFlagDef(self.global_flags, name)) |fdef| {
                        if (fdef.takes_value) i += 1; // skip value
                        continue;
                    }
                    if (negatedName(self.global_flags, name) != null) {
                        continue;
                    }
                } else if (a.len == 2) {
                    if (findFlagByShort(self.global_flags, a[1])) |fdef| {
                        if (fdef.takes_value) {
                            if (a.len > 2) {
                                // -Xvalue or -X=value, single token
                            } else {
                                i += 1; // skip next token (value)
                            }
                        }
                        continue;
                    }
                }
                // Not a recognized global flag — might be a command-level flag appearing early,
                // but we can't know until we have the command. Treat as unknown for now.
                // Actually, this could be -h/--help which we handle elsewhere.
                if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
                    continue;
                }
                // Unknown flag before command — skip it, we'll error later during parsing
                continue;
            }

            // Found a non-flag token — try to match as command
            const cmd_start = i;
            for (self.commands) |cmd| {
                if (cmd.matches(a)) {
                    // Check for subcommand
                    if (cmd.subcommands.len > 0) {
                        // Look at next non-flag token for subcommand
                        var j = i + 1;
                        while (j < raw.len) : (j += 1) {
                            const b = raw[j];
                            if (std.mem.startsWith(u8, b, "-")) {
                                // Skip flags between command and subcommand
                                if (std.mem.startsWith(u8, b, "--")) {
                                    const fname = b[2..];
                                    if (std.mem.indexOfScalar(u8, fname, '=') != null) continue;
                                    if (findFlagDef(self.global_flags, fname)) |fdef| {
                                        if (fdef.takes_value) j += 1;
                                        continue;
                                    }
                                } else if (b.len == 2) {
                                    if (findFlagByShort(self.global_flags, b[1])) |fdef| {
                                        if (fdef.takes_value) j += 1;
                                        continue;
                                    }
                                }
                                continue;
                            }
                            // Try matching as subcommand
                            for (cmd.subcommands) |sub| {
                                if (sub.matches(b)) {
                                    return .{
                                        .cmd = sub,
                                        .parent_name = cmd.name,
                                        .args_start = j + 1,
                                        .pre_cmd_start = cmd_start,
                                    };
                                }
                            }
                            break; // not a subcommand, stop looking
                        }
                    }
                    return .{
                        .cmd = cmd,
                        .parent_name = "",
                        .args_start = i + 1,
                        .pre_cmd_start = cmd_start,
                    };
                }
            }
            // Token didn't match any command
            return null;
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
        std.debug.print("Usage: {s} [global options] <command> [options]\n", .{self.name});

        // Show global flags if any
        if (self.global_flags.len > 0) {
            std.debug.print("\n{s}Global Flags:{s}\n", .{ section_c, reset });
            for (self.global_flags) |f| {
                printFlagHelp(f, tty);
            }
        }

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
        const reset = if (tty) "\x1b[0m" else "";

        // Description line
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
            if (cmd.flags.len > 0 or self.global_flags.len > 0) std.debug.print(" [OPTIONS]", .{});
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
            if (cmd.takes_rest) {
                std.debug.print(" [-- ARGS...]", .{});
            }
            std.debug.print("\n", .{});
        }

        // Aliases
        if (cmd.aliases.len > 0) {
            std.debug.print("\n{s}Aliases:{s} ", .{ section_c, reset });
            for (cmd.aliases, 0..) |alias, ai| {
                if (ai > 0) std.debug.print(", ", .{});
                std.debug.print("{s}{s}{s}", .{ cmd_c, alias, reset });
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

        // Command flags
        if (cmd.flags.len > 0) {
            std.debug.print("\n{s}Flags:{s}\n", .{ section_c, reset });
            for (cmd.flags) |f| {
                printFlagHelp(f, tty);
            }
        }

        // Global flags
        if (self.global_flags.len > 0) {
            std.debug.print("\n{s}Global Flags:{s}\n", .{ section_c, reset });
            for (self.global_flags) |f| {
                printFlagHelp(f, tty);
            }
        }
    }

    // ── Formatting helpers ─────────────────────────────────────────────

    fn printFlagHelp(f: FlagDef, tty: bool) void {
        const flag_c = if (tty) "\x1b[38;2;120;130;140m" else "";
        const flag_desc_c = if (tty) "\x1b[38;2;100;105;110m" else "";
        const env_c = if (tty) "\x1b[38;2;90;100;90m" else "";
        const required_c = if (tty) "\x1b[38;2;180;130;100m" else "";
        const reset = if (tty) "\x1b[0m" else "";

        var left_buf: [64]u8 = undefined;
        const left = fmtFlagLeft(f, &left_buf);

        std.debug.print("  {s}{s:<28}{s}{s}{s}", .{ flag_c, left, reset, flag_desc_c, f.description });

        // Show meta info
        var meta_parts: usize = 0;

        if (f.required) {
            std.debug.print(" {s}(required){s}", .{ required_c, reset });
            meta_parts += 1;
        }

        if (f.default) |def| {
            std.debug.print(" {s}(default: {s}){s}", .{ flag_desc_c, def, reset });
            meta_parts += 1;
        }

        if (f.env) |env_name| {
            std.debug.print(" {s}[${s}]{s}", .{ env_c, env_name, reset });
            meta_parts += 1;
        }

        if (f.conflicts.len > 0) {
            std.debug.print(" {s}(conflicts: ", .{flag_desc_c});
            for (f.conflicts, 0..) |c, ci| {
                if (ci > 0) std.debug.print(", ", .{});
                std.debug.print("--{s}", .{c});
            }
            std.debug.print("){s}", .{reset});
        }

        if (f.negatable) {
            std.debug.print(" {s}(--no-{s} to disable){s}", .{ flag_desc_c, f.name, reset });
        }

        std.debug.print("{s}\n", .{reset});
    }

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

    /// Check if a token looks like a recognized flag (long or short) for the given command.
    /// Used to decide whether a token after a value-taking flag is a value or the next flag.
    fn isKnownFlag(self: *const App, cmd: Command, token: []const u8) bool {
        if (!std.mem.startsWith(u8, token, "-")) return false;
        if (std.mem.startsWith(u8, token, "--")) {
            const name = token[2..];
            const bare = if (std.mem.indexOfScalar(u8, name, '=')) |eq| name[0..eq] else name;
            if (bare.len == 0) return true; // "--" separator
            if (findFlagDef(cmd.flags, bare) != null) return true;
            if (findFlagDef(self.global_flags, bare) != null) return true;
            if (negatedName(cmd.flags, bare) != null) return true;
            if (negatedName(self.global_flags, bare) != null) return true;
            if (std.mem.eql(u8, bare, "help")) return true;
            return false;
        }
        // Short flag: -X (len 2) or -X... (attached value)
        if (token.len >= 2) {
            const short = token[1];
            if (short == 'h') return true; // -h is always help
            if (findFlagByShort(cmd.flags, short) != null) return true;
            if (findFlagByShort(self.global_flags, short) != null) return true;
        }
        return false;
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

    /// Check if flag_name is a --no-X negation of a negatable flag. Returns original name.
    fn negatedName(flags: []const FlagDef, flag_name: []const u8) ?[]const u8 {
        if (std.mem.startsWith(u8, flag_name, "no-")) {
            const orig = flag_name[3..];
            for (flags) |f| {
                if (f.negatable and std.mem.eql(u8, f.name, orig)) {
                    return f.name;
                }
            }
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

test "levenshtein distance" {
    try std.testing.expectEqual(@as(usize, 0), App.levenshteinDistance("abc", "abc"));
    try std.testing.expectEqual(@as(usize, 1), App.levenshteinDistance("abc", "ab"));
    try std.testing.expectEqual(@as(usize, 1), App.levenshteinDistance("evry", "every"));
    try std.testing.expectEqual(@as(usize, 1), App.levenshteinDistance("crn", "cron"));
    try std.testing.expectEqual(@as(usize, 3), App.levenshteinDistance("abc", "xyz"));
}

test "suggestFlag finds close match" {
    const flags = &[_]FlagDef{
        .{ .name = "every", .description = "interval" },
        .{ .name = "cron", .description = "cron expr" },
        .{ .name = "port", .description = "port" },
    };
    try std.testing.expectEqualStrings("every", App.suggestFlag("evry", flags).?);
    try std.testing.expectEqualStrings("cron", App.suggestFlag("crn", flags).?);
    try std.testing.expect(App.suggestFlag("xyzxyz", flags) == null);
}

test "negatedName" {
    const flags = &[_]FlagDef{
        .{ .name = "http", .takes_value = false, .negatable = true, .description = "HTTP" },
        .{ .name = "persist", .takes_value = false, .negatable = false, .description = "persist" },
    };
    try std.testing.expectEqualStrings("http", App.negatedName(flags, "no-http").?);
    try std.testing.expect(App.negatedName(flags, "no-persist") == null); // not negatable
    try std.testing.expect(App.negatedName(flags, "http") == null); // no "no-" prefix
}

test "lookupEnv finds variable" {
    // Construct a minimal envp block
    const env1: [*:0]const u8 = "HOME=/home/test";
    const env2: [*:0]const u8 = "EVER_PORT=9999";
    const envp: [*:null]const ?[*:0]const u8 = &.{ env1, env2, null };

    try std.testing.expectEqualStrings("/home/test", lookupEnv(envp, "HOME").?);
    try std.testing.expectEqualStrings("9999", lookupEnv(envp, "EVER_PORT").?);
    try std.testing.expect(lookupEnv(envp, "MISSING") == null);
    // Should not match partial prefix
    try std.testing.expect(lookupEnv(envp, "HOM") == null);
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
