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
//!
//! ## Module layout
//!
//! ```
//! cli.zig          ← this file (public re-exports)
//! cli/types.zig    ← App, Command, FlagDef, ArgDef, Context, etc.
//! cli/parse.zig    ← argument parsing and command dispatch
//! cli/help.zig     ← help output formatting
//! cli/colour.zig   ← named colour constants, TTY detection
//! cli/validate.zig ← Levenshtein, suggestions, flag lookup, validation
//! ```

pub const App = @import("cli/types.zig").App;
pub const Command = @import("cli/types.zig").Command;
pub const FlagDef = @import("cli/types.zig").FlagDef;
pub const ArgDef = @import("cli/types.zig").ArgDef;
pub const HelpSection = @import("cli/types.zig").HelpSection;
pub const HelpEntry = @import("cli/types.zig").HelpEntry;
pub const Context = @import("cli/types.zig").Context;
pub const ParseError = @import("cli/types.zig").ParseError;

// Pull in all sub-module tests when `zig build test` runs on this file.
comptime {
    _ = @import("cli/types.zig");
    _ = @import("cli/parse.zig");
    _ = @import("cli/help.zig");
    _ = @import("cli/colour.zig");
    _ = @import("cli/validate.zig");
}
