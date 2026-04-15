//! Named ANSI colour constants and TTY detection.
//!
//! All escape sequences live here — no other module should contain raw
//! `\x1b[…` literals. Colours are returned as empty strings when the
//! output is not a terminal, giving automatic no-colour fallback.

const std = @import("std");

// ── Raw escape sequences (private) ────────────────────────────────────

const esc_title = "\x1b[38;2;140;170;210m"; // rgb(140,170,210) — soft steel blue
const esc_section = "\x1b[38;2;150;165;100m\x1b[1m"; // rgb(150,165,100) bold — muted sage green
const esc_cmd = "\x1b[38;2;130;155;170m"; // rgb(130,155,170) — dusty teal
const esc_desc = "\x1b[38;2;130;135;140m"; // rgb(130,135,140) — warm grey
const esc_flag = "\x1b[38;2;120;130;140m"; // rgb(120,130,140) — slate grey
const esc_flag_desc = "\x1b[38;2;100;105;110m"; // rgb(100,105,110) — dim charcoal
const esc_env = "\x1b[38;2;90;100;90m"; // rgb(90,100,90) — dark olive
const esc_required = "\x1b[38;2;180;130;100m"; // rgb(180,130,100) — muted terracotta
const esc_reset = "\x1b[0m"; // reset all attributes

/// A resolved set of colour strings — either real escapes or empty.
pub const Palette = struct {
    title: []const u8,
    section: []const u8,
    cmd: []const u8,
    desc: []const u8,
    flag: []const u8,
    flag_desc: []const u8,
    env: []const u8,
    required: []const u8,
    reset: []const u8,
};

/// Palette with all escapes enabled.
const palette_colour = Palette{
    .title = esc_title,
    .section = esc_section,
    .cmd = esc_cmd,
    .desc = esc_desc,
    .flag = esc_flag,
    .flag_desc = esc_flag_desc,
    .env = esc_env,
    .required = esc_required,
    .reset = esc_reset,
};

/// Palette with every field set to "".
const palette_plain = Palette{
    .title = "",
    .section = "",
    .cmd = "",
    .desc = "",
    .flag = "",
    .flag_desc = "",
    .env = "",
    .required = "",
    .reset = "",
};

/// Detect whether stderr is a TTY.
pub fn isTty() bool {
    return std.c.isatty(std.posix.STDERR_FILENO) != 0;
}

/// Return the appropriate palette for the current output.
pub fn detect() Palette {
    return if (isTty()) palette_colour else palette_plain;
}

// ── Tests ──────────────────────────────────────────────────────────────

test "plain palette has empty strings" {
    const p = palette_plain;
    try std.testing.expectEqual(@as(usize, 0), p.title.len);
    try std.testing.expectEqual(@as(usize, 0), p.reset.len);
    try std.testing.expectEqual(@as(usize, 0), p.section.len);
}

test "colour palette has non-empty strings" {
    const p = palette_colour;
    try std.testing.expect(p.title.len > 0);
    try std.testing.expect(p.reset.len > 0);
    try std.testing.expect(p.section.len > 0);
}
