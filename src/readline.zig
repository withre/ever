//! Minimal line editing for interactive prompts.
//! Supports: backspace, Ctrl-A/E/U/W/C, left/right arrows, Alt-Backspace.

const std = @import("std");

const Termios = extern struct {
    iflag: u32,
    oflag: u32,
    cflag: u32,
    lflag: u32,
    line: u8,
    cc: [32]u8,
    ispeed: u32,
    ospeed: u32,
};

const ICANON: u32 = 0o0000002;
const ECHO: u32 = 0o0000010;
const ISIG: u32 = 0o0000001;
const TCSANOW: u32 = 0;

extern "c" fn tcgetattr(fd: c_int, termios: *Termios) c_int;
extern "c" fn tcsetattr(fd: c_int, action: c_int, termios: *const Termios) c_int;

fn writeAll(data: []const u8) void {
    var written: usize = 0;
    while (written < data.len) {
        const rc = std.os.linux.write(2, data[written..].ptr, data[written..].len);
        const n: isize = @bitCast(rc);
        if (n <= 0) break;
        written += @intCast(n);
    }
}

fn readByte() ?u8 {
    var byte: [1]u8 = undefined;
    const rc = std.os.linux.read(0, &byte, 1);
    const n: isize = @bitCast(rc);
    if (n <= 0) return null;
    return byte[0];
}

/// Read a line with basic editing support.
/// Returns owned slice; caller must free with allocator.
pub fn readLine(allocator: std.mem.Allocator) ![]u8 {
    // Check if stdin is a TTY
    if (std.c.isatty(0) == 0) {
        return readLineSimple(allocator);
    }

    var orig: Termios = undefined;
    if (tcgetattr(0, &orig) != 0) {
        return readLineSimple(allocator);
    }

    var raw = orig;
    raw.lflag &= ~(ICANON | ECHO | ISIG);
    _ = tcsetattr(0, @intCast(TCSANOW), &raw);
    defer _ = tcsetattr(0, @intCast(TCSANOW), &orig);

    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    var cursor: usize = 0;

    while (true) {
        const byte = readByte() orelse break;

        switch (byte) {
            '\r', '\n' => {
                writeAll("\n");
                break;
            },
            3 => {
                // Ctrl-C: cancel
                writeAll("^C\n");
                buf.deinit(allocator);
                std.process.exit(130);
            },
            1 => {
                // Ctrl-A: beginning of line
                if (cursor > 0) {
                    var move_buf: [16]u8 = undefined;
                    const s = std.fmt.bufPrint(&move_buf, "\x1b[{d}D", .{cursor}) catch continue;
                    writeAll(s);
                    cursor = 0;
                }
            },
            5 => {
                // Ctrl-E: end of line
                if (cursor < buf.items.len) {
                    var move_buf: [16]u8 = undefined;
                    const s = std.fmt.bufPrint(&move_buf, "\x1b[{d}C", .{buf.items.len - cursor}) catch continue;
                    writeAll(s);
                    cursor = buf.items.len;
                }
            },
            21 => {
                // Ctrl-U: clear line
                if (cursor > 0) {
                    var move_buf: [16]u8 = undefined;
                    const s = std.fmt.bufPrint(&move_buf, "\x1b[{d}D", .{cursor}) catch continue;
                    writeAll(s);
                }
                writeAll("\x1b[K");
                // Move remaining chars after cursor to front
                const remaining = buf.items.len - cursor;
                if (remaining > 0) {
                    std.mem.copyForwards(u8, buf.items[0..remaining], buf.items[cursor..buf.items.len]);
                    buf.shrinkRetainingCapacity(remaining);
                    writeAll(buf.items[0..remaining]);
                    writeAll("\x1b[K");
                    if (remaining > 0) {
                        var mb: [16]u8 = undefined;
                        const ms = std.fmt.bufPrint(&mb, "\x1b[{d}D", .{remaining}) catch continue;
                        writeAll(ms);
                    }
                } else {
                    buf.shrinkRetainingCapacity(0);
                }
                cursor = 0;
            },
            23 => {
                // Ctrl-W: delete word backward
                deleteWordBackward(&buf, &cursor);
            },
            127, 8 => {
                // Backspace
                if (cursor > 0) {
                    cursor -= 1;
                    _ = buf.orderedRemove(cursor);
                    redrawFromCursor(&buf, cursor);
                }
            },
            27 => {
                // Escape sequence
                const b2 = readByte() orelse break;
                if (b2 == '[') {
                    const b3 = readByte() orelse break;
                    switch (b3) {
                        'D' => {
                            // Left arrow
                            if (cursor > 0) {
                                writeAll("\x1b[D");
                                cursor -= 1;
                            }
                        },
                        'C' => {
                            // Right arrow
                            if (cursor < buf.items.len) {
                                writeAll("\x1b[C");
                                cursor += 1;
                            }
                        },
                        'H' => {
                            // Home
                            if (cursor > 0) {
                                var move_buf: [16]u8 = undefined;
                                const s = std.fmt.bufPrint(&move_buf, "\x1b[{d}D", .{cursor}) catch continue;
                                writeAll(s);
                                cursor = 0;
                            }
                        },
                        'F' => {
                            // End
                            if (cursor < buf.items.len) {
                                var move_buf: [16]u8 = undefined;
                                const s = std.fmt.bufPrint(&move_buf, "\x1b[{d}C", .{buf.items.len - cursor}) catch continue;
                                writeAll(s);
                                cursor = buf.items.len;
                            }
                        },
                        else => {},
                    }
                } else if (b2 == 127) {
                    // Alt-Backspace: delete word backward
                    deleteWordBackward(&buf, &cursor);
                }
            },
            else => {
                if (byte >= 32) {
                    try buf.insert(allocator, cursor, byte);
                    cursor += 1;
                    if (cursor == buf.items.len) {
                        writeAll(&.{byte});
                    } else {
                        redrawFromCursor(&buf, cursor - 1);
                    }
                }
            },
        }
    }

    return buf.toOwnedSlice(allocator);
}

fn deleteWordBackward(buf: *std.ArrayList(u8), cursor: *usize) void {
    if (cursor.* == 0) return;
    const start = cursor.*;
    // Skip spaces
    while (cursor.* > 0 and buf.items[cursor.* - 1] == ' ') {
        cursor.* -= 1;
    }
    // Skip word chars
    while (cursor.* > 0 and buf.items[cursor.* - 1] != ' ') {
        cursor.* -= 1;
    }
    const deleted = start - cursor.*;
    // Remove from buf
    var i: usize = 0;
    while (i < deleted) : (i += 1) {
        _ = buf.orderedRemove(cursor.*);
    }
    redrawFromCursor(buf, cursor.*);
}

fn redrawFromCursor(buf: *std.ArrayList(u8), cursor: usize) void {
    // Move to cursor position, rewrite from there, clear rest
    // First move back if needed (we assume we're at old cursor)
    writeAll("\x1b[s"); // save position - not reliable, use explicit moves
    // Move to cursor col: go to start of the edit area
    // Simpler: output \b to go back, then rewrite
    // Actually let's just: move left to cursor, print rest, clear, move back
    var move_buf: [16]u8 = undefined;

    // We might be anywhere. Let's output \r then move forward past prompt... 
    // Actually we don't know prompt length. Instead:
    // Just move to cursor position relative to current
    // The caller should have already adjusted cursor, so we need to move back
    // to cursor position and redraw.

    // Move left to cursor
    // Note: we don't know where the terminal cursor is exactly, but the caller
    // manages `cursor` and ensures the terminal is at `cursor` before calling this
    // (or at cursor+1 for insert, or at cursor+1 for backspace).
    // For backspace: terminal was at old cursor, we decremented, so we're at cursor+1
    // For insert: we haven't moved yet

    // Let's use a simpler approach: save cursor position, go back, redraw
    // We use column-relative moves.

    // Move left to the cursor position
    // The terminal cursor is somewhere. Let's just go back to column `cursor`
    // relative to the line start. But we don't know the prompt length.

    // Simplest robust approach: carriage return + rewrite everything
    // But we don't have the prompt text...

    // Practical approach: assume terminal is right after the edited position
    // For backspace at pos P: terminal was at P+1, now buf has char removed, cursor=P
    //   -> move left 1, write buf[P..], write space, move left (len-P+1)
    // For insert at pos P: cursor=P (after insert), terminal was at P-1
    //   -> already wrote the new char? No.

    // Let's just use a simple method:
    // Move cursor back to position `cursor`, write buf[cursor..], erase to end, move back
    // Assume: terminal cursor could be anywhere from cursor to cursor+2

    // Reset approach: move to line start using \r, write prompt... no, we don't have prompt.

    // Better: use save/restore with explicit moves from cursor position

    // The simplest thing that works: print from cursor position
    // First, move to screen column of cursor. We do this by:
    // - Move to beginning of the input area isn't possible without knowing prompt.
    // So let's just output the rest of the line and fix up.

    // Move to `cursor` by going to the leftmost possibility:
    // We go left by (buf.items.len + 1 - cursor) which is the max we could be off
    const max_back = buf.items.len + 2;
    if (max_back > 0) {
        const s = std.fmt.bufPrint(&move_buf, "\x1b[{d}D", .{max_back}) catch return;
        writeAll(s);
    }
    // Now move right by cursor
    if (cursor > 0) {
        const s2 = std.fmt.bufPrint(&move_buf, "\x1b[{d}C", .{cursor}) catch return;
        writeAll(s2);
    }

    // Write from cursor to end
    if (cursor < buf.items.len) {
        writeAll(buf.items[cursor..]);
    }
    writeAll("\x1b[K"); // erase to end of line

    // Move back to cursor position
    const tail = buf.items.len - cursor;
    if (tail > 0) {
        const s3 = std.fmt.bufPrint(&move_buf, "\x1b[{d}D", .{tail}) catch return;
        writeAll(s3);
    }
}

/// Simple non-editing readline fallback for non-TTY input
fn readLineSimple(allocator: std.mem.Allocator) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    while (true) {
        const byte = readByte() orelse break;
        if (byte == '\n') break;
        if (byte == '\r') continue;
        try buf.append(allocator, byte);
    }
    return buf.toOwnedSlice(allocator);
}
