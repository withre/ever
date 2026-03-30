# Chronoa Feedback from Ever Development

## Implemented ✓

All high-priority feedback has been addressed:

1. ✅ **Auto-Enter on send** — `chronoa send agent "text"` now sends Enter automatically. Use `--raw` to suppress.
2. ✅ **`--file` for send** — `chronoa send agent --file /tmp/instructions.md` reads and sends file contents.
3. ✅ **`--label` on new** — `chronoa new --daemon --label "implementing hooks" worker -- pi ...`

## Still desired

4. `chronoa output --follow` — stream output like `tail -f` without full attach
