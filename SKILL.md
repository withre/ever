---
name: ever
description: Event tracking and inter-agent communication via Ever event storage. Use when you need to report progress, signal other agents, track task state, subscribe to events, schedule recurring work, or trigger commands from events.
compatibility: Requires ever binary (zig build or nix build)
metadata:
  version: "0.1"
  author: Re
---

# Ever — Event Tracking & Inter-Agent Communication

Ever is a lightweight event storage system. Agents use it to publish events about their work and subscribe to events from other agents. Any process that can run shell commands can participate in Ever-based coordination.

## Prerequisites

1. **ever** — the event store binary
2. A running Ever store: `ever start` (default TCP port 7890, HTTP port 8890)

## Core Concepts

- **Topics** — named event streams (e.g. `agent.build`, `task.complete`, `file.modified`)
- **Events** — JSON payloads published to topics
- **Subscriptions** — reading events by topic, prefix, or pattern
- **Hooks** — server-side persistent triggers that run commands on matching events
- **Timers** — scheduled event publishing (interval, cron, one-shot)
- Topics use `.` as separator. Naming convention: `<domain>.<action>` or `<domain>.<entity>.<action>`

## Quick Reference

```bash
# Store lifecycle
ever start                                # start (foreground, TCP :7890, HTTP :8890)
ever start --data-dir ./mydata            # custom data directory
ever start -p 8000                        # custom TCP port
ever start --http-port 9000               # custom HTTP API port
ever start --no-http                      # disable HTTP API
ever status                               # show store stats (topics, events, disk)
ever status --json                        # machine-readable output
ever status --data-dir ./mydata           # check a specific data dir
ever version                              # print version

# Topics
ever topic create agent.tasks             # create a topic
ever topic add agent.tasks                # alias for create
ever topic list                           # list all topics
ever topic delete agent.tasks             # delete a topic
ever topic rm agent.tasks                 # alias for delete
ever topic remove agent.tasks             # alias for delete

# Publish
ever pub agent.tasks '{"status":"done"}'

# Subscribe
ever sub agent.tasks                      # exact topic
ever sub agent.                           # prefix — all agent.* topics (no quoting!)
ever sub .                                # all topics (no quoting!)
ever sub 'agent.*.complete'               # wildcard (needs shell quoting)
ever sub agent.tasks --from 5 --max 10    # offset + limit
ever sub agent.tasks --follow             # tail-f style: stream new events
ever sub agent.tasks --json-values        # output just the JSON value

# Wait (blocking)
ever wait agent.tasks                     # block until 1 event arrives
ever wait agent.tasks --count 5           # block until 5 events
ever wait agent.tasks --timeout 30        # exit 1 if no events within 30s

# Watch + run command (client-side, ephemeral)
ever on agent.build.complete -- ./deploy.sh
ever on --once task.assigned -- ./process.sh

# Hooks (server-side, persistent)
ever hook add agent.build.complete --cmd './deploy.sh'
ever hook add agent. -- ./log-agent.sh    # prefix pattern
ever hook add --once deploy.trigger -- ./deploy.sh
ever hook add --name my-deploy deploy.trigger -- ./deploy.sh
ever hook list                            # list all hooks
ever hook rm 1                            # remove by ID
ever hook rm my-deploy                    # remove by name
ever hook ps                              # show running hook processes
ever hook logs 1                          # show hook execution output
ever hook new                             # interactive creation

# Timers (scheduled event publishing)
ever timer add heartbeat --every 30s system.heartbeat
ever timer add --every 5m cron.cleanup '{"action":"gc"}'
ever timer add --in 10s deploy.trigger '{"env":"staging"}'
ever timer add --cron '*/5 * * * *' cron.check
ever timer list                           # list all timers
ever timer info heartbeat                 # show timer details
ever timer rm heartbeat                   # remove a timer
ever timer new                            # interactive creation
```

## Global Flags & Environment Variables

Global flags work **before or after** the command:

```bash
ever -p 8000 pub mytopic '{"x":1}'       # flag before command
ever pub mytopic '{"x":1}' -p 8000       # flag after command
ever -a 10.0.0.5 -p 9000 sub events.     # remote store
```

| Flag | Short | Env Var | Default | Description |
|------|-------|---------|---------|-------------|
| `--address` | `-a` | `EVER_HOST` | `127.0.0.1` | Store address |
| `--port` | `-p` | `EVER_PORT` | `7890` | Store TCP port |

Priority: **flag > env var > default**. Help output shows env binding as `[$EVER_PORT]`.

Additional env vars for `start` and `status`:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--data-dir` | `EVER_DATA_DIR` | `./data` | Data directory |
| `--http-port` | `EVER_HTTP_PORT` | `8890` | HTTP API port |

## Store Lifecycle

### `ever start`

Starts the store in the foreground. Press Ctrl-C to stop.

```bash
ever start                                    # defaults: TCP :7890, HTTP :8890, data ./data
ever start --data-dir /var/lib/ever -p 9000   # production setup
ever start --no-http                          # TCP only, no HTTP API
EVER_PORT=9000 EVER_DATA_DIR=/tmp/test ever start  # via env vars
```

The store acquires a lockfile (`ever.lock`) — only one store per data directory.

### `ever status`

Inspect a store's data directory **without connecting** to a running server:

```bash
ever status                    # default ./data directory
ever status --data-dir /var/lib/ever
ever status --json             # JSON output for scripting
```

## Subscription Patterns

The two most common patterns need NO shell quoting:

| Pattern | Meaning | Shell-safe? |
|---------|---------|-------------|
| `agent.tasks` | Exact topic | Yes |
| `agent.` | Everything under agent | Yes (trailing dot) |
| `.` | All topics | Yes |
| `agent.*.done` | Wildcard (one segment) | No (quote it) |

## Streaming & Waiting

### `ever sub --follow` — Tail-f Style Streaming

Keeps the connection open and prints new events as they arrive:

```bash
ever sub agent. --follow                      # stream all agent events
ever sub task.results --follow --json-values  # just payloads (great for piping)
```

`--follow` never exits on its own — kill with Ctrl-C or a signal.

### `ever sub --json-values` — Raw JSON Output

Strips the `[topic:offset]` wrapper and outputs just the event's JSON value:

```bash
# Normal: [task.build:0] {"status":"complete","duration_ms":1234}
# With --json-values: {"status":"complete","duration_ms":1234}
ever sub task.build --json-values | jq .status
```

### `ever wait` — Block Until Events Arrive

Exit code 0 = events arrived, exit code 1 = timeout:

```bash
ever wait task.complete                           # wait forever for 1 event
ever wait task.complete --count 3 --timeout 30    # 3 events or 30s timeout
ever wait task.complete --from 10 --timeout 60    # only count from offset 10+
ever wait task.complete --json-values             # output payloads when done

# Script synchronization
if ever wait build.done --timeout 300; then
  ./deploy.sh
else
  echo "Build timed out" >&2; exit 1
fi
```

## Event Triggers

### `ever on` — Client-Side (ephemeral, development)

Runs as a long-lived client process. Dies when you kill it.

```bash
ever on agent.build.complete -- ./deploy.sh       # exact topic
ever on agent. -- ./log-agent.sh                  # prefix match
ever on --once task.assigned -- ./process.sh      # exit after first event
```

The command receives:
- **stdin**: event JSON (`{"topic":"...","offset":42,"key":"...","value":"..."}`)
- **env vars**: `EVER_TOPIC`, `EVER_OFFSET`, `EVER_KEY`, `EVER_TIMESTAMP`

### `ever hook` — Server-Side (persistent, production)

Registered with the store, persists across restarts. No client process to keep alive.

```bash
# Register hooks
ever hook add agent.build.complete -- ./deploy.sh
ever hook add --name my-deploy deploy.trigger -- ./deploy.sh
ever hook add --once deploy.trigger -- ./deploy.sh    # fires once, auto-removes
ever hook add agent. -- ./log-agent.sh                # prefix pattern

# List hooks
ever hook list
# ID  NAME                 Pattern                  Command                        Last Offset

# Remove by ID or name
ever hook rm 1
ever hook rm my-deploy

# Monitor running hook processes
ever hook ps
# HOOK  PID     PATTERN                  COMMAND                        ELAPSED

# View hook execution logs
ever hook logs 1
# Log file: /path/to/hook-1.log
# ---
# [metadata header: hook ID, pattern, command, topic, offset, payload]
# [stdout/stderr from command]

# Interactive creation (prompts for pattern, command, one-shot)
ever hook new
```

Hook log files contain a metadata header with hook ID, pattern, command, matched topic, event offset, and payload — followed by the command's stdout/stderr.

Hooks execute via fork/exec from the store's hook daemon thread. Same stdin/env vars as `ever on`. The working directory is captured at registration time.

**`--once`**: Fires exactly once, then auto-removed. Ideal for one-time triggers.

**`--name`**: Assign a human-readable name for easy reference in `hook rm`.

### When to use which

| Aspect | `ever on` | `ever hook add` |
|--------|-----------|-----------------|
| Persistence | Dies with process | Survives restart |
| Setup | Run a command | Register via API |
| Environment | Your terminal/cwd | Store's context |
| Use case | Development, ad-hoc | Production, long-lived |

## Timers

Timers publish events on a schedule. Three schedule types:

### Interval (`--every`)

```bash
ever timer add heartbeat --every 30s system.heartbeat
ever timer add cleanup --every 1h cron.cleanup '{"action":"gc"}'
```

Duration formats: `5s`, `5m`, `2h`, `1d`

### Cron (`--cron`)

```bash
ever timer add nightly --cron '0 2 * * *' cron.backup '{"type":"full"}'
ever timer add frequent --cron '*/5 * * * *' cron.check
```

Standard 5-field cron expressions (minute hour day month weekday).

### One-Shot (`--in`)

Fires once after a delay, then removed:

```bash
ever timer add --in 10s deploy.trigger '{"env":"staging"}'
ever timer add --in 2m reminder.check
```

### Timer Management

```bash
ever timer list                   # list all timers with fire counts
# NAME                 SCHEDULE        TOPIC                     FIRES

ever timer info heartbeat         # detailed timer info
# Name:        heartbeat
# Schedule:    every 30s
# Topic:       system.heartbeat
# Payload:     {}
# Last fired:  1713234567890
# Fire count:  42
# Persistent:  yes

ever timer rm heartbeat           # remove (aliases: remove, delete)

ever timer new                    # interactive creation
# Timer name: ...
# Schedule type (every/cron/in): ...
# Interval/Cron expression/Delay: ...
# Topic: ...
# Payload [{}]: ...
# Persistent (y/n) [y]: ...
```

### Timer+Hook Pattern (Cron Replacement)

Combine timers and hooks to replace traditional cron:

```bash
# Timer publishes on schedule
ever timer add --every 5m cron.cleanup

# Hook runs command when timer fires
ever hook add cron.cleanup -- ./cleanup.sh

# Result: ./cleanup.sh runs every 5 minutes, managed entirely by Ever
```

This is more observable than cron — you can see fire history via `ever sub cron.cleanup` and hook output via `ever hook logs`.

## HTTP API

The HTTP API (default port 8890) provides REST access to the same operations. Disable with `--no-http`.

### Endpoints

```bash
# Create topic
curl -X POST http://localhost:8890/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"agent.tasks"}'

# List topics
curl http://localhost:8890/topics

# Delete topic
curl -X DELETE http://localhost:8890/topics/agent.tasks

# Publish event
curl -X POST http://localhost:8890/topics/agent.tasks/events \
  -H 'Content-Type: application/json' \
  -d '{"value":"{\"status\":\"done\"}"}'

# Fetch events (with optional offset and limit query params)
curl 'http://localhost:8890/topics/agent.tasks/events?offset=0&limit=100'
```

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/topics` | Create topic (body: `{"name":"..."}`) |
| `GET` | `/topics` | List all topics |
| `DELETE` | `/topics/{name}` | Delete topic |
| `POST` | `/topics/{name}/events` | Publish event (body: `{"value":"..."}`) |
| `GET` | `/topics/{name}/events` | Fetch events (`?offset=0&limit=100`) |

## Command Aliases

| Command | Aliases |
|---------|---------|
| `topic create` | `topic add` |
| `topic delete` | `topic rm`, `topic remove` |
| `hook rm` | `hook remove`, `hook delete` |
| `timer rm` | `timer remove`, `timer delete` |

## Agent Communication Patterns

### Pattern 1: Progress Reporting

```bash
# Agent publishes progress
ever pub task.build '{"step":"compile","status":"started"}'
ever pub task.build '{"step":"compile","status":"complete","duration_ms":1234}'

# Another agent or human monitors
ever sub task. --follow
```

### Pattern 2: Task Handoff

```bash
# Agent A finishes and signals
ever pub pipeline.stage1 '{"status":"done","output":"/tmp/result.json"}'

# Agent B blocks until signal arrives
ever wait pipeline.stage1 --timeout 60
```

### Pattern 3: Scheduled Automation (Timer+Hook)

```bash
# Every 5 minutes, publish a trigger event
ever timer add --every 5m cron.healthcheck

# Hook picks it up and runs the check
ever hook add cron.healthcheck -- ./healthcheck.sh

# One-shot: deploy in 10 minutes
ever timer add --in 10m deploy.staging
ever hook add --once deploy.staging -- ./deploy.sh --env staging
```

### Pattern 4: Agent Notification via External Tools

```bash
# Hook sends notification when errors occur
ever hook add work.errors -- sh -c 'chronoa send "Ever error: $(cat)"'

# Hook triggers another agent
ever hook add task.ready -- sh -c 'echo "New task available" | agent-notify build-agent'
```

### Pattern 5: Watching for Events in Agent Prompts

```markdown
# Worker Agent Prompt

Before starting work:
1. Check for tasks: `ever sub work.assigned --max 1 --json-values`
2. Report completion: `ever pub work.complete '{"task":"<name>","status":"done"}'`
3. Report errors: `ever pub work.errors '{"task":"<name>","error":"<desc>"}'`
```

## Topic Naming Conventions

```
agent.<name>           Agent-specific events (agent.build-01)
task.<name>            Task lifecycle events (task.compile)
file.<action>          File system events (file.modified)
pipeline.<stage>       Pipeline stages (pipeline.build)
work.assigned          Task assignment queue
work.complete          Task completion signals
work.errors            Error reports
cron.<name>            Timer-triggered events (cron.cleanup)
deploy.<env>           Deployment triggers (deploy.staging)
system.<metric>        System events (system.heartbeat)
notify.<target>        Notifications for specific agents
```

## Data Format

Events are JSON strings. Recommended fields:

```json
{
  "status": "complete",
  "agent": "build-01",
  "task": "compile",
  "timestamp": "2026-04-16T...",
  "duration_ms": 1234,
  "output": "/path/to/result",
  "error": "description"
}
```

## Architecture Notes

- **Shared append-only log** — all topics write to one file sequentially
- **Mutex-serialized writes** — thread-safe for concurrent publishers
- **Topics are logical indexes**, not separate files — 100 topics = same I/O as 1
- **Binary protocol** — 6-byte header + JSON body over TCP
- **HTTP API** — REST interface on separate port (default 8890)
- **Hook daemon** — background thread, fork/exec, PID tracking, log capture
- **Timer daemon** — background thread, publishes events on schedule
- **Recovery** — rebuilds state from log + hooks.json + timers.json on startup
- **Static binary**, no runtime dependencies
- Default TCP port: 7890, HTTP port: 8890

Run multiple stores only for **data isolation** (separate projects). Never for performance — one store handles it all.
