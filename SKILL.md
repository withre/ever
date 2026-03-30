---
name: ever
description: Event tracking and inter-agent communication via Ever event storage. Use when you need to report progress, signal other agents, track task state, or subscribe to events from other processes.
compatibility: Requires ever binary (zig build or nix build)
metadata:
  version: "0.1"
  author: Re
---

# Ever — Event Tracking & Inter-Agent Communication

Ever is a lightweight event storage system. Agents use it to publish events about their work and subscribe to events from other agents. Any process that can run shell commands can participate in Ever-based coordination.

## Prerequisites

1. **ever** — the event store binary
2. A running Ever store: `ever store start` (default port 7890)

## Core Concepts

- **Topics** — named event streams (e.g. `agent.build`, `task.complete`, `file.modified`)
- **Events** — JSON payloads published to topics
- **Subscriptions** — reading events by topic, prefix, or pattern
- Topics use `.` as separator. Naming convention: `<domain>.<action>` or `<domain>.<entity>.<action>`

## Quick Reference

```bash
# Store lifecycle
ever store start                          # start (foreground, port 7890)
ever store start --data-dir ./data        # custom data directory
ever store start --port 8000              # custom port

# Topics
ever topic create agent.tasks
ever topic list
ever topic delete agent.tasks

# Publish
ever pub agent.tasks '{"agent":"build-01","status":"complete"}'

# Subscribe
ever sub agent.tasks                      # exact topic
ever sub agent.                           # prefix — all agent.* topics (no quoting!)
ever sub .                                # all topics (no quoting!)
ever sub 'agent.*.complete'               # wildcard (needs shell quoting)
ever sub agent.tasks --from 5 --max 10    # offset + limit

# Version
ever version
```

## Subscription Patterns

The two most common patterns need NO shell quoting:

| Pattern | Meaning | Shell-safe? |
|---------|---------|-------------|
| `agent.tasks` | Exact topic | Yes |
| `agent.` | Everything under agent | Yes (trailing dot) |
| `.` | All topics | Yes |
| `agent.*.done` | Wildcard (one segment) | No (quote it) |

## Agent Communication Patterns

### Pattern 1: Progress Reporting

An agent publishes events as it works, so other agents or humans can monitor:

```bash
# Agent publishes progress
ever pub task.build '{"step":"compile","status":"started"}'
# ... does work ...
ever pub task.build '{"step":"compile","status":"complete","duration_ms":1234}'
ever pub task.build '{"step":"test","status":"started"}'

# Another agent or human monitors
ever sub task.
```

### Pattern 2: Task Handoff

Agent A completes work and signals Agent B via an event. Agent B watches for it:

```bash
# Agent A finishes and publishes
ever pub pipeline.stage1 '{"status":"done","output":"/tmp/result.json"}'

# Agent B polls for the signal
ever sub pipeline.stage1 --from 0 --max 1
```

### Pattern 3: Watching for Events in Agent Prompts

When writing a prompt for an agent, instruct it to check Ever for tasks and report results:

```markdown
# Worker Agent Prompt

You are a worker agent. Before starting work:

1. Check for assigned tasks:
   ```
   ever sub work.assigned --max 1
   ```

2. When you complete a task, report it:
   ```
   ever pub work.complete '{"task":"<task-name>","status":"done"}'
   ```

3. If you encounter an error, report it:
   ```
   ever pub work.errors '{"task":"<task-name>","error":"<description>"}'
   ```
```

## Topic Naming Conventions

```
agent.<name>           Agent-specific events (agent.build-01)
task.<name>            Task lifecycle events (task.compile)
file.<action>          File system events (file.modified, file.created)
pipeline.<stage>       Pipeline stages (pipeline.build, pipeline.test)
work.assigned          Task assignment queue
work.complete          Task completion signals
work.errors            Error reports
notify.<target>        Notifications for specific agents
```

## Data Format

Events are JSON strings. Recommended fields:

```json
{
  "status": "complete",         // started, running, complete, failed
  "agent": "build-01",          // which agent
  "task": "compile",            // what task
  "timestamp": "2026-03-30T...",// when (Ever also records its own timestamp)
  "duration_ms": 1234,          // how long
  "output": "/path/to/result",  // where to find results
  "error": "description"        // what went wrong (if failed)
}
```

## Event Triggers

Two modes: client-side (`ever on`) and server-side (`ever hook`).

### `ever on` — Client-Side (ad-hoc, development)

Runs as a long-lived client process. Dies when you kill it.

```bash
# Watch and run command for each event
ever on agent.build.complete -- ./deploy.sh

# Prefix match (no quoting needed)
ever on agent. -- ./log-agent.sh

# One-shot (exit after first event)
ever on --once task.assigned -- ./process.sh
```

The command receives:
- **stdin**: event JSON (`{"topic":"...","offset":42,"key":"...","value":"..."}`)
- **env vars**: `EVER_TOPIC`, `EVER_OFFSET`, `EVER_KEY`, `EVER_TIMESTAMP`

### `ever hook` — Server-Side (persistent, production)

Registered with the store, persists across restarts. No client process to keep alive.

```bash
# Register a hook
ever hook add agent.build.complete -- ./deploy.sh
# Output: Hook #1 registered

# Register with prefix pattern
ever hook add agent. -- ./log-agent.sh

# List all hooks
ever hook list
# ID  Pattern                Command         Last Offset
# 1   agent.build.complete   ./deploy.sh     42
# 2   agent.                 ./log-agent.sh  45

# Remove a hook
ever hook rm 1
```

Hooks run as fork/exec from the store's hook daemon thread. Same stdin/env vars as `ever on`.

### When to use which

| Aspect | `ever on` | `ever hook add` |
|--------|-----------|-----------------|
| Persistence | Dies with process | Survives restart |
| Setup | Run a command | Register via API |
| Environment | Your terminal/cwd | Store's context |
| Use case | Development, ad-hoc | Production, long-lived |

## Architecture Notes

- Ever uses a **shared append-only log** — all topics write to one file sequentially
- Writes are mutex-serialized (thread-safe for concurrent publishers)
- Topics are logical indexes, not separate files
- Cross-topic subscription (`agent.`) reads from the shared log efficiently
- Default port: 7890
- Binary protocol (6-byte header + JSON body) over TCP
- Static binary, no runtime dependencies

## One Store, Many Topics

A single Ever store handles all your topics efficiently. There's no performance reason to run separate stores — the shared log means 100 topics in one store is the same I/O cost as 1 topic. Use topic naming to organize:

```bash
# One store for everything
ever store start --data-dir ~/.local/share/ever

# Different domains, same store
ever topic create agent.build
ever topic create agent.test
ever topic create file.changes
ever topic create deploy.staging
ever topic create cron.cleanup

# Subscribe across domains
ever sub agent.           # all agent events
ever sub .                # everything
```

Run multiple stores only when you need **data isolation** (separate projects, separate backup, separate access). Never for performance — one store handles it all.
