# Pi Orchestrate

[`@zachwill/pi-orchestrate`](https://www.npmjs.com/package/@zachwill/pi-orchestrate) lets a Pi session delegate work to direct child sessions. It requires Pi 0.85.0 or newer.

- Each worker gets a focused brief and a separate conversation.
- Workers run independently and return their results to the parent.
- Only the parent can delegate; workers cannot create more workers.

## The model

Each `orchestrate` dispatch creates a fresh worker session with its own transcript. The worker receives a complete brief from the parent but not the parent's conversation.

A worker definition is reusable configuration: it selects the worker's prompt, tools, lifecycle, and optional model settings. It is not a running or retained session.

Independent workers dispatched together run concurrently. Their results return only to the parent session that started them, and the parent synthesizes the group after every worker finishes. A rejected or failed worker does not cancel its peers. Orchestration dispatched alongside unrelated tool calls runs inline instead of in the background.

Workers have one of two lifecycles:

- A `one-shot` worker returns one result and stops.
- An `interactive` worker returns a result and remains available for follow-up work.

A **worker ID** identifies a worker session. A **run ID** identifies one generation within it. An interactive follow-up creates a new run while preserving the worker ID and prior session context.

Interactive workers remain available across session switches and extension reloads within the same Pi process. Closing one releases its retained session; process shutdown releases any that remain.

## Compaction

Parent compaction does not stop workers. Results that settle during compaction remain queued for the owning session and resume automatic delivery when that session becomes idle, including after failed or cancelled compaction. Grouped work still triggers synthesis only after the group settles.

Before each parent model request, the extension adds a fresh, owner-scoped snapshot of active workers and ready interactive sessions. This transient context survives compaction by being rebuilt from live state; it is not appended to the transcript. The snapshot is capped at 12 KiB and reports omitted workers or truncated assignments, with `worker_status` available for recovery rather than polling.

This state belongs to the running Pi process. Compaction does not require restarting workers, and persisted transcripts do not restore running workers after a process restart.

## Agent interface

Pi Orchestrate gives the parent five model-facing tools:

| Tool | Purpose |
| --- | --- |
| `orchestrate` | Start a fresh worker with a title and complete brief |
| `interactive_send` | Continue an owned interactive worker that is ready |
| `interactive_close` | Release an owned interactive worker that is ready |
| `worker_abort` | Stop active workers owned by the parent |
| `worker_status` | Inspect the trusted catalog and diagnose the parent's worker state |

The extension supplies the parent with the exact dispatch and lifecycle rules for these tools. The README describes their behavior rather than duplicating those model instructions.

## Worker definitions

The package includes four fallback definitions in [`examples/workers/`](examples/workers/): `scout` for small factual probes, `investigator` for read-only cross-file research, `worker` for bounded implementation, and `web` for public web research. The first three inherit the parent's active model. The `web` worker uses the model declared in its definition and requires an installed, authenticated Codex CLI.

Definitions are loaded by name in this precedence order:

1. Package fallbacks in `examples/workers/`
2. User definitions in `~/.pi/agent/pi-orchestrate/workers/*.md`
3. Project definitions in `<project>/.pi/pi-orchestrate/workers/*.md`, when Pi trusts the project

A later definition replaces an earlier definition with the same name. Untrusted projects contribute no project definitions. Copy a fallback into a user or project directory to replace it.

A definition is a Markdown file whose basename matches its `name`:

```md
---
name: reviewer
description: Reviews a bounded area and answers follow-up questions.
tools: read, grep, find, ls
lifecycle: interactive
---

Inspect only the assigned scope. Do not modify files. Return concise findings with file paths.
```

The frontmatter is strict:

| Field | Meaning |
| --- | --- |
| `name` | Required definition name; must match the filename |
| `description` | Required catalog description used when choosing a worker |
| `tools` | Required nonempty tool list |
| `lifecycle` | `one-shot` or `interactive`; defaults to `one-shot` |
| `model` | Optional `provider/model` coordinate; omission inherits the parent's model |
| `thinking` | Optional thinking level: `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max` |
| `skills` | Optional exact skill allowlist; `[]` disables skills and omission uses normal discovery |
| `compaction` | Optional Pi compaction settings: `enabled`, `reserveTokens`, and `keepRecentTokens` |

The Markdown body is the worker's nonempty system prompt. `tools` and `skills` accept either YAML arrays or comma-separated strings. Supported Pi tools are `read`, `bash`, `edit`, `write`, `grep`, `find`, and `ls`. Unknown fields and malformed definitions are rejected and appear in catalog diagnostics.

Each `orchestrate` call starts a new worker session from the selected definition. Only `interactive_send` continues an existing session.

## Trust boundary

Workers run in the parent process and are not security sandboxes. They share its filesystem and environment permissions.

Workers can use global Pi settings, authentication, packages, extensions, skills, and context. Trusted projects may add project-scoped resources. Untrusted projects do not contribute project workers, settings, extensions, skills, or context; global resources remain available.

A definition's `tools` field controls Pi's tool allowlist, not operating-system authority. A worker with `bash` can start external processes, including other agent CLIs. A read-only prompt also does not prevent writes when the worker has a write-capable tool.

Concurrent workers share the same working tree, so overlapping write scopes can collide. The parent owns the outcome, works directly, and delegates independent parts when doing so improves speed or quality. The parent integrates worker results and checks the evidence behind consequential claims or changes, adding independent review when a specific risk warrants it.

Pi Orchestrate excludes itself from child sessions and keeps workers as direct Pi children.
