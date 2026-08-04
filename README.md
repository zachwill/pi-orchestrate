# Pi Orchestrate

[`@zachwill/pi-orchestrate`](https://www.npmjs.com/package/@zachwill/pi-orchestrate) lets a Pi agent delegate work to separate child sessions, run independent tasks concurrently, and synthesize their results.

## Install

```bash
pi install npm:@zachwill/pi-orchestrate
```

The package includes `scout`, `investigator`, `web`, and `worker` definitions. Pi packages and workers run with your system permissions, so review every package and worker definition you trust.

## How it works

The current Pi session remains the parent and owns the task. Each `orchestrate` call starts a fresh worker session with its own transcript and a complete brief from the parent.

Workers have one of two lifecycles:

- `one-shot` workers return one result and stop automatically.
- `interactive` workers return a result and remain `ready` for follow-up work.

A **worker ID** identifies a worker session. A **run ID** identifies one generation within that session. Each interactive follow-up creates a new run while keeping the same worker ID.

## Tools

Pi Orchestrate adds exactly five tools.

### `orchestrate`

```text
orchestrate({ worker, title, instructions })
```

Starts one worker. `title` is a short label; `instructions` is the complete, self-contained brief, including scope, constraints, success criteria, and expected output.

A sole call runs asynchronously. To run independent work concurrently, send the complete wave as sibling calls in one assistant response:

```text
orchestrate({
  worker: "investigator",
  title: "Trace configuration loading",
  instructions: "Trace configuration loading from entry point to runtime. Read only. Return the relevant symbols, file paths, and a concise data-flow summary."
})

orchestrate({
  worker: "investigator",
  title: "Trace shutdown cleanup",
  instructions: "Trace shutdown and cleanup behavior. Read only. Return the relevant symbols, lifecycle invariants, and uncovered edge cases."
})
```

A wave must contain all N intended `orchestrate` calls and no other tool calls. Form the whole wave before sending it; a successfully admitted asynchronous call ends the parent turn. When Pi provides a parallel tool dispatcher such as `multi_tool_use.parallel`, put all N `functions.orchestrate` entries in one dispatcher call. Otherwise, emit N native sibling calls.

Pure orchestration groups run asynchronously, and their siblings execute concurrently. Mixing another tool into the group makes orchestration inline and blocking. Inline work follows cancellation of the parent turn; accepted asynchronous work continues independently.

Each call validates its input, worker definition, and model before starting. Sibling calls are admitted independently, so one rejection or worker failure does not roll back its peers. There is no extension-level sibling-group limit or hidden throttle.

Asynchronous results return to the exact parent session automatically. A sibling wave produces one final parent synthesis turn after every admitted worker settles. If that parent is busy or inactive, results wait for it; they are never delivered to another session.

### `worker_abort`

```text
worker_abort({ worker_ids: ["worker-…"] })
worker_abort({ all: true })
```

Stops active workers owned by the current parent session. Explicit IDs are validated together. `{ all: true }` is a no-op when no owned workers are active and does not close interactive workers that are already `ready`.

Completed one-shot workers need no cleanup. Abort active interactive work before closing its retained session.

### `worker_status`

```text
worker_status({})
```

Returns the trusted worker catalog, catalog diagnostics, and the current parent session's worker and run state. It omits worker prompts and full task instructions.

Use it for diagnostics and recovery, not completion polling. Normal asynchronous results arrive automatically. The TUI widget separately shows active work, and the footer reports interactive workers that are ready.

### `interactive_close`

```text
interactive_close({ worker_id: "worker-…" })
```

Closes an owned interactive worker whose status is `ready`. It releases the retained session; it cannot close active work or a one-shot worker.

### `interactive_send`

```text
interactive_send({
  worker_id: "worker-…",
  instructions: "Verify the first risk against the tests and cite the relevant cases."
})
```

Starts a follow-up run on an owned interactive worker whose status is `ready`. The worker keeps its ID and prior session context. Send `interactive_send` as the only tool call in the assistant message to run it asynchronously; sibling tool calls make it inline and blocking.

Interactive workers remain available across extension reloads and session switches within the same Pi process. Close them when their continuity is no longer useful. Runtime shutdown releases retained sessions automatically.

## Parent responsibilities

The parent agent still owns the result:

- Delegate bounded, independent scopes and materially distinct validation perspectives. Keep trivial or tightly coupled work in the parent.
- Use as many workers as the task supports instead of a small fixed count. User-named roles and counts are a floor unless the user sets an exact cap.
- Give each worker a complete brief and non-overlapping write scope. Intentional overlap should serve a distinct review perspective.
- Dispatch each full wave together. As findings expose new independent work, dispatch another full wave.
- Review the evidence and changes, resolve conflicts, verify the integrated result, and answer from the parent session.

## Configure workers

Worker definitions are loaded by name in this precedence order:

1. Package fallbacks in [`examples/workers/`](examples/workers/)
2. User definitions in `~/.pi/agent/pi-orchestrate/workers/*.md`
3. Project definitions in `<project>/.pi/pi-orchestrate/workers/*.md`, when Pi trusts the project

A later definition replaces an earlier definition with the same name. Untrusted projects contribute no project definitions.

The bundled `scout`, `investigator`, and `worker` inherit the parent's active model. The bundled `web` worker requires an installed, authenticated Codex CLI and uses `openai-codex/gpt-5.6-sol`. Copy a fallback into a user or project directory to customize it.

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

Required fields are `name`, `description`, a nonempty `tools` list, and `lifecycle` (`one-shot` or `interactive`). The Markdown body is the worker's nonempty system prompt.

Optional fields are `model`, `thinking`, `skills`, and `compaction`. An omitted `model` inherits the parent's model. For `skills`, omission uses normal discovery, a list is an exact allowlist, and `[]` disables skills.

Definitions are strict, regular non-symlink `.md` files up to 64 KiB. Supported Pi tools are `read`, `bash`, `edit`, `write`, `grep`, `find`, and `ls`. Grant the smallest useful set: a read-only prompt does not prevent writes when the worker has tools with write authority.

A definition is reusable dispatch configuration, not a retained session. Each `orchestrate` call starts a new worker session; only `interactive_send` continues an existing one.

## Trust and isolation

Workers receive fresh durable Pi child-session lineage without the parent's conversation. They run in the parent process and are not security sandboxes: they share its filesystem and environment permissions.

Workers use global Pi settings, authentication, packages, extensions, skills, and context. Trusted projects may add project-scoped resources. Untrusted projects do not contribute project workers, settings, extensions, skills, or context; global resources remain available.

A definition's `tools` field controls Pi's tool allowlist, not operating-system authority. A worker with `bash` can start external processes, including agent CLIs. Give concurrent workers separate write scopes and inspect their changes in the parent.

Pi Orchestrate excludes itself from child sessions and instructs workers not to create descendant Pi worker sessions. Workers remain direct Pi children.
