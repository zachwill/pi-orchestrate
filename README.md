# Pi Orchestrate

[`@zachwill/pi-orchestrate`](https://www.npmjs.com/package/@zachwill/pi-orchestrate) adds concurrent worker orchestration to [Pi](https://pi.dev). A parent agent can delegate bounded work to isolated child sessions, run independent tasks concurrently, and synthesize the results.

## Install

```bash
pi install npm:@zachwill/pi-orchestrate
```

Pi packages run with your system permissions. Review this package and every worker definition you trust.

## Tools

Pi Orchestrate adds exactly five tools:

| Tool | Call | Purpose |
| --- | --- | --- |
| `orchestrate` | `orchestrate({ worker, title, instructions })` | Start one worker task |
| `orchestration_status` | `orchestration_status({})` | Inspect the trusted catalog, diagnostics, runs, and worker states |
| `worker_send` | `worker_send({ worker_id, instructions })` | Send a follow-up to a ready reusable worker |
| `worker_abort` | `worker_abort({ worker_ids })` or `worker_abort({ all: true })` | Stop active owned work |
| `worker_close` | `worker_close({ worker_id })` | Close a ready reusable worker |

`title` is a label. `instructions` is the complete worker brief. Collapsed tool calls preview those instructions; expanded calls show them in full.

## Dispatch

Each `orchestrate` call validates its input, worker definition, and model before allocating IDs or starting a session. Calls are admitted independently: a rejected sibling does not block valid siblings. After admission, a startup or prompt failure settles only that worker as `failed`.

Pi executes sibling tool calls concurrently. There is no extension-level sibling-group cap or hidden throttle. Before dispatching, enumerate the full wave. If an intended asynchronous wave has N workers, the next assistant response must contain exactly N separate, fully briefed `orchestrate` calls; one call is valid only when N=1. Form all N calls before emitting or finalizing the response because a successfully admitted sole async call returns `terminate: true` and ends the parent turn, so omitted siblings cannot be added afterward. Never emit one call and wait for its result before forming the rest of the wave.

When the host provides a parallel tool dispatcher, use it to submit the complete wave as one tool-call group. For example, with `multi_tool_use.parallel`, make one dispatcher call whose `tool_uses` contains exactly N `functions.orchestrate` entries and no other tools. Without a dispatcher, emit N native sibling `orchestrate` calls in one assistant response. In either form, the resulting expanded tool-call group must contain exactly those N orchestration calls and no other tool calls. Harmless response text does not affect runtime classification. A three-worker wave is one assistant response that submits all three `orchestrate({ worker, title, instructions })` calls together.

Execution mode depends on the complete tool-call group:

- Pi Orchestrate treats a successfully admitted sole `orchestrate` call as async.
- Pi Orchestrate treats a successfully admitted pure group of sibling `orchestrate` calls as async; Pi executes the siblings concurrently.
- Mixing `orchestrate` with any other tool makes the orchestration calls inline and blocking.
- `worker_send` is asynchronous only when it is the sole tool call in the message.

Inline work follows the parent turn's cancellation signal. Accepted asynchronous work detaches from that signal and continues independently.

## Results and ownership

Asynchronous worker results enter the transcript individually. An ungrouped result starts a parent synthesis turn. Results from a sibling orchestration group share one final synthesis turn after every admitted member settles.

All state and delivery are owner-scoped. If an owning session is busy or inactive, completed results queue until that exact session is active and idle again. They are never delivered to another session.

`orchestration_status` is for diagnostics and recovery, not completion polling. It exposes bounded owner-scoped state without full task instructions or worker prompts.

The bottom widget shows active work only. Completed, failed, aborted, and reusable ready workers disappear immediately. Inline work shows its current response in the live tool output while it blocks.

## Lifecycle

A run represents one worker generation. A worker ID identifies the live worker session.

- A **one-shot** worker succeeds as `completed` and terminates.
- A **reusable** worker succeeds as `ready` and keeps the same worker ID.
- `worker_send` starts a new run on that ready reusable worker.
- `worker_close` closes a ready reusable worker.
- `worker_abort` stops active work only; `{ all: true }` does not close ready workers.

Workers, runs, and queued delivery survive extension reloads and session switches within the same Pi process. Reusable workers do not survive process exit, so close them when continuity is no longer needed.

## Parent contract

Pi Orchestrate injects the authoritative orchestration contract and trusted catalog into the parent system prompt. The parent remains responsible for the task end to end:

1. Keep trivial or tightly coupled work in the parent. For broad work, spin up as many workers as needed to cover every useful bounded independent scope and distinct validation perspective; never use a small default or the number of roles the user names. Named workers and counts are a floor unless the user explicitly states an exact cap; the same role can be instantiated for multiple scopes.
2. Give every worker a thorough, self-contained brief with the objective, paths and scope, context, success criteria, and expected output. Distinct validation perspectives can intentionally overlap, but avoid accidental duplicate work. State forbidden actions explicitly.
3. For an intended asynchronous wave of N workers, submit exactly N fully briefed `orchestrate` calls together and no other tool calls. Prefer one parallel dispatcher call containing N orchestration entries when a dispatcher such as `multi_tool_use.parallel` is available; otherwise emit N native siblings in one assistant response. Harmless response text does not affect runtime classification. A single call is valid only for N=1. Form the complete group before emitting it because a successfully admitted sole async call returns `terminate: true` and ends the turn; never emit one call and wait for its result before forming the rest.
4. As results expose new independent work, dispatch each full adaptive wave in parallel and continue until the whole task is complete.
5. Review evidence and changes, resolve conflicts, integrate deliberately, and verify the result.
6. Produce the final answer from the parent session.

Workers provide bounded evidence or changes. They do not replace parent judgment.

## Worker catalog

Definitions are loaded by name in this precedence order, from lowest to highest:

1. Package fallbacks in [`examples/workers/`](examples/workers/)
2. User definitions in `~/.pi/agent/pi-orchestrate/workers/*.md`
3. Project definitions in `<project>/.pi/pi-orchestrate/workers/*.md`, only after Pi trusts the project

A higher-precedence definition replaces a lower one with the same `name`. Pi performs no project-worker discovery for an untrusted project.

The package includes `scout`, `investigator`, `web`, and `worker` fallbacks. `scout`, `investigator`, and `worker` omit `model`, so they inherit the parent's active model at dispatch. `web` uses an installed, authenticated Codex CLI for public-web research and pins its Pi session and searches to `gpt-5.6-sol`. To customize one, copy its definition to the user or project directory and keep the same filename and `name`. Add an explicit model only when that worker needs one.

## Worker definitions

A worker is a regular Markdown file whose basename matches its `name`:

```md
---
name: reviewer
description: Reviews a bounded change and returns evidence.
tools: read, grep, find, ls, bash
lifecycle: reusable
---

Inspect the assigned scope and return concise findings with file paths.
```

| Field | Rule |
| --- | --- |
| `name` | Required; must match the filename |
| `description` | Required; used by the parent to choose a worker |
| `tools` | Required, nonempty list using `read`, `bash`, `edit`, `write`, `grep`, `find`, or `ls` |
| `lifecycle` | Required; exactly `one-shot` or `reusable` |
| `model` | Optional `provider/model`; omitted inherits the parent model |
| `thinking` | Optional Pi thinking level |
| `skills` | Optional; omitted uses normal discovery, a list is an exact allowlist, and `[]` disables skills |
| `compaction` | Optional worker compaction settings |

The Markdown body is the worker system prompt and must be nonempty. Unknown fields, invalid values, symlinks, and filename/name mismatches invalidate a definition.

Grant the smallest useful tool set. A read-only prompt is not enforcement when the worker has tools that can write.

## Trust and isolation

Workers receive fresh durable Pi session lineage without the parent's conversation. They still run in the parent process and are not security sandboxes: they share your filesystem and environment permissions.

Workers use normal global Pi settings, authentication, packages, extensions, skills, and context. A trusted project may also contribute project-scoped definitions, settings, extensions, skills, and context. An untrusted project contributes none of those project-scoped resources; global resources remain available.

Other configured extensions, including provider integrations such as `@benvargas/pi-claude-code-use`, load normally in worker sessions. Pi Orchestrate excludes itself, so workers remain direct Pi children. The injected boundary forbids recursive Pi Orchestrate delegation and descendant Pi worker sessions.

The worker definition controls the Pi tool allowlist, not operating-system authority. A trusted worker with `bash` can launch explicitly required external processes, including agent CLIs.

Pi Orchestrate performs no automatic filesystem writes. Workers write only through their granted tools and instructions. Give concurrent workers non-overlapping write scopes, then inspect and verify their changes in the parent.
