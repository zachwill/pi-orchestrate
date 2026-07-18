# Pi Orchestrate

[`@zachwill/pi-orchestrate`](https://www.npmjs.com/package/@zachwill/pi-orchestrate) adds concurrent worker orchestration to [Pi](https://pi.dev). It helps a parent agent delegate bounded work, message independent workers concurrently, and synthesize each response as it arrives.

## Install

```bash
pi install npm:@zachwill/pi-orchestrate
```

Pi packages execute with your system permissions. Review the package and worker definitions before trusting them.

## Public tools

Pi Orchestrate adds exactly five tools:

- `orchestrate` dispatches one task with `orchestrate({ worker, title, instructions })`. Send independent tasks as sibling `orchestrate` calls in the same assistant message.
- `orchestration_status` inspects the trusted catalog, catalog diagnostics, waves, and worker states without exposing full task instructions.
- `worker_send` sends follow-up instructions to an owned reusable worker in the `ready` state.
- `worker_abort` stops owned active work that is no longer needed.
- `worker_close` closes an owned reusable worker in the `ready` state.

Each `orchestrate` call first performs atomic input, catalog, and model preflight before its worker starts. Sibling calls are admitted independently: one rejected call does not prevent valid siblings from starting. After acceptance, a resource startup failure becomes that worker's `failed` result and does not roll back or stop sibling calls.

Pi executes sibling tool calls concurrently, so independent `orchestrate` calls start concurrently without an extension-level group limit or hidden throttle. Exact unchanged worker instructions remain visible in each tool call: collapsed calls preview the message, and expanded calls show the full brief. Titles are labels, never replacements for instructions.

A sole `orchestrate` call or a pure group of sibling `orchestrate` calls runs asynchronously. Pi accepts a pure group concurrently and yields the parent turn. Mixing `orchestrate` with another tool makes it inline and blocking. `worker_send` is asynchronous only as the sole tool call in its assistant message. Inline work receives the parent turn's cancellation signal. Accepted async work does not retain that signal and continues independently.

Async responses enter the transcript individually as workers settle. For a sibling dispatch group, only the final response starts the parent's synthesis turn. The bottom widget is ephemeral and shows active work only; completed, failed, aborted, and reusable ready workers disappear immediately. Inline responses accumulate in the live tool output while the call blocks.

`orchestration_status` is for diagnostics and recovery, never a normal completion mechanism. Do not poll it for completion. If the owning session becomes inactive, its workers continue and completed results remain queued. Those results become available only when that exact owning session resumes; they are never delivered to another session.

Worker IDs identify live worker sessions. A reusable worker keeps the same worker ID across `worker_send` follow-ups, with each follow-up result belonging to a new wave. One-shot workers finish as `completed`. Reusable workers deliver as `ready` and wait for `worker_send` or `worker_close`. Use `worker_abort` only for active work, not to close a ready worker.

## Parent orchestration contract

Pi Orchestrate automatically injects the authoritative orchestration contract and trusted worker catalog into the parent system prompt. In summary, the parent owns the task end to end:

1. Keep trivial or tightly coupled work in the parent session.
2. Give every delegated task a full brief: objective, paths and scope, forbidden actions, context, constraints, observable success, checks, and expected output.
3. Dispatch every known independent task with a sibling `orchestrate` call in the same assistant message.
4. Keep an async `orchestrate` call or pure sibling group separate from other tools, then yield after acceptance. Make `worker_send` the sole tool call when it should run asynchronously.
5. Review delivered evidence and changes, resolve conflicts, integrate deliberately, and run the relevant verification.
6. Deliver the final answer from the parent session.

Workers provide evidence or bounded changes. They do not replace parent judgment.

## Trusted worker catalog

Definitions are loaded by name with this precedence:

1. Package workers in [`examples/workers/`](examples/workers/) are active fallbacks automatically.
2. User definitions in `~/.pi/agent/pi-orchestrate/workers/*.md` override package fallbacks by name.
3. Project definitions in `<project>/.pi/pi-orchestrate/workers/*.md` override user and package definitions by name, but only after Pi trusts the project.

An untrusted project contributes no worker definitions. Review project definitions as part of Pi's normal project-trust flow before enabling them.

All three package fallbacks intentionally omit `model`, so they portably inherit the parent model active at dispatch. User and trusted project overrides are the model-specialization points: add `model` to an override only when that worker needs a specific provider/model.

To customize a fallback, create a Markdown definition manually at the user or project path with the same filename and `name`. The linked package fallbacks are templates. This instruction does not assume your shell is inside a source checkout or that an npm-installed package has a particular current working directory.

## Worker definitions

A worker is a strict Markdown system prompt. Its basename must equal its `name`:

```md
---
name: reviewer
description: Reviews a bounded change and returns evidence.
tools: read, grep, find, ls, bash
lifecycle: reusable
---

You are a review worker. Inspect only the assigned scope and return concise findings with file paths.
```

Frontmatter supports these fields:

- `name` and `description` are required.
- `tools` and `lifecycle` are required. Grant the smallest useful tool set.
- `model` is optional. When omitted, the worker inherits the parent model active when dispatched.
- `thinking`, `skills`, and `compaction` are optional.
- Omitted `skills` uses Pi's normal discovered skills. A nonempty `skills` list is an exact name allowlist, and `skills: []` disables skills.
- `lifecycle` must be exactly `one-shot` or `reusable`.
- The Markdown body must be nonempty.

Unknown fields, invalid values, filename/name mismatches, and empty bodies invalidate a definition. A read-only prompt is not enforcement when its tools can write.

## Lifecycle and process limits

Use one-shot workers for bounded investigation, review, and implementation. Use reusable workers only when follow-up continuity matters. Reusable workers remain live in memory while the Pi process is running; they do not survive process exit. Close ready reusable workers when the conversation is complete.

## Isolation, trust, and writes

Worker sessions are isolated from the parent's conversational context, but they run in-process and are not sandboxes. They share the parent process's filesystem and environment permissions. Treat worker prompts, optional skills, models, and tool grants as trusted code.

Workers use regular persisted Pi global settings, authentication, packages, extensions, skills, and context. Trusted projects also contribute their project settings and resources; untrusted projects do not. Extensions are active in print mode for the complete worker lifecycle, including resource discovery and provider request hooks. Pi Orchestrate excludes its own package before child extension factories execute, so workers remain direct children while other configured extensions—including provider integrations such as `@benvargas/pi-claude-code-use`—load normally. Worker definitions still provide the exact bounded tool allowlist.

Pi Orchestrate performs no automatic filesystem writes. A worker writes only when its instructions and granted tools cause it to do so. Parallel workers must have non-overlapping write scopes, and the parent must inspect and verify their changes.
