# Pi Orchestrate Contributor Guide

## Start here

Pi Orchestrate is a Pi package for concurrent, owner-scoped worker orchestration. The parent agent owns delegation, integration, review, and final verification; child workers perform bounded tasks in isolated sessions.

Read these before changing code:

1. `README.md` for the canonical public behavior, trust model, worker format, and terminology.
2. `extension/index.ts` for lifecycle wiring and composition.
3. The implementation module you will change and its matching file in `test/`.

Do not duplicate the README’s public contract here. Update the README and package-contract tests when public behavior changes.

## Architecture

Follow responsibilities rather than adding cross-layer shortcuts:

- `domain.ts` defines worker/run types, state transitions, IDs, and limits.
- `catalog.ts` and `contract.ts` own trusted worker discovery, strict parsing, source precedence, diagnostics, and parent prompt guidance.
- `runtime.ts` and `scheduler.ts` own admission, atomic preflight, concurrency, ownership, cancellation, reusable generations, settlement, and retained state.
- `worker-session.ts` creates durable isolated child sessions and reports usage, activity, and message direction back to the runtime.
- `worker-settlement.ts` owns the current persisted settlement schema and direct decoding.
- `host.ts` and `delivery.ts` preserve process-scoped work across extension reloads while delivering results only to the owning session.
- `tools.ts` defines the public tool schemas, execution adapters, streaming updates, and tool renderers.
- `presentation.ts` owns result messages, the active-worker widget, footer status, width-safe rendering, and UI disposal.
- `index.ts` binds these pieces to Pi lifecycle events.

`examples/workers/`, `catalog.ts`, and `README.md` are the canonical worker-definition references.

## Invariants

Preserve these contracts:

- Validate every task, worker reference, and model before allocating IDs, creating runtime records, or starting sessions. Preflight failure starts nothing.
- Once admitted, workers start independently and concurrently. One worker’s startup or prompt failure must not roll back its peers.
- A sole `orchestrate` or `worker_send` call is asynchronous. Sibling tool calls make it inline and blocking.
- Inline work follows the parent turn’s cancellation signal. Accepted async work is detached and delivered later to its exact owner session.
- State, worker operations, and delivery are owner-scoped. Never leak results or controls across sessions.
- An ungrouped async worker result starts a parent synthesis turn. Results from sibling calls share one final synthesis turn.
- One-shot workers terminate. Reusable workers retain identity while ready, accept follow-ups as new generations, and require `worker_close` when finished.
- Child sessions are direct children: no recursive extension loading or descendant orchestration.
- Project workers and project context are read only when Pi reports the project trusted.
- Cleanup is idempotent and best-effort without leaving lifecycle state unsettled.
- Model-facing output and collapsed UI stay bounded; structured details retain the complete state needed for reconstruction.

The state machine in `domain.ts`, executable tests, and then the README are authoritative when details are unclear.

## Sandcastle doctrine

This package is pre-1.0 and intentionally a sandcastle: build carefully for the work in front of us and expect its shape to change as real usage teaches us more.

- Keep the public orchestration contract, ownership boundaries, state transitions, trust model, and persisted contracts precise.
- Prefer direct, boring, domain-named code over generic frameworks, universal abstractions, deep nesting, or dense conditional expressions.
- Keep decisions close to the module that owns them. Extract only for real repetition, a stable boundary, or concrete lifecycle and validation value.
- Optimize for code that is easy to understand, replace, and reshape next week. Readability and deletion are features.
- When an idea changes, rename or replace the old seam directly. Do not preserve obsolete APIs, aliases, adapters, schemas, migrations, or terminology without a concrete compatibility requirement.
- Treat pre-1.0 legacy paths as liabilities by default. Delete dead compatibility code and its tests instead of making new work route around it.
- Use Effect where it makes failures, dependencies, interruption, concurrency, validation, observability, or resource ownership clearer. Keep straightforward synchronous domain logic as straightforward TypeScript.
- Preserve behavior deliberately, not accidentally. If compatibility still matters, name the supported boundary and justify it in code and tests.

## Implementation guidance

- Use Bun for installs, scripts, and tests.
- Keep TypeScript strict, ESM/NodeNext-compatible, and use `.js` specifiers for relative imports.
- Keep tool schemas strict. Throw from `execute` to signal a tool failure; an error-shaped return value is still a successful tool result.
- Keep `content` concise and model-facing. Put complete machine-readable state in `details`.
- Do not start timers, watchers, sessions, or other long-lived resources in the extension factory. Bind them on session start or demand and release them on shutdown.
- Guard terminal-only APIs with `ctx.mode === "tui"`.
- Every rendered line must fit its supplied width. Use Pi TUI width/ANSI helpers rather than string slicing.
- Renderers must handle partial and malformed persisted data, rebuild pre-themed content on `invalidate()`, reuse components where appropriate, and dispose timers/subscriptions exactly once.
- Preserve fresh child-session lineage, durable storage, selected tools/skills, trust boundaries, and model/auth inheritance.
- Consult the installed Pi docs and exported types before changing lifecycle, session, model, tool, package, or TUI behavior. Prefer SDK integration tests over assumptions from examples.

## Validation

`package.json` is the canonical command source.

```bash
bun install
bun run typecheck
bun test
```

Run the narrow matching suite while iterating, for example:

```bash
bun test test/runtime.test.ts
bun test test/worker-session.test.ts
bun test test/presentation.test.ts
```

Before finishing any change, run typecheck and the full test suite. Changes involving package contents, real Pi APIs, process lifetime, owner binding, or delivery should also be checked explicitly against `package.test.ts`, `sdk-integration.test.ts`, and `integration.test.ts`.

Tests mirror module ownership. Add focused regressions beside the affected module; use integration tests when behavior crosses lifecycle hooks, host attachments, runtime ownership, or delivery. Keep concurrency tests deterministic and cover stale generations, races, ownership isolation, and cleanup failures.
