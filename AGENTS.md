# Pi Orchestrate Contributor Instructions

Pi Orchestrate is a concurrent, owner-scoped worker runtime for Pi. Parent agents delegate bounded work to isolated child sessions while retaining responsibility for the task.

## Sources of Truth

Read these before changing code:

1. `README.md` defines public behavior, trust boundaries, worker definitions, and terminology.
2. `extension/index.ts` shows lifecycle wiring and module composition.
3. Read the implementation module you will change and its matching file in `test/`.

The domain state machine and executable tests define runtime behavior. `extension/contract.ts` defines the model-facing parent contract. `examples/workers/`, `extension/catalog.ts`, and the README define the worker format.

When public behavior changes, update the README and the relevant contract, package, SDK, and integration tests. Do not restate the public contract in this file.

## Module Ownership

Keep decisions in the module that owns them:

- `domain.ts` owns worker and run types, state transitions, IDs, and limits.
- `catalog.ts` and `contract.ts` own trusted worker discovery, parsing, precedence, diagnostics, and parent guidance.
- `runtime.ts` and `scheduler.ts` own admission, preflight, concurrency, ownership, cancellation, reusable generations, and retained state.
- `worker-session.ts` and `worker-settlement.ts` own durable child sessions, usage and activity reporting, message direction, and persisted settlement decoding.
- `host.ts` and `delivery.ts` own process-scoped persistence, owner binding, grouped synthesis, and exact-session delivery.
- `tools.ts` owns public schemas, execution adapters, streaming updates, and tool renderers.
- `presentation.ts` owns result messages, worker status, width-safe rendering, and UI disposal.
- `index.ts` binds lifecycle hooks and composes the extension. Do not move domain policy into it.

Do not add cross-layer shortcuts. Change an owning boundary directly instead of routing around it.

## Runtime Invariants

- Validate tasks, worker references, and models before allocating IDs, creating runtime records, or starting sessions. Preflight failure starts nothing.
- Once admitted, workers start and settle independently. One worker failure must not roll back its peers.
- Preserve the dispatch modes, grouped synthesis, lifecycle, and delivery behavior defined by the README and encoded in contract and integration tests.
- Scope state, operations, cancellation, and delivery to the exact owner. Never leak results or controls across sessions.
- Reject stale reusable generations and race-losing operations without corrupting current worker state.
- Keep child sessions as direct Pi Orchestrate children. Do not load Pi Orchestrate recursively or create descendant Pi worker sessions.
- Read project workers and project context only when Pi reports the project trusted.
- Make cleanup idempotent and best-effort while still settling lifecycle state.
- Keep model-facing output and collapsed UI bounded. Preserve the complete state required for reconstruction in structured details.

## Project Sandcastle Rules

The global Sandcastle Doctrine applies. This package is pre-1.0: keep its public boundaries precise and its internal shape easy to replace.

- Preserve ownership, trust, state-transition, and persisted-settlement contracts deliberately.
- Replace obsolete internal seams directly. Delete dead APIs, aliases, adapters, schemas, migrations, terminology, and tests instead of preserving legacy paths.
- Support backwards compatibility only for a named, concrete boundary with tests.
- Use Effect when it clarifies failures, dependencies, interruption, concurrency, validation, observability, or resource ownership. Keep straightforward synchronous domain logic straightforward.

## Implementation Rules

- Use Bun for installs, scripts, and tests.
- Keep TypeScript strict and ESM/NodeNext-compatible. Use `.js` specifiers for relative imports.
- Keep tool schemas strict. Throw from `execute` to signal failure; an error-shaped return value is still a successful tool result.
- Keep model-facing `content` concise and put complete machine-readable state in `details`.
- Do not start timers, watchers, sessions, or other long-lived resources in the extension factory. Bind them on session start or demand and release them on shutdown.
- Guard terminal-only APIs with `ctx.mode === "tui"`.
- Fit every rendered line to its supplied width using Pi TUI width and ANSI helpers rather than string slicing.
- Renderers must tolerate partial persisted data, rebuild pre-themed content on `invalidate()`, reuse components where appropriate, and dispose timers and subscriptions exactly once.
- Preserve fresh child-session lineage, durable storage, selected tools and skills, trust boundaries, and model and authentication inheritance.
- Read the installed Pi documentation and exported types before changing lifecycle, session, model, tool, package, or TUI behavior. Verify assumptions with SDK integration tests.

## Verification

`package.json` is the canonical command source.

Run the narrow matching test while working, then run both commands before finishing:

```bash
bun run typecheck
bun test
```

Changes involving package contents, real Pi APIs, process lifetime, owner binding, or delivery require explicit coverage in `test/package.test.ts`, `test/sdk-integration.test.ts`, and `test/integration.test.ts` as applicable.

Keep tests beside the module that owns the behavior. Use integration tests for lifecycle hooks, host attachments, runtime ownership, and delivery. Make concurrency tests deterministic and cover stale generations, races, ownership isolation, and cleanup failures.
