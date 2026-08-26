# Pi Orchestrate Contributor Instructions

Pi Orchestrate is a concurrent, owner-scoped worker extension for Pi. Parent agents delegate bounded work to isolated child sessions while retaining responsibility for the task.

## Sources of Truth

Read these before changing code:

1. `README.md` defines public behavior, trust boundaries, worker definitions, and terminology.
2. `extension/index.ts` is the extension entry point and composition root; `extension/package-root.ts` resolves the installed package root.
3. Read the owning implementation module you will change and its matching file in `test/`.

The state machine in `extension/orchestration/model.ts` and executable tests define orchestration behavior. `extension/parent/contract.ts` defines the model-facing parent contract. `examples/workers/`, `extension/catalog/definition.ts`, `extension/catalog/discovery.ts`, and the README define the worker format.

When public behavior changes, update the README and the relevant contract, package, SDK, and integration tests. Do not restate the public contract in this file.

## Module Ownership

Keep decisions in the module that owns them:

- `extension/catalog/definition.ts` owns worker-definition and catalog value types, supported tool names, catalog construction, and lookup.
- `extension/catalog/discovery.ts` owns frontmatter schemas, strict parsing and format validation, trusted source discovery, precedence, and diagnostics.
- `extension/orchestration/model.ts` owns worker and run types, state transitions, IDs, and limits.
- `extension/orchestration/settlement.ts` owns canonical settlement schemas, persisted decoding, and tool transport projections.
- `extension/orchestration/admission.ts` owns task, worker-reference, and model preflight decisions.
- `extension/orchestration/service.ts` owns admitted work, concurrency, owner-scoped state and operations, cancellation, interactive generations, retained sessions, settlement publication, and FiberMap/FiberSet coordination.
- `extension/worker/session.ts` owns durable direct child sessions, lineage, inherited Pi resources, prompting, usage and activity reporting, message direction, abort, and disposal.
- `extension/worker/child-sessions.ts` owns process-level child-session acquisition, adoption handoff, reclamation, and shutdown.
- `extension/parent/contract.ts` owns model-facing parent guidance and trusted catalog insertion.
- `extension/parent/dispatch-policy.ts` owns parent tool-call grouping, dispatch mode, and synthesis-group classification.
- `extension/parent/delivery.ts` owns owner binding, queued exact-session delivery, and grouped synthesis.
- `extension/parent/process-host.ts` owns the process-scoped orchestration host, Effect application composition, Pi-facing adapters, attachments, and shutdown.
- `extension/pi/tools.ts` owns public tool schemas, execution adapters, and streaming updates.
- `extension/pi/tool-renderer.ts` owns tool call and result renderers.
- `extension/pi/presentation.ts` owns result messages, worker status presentation, widgets, and footer state.
- `extension/pi/tui.ts` owns shared width-safe rendering, appearance, timing, and component disposal helpers.
- `extension/package-root.ts` owns installed package-root resolution.
- `extension/index.ts` binds lifecycle hooks and composes the extension. Do not move orchestration policy into it.

Do not add cross-layer shortcuts. Change an owning boundary directly instead of routing around it.

## Orchestration Invariants

- Validate tasks, worker references, and models before allocating IDs, creating orchestration records, or starting sessions. Preflight failure starts nothing.
- Once admitted, workers start and settle independently. One worker failure must not roll back its peers.
- Preserve the dispatch modes, grouped synthesis, lifecycle, and delivery behavior defined by the README and encoded in contract and integration tests.
- Scope state, operations, cancellation, and delivery to the exact owner. Never leak results or controls across sessions.
- Reject stale interactive generations and race-losing operations without corrupting current worker state.
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
AGENT=1 bun run typecheck
AGENT=1 bun test
```

Changes involving package contents, real Pi APIs, process lifetime, owner binding, or delivery require explicit coverage in `test/package.test.ts`, `test/sdk-integration.test.ts`, and `test/integration.test.ts` as applicable.

Keep tests beside the module that owns the behavior. Use integration tests for lifecycle hooks, host attachments, orchestration ownership, and delivery. Make concurrency tests deterministic and cover stale generations, races, ownership isolation, and cleanup failures.
