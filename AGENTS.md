# Pi Orchestrate Contributor Instructions

Pi Orchestrate delegates work to direct child Pi sessions. Changes must preserve exact-owner isolation, independent worker lifecycles, and reliable result delivery.

## Start Here

- Read the implementation that owns the change and its relevant tests. Unit tests generally mirror `extension/` paths under `test/`; extension wiring is tested in `test/integration.test.ts`.
- `README.md` explains human-facing behavior, configuration, and trust boundaries. `extension/parent/contract.ts` defines the parent model's operating instructions. Worker definitions are specified by `extension/catalog/discovery.ts` and illustrated in `examples/workers/`.
- `extension/orchestration/model.ts` defines worker-status transitions; `extension/orchestration/service.ts` owns the orchestration lifecycle. Their tests establish behavior beyond the README's overview.
- Before changing Pi API usage, read the relevant Pi documentation, exported types, and implementation at the repository-pinned dependency version. Check compatibility against the supported range in `package.json`; newer globally installed documentation is supplementary.

Update the README when a change affects human setup, usage, or consequential expectations. Keep implementation mechanics in code and tests, and model-operating instructions in the parent contract. Do not turn this file into another behavioral specification.

## Ownership

Change the owning boundary rather than adding cross-layer shortcuts. Paths below are relative to `extension/`.

| Module | Responsibility |
| --- | --- |
| `catalog/definition.ts` | Worker and catalog types, supported tools, catalog construction and lookup |
| `catalog/discovery.ts` | Definition parsing, trusted discovery, precedence, and diagnostics |
| `orchestration/model.ts` | Worker/run records, IDs, worker-status transitions, and task-size limits |
| `orchestration/admission.ts` | Task, worker-reference, and model preflight |
| `orchestration/settlement.ts` | Settlement schemas, persisted decoding, and transport projections |
| `orchestration/service.ts` | Admitted work, concurrency, cancellation, interactive generations, retained sessions, and settlement publication |
| `worker/session.ts` | Direct child sessions, lineage, inherited resources and authentication, prompting, observation, and disposal |
| `worker/child-sessions.ts` | Process-level child-session acquisition, adoption, reclamation, and shutdown |
| `parent/contract.ts` | Parent guidance and trusted catalog insertion |
| `parent/dispatch-policy.ts` | Tool-call grouping, dispatch mode, and synthesis-group classification |
| `parent/delivery.ts` | Owner bindings, queued result delivery, and grouped synthesis |
| `parent/worker-context.ts` | Bounded, owner-filtered transient worker context |
| `parent/process-host.ts` | Process host, Effect composition, Pi-facing adapters, and attachment lifetime |
| `pi/tools.ts` | Public tool schemas, execution adapters, and streaming updates |
| `pi/tool-renderer.ts`, `pi/presentation.ts`, `pi/tui.ts` | Tool rendering, result/status presentation, and shared width-safe UI helpers, respectively |
| `package-root.ts` | Installed package-root resolution |
| `index.ts` | Lifecycle hooks and composition; not orchestration policy |

## Invariants

- Validate tasks, worker references, and models before allocating IDs, creating records, or starting sessions. Preflight failure starts nothing.
- Admitted workers start and settle independently. One failure must not roll back peers. Preserve dispatch and grouped-synthesis semantics when changing execution or delivery.
- Scope state, controls, context projections, and delivery to the exact owner. Reject stale worker generations and parent bindings without changing current state.
- Keep workers as direct Pi children. Do not load Pi Orchestrate recursively or create descendant Pi worker sessions.
- Load project workers and resources only when Pi reports the project trusted. Preserve child lineage, durable transcripts, selected tools/skills, and model/authentication inheritance.
- Do not start long-lived resources in the extension factory. Acquire them on session start or demand, and release them according to their session, worker, or process ownership. Reload and session replacement must not destroy process-owned work.
- Cleanup must be idempotent and best-effort without hiding failures or preventing lifecycle settlement.

## Implementation

- Use Bun for installs, scripts, and tests. Keep TypeScript strict and ESM/NodeNext-compatible, with `.ts` relative imports: this package ships raw TypeScript.
- Keep tool schemas strict. Throw from `execute` to signal failure; an error-shaped return value is still a successful tool result.
- Bound model-facing content and collapsed UI. Put complete reconstruction data in structured `details`, not unbounded text.
- Guard terminal-only APIs with `ctx.mode === "tui"`. Fit rendered lines using Pi TUI width and ANSI helpers, not string slicing.
- Renderers must tolerate partial persisted data, rebuild themed content on `invalidate()`, and dispose timers and subscriptions exactly once.

## Design

This package is pre-1.0. Keep ownership, trust, and persisted-settlement contracts precise while leaving internals easy to replace.

- Replace obsolete implementations directly; remove dead adapters, aliases, tests, and documentation. Preserve compatibility only for a named requirement with tests.
- Use Effect where it clarifies failures, concurrency, interruption, dependencies, or resource ownership. Keep straightforward synchronous domain logic straightforward.

## Verification

Choose coverage by the behavior at risk:

- Owning module tests: domain behavior and local regressions.
- `test/integration.test.ts`: lifecycle hooks, attachments, ownership, and delivery wiring.
- `test/sdk-integration.test.ts`: assumptions about real Pi APIs and the supported dependency baseline.
- `test/package.test.ts`: published contents, manifests, resource discovery, and package loading.

Use deterministic synchronization for concurrency tests. Cover relevant stale generations, races, owner isolation, and cleanup failures; do not add unrelated suites merely because a change touches a shared module.

For code or dependency changes, run focused tests while working, then both commands before finishing. `package.json` is the command source of truth.

```bash
AGENT=1 bun run typecheck
AGENT=1 bun test
```

For documentation-only changes, verify factual claims, referenced paths, commands, and links. Run executable checks when the documentation changes a runnable example or exposes an implementation assumption that needs validation.
