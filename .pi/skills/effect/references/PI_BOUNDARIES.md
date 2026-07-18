# Effect At Pi Boundaries

Use this for Pi extensions, tools, hooks, sessions, child agents, orchestration, delivery, and process-host ownership.

This file is normative for pi-orchestrate. `PI_EVIDENCE.md` records the external research behind it and is not required for ordinary implementation.

## Keep Pi APIs Native

Pi expects Promise and callback surfaces. Build internal behavior as one Effect where Effect adds value, then run it at the `execute`, hook, session, or host boundary.

```text
Pi Promise/callback API -> Effect workflow -> Pi Promise result
```

Do not leak Effect requirements into Pi SDK interfaces or call `Effect.runPromise` repeatedly inside the workflow.

When a Pi boundary receives an `AbortSignal`:

1. connect it to Effect interruption
2. forward interruption to underlying Pi/provider/platform operations
3. preserve the caller's original `signal.reason` at Promise rejection

Effect interruption alone does not guarantee the rejection identity expected by Pi callers.

## Ownership Matrix

| Thing | Product owner and lifetime | Abort versus close | Boundary |
|---|---|---|---|
| Process host | Pi process; survives extension reload attachments | Process shutdown closes host-held resources best-effort | Process-scoped host value with Promise lifecycle adapters |
| Scheduler | Process-host/runtime scheduler shared across owners | Parent-turn cancellation affects that owner's inline work; shutdown settles admitted work | Internal Effect only where interruption or finalization helps |
| Worker workflow | One admitted generation or wave result | Abort interrupts active work; settlement completes it | Scoped operation surfaced as Promise to the tool |
| Retained session | Reusable worker identity across generations | `worker_abort` stops active work; `worker_close` disposes the ready session | Explicit retained owner and close handle |
| Prompt generation | One child creation or generation | Cancellation abandons the generation; no separate durable resource | Value or Effect under the worker workflow |
| Catalog snapshot | One atomic preflight decision | Immutable after admission; refresh creates a new snapshot | Validated value passed to runtime, not a Context tag |

Scope and finalizer ownership must follow this matrix. Extension attachment reload is not process shutdown. Prompt interruption is not retained-session disposal.

## Values Versus Capabilities

Pass these as validated values:

- selected worker definition
- model/provider selection
- catalog snapshot
- owner and wave IDs
- per-request policy
- configured provider instance used by one operation

Use `Context.Service` for stable shared capabilities such as stores, SDK adapters, process services, or transports when substitution and lifecycle justify it.

Do not mint a service tag for every configured worker, model, or request. Do not hide authority-bearing capabilities behind ambient defaults.

## Preserve Direct Domain Machinery

Keep these as direct TypeScript unless Effect provides a concrete improvement:

- synchronous state transitions and invariant checks
- owner-scoped maps and indexes
- immediate listeners used by one projection
- delivery bookkeeping
- immutable preflight snapshots

Do not replace an authoritative state machine with fibers, Streams, PubSub, or Ref merely to make it "more Effect." Domain settlement and observational session events are different concepts and should remain separate.

Use Effect when the behavior needs typed boundary validation, interruption, resource-safe acquisition, structured concurrency, retries, schedules, or a genuinely multi-consumer stream.

## Admission And Authority

Validate tasks, worker references, model choices, and ownership before allocating IDs or starting sessions. Atomic preflight failure starts nothing.

Once admitted, workers start independently. One startup failure must not roll back successfully admitted peers.

Child sessions must have reduced authority:

- no recursive orchestration tools
- no unintended extension loading
- only required tools and skills
- trusted project context only when Pi reports trust
- discovery and health probes use cheaper, disposable paths

Capability minimization is stronger than prompt instructions saying not to use a tool.

## Reusable Sessions

A retained sequential worker is a durable product scope even if implemented without Effect Scope.

Keep lifecycle operations explicit:

- create/acquire session
- prompt one generation
- abort active generation
- return to ready
- close retained session
- evict through the same close policy

Aggregate the session, subscriptions, resource loaders, and clients under an Effect scope only when teardown complexity earns it. Do not add Scope merely to wrap one disposable.

## Events And Background Work

Keep direct synchronous observation when one event source updates tightly coupled activity, usage, and presentation state.

Introduce Queue, PubSub, or Stream when:

- producer and consumer need backpressure or buffering
- multiple independent consumers share one raw source
- RPC or API consumers need streaming
- a callback source needs structured interruption

Normalize a shared raw event source once when independent consumers would otherwise parse it differently. Do not collapse runtime settlements and session observations into one event algebra.

Long-lived consumers belong to the owner scope and must be forked. Never block Layer acquisition on a forever consumer.

## Process Lifetime And Delivery

Detached asynchronous work needs a process-level owner plus explicit owner-scoped records. A FiberMap alone is not the orchestration domain model.

Preserve these delivery semantics:

- inline work follows the parent turn's cancellation
- accepted async work is detached from that turn
- results deliver only to the exact owner session
- intermediate settlements may deliver without a synthesis turn
- the final settlement boundary starts one synthesis turn
- extension reloads must not lose process-owned work

Shutdown is idempotent and best-effort without leaving admitted lifecycle state unsettled.

## Boundary Tests

Test Pi integration at the SDK boundary, not only as isolated Effects:

- exact caller abort reason
- active abort versus retained close
- owner isolation
- extension reload attachment behavior
- partial child-session acquisition
- unsubscribe/abort/dispose failures
- repeated shutdown
- stale generation completion
- reduced child tools and project trust
- exact settlement delivery boundary

Use `bun:test`; read `TESTING.md` and `RESOURCES_CONCURRENCY.md`.
