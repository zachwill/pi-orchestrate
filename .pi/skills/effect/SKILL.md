---
name: effect
description: "Design, implement, migrate, review, and test production TypeScript with Effect v4. Use for domain modeling, Effect workflows, services, layers, schemas, configuration, schedules, caches, streams, HTTP clients, concurrency, resources, Promise interop, or Effect code review."
license: MIT
---

# Effect

Write Effect code whose domain, failures, dependencies, lifetime, interruption, tests, and telemetry are obvious from the local code.

Good Effect code is mechanically consistent rather than combinator-dense. Keep pure logic pure. Use Effect where behavior has meaningful failure, dependencies, concurrency, cancellation, resource ownership, retries, scheduling, observability, or boundary validation.

## Source Authority

Check these before guessing:

1. the nearest `AGENTS.md` and project-local Effect practices
2. the installed `effect` declarations and source at the repository-pinned version
3. upstream source at that exact version when local source is incomplete
4. current upstream only as non-authoritative research

This repository pins `effect@4.0.0-beta.99`. Verify beta-sensitive Layer, FiberMap, ManagedRuntime, Schema, testing, and unstable APIs before use.

Established project conventions override generic examples. Preserve behavior before changing architecture unless the task explicitly requests both.

## Repository Non-Negotiables

- Keep Pi-facing tools, hooks, and sessions as Promise/callback adapters where the SDK requires them.
- Keep TypeBox for Pi tool parameter schemas. Use Effect Schema for application boundaries such as worker frontmatter and persisted message details.
- Prefer `Schema.Struct(...)` plus a same-name `interface`; do not default to `Schema.Class`.
- Use `Schema.TaggedErrorClass` for expected Effect errors when it fits.
- Prefer `Context.Service` for application capabilities.
- Keep `bun:test`; run Effect at the Bun test boundary and use Effect testing primitives internally.
- Preserve caller `AbortSignal.reason` explicitly at Promise boundaries.
- Keep the orchestration state machine, synchronous listeners, and delivery bookkeeping as direct TypeScript unless Effect adds concrete validation, interruption, concurrency, or resource-safety value.

## Working Method

Before writing `Effect.gen`:

1. Model meaningful domain distinctions.
2. Identify untrusted boundaries and decode once.
3. Name expected failures at the abstraction level callers can use.
4. Separate configured values from shared capabilities.
5. Match resource and fiber lifetimes to product ownership.
6. Decide how interruption reaches external work.
7. Identify the narrow external capability tests will replace.
8. Choose operation names and bounded telemetry attributes.

Keep Effects as values until a real executable, Pi, or test boundary. Compose implementation Layers at that edge rather than deep in business workflows.

## Branch Chooser

Read every reference matching the task before editing.

- Domain modeling judgment, failures, dependency design, observability, primitives, or review: `references/DESIGN.md`
- Schemas, brands, variants, optional keys, construction, or decoding: `references/SCHEMA.md`
- Services, module surfaces, Layers, runtime wiring, or `Effect.fn`: `references/SERVICES_LAYERS.md`
- Scope, acquisition, finalization, fibers, interruption, cancellation, or runtimes: `references/RESOURCES_CONCURRENCY.md`
- Promise-to-Effect migration, behavioral parity, or temporary bridges: `references/MIGRATION.md`
- Runtime config, environment variables, `ConfigProvider`, or `layerConfig`: `references/CONFIG.md`
- Retry, repeat, polling, backoff, jitter, rate limits, or pass loops: `references/SCHEDULING.md`
- Memoization, keyed TTL caches, lookup dedupe, or request batching: `references/CACHING.md`
- Streams, event sources, async iterables, queues, PubSub, pagination, or backpressure: `references/STREAMS.md`
- Outgoing HTTP, status handling, retries, or rate limiting: `references/HTTP_CLIENTS.md`
- Tests, virtual time, synchronization, test Layers, or fakes: `references/TESTING.md`
- Pi SDK, extension, session, child-agent, or orchestration boundaries: `references/PI_BOUNDARIES.md`
- Research provenance behind Pi guidance, only when auditing the guidance itself: `references/PI_EVIDENCE.md`

## Universal Rules

- Decode and normalize unknown values at ingress; do not revalidate established domain values.
- Keep service interfaces small, semantic, and free of adapter mechanics.
- Request stable capabilities where used; pass selected workers, models, snapshots, IDs, and request policies as values.
- Use `Effect.fn("Domain.operation")` at meaningful service, workflow, tool, provider, and persistence boundaries.
- Use `Effect.gen` for sequential workflows and pipelines for local transformations.
- Translate infrastructure failures at service boundaries. Recover only where the current boundary has a truthful response.
- Use Effect's coordination and lifecycle primitives instead of manual mutable approximations.
- Build realistic fakes for narrow external capabilities and test the real application workflow.
- Keep Promise interop at framework and SDK edges; avoid Promise–Effect ping-pong.
- Prefer readable domain helpers over clever combinator chains.

## Do Nots

- Do not use `as any`, non-null assertions, or unchecked casts to bypass Effect typing.
- Do not wrap pure functions or static constants in Effect or services without a concrete benefit.
- Do not hide required authority or infrastructure behind `Context.Reference` defaults, deep `Effect.provide`, or transitive default Layers.
- Do not use `Layer.mergeAll` or `provideMerge` as blind make-it-compile tools.
- Do not swallow causes, exhausted retries, finalizer failures, or interruption without an explicit policy.
- Do not add arbitrary sleeps to tests or hand-roll caches, queues, in-flight dedupe, or resource ownership when Effect already models the protocol.

## Review Standard

A reader should be able to answer locally:

- What does this operation need and return?
- How can it fail, and at what abstraction level?
- Which values have already been validated?
- What does it own, and who closes it?
- Can interruption reach the underlying work?
- Which capability is replaced in tests?
- Where does the operation appear in traces and logs?

If those answers are obscured, simplify the design before adding more Effect machinery.
