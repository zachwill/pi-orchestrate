# Resources, Concurrency, And Interruption

Use this when code acquires resources, forks work, waits for completion, adapts cancellation, owns a runtime, or manages reusable sessions.

## Lifetime Follows Product Ownership

A scope should mean a real owner lifetime:

- one operation or admitted generation
- one retained reusable worker
- one owning Pi session
- one process host

Do not add a Scope merely because one object has a `close` method. Use it when acquisition, partial failure, interruption, or multiple finalizers need structured ownership.

Whenever code opens a session, socket, listener, subscription, timer, file handle, child process, or client, answer:

- Who owns it?
- When does that owner end?
- What happens if later acquisition fails?
- In what order do finalizers run?
- What if cleanup fails or runs twice?

Use `Effect.acquireRelease` for scoped resources and `Effect.acquireUseRelease` when the acquire/use/release shape should remain local. A Layer that acquires resources owns them for the Layer's scope.

## Structured Concurrency Is The Default

Fork work into the scope that owns it:

```ts
const consumerLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const events = yield* Events.Service

    yield* events.stream.pipe(
      Stream.runForEach(handleEvent),
      Effect.forkScoped,
    )
  }),
)
```

Layer acquisition must complete. Never run a forever stream or loop inline while constructing a Layer.

Use:

- `Effect.forkScoped` for ordinary background work owned by the current scope
- `Effect.forkIn(scope)` when service methods must add work to the Layer's captured lifetime
- `FiberSet` for a dynamic homogeneous collection of owned fibers
- `FiberMap` for keyed work with replacement or per-key ownership semantics

Do not detach work merely to make a caller return sooner. Detached work needs an explicit durable owner, settlement policy, and shutdown behavior.

## Coordination Primitives State The Protocol

- `Deferred`: create now, complete once later
- `Queue`: each item is handed to a consumer
- `PubSub`: each subscriber receives each event
- `SubscriptionRef`: current value plus changes
- `Ref`: concurrent state without a handoff protocol
- `Semaphore`: bounded shared access
- `Stream`: many ordered values with pull and backpressure

Do not recreate these with booleans, callback arrays, EventEmitters, stored Promise resolvers, or scattered maps of fibers.

Long duration is not automatically slowness. A fiber awaiting a Deferred, queue item, schedule, or stream element is suspended. Ask what resource remains held, whether the wait is interruptible, and which owner controls it.

## Cancellation Is Not Disposal

Keep these domain actions distinct:

- **interrupt/abort:** stop the active operation or generation
- **close/dispose:** release a reusable resource permanently
- **evict:** remove retained state and execute the same close policy
- **shutdown:** settle or interrupt process-owned work and release host resources

For pi-orchestrate, aborting a prompt must not implicitly close its reusable session. `worker_abort` stops active work; `worker_close` disposes the ready retained worker. Preserve those semantics in names, states, and tests.

An `Effect.async` canceler or interrupted Promise adapter can stop an active subprocess or request. It does not decide whether a durable session should remain available afterward; the domain owner decides that.

## Promise Interruption

Forward Effect interruption to external APIs:

```ts
const request = Effect.tryPromise({
  try: (signal) => fetch(url, { signal }),
  catch: (cause) => new RequestError({ operation: "Provider.request", cause }),
})
```

Use the signal supplied to `Effect.tryPromise` for fetch, SDK calls, child processes, database operations, and platform APIs that support cancellation.

At a public Promise adapter receiving a caller `AbortSignal`, preserve `signal.reason` explicitly after or alongside Effect interruption. Cancellation identity is part of the Pi boundary contract and is not guaranteed by generic fiber interruption.

Keep interruption distinct from expected typed failure and defects. Retry typed transient failures only; do not retry interruption or defects.

## Managed Runtimes

Create a ManagedRuntime only when repeated Promise/callback entry points need one shared built Layer graph. Its owner must be explicit, and that owner must dispose it exactly once or idempotently at shutdown.

Do not construct runtimes per request, inside service methods, or merely to avoid expressing requirements. Do not let extension reload attachment lifetimes accidentally own process-scoped work.

For Pi, verify ManagedRuntime APIs against the pinned Effect beta before use. A direct scoped `Effect.runPromise` adapter is often clearer for one operation.

## Finalizer Policy

Finalization is uninterruptible by default, but failure policy remains a domain decision.

- Preserve teardown order when one resource depends on another.
- Characterize partial acquisition: if B fails after A succeeds, A must release.
- Make cleanup idempotent when multiple lifecycle paths can converge.
- Report finalizer failures when they affect authority, persistence, or durable resource state.
- Use best-effort cleanup only where the product genuinely accepts degraded teardown.
- Never infer that deleting a registry record disposed the resource it referenced.

## Testing

Use deterministic observations, not sleeps. Cover:

- interruption reaches the underlying operation
- caller abort reason is preserved at Promise egress
- acquisition fails after earlier resources succeed
- finalizers run in order
- abort, unsubscribe, dispose, or close throws
- cleanup repeats safely
- stale fiber completion cannot mutate a newer generation
- abort leaves a reusable resource ready when specified
- close disposes the durable resource exactly once

Read `TESTING.md` for Bun and TestClock patterns and `PI_BOUNDARIES.md` for the repository ownership matrix.
