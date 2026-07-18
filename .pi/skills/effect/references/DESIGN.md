# Domain-First Effect Design

Use this when shaping an Effect module, reviewing code quality, choosing primitives, defining errors, or deciding whether Effect belongs at all.

## Start With The Domain

Do not begin with `Effect.gen`. First model distinctions that prevent operational mistakes.

Spend types on values that can realistically be confused, persisted incorrectly, or passed across boundaries:

- worker, owner, session, wave, message, workspace, and model IDs
- validated URLs and normalized addresses
- positive counts, capacities, and durations
- timestamps with different meanings
- currencies, units, and provider-specific references

Do not brand every string. A brand should remove a real class of mistakes.

Decode untrusted input once:

```text
unknown input -> decode and normalize -> valid domain value -> application workflow
```

Once a value exists as a validated domain type, internal functions should not re-check its boundary invariants. Read `SCHEMA.md` for the repository's Schema forms.

## Use Effect Around Behavior

Keep a computation as plain TypeScript when it is pure, synchronous, total, and dependency-free.

Effect earns its weight when behavior has meaningful:

- expected failure
- dependencies
- concurrency or coordination
- cancellation
- resource ownership
- retries or scheduling
- observability
- boundary validation

```ts
const workerLabel = (worker: Worker): string => `${worker.name} (${worker.state})`

const loadWorker = Effect.fn("Worker.load")(function* (id: WorkerId) {
  const store = yield* WorkerStore.Service
  return yield* store.findById(id)
})
```

Do not wrap `workerLabel` in `Effect.succeed`. Call pure logic from an Effect workflow when needed.

## Services Are Domain Capabilities

A service represents a coherent thing the application can do. Its name should belong in the product glossary.

Good examples:

- `WorkerStore`
- `Catalog`
- `ChildSession`
- `Delivery`
- `ModelProvider`
- `Telemetry`

Avoid generic `Utils`, `Manager`, or `Helpers` services containing unrelated operations.

Keep interfaces small and semantic. Expose `findWorker`, `deliverSettlement`, or `runGeneration`, not raw SQL, SDK clients, connections, or transport request methods. The implementation owns those mechanics.

Use a service when a capability performs effects, owns resources, varies by environment, has multiple implementations, or needs narrow substitution in tests. Keep static values and configured per-operation policy as ordinary values.

## Errors Follow The Abstraction

Expected failures deserve names. Defects do not.

Expected failures include invalid boundary input, missing records, rejected actions, denied authority, provider rejection, and known timeouts. Violated invariants and impossible internal states may remain defects.

A service should expose errors its callers can usefully distinguish. Translate low-level failures near the adapter boundary:

```text
SocketError | JsonError | DriverError
                  -> WorkerStore.ReadError
```

Preserve useful diagnostic evidence in the translated error, including an operation label and safe identifiers. Do not leak secrets or private payloads.

Catch errors only where the current layer has a truthful response: retry, fallback, transport mapping, item isolation, or deliberate degradation. Otherwise preserve the failure.

## Dependencies Emerge From Use

Request shared capabilities at the workflow location that uses them:

```ts
const runGeneration = Effect.fn("Worker.runGeneration")(function* (input: GenerationInput) {
  const sessions = yield* ChildSession.Service
  const delivery = yield* Delivery.Service

  const result = yield* sessions.run(input)
  yield* delivery.publish(result)
  return result
})
```

Do not manually thread stable application capabilities through every function. Do pass configured values—selected worker definitions, model choices, catalog snapshots, request policy, and IDs—as ordinary arguments.

Select implementations at the executable boundary. Deep `Effect.provide(...)` calls conceal authority and hard-code production behavior.

## Workflows And Transformations

Use `Effect.gen` when the operation tells a sequential domain story. Use pipelines for local transformation, classification, retry, logging, or recovery.

Prefer a named domain helper over compressed combinator cleverness when it makes branches and failures easier to scan. Effect code should read like application code, not an encoding exercise.

Use `Effect.fn("Domain.operation")` for:

- public service methods
- provider and persistence calls
- jobs and workflows
- tool execution
- expensive or failure-prone internal operations

Use `Effect.fnUntraced` only for internal helpers where trace and stack metadata are intentionally unnecessary. Do not label trivial pure arithmetic or every local expression.

## Coordination And Lifetime

Choose the primitive that states the protocol:

- `Deferred`: create now, complete exactly once later
- `Queue`: producer/consumer handoff
- `PubSub`: every subscriber receives events
- `SubscriptionRef`: current value plus updates
- `Ref`: concurrent state
- `Semaphore`: bounded access
- `Fiber`: managed concurrent computation
- `Stream`: multiple values over time
- `Scope`: resource lifetime and finalization

Do not recreate these with booleans, callback arrays, event emitters, manual Promise resolvers, or maps of unowned work.

Long duration is not automatically slowness. A fiber awaiting a `Deferred`, queue item, schedule, or stream element is suspended. Performance review should ask:

- Is it doing work or waiting semantically?
- Which resource remains held while it waits?
- Can it be interrupted?
- Which scope or owner controls it?

Whenever code opens a socket, session, subscription, listener, timer, child process, or file handle, make ownership and closure explicit. Use `acquireRelease`, scoped layers, or another lifetime that matches the product owner—not merely the nearest function.

## Interruption And Promise Boundaries

Interruption must reach the underlying operation. Forward the signal provided by `Effect.tryPromise` to APIs that support cancellation.

At a public Promise boundary, also preserve the caller's original `AbortSignal.reason`; Effect interruption alone does not guarantee the same rejection identity.

Keep the shape simple:

```text
Promise/callback framework -> one adapter -> Effect core -> one adapter -> framework
```

Avoid calling `Effect.runPromise` from inside Effect services or repeatedly converting between Promise and Effect.

## Observability

Operation names should form a readable trace narrative:

```text
Worker.runGeneration
  WorkerSession.create
  WorkerSession.prompt
  Delivery.publish
```

Add bounded domain context to spans and logs when it explains the path:

- worker, owner, wave, or session ID
- provider and model
- operation or tool name
- item count, queue depth, retry count, or cache result

Do not log full prompts, credentials, private messages, or unbounded payloads. Use `Effect.fn` names and spans in addition to error operation labels, not instead of them.

## Mechanical Consistency

Choose a repeatable module shape and use it throughout a bounded codebase. A typical application module exposes:

1. domain schemas and interfaces
2. typed errors
3. service interface and tag
4. live Layer
5. intentional test Layer or fake
6. a narrow public module surface

Consistency helps readers scan, makes anomalies visible in review, and lets agents propagate an established design without inventing new architecture.

Handcraft and carefully verify the first few canonical modules. Subsequent conversions should name those examples, preserve behavior, and prohibit new abstractions.

## Review Questions

For each non-trivial operation, verify:

- Domain: are meaningful distinctions represented, and was unknown input decoded once?
- Return: is the success value precise?
- Failure: are expected errors named at the caller's abstraction level?
- Requirements: are capabilities visible without deep provisioning?
- Lifetime: who owns and closes every resource and background fiber?
- Interruption: does cancellation reach external work and preserve boundary semantics?
- Test: can a narrow external capability be replaced while the real workflow runs?
- Telemetry: does the operation have a useful name and bounded domain context?

If the answer requires broad repository archaeology, the local design is hiding too much.
