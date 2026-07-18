# Testing Effect Code With Bun

Use this when testing Effect workflows, time, schedules, retries, concurrency, resources, services, config, or fakes in this repository.

## Repository Default

Keep `bun:test`. Run the Effect from an async Bun test boundary.

```ts
import { expect, test } from "bun:test"
import { Effect } from "effect"

test("finds a worker", async () => {
  const worker = await Effect.runPromise(
    findWorker(workerId).pipe(Effect.provide(WorkerStore.testLayer)),
  )

  expect(worker.id).toBe(workerId)
})
```

Do not add another test runner solely to obtain an Effect-specific test wrapper. A small project-local helper can remove repeated `Effect.runPromise` boilerplate if the repository develops enough Effect-heavy tests, but it should preserve Bun's test lifecycle and reporting.

## Defaults

- Test the real workflow and replace the narrow external capability.
- Use explicit test Layers and `ConfigProvider` instead of mutating globals.
- Use `TestClock` for sleeps, schedules, retries, leases, cache TTLs, and timeouts.
- Fork sleeping or scheduled effects before advancing virtual time.
- Synchronize concurrent tests with `Deferred`, `Queue`, `Latch`, `Ref`, or explicit hooks.
- Assert typed failures, interruption, finalization, retry bounds, idempotency, concurrency laws, ownership, and malformed persistence where relevant.
- Use real time only when integration with the live platform clock is itself the behavior under test.

## Fakes Over Shallow Mocks

Fake the narrow expensive or nondeterministic capability, not the application behavior under test.

For an orchestration workflow, fake the child model/session/provider boundary while running real admission, state transitions, observation, settlement, and delivery logic. A useful fake preserves the boundary's protocol—streaming, failures, interruption, or retained identity—instead of returning an oversimplified scalar.

Use `Layer.succeed` for a complete static fake. Use `Layer.effectContext` when one stateful test object should satisfy both the production service tag and a test control tag. Use `Layer.mock` only for tiny local partial mocks where omitted members should fail loudly.

## First-Class Stateful Test Services

Expose production behavior through the real service tag and controls through a separate test tag.

```ts
export interface TestInterface extends Interface {
  readonly sentMessages: () => Effect.Effect<ReadonlyArray<Message>>
  readonly failNextSend: (error: SendError) => Effect.Effect<void>
}

export class TestService extends Context.Service<TestService, TestInterface>()(
  "@app/Notifier/Test",
) {}

export const testLayer = Layer.effectContext(
  Effect.gen(function* () {
    const sent = yield* Ref.make<ReadonlyArray<Message>>([])
    const nextFailure = yield* Ref.make<Option.Option<SendError>>(Option.none())

    const service = TestService.of({
      send: Effect.fn("Notifier.Test.send")(function* (message) {
        const failure = yield* Ref.getAndSet(nextFailure, Option.none())
        if (Option.isSome(failure)) return yield* Effect.fail(failure.value)
        yield* Ref.update(sent, (messages) => [...messages, message])
      }),
      sentMessages: Effect.fn("Notifier.Test.sentMessages")(function* () {
        return yield* Ref.get(sent)
      }),
      failNextSend: Effect.fn("Notifier.Test.failNextSend")(function* (error) {
        yield* Ref.set(nextFailure, Option.some(error))
      }),
    })

    return Context.empty().pipe(
      Context.add(Service, service),
      Context.add(TestService, service),
    )
  }),
)
```

The same object backs both tags. Production code depends only on `Service`; tests use `TestService` for control and inspection.

## Synchronization Instead Of Sleeps

Use primitives that expose the protocol deterministically:

- `Deferred`: readiness, one-time completion, or release signal
- `Queue`: test-driven inputs or observed outputs
- `Latch`: reusable open/close gate
- `Ref`: observation state when no handoff is required
- explicit hooks: a production boundary already has a meaningful deterministic event

```ts
test("publishes exactly once", async () => {
  const program = Effect.scoped(
    Effect.gen(function* () {
      const published = yield* Queue.unbounded<Message>()
      const ready = yield* Deferred.make<void>()

      yield* makeWorker({
        onReady: Deferred.succeed(ready, undefined),
        onPublish: (message) => Queue.offer(published, message),
      }).pipe(Effect.forkScoped)

      yield* Deferred.await(ready)
      return yield* Queue.take(published)
    }),
  )

  expect(await Effect.runPromise(program)).toEqual(expectedMessage)
})
```

Do not add arbitrary `Effect.sleep` or Promise timeouts to make a race "probably ready." If the test cannot observe readiness, improve the fake or boundary.

## Virtual Time

Import the pinned v4 testing API from `effect/testing` and verify its declarations before use.

```ts
import { TestClock } from "effect/testing"

const program = Effect.gen(function* () {
  const fiber = yield* operationWithRetry.pipe(Effect.fork)
  yield* TestClock.adjust("5 seconds")
  return yield* Fiber.join(fiber)
}).pipe(Effect.provide(TestClock.layer()))
```

Advance time only after the effect that sleeps has started. When necessary, use a `Deferred` or `Latch` to prove readiness before adjusting the clock.

Assert exact schedule semantics: the source effect runs once before stepping the schedule, and `Schedule.recurs(n)` permits `n` additional attempts.

## Config In Tests

Use `ConfigProvider.layer(ConfigProvider.fromUnknown(...))` when the test should exercise Config decoding.

Use `Layer.succeed(AppConfiguration.Service, config)` when the application already wraps decoded config in a service and the test does not need to exercise environment parsing.

Never mutate `process.env` concurrently across tests when a provider or service layer can express the input locally.

## Resource And Interruption Tests

For scoped resources and Promise adapters, test more than the success path:

- acquisition fails after an earlier resource succeeded
- interruption reaches the underlying API
- the caller's `AbortSignal.reason` is preserved at the Promise boundary
- finalizers run in the intended order
- unsubscribe, abort, dispose, or close fails
- cleanup is repeated and remains idempotent
- reusable-resource abort and durable close remain distinct

Use explicit observations rather than timing assumptions. Finalizer and ownership bugs are concurrency bugs even when the happy path is synchronous.
