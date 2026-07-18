# Services, Layers, And Operations

Use this when defining service tags, capability interfaces, Layer implementations, runtime wiring, test services, or `Effect.fn` boundaries.

## Services Are Domain Capabilities

A service represents a coherent thing the application can do. Its name should belong in the product glossary: `WorkerStore`, `Catalog`, `Delivery`, `ChildSession`, or `ModelProvider`.

Avoid generic `Utils`, `Manager`, and `Helpers` services containing unrelated operations.

Keep interfaces small and semantic. Expose `findById`, `runGeneration`, or `publishSettlement`, not database connections, SQL strings, raw SDK clients, or transport mechanics. The implementation owns those details.

Use a service when a capability performs effects, owns resources, varies by environment, has multiple implementations, or needs narrow substitution in tests. Keep static constants and configured per-operation data as ordinary values.

## Standard Service Shape

```ts
export interface Interface {
  readonly get: (id: WorkerId) => Effect.Effect<Worker, NotFound | ReadError>
  readonly save: (worker: Worker) => Effect.Effect<void, WriteError>
}

export class Service extends Context.Service<Service, Interface>()(
  "@app/WorkerStore",
) {}
```

Use descriptive service class names instead when the module does not provide a domain namespace:

```ts
export class WorkerStore extends Context.Service<WorkerStore, Interface>()(
  "@app/WorkerStore",
) {}
```

Follow the surrounding module export style. Do not introduce TypeScript `namespace` declarations or self-referential namespace projections merely for organization.

## Live Implementations

Default effectful implementations to `Layer.effect` and return the tag's validated constructor:

```ts
export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient

    const get = Effect.fn("WorkerStore.get")(function* (id: WorkerId) {
      const row = yield* sql.findWorker(id).pipe(
        readError("WorkerStore.get", { workerId: id }),
      )

      if (row === undefined) {
        return yield* Effect.fail(new NotFound({ workerId: id }))
      }

      return yield* Schema.decodeUnknownEffect(Worker)(row).pipe(
        readError("WorkerStore.decode", { workerId: id }),
      )
    })

    const save = Effect.fn("WorkerStore.save")(function* (worker: Worker) {
      yield* sql.saveWorker(worker).pipe(
        writeError("WorkerStore.save", { workerId: worker.id }),
      )
    })

    return Service.of({ get, save })
  }),
)
```

Choose the constructor that matches acquisition:

```ts
Layer.succeed(Service, impl)       // already-built value
Layer.sync(Service, () => impl)    // lazy synchronous acquisition
Layer.effect(Service, makeEffect)  // effectful acquisition
```

Use `Layer.effectContext` when one acquisition intentionally provides several tags, especially one stateful fake backing production and test-control interfaces. Use `Layer.unwrap` when configuration or runtime discovery selects a Layer.

## Dependencies Emerge From Use

Request shared capabilities where the workflow uses them:

```ts
const runGeneration = Effect.fn("Worker.runGeneration")(function* (input: GenerationInput) {
  const sessions = yield* ChildSession.Service
  const delivery = yield* Delivery.Service

  const result = yield* sessions.run(input)
  yield* delivery.publish(result)
  return result
})
```

Pass selected workers, models, catalog snapshots, IDs, and per-request policy as arguments. These are configured values, not stable shared capabilities.

Do not repeatedly pass stores and adapters positionally through the application. Do not mint a tag for every configured instance.

## Runtime Wiring

Business workflows request capabilities; executable-boundary wiring chooses implementations.

```ts
const InfrastructureLive = Layer.mergeAll(DatabaseLive, TelemetryLive)

const DomainLive = Layer.mergeAll(WorkerStoreLive, DeliveryLive).pipe(
  Layer.provide(InfrastructureLive),
)

const AppLive = SchedulerLive.pipe(Layer.provide(DomainLive))
```

The root graph should reveal the broad production architecture.

- Use `Layer.provide` when an implementation dependency should be hidden.
- Use `Layer.provideMerge` only when that dependency intentionally remains available downstream.
- Use `Layer.mergeAll` for independent services that should all remain exposed.
- Prefer flat, named, topologically understandable subgraphs.
- Keep authority-bearing infrastructure and environment-specific choices explicit at the root.
- Use `Layer.fresh` or local provision only when a test or operation needs isolated acquisition.
- Build and memoize shared clients, caches, and Layers at their owning lifetime, not per operation.

Do not use `provideMerge` or repeated deep `Effect.provide` calls to make types compile. They conceal dependencies and can duplicate resource acquisition.

## Long-Lived Work

A Layer that starts a stream, listener, worker, subscription, or repeat loop must fork that work into the Layer scope. Acquisition itself must finish.

```ts
export const consumerLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const events = yield* Events.Service

    yield* events.stream.pipe(
      Stream.runForEach(handleEvent),
      Effect.forkScoped,
    )
  }),
)
```

Use `Effect.forkScoped` for the ordinary case. If service methods must add work to the Layer lifetime, capture `Scope.Scope` during acquisition and use `Effect.forkIn(scope)` internally. Do not expose Scope in the public service interface.

Read `RESOURCES_CONCURRENCY.md` before introducing FiberSet, FiberMap, ManagedRuntime, or detached work.

## Effect.fn

Use `Effect.fn("Domain.operation")` for public service methods and meaningful provider, persistence, job, workflow, or tool boundaries.

Extra transforms receive `(effect, ...originalArgs)` and are useful when a whole-function wrapper needs the original arguments:

```ts
const readAttachment = Effect.fn("Attachment.read")(
  function* (ref: AttachmentRef) {
    return yield* api.read(ref)
  },
  (effect, ref) => effect.pipe(
    attachmentError("Attachment.read", { attachmentId: ref.id }),
  ),
)
```

Good transforms include error classification, localized recovery, spans, log annotations, retry, timeout, cleanup, and result mapping.

Keep the generator focused on the workflow. One or two transforms are usually enough. Handle local branches inside the generator rather than building a clever wrapper chain.

Name operations so traces read as a domain narrative. Add bounded identifiers, provider/model, item counts, retry counts, or cache outcomes when useful; never attach secrets or unbounded private payloads.

## Error Translation

Map infrastructure failures into errors at the service abstraction level. Callers should not need to distinguish socket, JSON, pool, and driver errors when their only truthful response is `WorkerStore.ReadError`.

For repeated operation-labeled mappings, prefer a shared curried helper:

```ts
const readError = operationError(ReadError.make)

const row = yield* query.pipe(
  readError("WorkerStore.get", { workerId }),
)
```

Use Effect operation names and spans in addition to error operation labels, not instead of them. Preserve only safe diagnostic evidence.

## Test Services

Build a real fake for the narrow capability. Use `Layer.succeed` for complete static fakes and `Layer.effectContext` for stateful fakes with a separate test-control tag. The same implementation object should satisfy both the production tag and test tag.

Production code depends only on the production service. Tests use the control tag to inspect outputs, enqueue responses, or fail the next operation. Read `TESTING.md` for the full pattern.
