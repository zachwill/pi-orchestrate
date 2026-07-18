# Migrating To Effect

Use this when converting Promise, callback, event-emitter, or manually managed concurrent code to Effect.

## Preserve Behavior First

The first Effect implementation should preserve observable behavior:

- output and serialization shape
- ordering and concurrency limits
- expected error semantics
- retry and timeout policy
- cancellation and abort reason
- resource cleanup and teardown order
- logging or delivery boundaries relied on by callers

Do not simultaneously rename the domain, redesign persistence, alter retries, reorder work, and optimize concurrency. Behavioral parity keeps regressions attributable. Improve the model in a separate deliberate change unless the user explicitly requests a combined redesign.

## Establish One New Source Of Truth

Move authoritative data definitions into the preferred model, then derive temporary compatibility representations from it.

```text
Effect Schema -> temporary legacy validator or transport adapter
```

Avoid maintaining independent Effect Schema, legacy validator, TypeScript interface, and wire definitions. Compatibility adapters should be one-way and deletable.

Decode at the old/new boundary. New internal code should receive validated domain values rather than legacy unknowns or raw strings.

## Keep One Interop Boundary

Prefer this shape:

```text
legacy Promise/callback world
        -> adapter
Effect application core
        -> adapter
legacy framework boundary
```

At ingress, wrap external Promise APIs with `Effect.tryPromise`, classify errors, and forward interruption signals. At egress, run the complete provided Effect once.

Do not call `Effect.runPromise` inside a service implementation or wrap an Effect-returning function in another Promise adapter. Promise–Effect ping-pong obscures errors, requirements, lifetime, and interruption.

## Convert Capabilities, Not Files Mechanically

Identify the narrow behavior boundary first:

- persistence
- provider or SDK
- child session
- file system
- transport
- telemetry

Give that capability a semantic service interface and adapt the old implementation behind it. Keep configured request data as function arguments rather than inventing tags for every value.

Migrate workflows to request capabilities where used. Choose live implementations in root Layer wiring rather than deep inside converted modules.

## Replace Manual Protocols With Matching Primitives

Preserve the protocol while replacing its mechanism:

- Promise resolver stored for later -> `Deferred`
- producer/consumer callbacks -> `Queue`
- broadcast event emitter -> `PubSub` or a Stream boundary
- mutable shared state -> `Ref` when Effect owns concurrent access
- concurrency counters -> `Semaphore` or bounded operator concurrency
- setInterval retry/poll loop -> `Schedule`
- open/use/finally-close -> `acquireRelease` or scoped Layer

Do not replace direct synchronous state machinery when Effect adds no lifecycle, interruption, validation, or concurrency value.

## Canonical Examples Before Propagation

Handcraft and review two or three representative modules before broad conversion. They should demonstrate:

- boundary Schema and domain values
- expected typed errors
- semantic service interface
- `Effect.fn` operation names
- live Layer and narrow fake
- root provisioning
- Promise adapter and interruption behavior where relevant

Then instruct agents to follow those exact modules, preserve behavior, and introduce no new abstractions. Mechanical consistency is the goal.

## Migration Tests

Characterize old behavior before or alongside conversion. Cover the places where interop changes semantics:

- malformed input
- expected and infrastructure failures
- caller abort reason
- interruption of underlying work
- partial acquisition and finalizer order
- duplicate completion or stale callback races
- exact retry count and virtual timing
- concurrent ordering and limits

Use the real workflow with a fake external capability. Avoid replacing the behavior under migration with a shallow mock.

## Exit Criteria

A migrated area is complete when:

- one representation owns each domain contract
- internal functions no longer accept unvalidated legacy values
- Promise interop exists only at real framework or SDK boundaries
- no service secretly runs or provides its own application runtime
- errors and requirements are visible in Effect types
- resources and background work have explicit owners
- behavior and interruption tests pass
- temporary adapters are clearly isolated and removable
