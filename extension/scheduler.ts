import { Cause, Context, Effect, FiberMap, FiberSet, Layer } from "effect";

export type WorkflowDefectHandler = (error: unknown) => void;

export interface GenerationSupervisorService<Key = unknown> {
  /** Starts a generation immediately, interrupting and replacing the previous generation at the key. */
  readonly start: (
    key: Key,
    workflow: Effect.Effect<void, never>,
    onDefect: WorkflowDefectHandler,
  ) => void;
  /** Interrupts the current generation at the key and waits for its fiber to settle. */
  readonly remove: (key: Key) => Effect.Effect<void>;
}

export class GenerationSupervisor extends Context.Service<
  GenerationSupervisor,
  GenerationSupervisorService
>()("@zachwill/pi-orchestrate/GenerationSupervisor") {}

export interface CleanupSupervisorService {
  /** Starts a best-effort cleanup Effect in the process-owned FiberSet. */
  readonly supervise: (cleanup: Effect.Effect<void, never>) => void;
  /** Waits until every currently supervised cleanup fiber has settled. */
  readonly awaitEmpty: () => Effect.Effect<void>;
}

export class CleanupSupervisor extends Context.Service<
  CleanupSupervisor,
  CleanupSupervisorService
>()("@zachwill/pi-orchestrate/CleanupSupervisor") {}

/** Process-scoped coordination graph. Its FiberMap and FiberSet live in the root ManagedRuntime scope. */
export const processSupervisorLayer = Layer.effectContext(
  Effect.gen(function* () {
    const generations = yield* FiberMap.make<unknown, void, never>();
    const runGeneration = yield* FiberMap.runtime(generations)<never>();
    const cleanups = yield* FiberSet.make<void, never>();
    const runCleanup = yield* FiberSet.runtime(cleanups)<never>();

    const generationSupervisor = GenerationSupervisor.of({
      start(key, workflow, onDefect) {
        const supervised = workflow.pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) return Effect.void;
            return Effect.sync(() => {
              try {
                onDefect(Cause.squash(cause));
              } catch {
                // Defect reporting must not become another unsupervised defect.
              }
            });
          }),
        );
        runGeneration(key, supervised);
      },
      remove: (key) => FiberMap.remove(generations, key),
    });

    const cleanupSupervisor = CleanupSupervisor.of({
      supervise(cleanup) {
        runCleanup(cleanup.pipe(Effect.catchCause(() => Effect.void)));
      },
      awaitEmpty: () => FiberSet.awaitEmpty(cleanups),
    });

    return Context.empty().pipe(
      Context.add(GenerationSupervisor, generationSupervisor),
      Context.add(CleanupSupervisor, cleanupSupervisor),
    );
  }),
);
