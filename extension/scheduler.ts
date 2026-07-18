import { Cause, Context, Effect, FiberMap, Layer, ManagedRuntime } from "effect";

export type WorkflowDefectHandler = (error: unknown) => void;

export interface WorkflowScheduler<Key> {
  /** Starts a workflow immediately, interrupting and replacing the previous workflow at the key. */
  start(
    key: Key,
    workflow: Effect.Effect<void, never>,
    onDefect: WorkflowDefectHandler,
  ): void;
  /** Interrupts the current workflow at the key and waits for its fiber to settle. */
  remove(key: Key): Promise<void>;
  /** Interrupts every retained workflow and closes the scheduler scope. */
  close(): Promise<void>;
}

interface WorkflowSupervisorService {
  readonly start: (
    key: unknown,
    workflow: Effect.Effect<void, never>,
  ) => void;
  readonly remove: (key: unknown) => Effect.Effect<void>;
}

class WorkflowSupervisor extends Context.Service<
  WorkflowSupervisor,
  WorkflowSupervisorService
>()("@zachwill/pi-orchestrate/WorkflowSupervisor") {}

const workflowSupervisorLayer = Layer.effect(
  WorkflowSupervisor,
  Effect.gen(function* () {
    const fibers = yield* FiberMap.make<unknown, void, never>();
    const run = yield* FiberMap.runtime(fibers)<never>();
    return WorkflowSupervisor.of({
      start(key, workflow) {
        run(key, workflow);
      },
      remove: (key) => FiberMap.remove(fibers, key),
    });
  }),
);

class EffectWorkflowScheduler<Key> implements WorkflowScheduler<Key> {
  private readonly managedRuntime = ManagedRuntime.make(workflowSupervisorLayer);
  private readonly supervisor = this.managedRuntime.runSync(WorkflowSupervisor);
  private closePromise: Promise<void> | undefined;

  start(
    key: Key,
    workflow: Effect.Effect<void, never>,
    onDefect: WorkflowDefectHandler,
  ): void {
    const supervised = workflow.pipe(
      Effect.catchCause((cause) => {
        if (!Cause.hasInterruptsOnly(cause)) {
          return Effect.sync(() => {
            try {
              onDefect(Cause.squash(cause));
            } catch {
              // Defect reporting must not become another unsupervised defect.
            }
          });
        }
        return Effect.void;
      }),
    );

    this.supervisor.start(key, supervised);
  }

  remove(key: Key): Promise<void> {
    return this.managedRuntime.runPromise(this.supervisor.remove(key));
  }

  close(): Promise<void> {
    this.closePromise ??= this.managedRuntime.dispose();
    return this.closePromise;
  }
}

export function createWorkflowScheduler<Key>(): WorkflowScheduler<Key> {
  return new EffectWorkflowScheduler<Key>();
}
