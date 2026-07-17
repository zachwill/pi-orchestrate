import { Cause, Effect, Exit, FiberMap, Scope } from "effect";

export type WorkflowDefectHandler = (error: unknown) => void;

export interface WorkflowScheduler<Key> {
  /** Starts a workflow immediately, interrupting and replacing the previous workflow at the key. */
  start(
    key: Key,
    workflow: () => Promise<void>,
    onDefect: WorkflowDefectHandler,
  ): void;
  /** Interrupts the current workflow at the key and waits for its fiber to settle. */
  remove(key: Key): Promise<void>;
  /** Interrupts every retained workflow and closes the scheduler scope. */
  close(): Promise<void>;
}

class EffectWorkflowScheduler<Key> implements WorkflowScheduler<Key> {
  private readonly scope = Scope.makeUnsafe("parallel");
  private readonly fibers: FiberMap.FiberMap<Key, void, never>;
  private closePromise: Promise<void> | undefined;

  constructor() {
    this.fibers = Effect.runSync(
      Scope.provide(this.scope)(FiberMap.make<Key, void, never>()),
    );
  }

  start(
    key: Key,
    workflow: () => Promise<void>,
    onDefect: WorkflowDefectHandler,
  ): void {
    const supervised = Effect.promise(workflow).pipe(
      Effect.catchCause((cause) => {
        if (!Cause.hasInterruptsOnly(cause)) {
          try {
            onDefect(Cause.squash(cause));
          } catch {
            // Defect reporting must not become another unsupervised defect.
          }
        }
        return Effect.void;
      }),
    );

    Effect.runSync(
      FiberMap.run(this.fibers, key, supervised, { startImmediately: true }),
    );
  }

  async remove(key: Key): Promise<void> {
    await Effect.runPromise(FiberMap.remove(this.fibers, key));
  }

  close(): Promise<void> {
    if (!this.closePromise) {
      this.closePromise = Effect.runPromise(Scope.close(this.scope, Exit.void));
    }
    return this.closePromise;
  }
}

export function createWorkflowScheduler<Key>(): WorkflowScheduler<Key> {
  return new EffectWorkflowScheduler<Key>();
}
