import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  FiberSet,
  Layer,
  Schema,
} from "effect";
import {
  CANCELLATION_GRACE_MS,
  EMPTY_WORKER_USAGE,
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  OrchestrateTaskInput,
  RunId,
  WorkerId,
  createRandomIdFactories,
  findWorkerByName,
  isTerminalWorkerStatus,
  transitionWorkerStatus,
  type OrchestrateIdFactories,
  type RunMode,
  type RunRecord,
  type WorkerCatalog,
  type WorkerDefinition,
  type WorkerOutcome,
  type WorkerRecord,
  type WorkerUsage,
} from "./domain.js";
import {
  ChildSessions,
  type ChildSessionsService,
  type WorkerSessionAbortError,
  type WorkerSessionHandle,
  type WorkerSessionObservation,
} from "./worker-session.js";
import type {
  SettlementFailureStage,
  WorkerSettlement,
} from "./worker-settlement.js";

export const MAX_TERMINAL_WORKER_HISTORY = 100;
export const MAX_COMPLETED_RUN_HISTORY = 100;
export const SHUTDOWN_CLEANUP_GRACE_MS = CANCELLATION_GRACE_MS;

export interface OrchestrationContext {
  readonly ownerSessionId: string;
  readonly cwd: string;
  readonly agentDir: string;
  readonly parentSessionFile: string | undefined;
  readonly projectTrusted: boolean;
  readonly catalog: WorkerCatalog;
  readonly parentModel?: Model<Api>;
  readonly modelRegistry: ModelRegistry;
  readonly synthesisGroup?: {
    readonly id: string;
    readonly size: number;
  };
}

export interface AcceptedRun {
  readonly id: RunId;
  readonly workerId: WorkerId;
}

export interface RunResult {
  readonly workerId: WorkerId;
  readonly worker: string;
  readonly title: string;
  readonly status: "completed" | "ready" | "failed" | "aborted";
  readonly outcome: Exclude<WorkerOutcome, { readonly status: "closed" }>;
  readonly usage: WorkerUsage;
  readonly startedAt: number;
  readonly settledAt: number;
  readonly sessionFile: string | undefined;
}

export interface CompletedRun {
  readonly id: RunId;
  readonly ownerSessionId: string;
  readonly mode: RunMode;
  readonly result: RunResult;
}

export interface RuntimeSnapshot {
  readonly runs: readonly RunRecord[];
  readonly workers: readonly WorkerRecord[];
}

export type SettlementListener = (settlement: WorkerSettlement) => void;
export type UnsubscribeSettlement = () => void;
export type StateListener = (snapshot: RuntimeSnapshot) => void;

export interface AbortTarget {
  readonly workerIds?: readonly string[];
  readonly all?: boolean;
}

export interface OrchestrationLayerOptions {
  readonly idFactories?: OrchestrateIdFactories;
}

const OrchestrationOperation = Schema.Literals([
  "orchestrate",
  "sendInteractive",
  "abort",
  "closeInteractive",
  "snapshot",
]);
type OrchestrationOperation = typeof OrchestrationOperation.Type;

const OrchestrationRejectionReason = Schema.Literals([
  "shutdown",
  "validation",
  "ownership",
  "target",
  "worker-state",
  "unknown-worker",
  "model-unavailable",
]);
type OrchestrationRejectionReason = typeof OrchestrationRejectionReason.Type;

export class OrchestrationActionRejected extends Schema.TaggedError<OrchestrationActionRejected>()(
  "Orchestration.ActionRejected",
  {
    operation: OrchestrationOperation,
    reason: OrchestrationRejectionReason,
    message: Schema.String,
  },
) {}

export interface OrchestrationService {
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    onSettlement?: SettlementListener,
  ): Effect.Effect<CompletedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "async",
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "inline",
    onSettlement?: SettlementListener,
  ): Effect.Effect<CompletedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected>;
  readonly abort: (
    ownerSessionId: string,
    target: AbortTarget,
  ) => Effect.Effect<void, OrchestrationActionRejected>;
  readonly closeInteractive: (
    ownerSessionId: string,
    workerId: string,
  ) => Effect.Effect<void, OrchestrationActionRejected>;
  readonly snapshot: (
    ownerSessionId: string,
  ) => Effect.Effect<RuntimeSnapshot, OrchestrationActionRejected>;
  readonly subscribeSettlement: (
    listener: SettlementListener,
  ) => UnsubscribeSettlement;
  readonly subscribeState: (
    ownerSessionId: string,
    listener: StateListener,
  ) => () => void;
  readonly shutdown: () => Effect.Effect<void>;
}

export class Orchestration extends Context.Service<Orchestration, OrchestrationService>()(
  "@zachwill/pi-orchestrate/Orchestration",
) {}

interface RuntimeWorker {
  readonly record: WorkerRecord;
  readonly context: OrchestrationContext;
  readonly definition: WorkerDefinition;
  // Worker-local authority fence; stale asynchronous callbacks must revalidate it.
  readonly generation: number;
  readonly session?: WorkerSessionHandle;
  readonly observationRelease?: () => void;
  readonly cancellation?: Deferred.Deferred<void>;
}

interface RunningRuntimeRun {
  readonly _tag: "running";
  readonly record: RunRecord;
  readonly completion: Deferred.Deferred<CompletedRun>;
  readonly settlementListener?: SettlementListener;
}

type RuntimeRun = RunningRuntimeRun | RunRecord;
type RuntimeLifecycle = "open" | "shutting-down" | "shutdown";

interface RuntimeState {
  workers: Map<WorkerId, RuntimeWorker>;
  runs: Map<RunId, RuntimeRun>;
  terminalWorkerOrder: WorkerId[];
  completedRunOrder: RunId[];
  settlementSequence: number;
  lifecycle: RuntimeLifecycle;
}

type CommittedAction = () => void;
type PostCommitAction = (
  state: RuntimeState,
  settlementListeners: ReadonlySet<SettlementListener>,
  stateListeners: ReadonlyMap<string, ReadonlySet<StateListener>>,
) => CommittedAction;

interface TransactionMutation<A> {
  readonly value: A;
  readonly actions?: readonly PostCommitAction[];
}

type Decision<A> =
  | {
      readonly _tag: "accepted";
      readonly value: A;
    }
  | {
      readonly _tag: "rejected";
      readonly error: OrchestrationActionRejected;
    };

class StatefulOrchestration implements OrchestrationService {
  private readonly actionQueue: CommittedAction[] = [];
  private readonly settlementListeners = new Set<SettlementListener>();
  private readonly stateListeners = new Map<string, Set<StateListener>>();
  private drainingActions = false;
  private state: RuntimeState;

  constructor(
    private readonly childSessions: ChildSessionsService,
    private readonly generations: FiberMap.FiberMap<WorkerId, void, never>,
    private readonly runGeneration: (
      key: WorkerId,
      effect: Effect.Effect<void, never>,
    ) => Fiber.Fiber<void, never>,
    private readonly cancellations: FiberSet.FiberSet<void, never>,
    private readonly runCancellation: (
      effect: Effect.Effect<void, never>,
    ) => Fiber.Fiber<void, never>,
    private readonly cleanups: FiberSet.FiberSet<void, never>,
    private readonly runCleanup: (
      effect: Effect.Effect<void, never>,
    ) => Fiber.Fiber<void, never>,
    private readonly clock: Clock.Clock,
    private readonly idFactories: OrchestrateIdFactories,
    private readonly shutdownCompletion: Deferred.Deferred<void>,
  ) {
    this.state = initialState();
  }

  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    onSettlement?: SettlementListener,
  ): Effect.Effect<CompletedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected> {
    return Effect.fn("Orchestration.orchestrate")(function* (
      this: StatefulOrchestration,
    ) {
      const validated = yield* this.validateTask(context, task, mode);
      const preflight = this.preflightOpen("orchestrate");
      if (preflight._tag === "rejected") yield* Effect.fail(preflight.error);
      // Keep user-supplied ID factories outside the transaction: a reentrant
      // factory cannot commit an outer stale draft. Admission rechecks all
      // authority below, so a race may burn an ID but cannot create a run.
      const runId = this.idFactories.runId();
      const workerId = this.idFactories.workerId();
      const completion = yield* Deferred.make<CompletedRun>();
      const now = this.clock.currentTimeMillisUnsafe();
      const runRecord = makeRunRecord(runId, workerId, context, mode, now);
      const workerRecord = makeWorkerRecord(
        workerId,
        runId,
        context.ownerSessionId,
        validated.definition,
        validated.task,
        now,
      );

      const admission = this.transact((draft) => {
        const open = openDecision(draft, "orchestrate");
        if (open._tag === "rejected") return { value: open };
        if (draft.runs.has(runId)) throw new Error(`Duplicate run ID: ${runId}`);
        if (draft.workers.has(workerId)) {
          throw new Error(`Duplicate worker ID: ${workerId}`);
        }

        draft.runs.set(runId, {
          _tag: "running",
          record: runRecord,
          completion,
          ...(onSettlement ? { settlementListener: onSettlement } : {}),
        });
        draft.workers.set(workerId, {
          record: workerRecord,
          context,
          definition: validated.definition,
          generation: 1,
        });

        return {
          value: accepted(undefined),
          actions: [
            publishState(context.ownerSessionId),
            runAction(() => this.launchBootstrap(workerId, 1)),
          ],
        };
      });
      if (admission._tag === "rejected") return yield* Effect.fail(admission.error);

      if (mode === "inline") {
        return yield* this.awaitInlineRun(runRecord, completion);
      }
      return freezeAcceptedRun(runId, workerId);
    }).call(this);
  }

  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "async",
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "inline",
    onSettlement?: SettlementListener,
  ): Effect.Effect<CompletedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected> {
    return Effect.fn("Orchestration.sendInteractive")(function* (
      this: StatefulOrchestration,
    ) {
      yield* validateContextOwner("sendInteractive", context.ownerSessionId);
      yield* validateMode("sendInteractive", mode);
      yield* validateText(
        "sendInteractive",
        "instructions",
        instructions,
        MAX_WORKER_INSTRUCTIONS_LENGTH,
      );
      const validatedWorkerId = yield* validateWorkerId(
        "sendInteractive",
        workerId,
      );
      const preflight = this.preflightReadyInteractive(
        context.ownerSessionId,
        validatedWorkerId,
      );
      if (preflight._tag === "rejected") yield* Effect.fail(preflight.error);
      // See orchestrate: allocation remains outside copy-on-write reducers so
      // reentrant factories cannot invalidate transaction atomicity.
      const runId = this.idFactories.runId();
      const completion = yield* Deferred.make<CompletedRun>();
      const now = this.clock.currentTimeMillisUnsafe();
      const runRecord: RunRecord = {
        id: runId,
        ownerSessionId: context.ownerSessionId,
        workerId: validatedWorkerId,
        mode,
        state: "running",
        createdAt: now,
      };

      const admission = this.transact((draft) => {
        const open = openDecision(draft, "sendInteractive");
        if (open._tag === "rejected") return { value: open };
        const ownership = ownedWorkerDecision(
          draft,
          "sendInteractive",
          context.ownerSessionId,
          validatedWorkerId,
        );
        if (ownership._tag === "rejected") return { value: ownership };

        const worker = ownership.value;
        if (
          worker.record.lifecycle !== "interactive" ||
          worker.record.status !== "ready" ||
          !worker.session
        ) {
          return {
            value: rejected(
              "sendInteractive",
              "worker-state",
              "interactive_send requires an owned ready interactive worker",
            ),
          };
        }
        if (draft.runs.has(runId)) throw new Error(`Duplicate run ID: ${runId}`);

        const generation = worker.generation + 1;
        const session = worker.session;
        const runningRecord: WorkerRecord = {
          ...transitionWorkerStatus(worker.record, "running"),
          runId,
          instructions,
          activity: undefined,
          messageDirection: "to-model",
          startedAt: now,
          settledAt: undefined,
        };
        draft.runs.set(runId, {
          _tag: "running",
          record: runRecord,
          completion,
          ...(onSettlement ? { settlementListener: onSettlement } : {}),
        });
        draft.workers.set(validatedWorkerId, {
          ...worker,
          record: runningRecord,
          generation,
          observationRelease: undefined,
          cancellation: undefined,
        });

        return {
          value: accepted(undefined),
          actions: [
            runAction(() => {
              safelyCall(worker.observationRelease);
              this.subscribeObservation(validatedWorkerId, generation, session);
            }),
            publishState(context.ownerSessionId),
            runAction(() => {
              this.launchPrompt(
                validatedWorkerId,
                generation,
                session,
                instructions,
              );
            }),
          ],
        };
      });
      if (admission._tag === "rejected") return yield* Effect.fail(admission.error);

      if (mode === "inline") {
        return yield* this.awaitInlineRun(runRecord, completion);
      }
      return freezeAcceptedRun(runId, validatedWorkerId);
    }).call(this);
  }

  abort(
    ownerSessionId: string,
    target: AbortTarget,
  ): Effect.Effect<void, OrchestrationActionRejected> {
    return Effect.fn("Orchestration.abort")(function* (
      this: StatefulOrchestration,
    ) {
      yield* validateContextOwner("abort", ownerSessionId);
      const validatedTarget = yield* validateAbortTarget(target);
      const candidateIds = validatedTarget._tag === "ids"
        ? validatedTarget.workerIds
        : activeWorkerIds(this.current(), ownerSessionId);
      const candidates = yield* makeCancellationCandidates(candidateIds);
      const cancellation = this.beginCancellation(
        "abort",
        ownerSessionId,
        validatedTarget,
        candidates,
      );
      if (cancellation._tag === "rejected") {
        return yield* Effect.fail(cancellation.error);
      }
      yield* awaitAll(cancellation.value);
    }).call(this);
  }

  closeInteractive(
    ownerSessionId: string,
    workerId: string,
  ): Effect.Effect<void, OrchestrationActionRejected> {
    return Effect.fn("Orchestration.closeInteractive")(function* (
      this: StatefulOrchestration,
    ) {
      yield* validateContextOwner("closeInteractive", ownerSessionId);
      const id = yield* validateWorkerId("closeInteractive", workerId);
      const now = this.clock.currentTimeMillisUnsafe();
      const decision = this.transact((draft) => {
        const open = openDecision(draft, "closeInteractive");
        if (open._tag === "rejected") return { value: open };
        const ownership = ownedWorkerDecision(
          draft,
          "closeInteractive",
          ownerSessionId,
          id,
        );
        if (ownership._tag === "rejected") return { value: ownership };
        if (
          ownership.value.record.lifecycle !== "interactive" ||
          ownership.value.record.status !== "ready"
        ) {
          return {
            value: rejected(
              "closeInteractive",
              "worker-state",
              "interactive_close requires an owned ready interactive worker",
            ),
          };
        }
        const actions = this.closeReadyWorker(draft, ownership.value, now);
        actions.push(...stateActionsAfterPrune(draft, ownerSessionId));
        return {
          value: accepted(undefined),
          actions,
        };
      });
      if (decision._tag === "rejected") yield* Effect.fail(decision.error);
    }).call(this);
  }

  snapshot(
    ownerSessionId: string,
  ): Effect.Effect<RuntimeSnapshot, OrchestrationActionRejected> {
    return validateContextOwner("snapshot", ownerSessionId).pipe(
      Effect.andThen(Effect.sync(() => snapshotFor(this.current(), ownerSessionId))),
    );
  }

  subscribeSettlement(listener: SettlementListener): UnsubscribeSettlement {
    if (typeof listener !== "function") {
      throw new Error("Settlement listener must be a function");
    }
    if (this.state.lifecycle !== "open") return noOp;
    this.settlementListeners.add(listener);

    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.settlementListeners.delete(listener);
    };
  }

  subscribeState(ownerSessionId: string, listener: StateListener): () => void {
    if (typeof ownerSessionId !== "string" || ownerSessionId.trim() === "") {
      throw new Error("ownerSessionId must not be blank");
    }
    if (typeof listener !== "function") {
      throw new Error("State listener must be a function");
    }

    if (this.state.lifecycle !== "open") return noOp;
    const ownerListeners = this.stateListeners.get(ownerSessionId) ?? new Set();
    ownerListeners.add(listener);
    this.stateListeners.set(ownerSessionId, ownerListeners);
    this.enqueueActions([
      publishStateTo(ownerSessionId, listener)(
        this.state,
        this.settlementListeners,
        this.stateListeners,
      ),
    ]);

    let active = true;
    return () => {
      if (!active) return;
      active = false;
      ownerListeners.delete(listener);
      if (ownerListeners.size === 0) this.stateListeners.delete(ownerSessionId);
    };
  }

  /** Calling shutdown closes admission synchronously, before the returned Effect runs. */
  shutdown(): Effect.Effect<void> {
    const first = this.transact((draft) => {
      if (draft.lifecycle !== "open") return { value: false };
      draft.lifecycle = "shutting-down";
      return { value: true };
    });
    if (!first) return Deferred.await(this.shutdownCompletion);

    return this.performShutdown().pipe(
      Effect.ensuring(
        Effect.sync(() => {
          this.transact((draft) => {
            draft.lifecycle = "shutdown";
            for (const [runId, run] of draft.runs) {
              if (isRunningRuntimeRun(run) && run.settlementListener) {
                draft.runs.set(runId, { ...run, settlementListener: undefined });
              }
            }
            return { value: undefined };
          });
          this.settlementListeners.clear();
          this.stateListeners.clear();
          Deferred.doneUnsafe(this.shutdownCompletion, Effect.void);
        }),
      ),
    );
  }

  private performShutdown(): Effect.Effect<void> {
    return Effect.fn("Orchestration.shutdown")(function* (
      this: StatefulOrchestration,
    ) {
      const now = this.clock.currentTimeMillisUnsafe();
      this.transact((draft) => {
        const owners = new Set<string>();
        const actions: PostCommitAction[] = [];
        for (const worker of draft.workers.values()) {
          if (
            worker.record.lifecycle === "interactive" &&
            worker.record.status === "ready"
          ) {
            this.closeReadyWorker(draft, worker, now, actions);
            owners.add(worker.record.ownerSessionId);
          }
        }
        addAll(owners, pruneHistory(draft));
        actions.push(...publishOwners(owners));
        return { value: undefined, actions };
      });

      const activeIds = activeWorkerIds(this.current());
      const candidates = yield* makeCancellationCandidates(activeIds);
      const cancellations = this.beginShutdownCancellation(candidates);
      yield* awaitAll(cancellations);
      yield* this.childSessions.shutdown().pipe(Effect.catchCause(() => Effect.void));
      yield* FiberSet.awaitEmpty(this.cleanups).pipe(
        Effect.timeoutOption(SHUTDOWN_CLEANUP_GRACE_MS),
        Effect.ignore,
      );
    }).call(this);
  }

  private validateTask(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
  ): Effect.Effect<
    { definition: WorkerDefinition; task: OrchestrateTaskInput },
    OrchestrationActionRejected
  > {
    return Effect.gen(function* () {
      yield* validateContextOwner("orchestrate", context.ownerSessionId);
      yield* validateMode("orchestrate", mode);
      if (!task || typeof task !== "object" || Array.isArray(task)) {
        return yield* rejectAction(
          "orchestrate",
          "validation",
          "orchestrate requires one task object",
        );
      }
      if (context.synthesisGroup) {
        yield* validateText(
          "orchestrate",
          "synthesis group ID",
          context.synthesisGroup.id,
          MAX_WORKER_TITLE_LENGTH,
        );
        if (mode !== "async") {
          return yield* rejectAction(
            "orchestrate",
            "validation",
            "Sibling synthesis requires an async task",
          );
        }
        if (
          !Number.isSafeInteger(context.synthesisGroup.size) ||
          context.synthesisGroup.size < 2
        ) {
          return yield* rejectAction(
            "orchestrate",
            "validation",
            "Synthesis group size must be an integer of at least 2",
          );
        }
      }
      yield* validateText(
        "orchestrate",
        "worker",
        task.worker,
        MAX_WORKER_TITLE_LENGTH,
      );
      yield* validateText(
        "orchestrate",
        "title",
        task.title,
        MAX_WORKER_TITLE_LENGTH,
      );
      yield* validateText(
        "orchestrate",
        "instructions",
        task.instructions,
        MAX_WORKER_INSTRUCTIONS_LENGTH,
      );
      const definition = findWorkerByName(context.catalog, task.worker);
      if (!definition) {
        return yield* rejectAction(
          "orchestrate",
          "unknown-worker",
          `Unknown worker: ${task.worker}`,
        );
      }
      const configured = definition.model;
      if (!configured && !context.parentModel) {
        return yield* rejectAction(
          "orchestrate",
          "model-unavailable",
          `Worker "${definition.name}" has no configured model and no parent model is available`,
        );
      }
      if (
        configured &&
        !context.modelRegistry.find(configured.provider, configured.modelId)
      ) {
        return yield* rejectAction(
          "orchestrate",
          "model-unavailable",
          `Worker "${definition.name}" configured model "${configured.provider}/${configured.modelId}" was not found`,
        );
      }
      return { definition, task };
    });
  }

  private launchBootstrap(workerId: WorkerId, generation: number): void {
    this.launchGeneration(
      workerId,
      generation,
      this.bootstrapAndPrompt(workerId, generation),
    );
  }

  private launchPrompt(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    instructions: string,
  ): void {
    this.launchGeneration(
      workerId,
      generation,
      this.executePrompt(workerId, generation, session, instructions),
    );
  }

  private launchGeneration(
    workerId: WorkerId,
    generation: number,
    workflow: Effect.Effect<void, never>,
  ): void {
    try {
      const fiber = this.runGeneration(
        workerId,
        workflow.pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) return Effect.void;
            return Effect.sync(() => {
              this.settleWorkflowDefect(workerId, generation, Cause.squash(cause));
            });
          }),
        ),
      );
      const exit = fiber.pollUnsafe();
      // A closed FiberMap rejects admission with an interrupted sentinel.
      if (exit && Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
        this.settleGenerationLaunchFailure(workerId, generation);
      }
    } catch (error) {
      this.settleWorkflowDefect(workerId, generation, error);
    }
  }

  private settleGenerationLaunchFailure(
    workerId: WorkerId,
    generation: number,
  ): void {
    this.settleActiveWorker(
      workerId,
      generation,
      ["starting", "running"],
      "failed",
      {
        status: "failed",
        message: "Worker generation could not start because orchestration is closed",
      },
      "workflow",
      true,
    );
  }

  private bootstrapAndPrompt(
    workerId: WorkerId,
    generation: number,
  ): Effect.Effect<void, never> {
    return Effect.gen({ self: this }, function* () {
      const session = yield* this.bootstrap(workerId, generation);
      if (!session) return;
      const worker = this.current().workers.get(workerId);
      if (!worker) return;
      yield* this.executePrompt(
        workerId,
        generation,
        session,
        worker.record.instructions,
      );
    });
  }

  private bootstrap(
    workerId: WorkerId,
    generation: number,
  ): Effect.Effect<WorkerSessionHandle | undefined, never> {
    return Effect.suspend(() => {
      const expected = this.current().workers.get(workerId);
      if (!expected || expected.generation !== generation) {
        return Effect.succeed(undefined);
      }
      return this.childSessions.acquire(
        {
          cwd: expected.context.cwd,
          agentDir: expected.context.agentDir,
          parentSessionFile: expected.context.parentSessionFile,
          projectTrusted: expected.context.projectTrusted,
          definition: expected.definition,
          parentModel: expected.context.parentModel,
          modelRegistry: expected.context.modelRegistry,
        },
        (session) => this.adoptCreatedSession(workerId, generation, session),
      ).pipe(
        Effect.match({
          onFailure: (error) => {
            this.settleCreationFailure(workerId, generation, error);
            return undefined;
          },
          onSuccess: (session) => session,
        }),
      );
    });
  }

  private adoptCreatedSession(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
  ): WorkerSessionHandle | undefined {
    let release: () => void;
    try {
      release = session.subscribeObservation((observation) => {
        this.updateObservation(workerId, generation, session, observation);
      });
    } catch (error) {
      this.settleCreationFailure(workerId, generation, error);
      return undefined;
    }

    const adopted = this.transact((draft) => {
      const worker = draft.workers.get(workerId);
      if (
        draft.lifecycle !== "open" ||
        !worker ||
        worker.generation !== generation ||
        worker.record.status !== "starting"
      ) {
        return {
          value: false,
          actions: [runAction(() => safelyCall(release))],
        };
      }
      draft.workers.set(workerId, {
        ...worker,
        session,
        observationRelease: release,
        record: {
          ...transitionWorkerStatus(worker.record, "running"),
          sessionFile: session.sessionFile,
        },
      });
      return {
        value: true,
        actions: [publishState(worker.record.ownerSessionId)],
      };
    });
    return adopted ? session : undefined;
  }

  private subscribeObservation(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
  ): void {
    let release: () => void;
    try {
      release = session.subscribeObservation((observation) => {
        this.updateObservation(workerId, generation, session, observation);
      });
    } catch (error) {
      this.settleWorkflowDefect(workerId, generation, error);
      return;
    }

    this.transact((draft) => {
      const worker = draft.workers.get(workerId);
      if (
        !worker ||
        worker.generation !== generation ||
        worker.session !== session ||
        worker.record.status !== "running"
      ) {
        return {
          value: undefined,
          actions: [runAction(() => safelyCall(release))],
        };
      }
      draft.workers.set(workerId, { ...worker, observationRelease: release });
      return { value: undefined };
    });
  }

  private executePrompt(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    instructions: string,
  ): Effect.Effect<void, never> {
    return Effect.suspend(() => {
      const worker = this.current().workers.get(workerId);
      if (
        !worker ||
        worker.record.status !== "running" ||
        worker.generation !== generation ||
        worker.session !== session
      ) {
        return Effect.void;
      }
      return session.prompt(instructions).pipe(
        Effect.match({
          onFailure: (error): WorkerOutcome => ({
            status: "failed",
            message: describeError(error, "Worker prompt failed"),
          }),
          onSuccess: (outcome) => outcome,
        }),
        Effect.tap((outcome) => Effect.sync(() => {
          this.settleOutcome(workerId, generation, session, outcome);
        })),
        Effect.asVoid,
      );
    });
  }

  private settleCreationFailure(
    workerId: WorkerId,
    generation: number,
    error: unknown,
  ): void {
    this.settleActiveWorker(
      workerId,
      generation,
      ["starting"],
      "failed",
      {
        status: "failed",
        message: describeError(error, "Worker session creation failed"),
      },
      "startup",
      false,
    );
  }

  private settleWorkflowDefect(
    workerId: WorkerId,
    generation: number,
    error: unknown,
  ): void {
    this.settleActiveWorker(
      workerId,
      generation,
      ["starting", "running", "stopping"],
      "failed",
      {
        status: "failed",
        message: describeError(error, "Worker workflow failed"),
      },
      "workflow",
      true,
    );
  }

  private settleOutcome(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    outcome: WorkerOutcome,
  ): void {
    const worker = this.current().workers.get(workerId);
    if (
      !worker ||
      worker.generation !== generation ||
      worker.session !== session ||
      worker.record.status !== "running"
    ) {
      return;
    }

    let settledOutcome = outcome;
    let status: "ready" | "completed" | "failed" | "aborted";
    if (outcome.status === "failed" || outcome.status === "aborted") {
      status = outcome.status;
    } else if (
      worker.record.lifecycle === "interactive" &&
      outcome.status === "ready"
    ) {
      status = "ready";
    } else if (
      worker.record.lifecycle === "one-shot" &&
      outcome.status === "completed"
    ) {
      status = "completed";
    } else {
      status = "failed";
      settledOutcome = {
        status: "failed",
        message: `Worker session returned ${outcome.status} for a ${worker.record.lifecycle} worker`,
      };
    }
    this.settleActiveWorker(
      workerId,
      generation,
      ["running"],
      status,
      settledOutcome,
      status === "failed" ? "prompt" : undefined,
      status !== "ready",
    );
  }

  private settleActiveWorker(
    workerId: WorkerId,
    generation: number,
    expectedStatuses: readonly WorkerRecord["status"][],
    status: "ready" | "completed" | "failed" | "aborted",
    outcome: WorkerOutcome,
    failureStage: SettlementFailureStage | undefined,
    dispose: boolean,
  ): void {
    const settledAt = this.clock.currentTimeMillisUnsafe();
    this.transact((draft) => {
      const worker = draft.workers.get(workerId);
      if (
        !worker ||
        worker.generation !== generation ||
        !expectedStatuses.includes(worker.record.status)
      ) {
        return { value: undefined };
      }

      const settledRecord: WorkerRecord = {
        ...transitionWorkerStatus(worker.record, status),
        activity: undefined,
        outcome: copyOutcome(outcome),
        settledAt,
      };
      const actions: PostCommitAction[] = [];
      let settledWorker: RuntimeWorker = { ...worker, record: settledRecord };
      if (dispose) {
        actions.push(...this.releaseWorkerResources(settledWorker));
        settledWorker = {
          ...settledWorker,
          session: undefined,
          observationRelease: undefined,
        };
      }
      draft.workers.set(workerId, settledWorker);
      const settlement = makeSettlement(
        draft,
        settledWorker,
        settledAt,
        failureStage,
      );
      actions.push(...completeRun(draft, workerId));
      if (settlement) actions.push(settlement);
      if (isTerminalWorkerStatus(status)) rememberTerminalWorker(draft, workerId);
      actions.push(...stateActionsAfterPrune(draft, worker.record.ownerSessionId));
      return { value: undefined, actions };
    });
  }

  private updateObservation(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    observation: WorkerSessionObservation,
  ): void {
    this.transact((draft) => {
      const worker = draft.workers.get(workerId);
      if (
        !worker ||
        worker.generation !== generation ||
        worker.session !== session ||
        (worker.record.status !== "starting" && worker.record.status !== "running")
      ) {
        return { value: undefined };
      }
      const nextRecord: WorkerRecord = {
        ...worker.record,
        usage: copyUsage(observation.usage),
        activity: observation.activity,
        messageDirection: observation.messageDirection,
      };
      if (observationsEqual(worker.record, nextRecord)) {
        return { value: undefined };
      }
      draft.workers.set(workerId, { ...worker, record: nextRecord });
      return {
        value: undefined,
        actions: [publishState(worker.record.ownerSessionId)],
      };
    });
  }

  private beginCancellation(
    operation: "abort",
    ownerSessionId: string,
    target: ValidatedAbortTarget,
    candidates: ReadonlyMap<WorkerId, Deferred.Deferred<void>>,
  ): Decision<readonly Deferred.Deferred<void>[]> {
    return this.transact((draft) => {
      const open = openDecision(draft, operation);
      if (open._tag === "rejected") return { value: open };
      const workers = target._tag === "all"
        ? [...draft.workers.values()].filter((worker) => (
            worker.record.ownerSessionId === ownerSessionId &&
            isActiveWorkerStatus(worker.record.status)
          ))
        : target.workerIds.map((id) => draft.workers.get(id));

      if (target._tag === "ids") {
        for (let index = 0; index < target.workerIds.length; index += 1) {
          const worker = workers[index];
          if (!worker || worker.record.ownerSessionId !== ownerSessionId) {
            return {
              value: rejected(
                operation,
                "ownership",
                "Worker is not owned by this session",
              ),
            };
          }
          if (worker.record.status === "ready") {
            return {
              value: rejected(
                operation,
                "worker-state",
                "Ready interactive workers are not active; use interactive_close",
              ),
            };
          }
          if (!isActiveWorkerStatus(worker.record.status)) {
            return {
              value: rejected(
                operation,
                "worker-state",
                "worker_abort requires owned active workers",
              ),
            };
          }
        }
      }

      const activeWorkers = workers.filter(isRuntimeWorker);
      const marked = this.markWorkersStopping(draft, activeWorkers, candidates);
      return {
        value: accepted(marked.completions),
        actions: [
          ...marked.actions,
          ...publishOwners(new Set(
            activeWorkers.map((worker) => worker.record.ownerSessionId),
          )),
        ],
      };
    });
  }

  private beginShutdownCancellation(
    candidates: ReadonlyMap<WorkerId, Deferred.Deferred<void>>,
  ): readonly Deferred.Deferred<void>[] {
    return this.transact((draft) => {
      const workers = [...draft.workers.values()].filter((worker) => (
        isActiveWorkerStatus(worker.record.status)
      ));
      const marked = this.markWorkersStopping(draft, workers, candidates);
      return {
        value: marked.completions,
        actions: [
          ...marked.actions,
          ...publishOwners(new Set(
            workers.map((worker) => worker.record.ownerSessionId),
          )),
        ],
      };
    });
  }

  // Commit stopping and one shared completion before post-commit physical abort
  // and generation interruption; every cancellation caller joins it.
  private markWorkersStopping(
    draft: RuntimeState,
    workers: readonly RuntimeWorker[],
    candidates: ReadonlyMap<WorkerId, Deferred.Deferred<void>>,
  ): {
    readonly completions: readonly Deferred.Deferred<void>[];
    readonly actions: readonly PostCommitAction[];
  } {
    const completions: Deferred.Deferred<void>[] = [];
    const actions: PostCommitAction[] = [];
    for (const worker of workers) {
      if (worker.cancellation) {
        completions.push(worker.cancellation);
        continue;
      }
      const completion = candidates.get(worker.record.id);
      if (!completion) {
        throw new Error(`Missing cancellation candidate: ${worker.record.id}`);
      }
      const stoppingRecord = worker.record.status === "stopping"
        ? worker.record
        : {
            ...transitionWorkerStatus(worker.record, "stopping"),
            activity: undefined,
          };
      draft.workers.set(worker.record.id, {
        ...worker,
        record: stoppingRecord,
        cancellation: completion,
      });
      completions.push(completion);
      actions.push(runAction(() => {
        this.launchCancellationOrSettleAfterClosure(worker.record.id, completion);
      }));
    }
    return { completions, actions };
  }

  private launchCancellationOrSettleAfterClosure(
    workerId: WorkerId,
    completion: Deferred.Deferred<void>,
  ): void {
    const fiber = this.runCancellation(this.cancelWorker(workerId, completion));
    const exit = fiber.pollUnsafe();
    // A closed FiberSet rejects admission with an interrupted sentinel.
    if (!exit || Exit.isSuccess(exit) || !Cause.hasInterruptsOnly(exit.cause)) return;

    const worker = this.current().workers.get(workerId);
    if (worker?.record.status === "stopping") {
      this.settleActiveWorker(
        workerId,
        worker.generation,
        ["stopping"],
        "aborted",
        { status: "aborted" },
        "cancellation",
        true,
      );
    }
    this.completeCancellation(workerId, completion);
  }

  private cancelWorker(
    workerId: WorkerId,
    completion: Deferred.Deferred<void>,
  ): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      const session = this.current().workers.get(workerId)?.session;
      if (session) {
        yield* session.abort().pipe(
          Effect.catchTag(
            "WorkerSession.AbortError",
            (_: WorkerSessionAbortError) => Effect.void,
          ),
          Effect.timeoutOption(CANCELLATION_GRACE_MS),
          Effect.ignore,
        );
      }
      const removal = yield* FiberSet.run(
        this.cancellations,
        FiberMap.remove(this.generations, workerId).pipe(
          Effect.catchCause(() => Effect.void),
        ),
      );
      yield* Fiber.await(removal).pipe(
        Effect.timeoutOption(CANCELLATION_GRACE_MS),
        Effect.ignore,
      );
      this.settleActiveWorker(
        workerId,
        this.current().workers.get(workerId)?.generation ?? -1,
        ["stopping"],
        "aborted",
        { status: "aborted" },
        "cancellation",
        true,
      );
    }).pipe(
      Effect.catchCause((cause) => Effect.sync(() => {
        const generation = this.current().workers.get(workerId)?.generation;
        if (generation === undefined) return;
        this.settleActiveWorker(
          workerId,
          generation,
          ["stopping"],
          "failed",
          {
            status: "failed",
            message: describeError(
              Cause.squash(cause),
              "Worker cancellation failed",
            ),
          },
          "cancellation",
          true,
        );
      })),
      Effect.ensuring(
        Effect.sync(() => this.completeCancellation(workerId, completion)),
      ),
    );
  }

  private completeCancellation(
    workerId: WorkerId,
    completion: Deferred.Deferred<void>,
  ): void {
    this.transact((draft) => {
      const worker = draft.workers.get(workerId);
      if (worker?.cancellation === completion) {
        draft.workers.set(workerId, { ...worker, cancellation: undefined });
      }
      return { value: undefined };
    });
    Deferred.doneUnsafe(completion, Effect.void);
  }

  private awaitInlineRun(
    run: RunRecord,
    completion: Deferred.Deferred<CompletedRun>,
  ): Effect.Effect<CompletedRun> {
    return Deferred.await(completion).pipe(
      Effect.onInterrupt(() => this.cancelExactRun(run)),
    );
  }

  private cancelExactRun(run: RunRecord): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      const candidate = yield* Deferred.make<void>();
      const candidates = new Map([[run.workerId, candidate]]);
      const completions = this.transact((draft) => {
        const worker = draft.workers.get(run.workerId);
        if (
          !worker ||
          worker.record.ownerSessionId !== run.ownerSessionId ||
          worker.record.runId !== run.id ||
          !isActiveWorkerStatus(worker.record.status)
        ) {
          return { value: [] as readonly Deferred.Deferred<void>[] };
        }
        const marked = this.markWorkersStopping(draft, [worker], candidates);
        return {
          value: marked.completions,
          actions: [...marked.actions, publishState(run.ownerSessionId)],
        };
      });
      yield* awaitAll(completions);
    });
  }

  private closeReadyWorker(
    draft: RuntimeState,
    worker: RuntimeWorker,
    settledAt: number,
    actions: PostCommitAction[] = [],
  ): PostCommitAction[] {
    const closedRecord: WorkerRecord = {
      ...transitionWorkerStatus(worker.record, "closed"),
      activity: undefined,
      outcome: { status: "closed" },
      settledAt,
    };
    actions.push(...this.releaseWorkerResources(worker));
    draft.workers.set(worker.record.id, {
      ...worker,
      record: closedRecord,
      session: undefined,
      observationRelease: undefined,
    });
    rememberTerminalWorker(draft, worker.record.id);
    return actions;
  }

  private releaseWorkerResources(worker: RuntimeWorker): PostCommitAction[] {
    const actions: PostCommitAction[] = [];
    if (worker.observationRelease) {
      actions.push(runAction(() => safelyCall(worker.observationRelease)));
    }
    const session = worker.session;
    if (session) {
      actions.push(runAction(() => {
        this.launchCleanupOrJoinAfterClosure(
          Effect.suspend(() => session.dispose()).pipe(
            Effect.catchCause(() => Effect.void),
            Effect.uninterruptible,
          ),
        );
      }));
    }
    return actions;
  }

  private launchCleanupOrJoinAfterClosure(cleanup: Effect.Effect<void>): void {
    if (this.cleanups.state._tag === "Closed") {
      Effect.runFork(cleanup);
      return;
    }
    const fiber = this.runCleanup(cleanup);
    const exit = fiber.pollUnsafe();
    if (exit && Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
      // FiberSet.runtime returns an already-interrupted sentinel when closure
      // wins admission. If admission won and closure interrupted the real fiber,
      // repeating the cached uninterruptible disposal only joins that cleanup.
      Effect.runFork(cleanup);
    }
  }

  private preflightOpen(
    operation: OrchestrationOperation,
  ): Decision<void> {
    return this.transact((draft) => ({
      value: openDecision(draft, operation),
    }));
  }

  private preflightReadyInteractive(
    ownerSessionId: string,
    workerId: WorkerId,
  ): Decision<void> {
    return this.transact((draft) => {
      const open = openDecision(draft, "sendInteractive");
      if (open._tag === "rejected") return { value: open };
      const ownership = ownedWorkerDecision(
        draft,
        "sendInteractive",
        ownerSessionId,
        workerId,
      );
      if (ownership._tag === "rejected") return { value: ownership };
      const worker = ownership.value;
      if (
        worker.record.lifecycle !== "interactive" ||
        worker.record.status !== "ready" ||
        !worker.session
      ) {
        return {
          value: rejected(
            "sendInteractive",
            "worker-state",
            "interactive_send requires an owned ready interactive worker",
          ),
        };
      }
      return { value: accepted(undefined) };
    });
  }

  private current(): RuntimeState {
    return this.state;
  }

  private transact<A>(
    reducer: (draft: RuntimeState) => TransactionMutation<A>,
  ): A {
    const draft = makeDraft(this.state);
    const mutation = reducer(draft);
    // Commit before action factories capture the snapshot and callbacks can run.
    this.state = Object.freeze(draft);
    this.enqueueActions((mutation.actions ?? []).map((action) => action(
      this.state,
      this.settlementListeners,
      this.stateListeners,
    )));
    return mutation.value;
  }

  private enqueueActions(actions: readonly CommittedAction[]): void {
    // Reentrant actions queue behind this drain; drain all before rethrowing the first failure.
    this.actionQueue.push(...actions);
    if (this.drainingActions) return;

    this.drainingActions = true;
    let hasFailure = false;
    let firstFailure: unknown;
    try {
      while (this.actionQueue.length > 0) {
        const action = this.actionQueue.shift();
        if (!action) continue;
        try {
          action();
        } catch (error) {
          if (!hasFailure) firstFailure = error;
          hasFailure = true;
        }
      }
    } finally {
      this.drainingActions = false;
    }
    if (hasFailure) throw firstFailure;
  }
}

export function orchestrationLayer(
  options: OrchestrationLayerOptions = {},
): Layer.Layer<Orchestration, never, ChildSessions> {
  return Layer.effect(
    Orchestration,
    Effect.gen(function* () {
      const childSessions = yield* ChildSessions;
      // Scope finalizers run in reverse acquisition order. Keep cleanup open while
      // generation and cancellation interruption settle workers and enqueue disposal.
      const cleanups = yield* FiberSet.make<void, never>();
      const runCleanup = yield* FiberSet.runtime(cleanups)<never>();
      const cancellations = yield* FiberSet.make<void, never>();
      const runCancellation = yield* FiberSet.runtime(cancellations)<never>();
      const generations = yield* FiberMap.make<WorkerId, void, never>();
      const runGeneration = yield* FiberMap.runtime(generations)<never>();
      const clock = yield* Clock.Clock;
      const shutdownCompletion = yield* Deferred.make<void>();
      return Orchestration.of(new StatefulOrchestration(
        childSessions,
        generations,
        runGeneration,
        cancellations,
        runCancellation,
        cleanups,
        runCleanup,
        clock,
        options.idFactories ?? createRandomIdFactories(),
        shutdownCompletion,
      ));
    }),
  );
}

function initialState(): RuntimeState {
  return Object.freeze({
    workers: new Map<WorkerId, RuntimeWorker>(),
    runs: new Map<RunId, RuntimeRun>(),
    terminalWorkerOrder: [],
    completedRunOrder: [],
    settlementSequence: 0,
    lifecycle: "open",
  });
}

function makeDraft(state: RuntimeState): RuntimeState {
  return {
    ...state,
    workers: new Map(state.workers),
    runs: new Map(state.runs),
    terminalWorkerOrder: [...state.terminalWorkerOrder],
    completedRunOrder: [...state.completedRunOrder],
  };
}

function completeRun(
  state: RuntimeState,
  workerId: WorkerId,
): PostCommitAction[] {
  const worker = state.workers.get(workerId);
  if (!worker) return [];
  const run = state.runs.get(worker.record.runId);
  if (!run || !isRunningRuntimeRun(run) || !isCompletedRunWorker(worker.record)) {
    return [];
  }

  const completed = freezeCompletedRun(run.record, worker.record);
  state.runs.set(run.record.id, { ...run.record, state: "complete" });
  state.completedRunOrder.push(run.record.id);
  return [runAction(() => {
    Deferred.doneUnsafe(run.completion, Effect.succeed(completed));
  })];
}

function makeSettlement(
  draft: RuntimeState,
  worker: RuntimeWorker,
  settledAt: number,
  failureStage: SettlementFailureStage | undefined,
): PostCommitAction | undefined {
  const run = draft.runs.get(worker.record.runId);
  if (!run || !isCompletedRunWorker(worker.record)) {
    return undefined;
  }
  const runRecord = runtimeRunRecord(run);
  const sequence = draft.settlementSequence + 1;
  draft.settlementSequence = sequence;
  const settlement: WorkerSettlement = Object.freeze({
    eventId: `${sequence}:${runRecord.id}:${worker.record.id}:${worker.generation}`,
    sequence,
    ownerSessionId: worker.record.ownerSessionId,
    runId: runRecord.id,
    workerId: worker.record.id,
    generation: worker.generation,
    mode: runRecord.mode,
    worker: worker.record.worker,
    title: worker.record.title,
    lifecycle: worker.record.lifecycle,
    status: worker.record.status,
    outcome: Object.freeze(copyOutcome(worker.record.outcome)),
    ...(failureStage ? { failureStage } : {}),
    usage: Object.freeze(copyUsage(worker.record.usage)),
    startedAt: worker.record.startedAt,
    settledAt,
    ...(runRecord.synthesisGroupId && runRecord.synthesisGroupSize
      ? {
          synthesisGroupId: runRecord.synthesisGroupId,
          synthesisGroupSize: runRecord.synthesisGroupSize,
        }
      : {}),
    ...(worker.record.sessionFile !== undefined
      ? { sessionFile: worker.record.sessionFile }
      : {}),
  });
  return publishSettlement(
    settlement,
    isRunningRuntimeRun(run) ? run.settlementListener : undefined,
  );
}

function stateActionsAfterPrune(
  draft: RuntimeState,
  ownerSessionId: string,
): PostCommitAction[] {
  const owners = pruneHistory(draft);
  owners.add(ownerSessionId);
  return publishOwners(owners);
}

function pruneHistory(draft: RuntimeState): Set<string> {
  const owners = new Set<string>();
  while (draft.completedRunOrder.length > MAX_COMPLETED_RUN_HISTORY) {
    const runId = draft.completedRunOrder.shift();
    if (!runId) break;
    const run = draft.runs.get(runId);
    if (run) owners.add(runtimeRunRecord(run).ownerSessionId);
    draft.runs.delete(runId);
  }
  while (draft.terminalWorkerOrder.length > MAX_TERMINAL_WORKER_HISTORY) {
    const index = draft.terminalWorkerOrder.findIndex((workerId) => {
      const worker = draft.workers.get(workerId);
      return !worker || isTerminalWorkerStatus(worker.record.status);
    });
    if (index < 0) break;
    const removed = draft.terminalWorkerOrder.splice(index, 1)[0];
    if (!removed) break;
    const worker = draft.workers.get(removed);
    if (worker) owners.add(worker.record.ownerSessionId);
    draft.workers.delete(removed);
  }
  return owners;
}

function rememberTerminalWorker(
  draft: RuntimeState,
  workerId: WorkerId,
): void {
  if (!draft.terminalWorkerOrder.includes(workerId)) {
    draft.terminalWorkerOrder.push(workerId);
  }
}

function snapshotFor(
  state: RuntimeState,
  ownerSessionId: string,
): RuntimeSnapshot {
  return Object.freeze({
    runs: Object.freeze(
      [...state.runs.values()]
        .map(runtimeRunRecord)
        .filter((run) => run.ownerSessionId === ownerSessionId)
        .map(copyRunRecord),
    ),
    workers: Object.freeze(
      [...state.workers.values()]
        .map((worker) => worker.record)
        .filter((worker) => worker.ownerSessionId === ownerSessionId)
        .map(copyWorkerRecord),
    ),
  });
}

function runtimeRunRecord(run: RuntimeRun): RunRecord {
  return isRunningRuntimeRun(run) ? run.record : run;
}

function isRunningRuntimeRun(run: RuntimeRun): run is RunningRuntimeRun {
  return "_tag" in run;
}

function makeRunRecord(
  runId: RunId,
  workerId: WorkerId,
  context: OrchestrationContext,
  mode: RunMode,
  createdAt: number,
): RunRecord {
  return {
    id: runId,
    ownerSessionId: context.ownerSessionId,
    workerId,
    mode,
    state: "running",
    createdAt,
    ...(context.synthesisGroup
      ? {
          synthesisGroupId: context.synthesisGroup.id,
          synthesisGroupSize: context.synthesisGroup.size,
        }
      : {}),
  };
}

function makeWorkerRecord(
  workerId: WorkerId,
  runId: RunId,
  ownerSessionId: string,
  definition: WorkerDefinition,
  task: OrchestrateTaskInput,
  startedAt: number,
): WorkerRecord {
  return {
    id: workerId,
    worker: definition.name,
    ownerSessionId,
    runId,
    title: task.title,
    instructions: task.instructions,
    lifecycle: definition.lifecycle,
    status: "starting",
    usage: copyUsage(EMPTY_WORKER_USAGE),
    messageDirection: "to-model",
    startedAt,
  };
}

function publishState(ownerSessionId: string): PostCommitAction {
  return (state, _settlementListeners, stateListeners) => {
    const snapshot = snapshotFor(state, ownerSessionId);
    const listeners = [...(stateListeners.get(ownerSessionId) ?? [])];
    return () => {
      for (const listener of listeners) safelyNotify(() => listener(snapshot));
    };
  };
}

function publishStateTo(
  ownerSessionId: string,
  listener: StateListener,
): PostCommitAction {
  return (state) => {
    const snapshot = snapshotFor(state, ownerSessionId);
    return () => safelyNotify(() => listener(snapshot));
  };
}

function publishSettlement(
  settlement: WorkerSettlement,
  localListener: SettlementListener | undefined,
): PostCommitAction {
  return (_state, settlementListeners) => {
    const listeners = [...settlementListeners];
    return () => {
      if (localListener) safelyNotify(() => localListener(settlement));
      for (const listener of listeners) safelyNotify(() => listener(settlement));
    };
  };
}

function publishOwners(owners: ReadonlySet<string>): PostCommitAction[] {
  return [...owners].map(publishState);
}

function runAction(run: () => void): PostCommitAction {
  return () => run;
}

function openDecision(
  draft: RuntimeState,
  operation: OrchestrationOperation,
): Decision<void> {
  return draft.lifecycle === "open"
    ? accepted(undefined)
    : rejected(
        operation,
        "shutdown",
        "Orchestrator runtime is shutting down",
      );
}

function ownedWorkerDecision(
  draft: RuntimeState,
  operation: OrchestrationOperation,
  ownerSessionId: string,
  workerId: WorkerId,
): Decision<RuntimeWorker> {
  const worker = draft.workers.get(workerId);
  return !worker || worker.record.ownerSessionId !== ownerSessionId
    ? rejected(
        operation,
        "ownership",
        "Worker is not owned by this session",
      )
    : accepted(worker);
}

function accepted<A>(value: A): Decision<A> {
  return { _tag: "accepted", value };
}

function rejected(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): Decision<never> {
  return {
    _tag: "rejected",
    error: actionRejection(operation, reason, message),
  };
}

function actionRejection(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): OrchestrationActionRejected {
  return new OrchestrationActionRejected({ operation, reason, message });
}

function rejectAction(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): Effect.Effect<never, OrchestrationActionRejected> {
  return Effect.fail(actionRejection(operation, reason, message));
}

function validateContextOwner(
  operation: OrchestrationOperation,
  ownerSessionId: string,
): Effect.Effect<void, OrchestrationActionRejected> {
  return typeof ownerSessionId !== "string" || ownerSessionId.trim() === ""
    ? rejectAction(
        operation,
        "validation",
        "ownerSessionId must not be blank",
      )
    : Effect.void;
}

function validateWorkerId(
  operation: OrchestrationOperation,
  workerId: string,
): Effect.Effect<WorkerId, OrchestrationActionRejected> {
  if (typeof workerId !== "string" || workerId.trim() === "") {
    return rejectAction(
      operation,
      "validation",
      "worker_id must not be blank",
    );
  }
  return Schema.decodeUnknownEffect(WorkerId)(workerId).pipe(
    Effect.mapError(() => actionRejection(
      operation,
      "validation",
      "worker_id must use the canonical worker- prefix",
    )),
  );
}

function validateMode(
  operation: OrchestrationOperation,
  mode: RunMode,
): Effect.Effect<void, OrchestrationActionRejected> {
  return mode !== "async" && mode !== "inline"
    ? rejectAction(operation, "validation", "Invalid orchestration mode")
    : Effect.void;
}

function validateText(
  operation: OrchestrationOperation,
  name: string,
  value: string,
  maximumLength: number,
): Effect.Effect<void, OrchestrationActionRejected> {
  if (typeof value !== "string" || value.trim() === "") {
    return rejectAction(operation, "validation", `${name} must not be blank`);
  }
  return value.length > maximumLength
    ? rejectAction(
        operation,
        "validation",
        `${name} must be at most ${maximumLength} characters`,
      )
    : Effect.void;
}

type ValidatedAbortTarget =
  | {
      readonly _tag: "ids";
      readonly workerIds: readonly WorkerId[];
    }
  | {
      readonly _tag: "all";
    };

function validateAbortTarget(
  target: AbortTarget,
): Effect.Effect<ValidatedAbortTarget, OrchestrationActionRejected> {
  return Effect.gen(function* () {
    if (!target || typeof target !== "object") {
      return yield* rejectAction("abort", "target", "Invalid abort target");
    }
    const selected = [target.workerIds !== undefined, target.all !== undefined]
      .filter(Boolean).length;
    if (selected !== 1 || (target.all !== undefined && target.all !== true)) {
      return yield* rejectAction(
        "abort",
        "target",
        "Abort target must specify exactly one of workerIds or all: true",
      );
    }
    if (target.workerIds === undefined) return { _tag: "all" };
    if (!Array.isArray(target.workerIds) || target.workerIds.length === 0) {
      return yield* rejectAction(
        "abort",
        "target",
        "workerIds must contain at least one worker ID",
      );
    }
    const workerIds = yield* Effect.all(
      [...new Set(target.workerIds)].map((id) => validateWorkerId("abort", id)),
    );
    return { _tag: "ids", workerIds };
  });
}

function makeCancellationCandidates(
  workerIds: readonly WorkerId[],
): Effect.Effect<ReadonlyMap<WorkerId, Deferred.Deferred<void>>> {
  return Effect.forEach(workerIds, (workerId) => Deferred.make<void>().pipe(
    Effect.map((completion) => [workerId, completion] as const),
  )).pipe(Effect.map((entries) => new Map(entries)));
}

function activeWorkerIds(
  state: RuntimeState,
  ownerSessionId?: string,
): WorkerId[] {
  return [...state.workers.values()]
    .filter((worker) => (
      (ownerSessionId === undefined ||
        worker.record.ownerSessionId === ownerSessionId) &&
      isActiveWorkerStatus(worker.record.status)
    ))
    .map((worker) => worker.record.id);
}

function awaitAll(
  deferreds: readonly Deferred.Deferred<void>[],
): Effect.Effect<void> {
  return Effect.forEach(
    deferreds,
    (deferred) => Deferred.await(deferred),
    { concurrency: "unbounded", discard: true },
  );
}

function isRuntimeWorker(
  worker: RuntimeWorker | undefined,
): worker is RuntimeWorker {
  return worker !== undefined;
}

function isActiveWorkerStatus(status: WorkerRecord["status"]): boolean {
  return status === "starting" || status === "running" || status === "stopping";
}

function isCompletedRunWorker(
  record: WorkerRecord,
): record is WorkerRecord & {
  readonly status: RunResult["status"];
  readonly outcome: Exclude<WorkerOutcome, { readonly status: "closed" }>;
  readonly settledAt: number;
} {
  return (
    (record.status === "completed" ||
      record.status === "ready" ||
      record.status === "failed" ||
      record.status === "aborted") &&
    record.outcome !== undefined &&
    record.outcome.status !== "closed" &&
    record.settledAt !== undefined
  );
}

function observationsEqual(
  previous: WorkerRecord,
  next: WorkerRecord,
): boolean {
  return (
    previous.activity === next.activity &&
    previous.messageDirection === next.messageDirection &&
    usageEquals(previous.usage, next.usage)
  );
}

function usageEquals(left: WorkerUsage, right: WorkerUsage): boolean {
  return (
    left.input === right.input &&
    left.output === right.output &&
    left.cacheRead === right.cacheRead &&
    left.cacheWrite === right.cacheWrite &&
    left.cost === right.cost &&
    left.contextTokens === right.contextTokens &&
    left.turns === right.turns
  );
}

function copyUsage(usage: WorkerUsage): WorkerUsage {
  return {
    input: usage.input,
    output: usage.output,
    cacheRead: usage.cacheRead,
    cacheWrite: usage.cacheWrite,
    cost: usage.cost,
    contextTokens: usage.contextTokens,
    turns: usage.turns,
  };
}

function copyOutcome<Outcome extends WorkerOutcome>(outcome: Outcome): Outcome {
  return { ...outcome };
}

function copyRunRecord(run: RunRecord): RunRecord {
  return Object.freeze({ ...run });
}

function copyWorkerRecord(worker: WorkerRecord): WorkerRecord {
  return Object.freeze({
    ...worker,
    usage: Object.freeze(copyUsage(worker.usage)),
    ...(worker.outcome
      ? { outcome: Object.freeze(copyOutcome(worker.outcome)) }
      : {}),
  });
}

function freezeAcceptedRun(id: RunId, workerId: WorkerId): AcceptedRun {
  return Object.freeze({ id, workerId });
}

function freezeCompletedRun(
  run: RunRecord,
  record: WorkerRecord & {
    readonly status: RunResult["status"];
    readonly outcome: Exclude<WorkerOutcome, { readonly status: "closed" }>;
    readonly settledAt: number;
  },
): CompletedRun {
  const result: RunResult = Object.freeze({
    workerId: record.id,
    worker: record.worker,
    title: record.title,
    status: record.status,
    outcome: Object.freeze(copyOutcome(record.outcome)),
    usage: Object.freeze(copyUsage(record.usage)),
    startedAt: record.startedAt,
    settledAt: record.settledAt,
    sessionFile: record.sessionFile,
  });
  return Object.freeze({
    id: run.id,
    ownerSessionId: run.ownerSessionId,
    mode: run.mode,
    result,
  });
}

function describeError(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message !== "") return error.message;
  if (typeof error === "string" && error !== "") return error;
  return fallback;
}

function safelyCall(callback: (() => void) | undefined): void {
  if (callback) safelyNotify(callback);
}

function safelyNotify(callback: () => void): void {
  try {
    callback();
  } catch {
    // Observers and best-effort cleanup cannot block committed state transitions.
  }
}

function addAll(target: Set<string>, source: ReadonlySet<string>): void {
  for (const value of source) target.add(value);
}

function noOp(): void {}
