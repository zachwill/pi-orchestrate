import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Cause, Context, Deferred, Effect, Fiber, Layer, Schema } from "effect";
import {
  CANCELLATION_GRACE_MS,
  EMPTY_WORKER_USAGE,
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  createRandomIdFactories,
  findWorkerByName,
  isTerminalWorkerStatus,
  transitionWorkerStatus,
  type OrchestrateIdFactories,
  type OrchestrateTaskInput,
  type RunId,
  type RunMode,
  type RunRecord,
  type WorkerCatalog,
  type WorkerDefinition,
  type WorkerId,
  type WorkerOutcome,
  type WorkerRecord,
  type WorkerUsage,
} from "./domain.js";
import {
  CleanupSupervisor,
  GenerationSupervisor,
  type CleanupSupervisorService,
  type GenerationSupervisorService,
} from "./scheduler.js";
import {
  ChildSessions,
  type ChildSessionsService,
  type WorkerSessionAbortError,
  type WorkerSessionHandle,
} from "./worker-session.js";
import type {
  SettlementFailureStage,
  WorkerSettlement,
} from "./worker-settlement.js";

export const MAX_TERMINAL_WORKER_HISTORY = 100;
export const MAX_COMPLETED_RUN_HISTORY = 100;
/** Shutdown waits this long for interrupted workflows and supervised cleanup, then returns best-effort. */
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
export type StateListener = (ownerSessionId: string) => void;

export interface AbortTarget {
  readonly workerIds?: readonly string[];
  readonly all?: boolean;
}

export interface OrchestrationLayerOptions {
  readonly idFactories?: OrchestrateIdFactories;
  readonly clock?: () => number;
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
  readonly subscribeSettlement: (listener: SettlementListener) => UnsubscribeSettlement;
  readonly subscribeState: (listener: StateListener) => () => void;
  /** Closes admission synchronously when called, before teardown begins. */
  readonly shutdown: () => Effect.Effect<void>;
}

export class Orchestration extends Context.Service<Orchestration, OrchestrationService>()(
  "@zachwill/pi-orchestrate/Orchestration",
) {}


interface RuntimeEntry {
  readonly context: OrchestrationContext;
  readonly definition: WorkerDefinition;
  generation: number;
  session?: WorkerSessionHandle;
  unsubscribeUsage?: () => void;
  unsubscribeActivity?: () => void;
  unsubscribeMessageDirection?: () => void;
}

class StatefulOrchestration implements OrchestrationService {
  private readonly childSessions: ChildSessionsService;
  private readonly generations: GenerationSupervisorService;
  private readonly cleanup: CleanupSupervisorService;
  private readonly idFactories: OrchestrateIdFactories;
  private readonly clock: () => number;
  private readonly workers = new Map<WorkerId, WorkerRecord>();
  private readonly runs = new Map<RunId, RunRecord>();
  private readonly entries = new Map<WorkerId, RuntimeEntry>();
  private readonly runCompletions = new Map<RunId, Deferred.Deferred<CompletedRun>>();
  private readonly completedRuns = new Map<RunId, CompletedRun>();
  private readonly cancellations = new Map<WorkerId, Deferred.Deferred<void>>();
  private readonly terminalWorkerOrder: WorkerId[] = [];
  private readonly completedRunOrder: RunId[] = [];
  private readonly settlementListeners = new Set<SettlementListener>();
  private readonly runSettlementListeners = new Map<RunId, SettlementListener>();
  private readonly stateListeners = new Set<StateListener>();
  private settlementSequence = 0;
  private shuttingDown = false;
  private shutdownStarted = false;
  private readonly shutdownCompletion = Deferred.makeUnsafe<void>();

  constructor(
    childSessions: ChildSessionsService,
    generations: GenerationSupervisorService,
    cleanup: CleanupSupervisorService,
    options: OrchestrationLayerOptions,
  ) {
    this.childSessions = childSessions;
    this.generations = generations;
    this.cleanup = cleanup;
    this.idFactories = options.idFactories ?? createRandomIdFactories();
    this.clock = options.clock ?? Date.now;
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
    return Effect.gen({ self: this }, function* () {
      yield* this.requireOpen("orchestrate");
      const definition = yield* this.validateTask(context, task, mode);

      const runId = this.idFactories.runId();
      const workerId = this.idFactories.workerId();
      this.assertFreshIds(runId, workerId);

      const run: RunRecord = {
        id: runId,
        ownerSessionId: context.ownerSessionId,
        workerId,
        mode,
        state: "running",
        createdAt: this.clock(),
        ...(context.synthesisGroup
          ? {
              synthesisGroupId: context.synthesisGroup.id,
              synthesisGroupSize: context.synthesisGroup.size,
            }
          : {}),
      };
      const record: WorkerRecord = {
        id: workerId,
        worker: definition.name,
        ownerSessionId: context.ownerSessionId,
        runId,
        title: task.title,
        instructions: task.instructions,
        lifecycle: definition.lifecycle,
        status: "starting",
        usage: copyUsage(EMPTY_WORKER_USAGE),
        messageDirection: "to-model",
        startedAt: this.clock(),
      };

      const completion = Deferred.makeUnsafe<CompletedRun>();
      this.runs.set(runId, run);
      this.runCompletions.set(runId, completion);
      if (onSettlement) this.runSettlementListeners.set(runId, onSettlement);
      this.workers.set(workerId, record);
      this.entries.set(workerId, { context, definition, generation: 1 });
      this.emitState(context.ownerSessionId);
      this.launchBootstrap(workerId, 1);

      return yield* mode === "inline"
        ? this.awaitInlineRun(run, completion)
        : Effect.succeed(freezeAcceptedRun(runId, workerId));
    });
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
    return Effect.gen({ self: this }, function* () {
      yield* this.requireOpen("sendInteractive");
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

      const current = yield* this.ownedWorker(
        "sendInteractive",
        context.ownerSessionId,
        validatedWorkerId,
      );
      if (current.lifecycle !== "interactive" || current.status !== "ready") {
        return yield* rejectAction(
          "sendInteractive",
          "worker-state",
          "interactive_send requires an owned ready interactive worker",
        );
      }
      const entry = this.entries.get(validatedWorkerId);
      if (!entry?.session) throw new Error("Ready interactive worker has no session handle");

      const runId = this.idFactories.runId();
      if (this.runs.has(runId)) throw new Error(`Duplicate run ID: ${runId}`);
      const run: RunRecord = {
        id: runId,
        ownerSessionId: context.ownerSessionId,
        workerId: validatedWorkerId,
        mode,
        state: "running",
        createdAt: this.clock(),
      };
      const running: WorkerRecord = {
        ...transitionWorkerStatus(current, "running"),
        runId,
        instructions,
        activity: undefined,
        messageDirection: "to-model",
        startedAt: this.clock(),
        settledAt: undefined,
      };
      const completion = Deferred.makeUnsafe<CompletedRun>();

      entry.generation += 1;
      const generation = entry.generation;
      this.runs.set(runId, run);
      this.runCompletions.set(runId, completion);
      if (onSettlement) this.runSettlementListeners.set(runId, onSettlement);
      this.workers.set(validatedWorkerId, running);
      this.subscribeEntryObservability(validatedWorkerId, entry, entry.session, generation);
      this.emitState(context.ownerSessionId);
      this.launchPrompt(validatedWorkerId, generation, entry.session, instructions);

      return yield* mode === "inline"
        ? this.awaitInlineRun(run, completion)
        : Effect.succeed(freezeAcceptedRun(runId, validatedWorkerId));
    });
  }

  abort(
    ownerSessionId: string,
    target: AbortTarget,
  ): Effect.Effect<void, OrchestrationActionRejected> {
    return Effect.gen({ self: this }, function* () {
      yield* this.requireOpen("abort");
      yield* validateContextOwner("abort", ownerSessionId);
      const workerIds = yield* this.resolveAbortTargets(ownerSessionId, target);
      yield* this.cancelWorkers(workerIds);
    });
  }

  closeInteractive(
    ownerSessionId: string,
    workerId: string,
  ): Effect.Effect<void, OrchestrationActionRejected> {
    return Effect.gen({ self: this }, function* () {
      yield* this.requireOpen("closeInteractive");
      yield* validateContextOwner("closeInteractive", ownerSessionId);
      const validatedWorkerId = yield* validateWorkerId(
        "closeInteractive",
        workerId,
      );
      const current = yield* this.ownedWorker(
        "closeInteractive",
        ownerSessionId,
        validatedWorkerId,
      );
      if (current.lifecycle !== "interactive" || current.status !== "ready") {
        return yield* rejectAction(
          "closeInteractive",
          "worker-state",
          "interactive_close requires an owned ready interactive worker",
        );
      }
      this.closeReadyInteractiveWorker(current);
    });
  }

  snapshot(
    ownerSessionId: string,
  ): Effect.Effect<RuntimeSnapshot, OrchestrationActionRejected> {
    return Effect.gen({ self: this }, function* () {
      yield* validateContextOwner("snapshot", ownerSessionId);
      const runs = [...this.runs.values()]
        .filter((run) => run.ownerSessionId === ownerSessionId)
        .map(copyRunRecord);
      const workers = [...this.workers.values()]
        .filter((worker) => worker.ownerSessionId === ownerSessionId)
        .map(copyWorkerRecord);
      return Object.freeze({
        runs: Object.freeze(runs),
        workers: Object.freeze(workers),
      });
    });
  }

  subscribeSettlement(listener: SettlementListener): UnsubscribeSettlement {
    if (typeof listener !== "function") throw new Error("Settlement listener must be a function");
    if (this.shuttingDown) return noOp;
    this.settlementListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.settlementListeners.delete(listener);
    };
  }

  subscribeState(listener: StateListener): () => void {
    if (typeof listener !== "function") throw new Error("State listener must be a function");
    if (this.shuttingDown) return noOp;
    this.stateListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.stateListeners.delete(listener);
    };
  }

  shutdown(): Effect.Effect<void> {
    this.shuttingDown = true;
    return Effect.suspend(() => {
      if (this.shutdownStarted) return Deferred.await(this.shutdownCompletion);
      this.shutdownStarted = true;
      return this.performShutdown().pipe(
        Effect.ensuring(Effect.sync(() => {
          Deferred.doneUnsafe(this.shutdownCompletion, Effect.void);
        })),
      );
    });
  }

  private performShutdown(): Effect.Effect<void> {
    const runtime = this;
    return Effect.gen(function* () {
      runtime.closeReadyInteractiveWorkersForShutdown();
      const active = [...runtime.workers.values()]
        .filter((worker) => isActiveWorkerStatus(worker.status))
        .map((worker) => worker.id);
      yield* runtime.cancelWorkers(active);
      yield* runtime.childSessions.shutdown().pipe(Effect.catchCause(() => Effect.void));
      yield* runtime.awaitSupervisedCleanupBestEffort();
      runtime.runSettlementListeners.clear();
      runtime.settlementListeners.clear();
      runtime.stateListeners.clear();
    }).pipe(Effect.ensuring(Effect.sync(() => {
      this.runSettlementListeners.clear();
      this.settlementListeners.clear();
      this.stateListeners.clear();
    })));
  }

  private validateTask(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
  ): Effect.Effect<WorkerDefinition, OrchestrationActionRejected> {
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
      if (!configured) {
        if (!context.parentModel) {
          return yield* rejectAction(
            "orchestrate",
            "model-unavailable",
            `Worker "${definition.name}" has no configured model and no parent model is available`,
          );
        }
      } else {
        const selected = context.modelRegistry.find(
          configured.provider,
          configured.modelId,
        );
        if (!selected) {
          return yield* rejectAction(
            "orchestrate",
            "model-unavailable",
            `Worker "${definition.name}" configured model "${configured.provider}/${configured.modelId}" was not found`,
          );
        }
      }
      return definition;
    });
  }

  private assertFreshIds(runId: RunId, workerId: WorkerId): void {
    if (this.runs.has(runId)) throw new Error(`Duplicate run ID: ${runId}`);
    if (this.workers.has(workerId)) throw new Error(`Duplicate worker ID: ${workerId}`);
  }

  private launchBootstrap(workerId: WorkerId, generation: number): void {
    try {
      this.generations.start(
        workerId,
        this.bootstrapAndPrompt(workerId, generation),
        (error) => this.settleWorkflowDefect(workerId, generation, error),
      );
    } catch (error) {
      this.settleWorkflowDefect(workerId, generation, error);
    }
  }

  private launchPrompt(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    instructions: string,
  ): void {
    try {
      this.generations.start(
        workerId,
        this.executePrompt(workerId, generation, session, instructions),
        (error) => this.settleWorkflowDefect(workerId, generation, error),
      );
    } catch (error) {
      this.settleWorkflowDefect(workerId, generation, error);
    }
  }

  private bootstrapAndPrompt(
    workerId: WorkerId,
    generation: number,
  ): Effect.Effect<void, never> {
    const runtime = this;
    return Effect.gen(function* () {
      const session = yield* runtime.bootstrap(workerId, generation);
      if (!session) return;
      const current = runtime.workers.get(workerId);
      if (!current) return;
      yield* runtime.executePrompt(workerId, generation, session, current.instructions);
    });
  }

  private bootstrap(
    workerId: WorkerId,
    generation: number,
  ): Effect.Effect<WorkerSessionHandle | undefined, never> {
    return Effect.suspend(() => {
      const entry = this.entries.get(workerId);
      if (!entry) return Effect.succeed(undefined);

      return this.childSessions.acquire({
        cwd: entry.context.cwd,
        agentDir: entry.context.agentDir,
        parentSessionFile: entry.context.parentSessionFile,
        projectTrusted: entry.context.projectTrusted,
        definition: entry.definition,
        parentModel: entry.context.parentModel,
        modelRegistry: entry.context.modelRegistry,
      }, (session) => this.adoptCreatedSession(
        workerId,
        generation,
        entry,
        session,
      )).pipe(
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

  private canAdoptCreatedSession(
    workerId: WorkerId,
    generation: number,
    entry: RuntimeEntry,
  ): boolean {
    const current = this.workers.get(workerId);
    return !this.shuttingDown &&
      current?.status === "starting" &&
      this.entries.get(workerId) === entry &&
      entry.generation === generation;
  }

  private adoptCreatedSession(
    workerId: WorkerId,
    generation: number,
    entry: RuntimeEntry,
    session: WorkerSessionHandle,
  ): WorkerSessionHandle | undefined {
    const current = this.workers.get(workerId);
    if (!this.canAdoptCreatedSession(workerId, generation, entry) || !current) {
      return undefined;
    }

    try {
      entry.session = session;
      this.subscribeEntryObservability(workerId, entry, session, generation);
      this.workers.set(workerId, {
        ...transitionWorkerStatus(current, "running"),
        sessionFile: session.sessionFile,
      });
      this.emitState(current.ownerSessionId);
      return session;
    } catch (error) {
      this.unsubscribeEntryObservability(entry);
      entry.session = undefined;
      this.settleCreationFailure(workerId, generation, error);
      return undefined;
    }
  }

  private executePrompt(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    instructions: string,
  ): Effect.Effect<void, never> {
    return Effect.suspend(() => {
      const before = this.workers.get(workerId);
      const entry = this.entries.get(workerId);
      if (
        !before ||
        before.status !== "running" ||
        !entry ||
        entry.generation !== generation ||
        entry.session !== session
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
    const current = this.workers.get(workerId);
    const entry = this.entries.get(workerId);
    if (!current || current.status !== "starting" || entry?.generation !== generation) return;
    this.settleTerminalWorker(current, "failed", {
      status: "failed",
      message: describeError(error, "Worker session creation failed"),
    }, "startup");
  }

  private settleWorkflowDefect(
    workerId: WorkerId,
    generation: number,
    error: unknown,
  ): void {
    const current = this.workers.get(workerId);
    const entry = this.entries.get(workerId);
    if (!current || entry?.generation !== generation) return;
    if (!isActiveWorkerStatus(current.status)) {
      this.maybeCompleteRun(current.runId);
      return;
    }

    this.disposeEntrySession(entry);
    this.settleTerminalWorker(current, "failed", {
      status: "failed",
      message: describeError(error, "Worker workflow failed"),
    }, "workflow");
  }

  private settleOutcome(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    outcome: WorkerOutcome,
  ): void {
    const current = this.workers.get(workerId);
    const entry = this.entries.get(workerId);
    if (
      !current ||
      current.status !== "running" ||
      !entry ||
      entry.generation !== generation ||
      entry.session !== session
    ) {
      return;
    }

    let settledOutcome = outcome;
    let status: "ready" | "completed" | "failed" | "aborted";
    if (outcome.status === "failed" || outcome.status === "aborted") {
      status = outcome.status;
    } else if (current.lifecycle === "interactive" && outcome.status === "ready") {
      status = "ready";
    } else if (current.lifecycle === "one-shot" && outcome.status === "completed") {
      status = "completed";
    } else {
      status = "failed";
      settledOutcome = {
        status: "failed",
        message: `Worker session returned ${outcome.status} for a ${current.lifecycle} worker`,
      };
    }

    const settledAt = this.clock();
    this.workers.set(workerId, {
      ...transitionWorkerStatus(current, status),
      activity: undefined,
      outcome: copyOutcome(settledOutcome),
      settledAt,
    });
    if (status !== "ready") this.disposeEntrySession(entry);
    this.maybeCompleteRun(current.runId);
    const affectedOwners = new Set([current.ownerSessionId]);
    this.emitSettlement(
      workerId,
      generation,
      settledAt,
      status === "failed" ? "prompt" : undefined,
    );
    if (isTerminalWorkerStatus(status)) {
      addAll(affectedOwners, this.rememberTerminalWorker(workerId));
    } else {
      addAll(affectedOwners, this.pruneHistory());
    }
    this.emitStateForOwners(affectedOwners);
  }

  private settleTerminalWorker(
    current: WorkerRecord,
    status: "failed" | "aborted",
    outcome: WorkerOutcome,
    failureStage?: SettlementFailureStage,
  ): void {
    const settledAt = this.clock();
    this.workers.set(current.id, {
      ...transitionWorkerStatus(current, status),
      activity: undefined,
      outcome: copyOutcome(outcome),
      settledAt,
    });
    this.maybeCompleteRun(current.runId);
    const affectedOwners = new Set([current.ownerSessionId]);
    const generation = this.entries.get(current.id)?.generation;
    if (generation !== undefined) {
      this.emitSettlement(current.id, generation, settledAt, failureStage);
    }
    addAll(affectedOwners, this.rememberTerminalWorker(current.id));
    this.emitStateForOwners(affectedOwners);
  }

  private emitSettlement(
    workerId: WorkerId,
    generation: number,
    settledAt: number,
    failureStage?: SettlementFailureStage,
  ): void {
    const worker = this.workers.get(workerId);
    const run = worker ? this.runs.get(worker.runId) : undefined;
    if (!worker || !run || !worker.outcome || !isSettledWorkerStatus(worker.status)) return;
    if (worker.outcome.status === "closed") return;

    const sequence = ++this.settlementSequence;
    const settlement: WorkerSettlement = Object.freeze({
      eventId: `${sequence}:${run.id}:${workerId}:${generation}`,
      sequence,
      ownerSessionId: worker.ownerSessionId,
      runId: run.id,
      workerId,
      generation,
      mode: run.mode,
      worker: worker.worker,
      title: worker.title,
      lifecycle: worker.lifecycle,
      status: worker.status,
      outcome: Object.freeze(copyOutcome(worker.outcome)),
      ...(failureStage ? { failureStage } : {}),
      usage: Object.freeze(copyUsage(worker.usage)),
      startedAt: worker.startedAt,
      settledAt,
      ...(run.synthesisGroupId && run.synthesisGroupSize
        ? {
            synthesisGroupId: run.synthesisGroupId,
            synthesisGroupSize: run.synthesisGroupSize,
          }
        : {}),
      ...(worker.sessionFile !== undefined ? { sessionFile: worker.sessionFile } : {}),
    });

    const localListener = this.runSettlementListeners.get(run.id);
    this.runSettlementListeners.delete(run.id);
    notifySettlementListener(localListener, settlement);
    for (const listener of [...this.settlementListeners]) {
      notifySettlementListener(listener, settlement);
    }
  }

  private maybeCompleteRun(runId: RunId): void {
    if (this.completedRuns.has(runId)) return;
    const run = this.runs.get(runId);
    if (!run) return;
    const worker = this.workers.get(run.workerId);
    if (!worker || worker.runId !== runId || !worker.outcome || isActiveWorkerStatus(worker.status)) {
      return;
    }

    const completed = freezeCompletedRun(run, worker);
    this.completedRuns.set(runId, completed);
    this.completedRunOrder.push(runId);
    this.runs.set(runId, { ...run, state: "complete" });
    const completion = this.runCompletions.get(runId);
    if (completion) Deferred.doneUnsafe(completion, Effect.succeed(completed));
    this.runCompletions.delete(runId);
  }

  private rememberTerminalWorker(workerId: WorkerId): Set<string> {
    if (!this.terminalWorkerOrder.includes(workerId)) {
      this.terminalWorkerOrder.push(workerId);
    }
    return this.pruneHistory();
  }

  private pruneHistory(): Set<string> {
    const affectedOwners = new Set<string>();
    while (this.completedRunOrder.length > MAX_COMPLETED_RUN_HISTORY) {
      const runId = this.completedRunOrder.shift();
      if (!runId) break;
      this.completedRuns.delete(runId);
      const run = this.runs.get(runId);
      if (run) affectedOwners.add(run.ownerSessionId);
      this.runs.delete(runId);
    }

    while (this.terminalWorkerOrder.length > MAX_TERMINAL_WORKER_HISTORY) {
      const removableIndex = this.terminalWorkerOrder.findIndex((workerId) => {
        const worker = this.workers.get(workerId);
        if (!worker || !isTerminalWorkerStatus(worker.status)) return true;
        return this.runs.get(worker.runId)?.state !== "running";
      });
      if (removableIndex < 0) return affectedOwners;
      const [workerId] = this.terminalWorkerOrder.splice(removableIndex, 1);
      if (!workerId) return affectedOwners;
      const worker = this.workers.get(workerId);
      if (worker) affectedOwners.add(worker.ownerSessionId);
      this.workers.delete(workerId);
      this.entries.delete(workerId);
    }

    return affectedOwners;
  }

  private resolveAbortTargets(
    ownerSessionId: string,
    target: AbortTarget,
  ): Effect.Effect<WorkerId[], OrchestrationActionRejected> {
    return Effect.gen({ self: this }, function* () {
      if (!target || typeof target !== "object") {
        return yield* rejectAction("abort", "target", "Invalid abort target");
      }
      const candidate = target;
      const selected = [
        candidate.workerIds !== undefined,
        candidate.all !== undefined,
      ].filter(Boolean).length;
      if (selected !== 1 || (candidate.all !== undefined && candidate.all !== true)) {
        return yield* rejectAction(
          "abort",
          "target",
          "Abort target must specify exactly one of workerIds or all: true",
        );
      }

      if (candidate.workerIds !== undefined) {
        if (!Array.isArray(candidate.workerIds) || candidate.workerIds.length === 0) {
          return yield* rejectAction(
            "abort",
            "target",
            "workerIds must contain at least one worker ID",
          );
        }
        const unique = [...new Set(candidate.workerIds)];
        const validatedWorkerIds = yield* Effect.all(
          unique.map((workerId) => validateWorkerId("abort", workerId)),
        );
        for (const workerId of validatedWorkerIds) {
          const worker = yield* this.ownedWorker(
            "abort",
            ownerSessionId,
            workerId,
          );
          if (worker.status === "ready") {
            return yield* rejectAction(
              "abort",
              "worker-state",
              "Ready interactive workers are not active; use interactive_close",
            );
          }
          if (!isActiveWorkerStatus(worker.status)) {
            return yield* rejectAction(
              "abort",
              "worker-state",
              "worker_abort requires owned active workers",
            );
          }
        }
        return validatedWorkerIds;
      }

      return [...this.workers.values()]
        .filter(
          (worker) =>
            worker.ownerSessionId === ownerSessionId && isActiveWorkerStatus(worker.status),
        )
        .map((worker) => worker.id);
    });
  }

  private cancelWorkers(workerIds: readonly WorkerId[]): Effect.Effect<void> {
    const runtime = this;
    return Effect.gen(function* () {
      const owners = new Set<string>();
      const cancellations: Effect.Effect<void>[] = [];

      for (const workerId of workerIds) {
        const current = runtime.workers.get(workerId);
        if (!current || !isActiveWorkerStatus(current.status)) continue;
        if (current.status !== "stopping") {
          runtime.workers.set(workerId, {
            ...transitionWorkerStatus(current, "stopping"),
            activity: undefined,
          });
        }
        owners.add(current.ownerSessionId);
        cancellations.push(runtime.cancellationFor(workerId));
      }

      runtime.emitStateForOwners(owners);
      yield* Effect.all(cancellations, { concurrency: "unbounded" });
    });
  }

  private cancellationFor(workerId: WorkerId): Effect.Effect<void> {
    return Effect.suspend(() => {
      const existing = this.cancellations.get(workerId);
      if (existing) return Deferred.await(existing);

      const completion = Deferred.makeUnsafe<void>();
      this.cancellations.set(workerId, completion);
      return this.cancelWorker(workerId).pipe(
        Effect.catchCause((cause) => Effect.sync(() => {
          this.settleCancellationFailure(workerId, Cause.squash(cause));
        })),
        Effect.ensuring(Effect.sync(() => {
          if (this.cancellations.get(workerId) === completion) {
            this.cancellations.delete(workerId);
          }
          Deferred.doneUnsafe(completion, Effect.void);
        })),
      );
    });
  }

  private cancelWorker(workerId: WorkerId): Effect.Effect<void> {
    const runtime = this;
    return Effect.gen(function* () {
      const entry = runtime.entries.get(workerId);
      const session = entry?.session;

      if (session) {
        yield* session.abort().pipe(
          Effect.catchTag(
            "WorkerSession.AbortError",
            (_error: WorkerSessionAbortError) => Effect.void,
          ),
          Effect.timeoutOption(CANCELLATION_GRACE_MS),
        );
      }
      // FiberMap.remove normally remains awaited so in-grace generation finalization
      // completes before the worker settles. The detached waiter lets cancellation
      // abandon only a removal whose uninterruptible finalizer exceeds the grace;
      // the process-owned generation still retains and observes its physical cleanup.
      const removal = yield* runtime.generations.remove(workerId).pipe(
        Effect.catchCause(() => Effect.void),
        Effect.forkDetach,
      );
      yield* Fiber.await(removal).pipe(
        Effect.timeoutOption(CANCELLATION_GRACE_MS),
        Effect.ignore,
      );
      if (entry) runtime.disposeEntrySession(entry);
      const current = runtime.workers.get(workerId);
      if (current?.status === "stopping") {
        runtime.settleTerminalWorker(
          current,
          "aborted",
          { status: "aborted" },
          "cancellation",
        );
      }
    });
  }

  private settleCancellationFailure(workerId: WorkerId, error: unknown): void {
    const current = this.workers.get(workerId);
    if (!current || current.status !== "stopping") return;
    const entry = this.entries.get(workerId);
    if (entry) this.disposeEntrySession(entry);
    this.settleTerminalWorker(current, "failed", {
      status: "failed",
      message: describeError(error, "Worker cancellation failed"),
    }, "cancellation");
  }

  private cancelExactRun(run: RunRecord): Effect.Effect<void> {
    const worker = this.workers.get(run.workerId);
    const active = worker?.ownerSessionId === run.ownerSessionId &&
        worker.runId === run.id &&
        isActiveWorkerStatus(worker.status)
      ? [worker.id]
      : [];
    return this.cancelWorkers(active).pipe(
      Effect.tap(() => Effect.sync(() => this.maybeCompleteRun(run.id))),
    );
  }

  private awaitInlineRun(
    run: RunRecord,
    completion: Deferred.Deferred<CompletedRun>,
  ): Effect.Effect<CompletedRun> {
    return Deferred.await(completion).pipe(
      Effect.onInterrupt(() => this.cancelExactRun(run)),
    );
  }

  private closeReadyInteractiveWorker(current: WorkerRecord): void {
    const entry = this.entries.get(current.id);
    if (entry) this.disposeEntrySession(entry);
    this.workers.set(current.id, {
      ...transitionWorkerStatus(current, "closed"),
      activity: undefined,
      outcome: { status: "closed" },
      settledAt: this.clock(),
    });
    this.maybeCompleteRun(current.runId);
    const affectedOwners = this.rememberTerminalWorker(current.id);
    affectedOwners.add(current.ownerSessionId);
    this.emitStateForOwners(affectedOwners);
  }

  private closeReadyInteractiveWorkersForShutdown(): void {
    const readyInteractiveWorkers = [...this.workers.values()].filter(
      (worker) => worker.lifecycle === "interactive" && worker.status === "ready",
    );
    for (const worker of readyInteractiveWorkers) {
      this.closeReadyInteractiveWorker(worker);
    }
  }

  private subscribeEntryObservability(
    workerId: WorkerId,
    entry: RuntimeEntry,
    session: WorkerSessionHandle,
    generation: number,
  ): void {
    this.unsubscribeEntryObservability(entry);
    entry.unsubscribeUsage = session.subscribeUsage((usage) => {
      const latest = this.workers.get(workerId);
      if (!latest || entry.session !== session || entry.generation !== generation) return;
      if (latest.status !== "starting" && latest.status !== "running") return;
      this.workers.set(workerId, { ...latest, usage: copyUsage(usage) });
      this.emitState(latest.ownerSessionId);
    });
    entry.unsubscribeActivity = session.subscribeActivity((activity) => {
      const latest = this.workers.get(workerId);
      if (!latest || entry.session !== session || entry.generation !== generation) return;
      if (latest.status !== "starting" && latest.status !== "running") return;
      if (latest.activity === activity) return;
      this.workers.set(workerId, { ...latest, activity });
      this.emitState(latest.ownerSessionId);
    });
    entry.unsubscribeMessageDirection = session.subscribeMessageDirection((messageDirection) => {
      const latest = this.workers.get(workerId);
      if (!latest || entry.session !== session || entry.generation !== generation) return;
      if (latest.status !== "starting" && latest.status !== "running") return;
      if (latest.messageDirection === messageDirection) return;
      this.workers.set(workerId, { ...latest, messageDirection });
      this.emitState(latest.ownerSessionId);
    });
  }

  private emitState(ownerSessionId: string): void {
    for (const listener of [...this.stateListeners]) {
      try {
        listener(ownerSessionId);
      } catch {
        // One subscriber cannot prevent runtime mutations or other notifications.
      }
    }
  }

  private emitStateForOwners(ownerSessionIds: ReadonlySet<string>): void {
    for (const ownerSessionId of ownerSessionIds) this.emitState(ownerSessionId);
  }

  private ownedWorker(
    operation: OrchestrationOperation,
    ownerSessionId: string,
    workerId: WorkerId,
  ): Effect.Effect<WorkerRecord, OrchestrationActionRejected> {
    const worker = this.workers.get(workerId);
    return !worker || worker.ownerSessionId !== ownerSessionId
      ? rejectAction(operation, "ownership", "Worker is not owned by this session")
      : Effect.succeed(worker);
  }

  private unsubscribeEntryObservability(entry: RuntimeEntry): void {
    safelyCall(entry.unsubscribeUsage);
    entry.unsubscribeUsage = undefined;
    safelyCall(entry.unsubscribeActivity);
    entry.unsubscribeActivity = undefined;
    safelyCall(entry.unsubscribeMessageDirection);
    entry.unsubscribeMessageDirection = undefined;
  }

  private disposeEntrySession(entry: RuntimeEntry): void {
    this.unsubscribeEntryObservability(entry);
    if (entry.session) this.disposeSession(entry.session);
    entry.session = undefined;
  }

  private disposeSession(session: WorkerSessionHandle): void {
    this.cleanup.supervise(session.dispose());
  }

  private awaitSupervisedCleanupBestEffort(): Effect.Effect<void> {
    return this.cleanup.awaitEmpty().pipe(
      Effect.timeoutOption(SHUTDOWN_CLEANUP_GRACE_MS),
      Effect.ignore,
    );
  }

  private requireOpen(
    operation: OrchestrationOperation,
  ): Effect.Effect<void, OrchestrationActionRejected> {
    return this.shuttingDown
      ? rejectAction(operation, "shutdown", "Orchestrator runtime is shutting down")
      : Effect.void;
  }
}

export function orchestrationLayer(
  options: OrchestrationLayerOptions = {},
): Layer.Layer<Orchestration, never, ChildSessions | GenerationSupervisor | CleanupSupervisor> {
  return Layer.effect(
    Orchestration,
    Effect.gen(function* () {
      const childSessions = yield* ChildSessions;
      const generations = yield* GenerationSupervisor;
      const cleanup = yield* CleanupSupervisor;
      return Orchestration.of(new StatefulOrchestration(
        childSessions,
        generations,
        cleanup,
        options,
      ));
    }),
  );
}

function rejectAction(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): Effect.Effect<never, OrchestrationActionRejected> {
  return Effect.fail(new OrchestrationActionRejected({ operation, reason, message }));
}

function validateContextOwner(
  operation: OrchestrationOperation,
  ownerSessionId: string,
): Effect.Effect<void, OrchestrationActionRejected> {
  return typeof ownerSessionId !== "string" || ownerSessionId.trim() === ""
    ? rejectAction(operation, "validation", "ownerSessionId must not be blank")
    : Effect.void;
}

function validateWorkerId(
  operation: OrchestrationOperation,
  workerId: string,
): Effect.Effect<WorkerId, OrchestrationActionRejected> {
  return typeof workerId !== "string" || workerId.trim() === ""
    ? rejectAction(operation, "validation", "worker_id must not be blank")
    : Effect.succeed(workerId as WorkerId);
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

function describeError(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message !== "") return error.message;
  if (typeof error === "string" && error !== "") return error;
  return fallback;
}

function isActiveWorkerStatus(status: WorkerRecord["status"]): boolean {
  return status === "starting" || status === "running" || status === "stopping";
}

function isSettledWorkerStatus(
  status: WorkerRecord["status"],
): status is RunResult["status"] {
  return status === "completed" || status === "ready" || status === "failed" || status === "aborted";
}

function noOp(): void {}

function safelyCall(callback: (() => void) | undefined): void {
  if (!callback) return;
  try {
    callback();
  } catch {
    // Session cleanup is idempotent best-effort and must not strand lifecycle state.
  }
}

function addAll(target: Set<string>, source: ReadonlySet<string>): void {
  for (const value of source) target.add(value);
}

function notifySettlementListener(
  listener: SettlementListener | undefined,
  settlement: WorkerSettlement,
): void {
  if (!listener) return;
  try {
    listener(settlement);
  } catch {
    // One observer cannot prevent settlement or other observers from being notified.
  }
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
    ...(worker.outcome ? { outcome: Object.freeze(copyOutcome(worker.outcome)) } : {}),
  });
}

function freezeAcceptedRun(id: RunId, workerId: WorkerId): AcceptedRun {
  return Object.freeze({ id, workerId });
}

function freezeCompletedRun(
  run: RunRecord,
  record: WorkerRecord,
): CompletedRun {
  const outcome = record.outcome;
  if (!outcome || outcome.status === "closed") {
    throw new Error("Completed run requires a worker response outcome");
  }
  if (record.settledAt === undefined) {
    throw new Error("Completed run requires a settlement timestamp");
  }
  const result: RunResult = Object.freeze({
    workerId: record.id,
    worker: record.worker,
    title: record.title,
    status: record.status as RunResult["status"],
    outcome: Object.freeze(copyOutcome(outcome)),
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
