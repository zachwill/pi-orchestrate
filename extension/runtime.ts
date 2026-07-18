import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Effect, Option } from "effect";
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
  createWorkflowScheduler,
  type WorkflowScheduler,
} from "./scheduler.js";
import {
  resolveWorkerModel,
  type WorkerSessionFactory,
  type WorkerSessionHandle,
} from "./worker-session.js";
import type {
  SettlementFailureStage,
  WorkerSettlement,
} from "./worker-settlement.js";

export type {
  SettlementFailureStage,
  WorkerSettlement,
} from "./worker-settlement.js";

export const MAX_TERMINAL_WORKER_HISTORY = 100;
export const MAX_COMPLETED_RUN_HISTORY = 100;
/** Shutdown waits this long for interrupted bootstrap/prompt promises, then returns best-effort. */
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

export interface CompletedResult {
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
  readonly result: CompletedResult;
}

export interface RuntimeSnapshot {
  readonly runs: readonly RunRecord[];
  readonly workers: readonly WorkerRecord[];
}

export type SettlementListener = (settlement: WorkerSettlement) => void;
export type UnsubscribeSettlement = () => void;
export type StateListener = (ownerSessionId: string) => void;

export type AbortTarget =
  | {
      readonly workerIds: readonly WorkerId[];
      readonly all?: never;
    }
  | {
      readonly all: true;
      readonly workerIds?: never;
    };

export type DeadlineResult = "settled" | "timed-out";

export interface BestEffortDeadline {
  wait(promise: Promise<unknown>, timeoutMs: number): Promise<DeadlineResult>;
}

export interface OrchestratorRuntimeOptions {
  readonly workerSessionFactory: WorkerSessionFactory;
  readonly idFactories?: OrchestrateIdFactories;
  readonly clock?: () => number;
  readonly scheduler?: WorkflowScheduler<WorkerId>;
  readonly bestEffortDeadline?: BestEffortDeadline;
}

export interface OrchestratorRuntime {
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  abort(ownerSessionId: string, target: AbortTarget): Promise<void>;
  close(ownerSessionId: string, workerId: WorkerId): Promise<void>;
  snapshot(ownerSessionId: string): Promise<RuntimeSnapshot>;
  subscribeSettlement(listener: SettlementListener): UnsubscribeSettlement;
  subscribeState(listener: StateListener): () => void;
  shutdown(): Promise<void>;
}

interface RuntimeEntry {
  readonly context: OrchestrationContext;
  readonly definition: WorkerDefinition;
  generation: number;
  session?: WorkerSessionHandle;
  unsubscribeUsage?: () => void;
  unsubscribeActivity?: () => void;
  unsubscribeMessageDirection?: () => void;
}

interface RunWaiter {
  readonly promise: Promise<CompletedRun>;
  settled: boolean;
  onSettled?: () => void;
  resolve(run: CompletedRun): void;
}

const defaultBestEffortDeadline: BestEffortDeadline = {
  wait(promise, timeoutMs) {
    const settled = Effect.promise(() => promise.then(
      () => "settled" as const,
      () => "settled" as const,
    ));
    return Effect.runPromise(
      settled.pipe(
        Effect.timeoutOption(timeoutMs),
        Effect.map((result) => Option.getOrElse(result, () => "timed-out" as const)),
      ),
    );
  },
};

class DefaultOrchestratorRuntime implements OrchestratorRuntime {
  private readonly workerSessionFactory: WorkerSessionFactory;
  private readonly idFactories: OrchestrateIdFactories;
  private readonly clock: () => number;
  private readonly scheduler: WorkflowScheduler<WorkerId>;
  private readonly bestEffortDeadline: BestEffortDeadline;
  private readonly workers = new Map<WorkerId, WorkerRecord>();
  private readonly runs = new Map<RunId, RunRecord>();
  private readonly entries = new Map<WorkerId, RuntimeEntry>();
  private readonly runWaiters = new Map<RunId, RunWaiter>();
  private readonly completedRuns = new Map<RunId, CompletedRun>();
  private readonly cancellationPromises = new Map<WorkerId, Promise<void>>();
  private readonly cleanupOperations = new Set<Promise<void>>();
  private readonly terminalWorkerOrder: WorkerId[] = [];
  private readonly completedRunOrder: RunId[] = [];
  private readonly settlementListeners = new Set<SettlementListener>();
  private readonly runSettlementListeners = new Map<RunId, SettlementListener>();
  private readonly stateListeners = new Set<StateListener>();
  private settlementSequence = 0;
  private readonly disposedSessions = new WeakSet<WorkerSessionHandle>();
  private shuttingDown = false;
  private shutdownPromise: Promise<void> | undefined;

  constructor(options: OrchestratorRuntimeOptions) {
    this.workerSessionFactory = options.workerSessionFactory;
    this.idFactories = options.idFactories ?? createRandomIdFactories();
    this.clock = options.clock ?? Date.now;
    this.scheduler = options.scheduler ?? createWorkflowScheduler<WorkerId>();
    this.bestEffortDeadline = options.bestEffortDeadline ?? defaultBestEffortDeadline;
  }

  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  async orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    this.assertOpen();
    throwIfAborted(signal);
    const definition = this.validateTask(context, task, mode);

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

    const waiter = makeRunWaiter();
    this.runs.set(runId, run);
    this.runWaiters.set(runId, waiter);
    if (onSettlement) this.runSettlementListeners.set(runId, onSettlement);
    this.workers.set(workerId, record);
    this.entries.set(workerId, { context, definition, generation: 1 });
    this.emitState(context.ownerSessionId);
    this.launchBootstrap(workerId, 1);

    if (mode === "inline") {
      return this.awaitInlineRun(run, waiter, signal);
    }
    return freezeAcceptedRun(runId, workerId);
  }

  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  async send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    this.assertOpen();
    throwIfAborted(signal);
    validateContextOwner(context.ownerSessionId);
    validateMode(mode);
    validateText("instructions", instructions, MAX_WORKER_INSTRUCTIONS_LENGTH);

    const current = this.ownedWorker(context.ownerSessionId, workerId);
    if (current.lifecycle !== "reusable" || current.status !== "ready") {
      throw new Error("worker_send requires an owned ready reusable worker");
    }
    const entry = this.entries.get(workerId);
    if (!entry?.session) throw new Error("Ready reusable worker has no session handle");

    const runId = this.idFactories.runId();
    if (this.runs.has(runId)) throw new Error(`Duplicate run ID: ${runId}`);
    const run: RunRecord = {
      id: runId,
      ownerSessionId: context.ownerSessionId,
      workerId,
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
    const waiter = makeRunWaiter();

    entry.generation += 1;
    const generation = entry.generation;
    this.runs.set(runId, run);
    this.runWaiters.set(runId, waiter);
    if (onSettlement) this.runSettlementListeners.set(runId, onSettlement);
    this.workers.set(workerId, running);
    this.subscribeEntryObservability(workerId, entry, entry.session, generation);
    this.emitState(context.ownerSessionId);
    this.launchPrompt(workerId, generation, entry.session, instructions);

    if (mode === "inline") {
      return this.awaitInlineRun(run, waiter, signal);
    }
    return freezeAcceptedRun(runId, workerId);
  }

  async abort(ownerSessionId: string, target: AbortTarget): Promise<void> {
    this.assertOpen();
    validateContextOwner(ownerSessionId);
    const targets = this.resolveAbortTargets(ownerSessionId, target);
    await this.cancelWorkers(targets);
  }

  async close(ownerSessionId: string, workerId: WorkerId): Promise<void> {
    this.assertOpen();
    validateContextOwner(ownerSessionId);
    const current = this.ownedWorker(ownerSessionId, workerId);
    if (current.lifecycle !== "reusable" || current.status !== "ready") {
      throw new Error("worker_close requires an owned ready reusable worker");
    }
    this.closeReadyWorker(current);
  }

  async snapshot(ownerSessionId: string): Promise<RuntimeSnapshot> {
    validateContextOwner(ownerSessionId);
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
  }

  subscribeSettlement(listener: SettlementListener): UnsubscribeSettlement {
    if (typeof listener !== "function") throw new Error("Settlement listener must be a function");
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
    this.stateListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.stateListeners.delete(listener);
    };
  }

  shutdown(): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shuttingDown = true;
    this.shutdownPromise = this.performShutdown();
    return this.shutdownPromise;
  }

  private async performShutdown(): Promise<void> {
    try {
      this.closeReadyWorkersForShutdown();
      const active = [...this.workers.values()]
        .filter((worker) => isActiveWorkerStatus(worker.status))
        .map((worker) => worker.id);
      await this.cancelWorkers(active);
    } finally {
      try {
        await this.scheduler.close();
      } catch {
        // Scheduler closure is best-effort during process shutdown.
      } finally {
        await this.awaitTrackedCleanupBestEffort();
        this.runSettlementListeners.clear();
        this.settlementListeners.clear();
        this.stateListeners.clear();
      }
    }
  }

  private validateTask(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
  ): WorkerDefinition {
    validateContextOwner(context.ownerSessionId);
    validateMode(mode);
    if (!task || typeof task !== "object" || Array.isArray(task)) {
      throw new Error("orchestrate requires one task object");
    }
    if (context.synthesisGroup) {
      validateText("synthesis group ID", context.synthesisGroup.id, MAX_WORKER_TITLE_LENGTH);
      if (mode !== "async") throw new Error("Sibling synthesis requires an async task");
      if (!Number.isSafeInteger(context.synthesisGroup.size) || context.synthesisGroup.size < 2) {
        throw new Error("Synthesis group size must be an integer of at least 2");
      }
    }

    validateText("worker", task.worker, MAX_WORKER_TITLE_LENGTH);
    validateText("title", task.title, MAX_WORKER_TITLE_LENGTH);
    validateText("instructions", task.instructions, MAX_WORKER_INSTRUCTIONS_LENGTH);
    const definition = findWorkerByName(context.catalog, task.worker);
    if (!definition) throw new Error(`Unknown worker: ${task.worker}`);
    resolveWorkerModel(definition, context.parentModel, context.modelRegistry);
    return definition;
  }

  private assertFreshIds(runId: RunId, workerId: WorkerId): void {
    if (this.runs.has(runId)) throw new Error(`Duplicate run ID: ${runId}`);
    if (this.workers.has(workerId)) throw new Error(`Duplicate worker ID: ${workerId}`);
  }

  private launchBootstrap(workerId: WorkerId, generation: number): void {
    try {
      this.scheduler.start(
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
      this.scheduler.start(
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

      let creation: Promise<WorkerSessionHandle>;
      try {
        creation = this.workerSessionFactory.create({
          cwd: entry.context.cwd,
          agentDir: entry.context.agentDir,
          parentSessionFile: entry.context.parentSessionFile,
          projectTrusted: entry.context.projectTrusted,
          definition: entry.definition,
          parentModel: entry.context.parentModel,
          modelRegistry: entry.context.modelRegistry,
        });
      } catch (error) {
        this.settleCreationFailure(workerId, generation, error);
        return Effect.succeed(undefined);
      }

      this.trackCleanup(creation.then(() => undefined, () => undefined));
      void creation.then((session) => {
        if (!this.canAdoptCreatedSession(workerId, generation, entry)) {
          this.disposeSession(session);
        }
      }, () => undefined);

      return Effect.tryPromise({
        try: () => creation,
        catch: (error) => error,
      }).pipe(
        Effect.match({
          onFailure: (error) => {
            this.settleCreationFailure(workerId, generation, error);
            return undefined;
          },
          onSuccess: (session) => this.adoptCreatedSession(
            workerId,
            generation,
            entry,
            session,
          ),
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
      this.disposeSession(session);
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
      this.disposeEntrySession(entry);
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

      let prompt: Promise<WorkerOutcome>;
      try {
        prompt = session.prompt(instructions);
      } catch (error) {
        this.settleOutcome(workerId, generation, session, {
          status: "failed",
          message: describeError(error, "Worker prompt failed"),
        });
        return Effect.void;
      }
      this.trackCleanup(prompt.then(() => undefined, () => undefined));

      return Effect.tryPromise({
        try: () => prompt,
        catch: (error) => error,
      }).pipe(
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
    } else if (current.lifecycle === "reusable" && outcome.status === "ready") {
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
    this.runWaiters.get(runId)?.resolve(completed);
    this.runWaiters.delete(runId);
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

  private resolveAbortTargets(ownerSessionId: string, target: AbortTarget): WorkerId[] {
    if (!target || typeof target !== "object") throw new Error("Invalid abort target");
    const candidate = target as {
      workerIds?: readonly WorkerId[];
      all?: boolean;
    };
    const selected = [
      candidate.workerIds !== undefined,
      candidate.all !== undefined,
    ].filter(Boolean).length;
    if (selected !== 1 || (candidate.all !== undefined && candidate.all !== true)) {
      throw new Error("Abort target must specify exactly one of workerIds or all: true");
    }

    if (candidate.workerIds !== undefined) {
      if (!Array.isArray(candidate.workerIds) || candidate.workerIds.length === 0) {
        throw new Error("workerIds must contain at least one worker ID");
      }
      const unique = [...new Set(candidate.workerIds)];
      for (const workerId of unique) {
        const worker = this.ownedWorker(ownerSessionId, workerId);
        if (worker.status === "ready") {
          throw new Error("Ready reusable workers are not active; use worker_close");
        }
        if (!isActiveWorkerStatus(worker.status)) {
          throw new Error("worker_abort requires owned active workers");
        }
      }
      return unique;
    }

    return [...this.workers.values()]
      .filter(
        (worker) =>
          worker.ownerSessionId === ownerSessionId && isActiveWorkerStatus(worker.status),
      )
      .map((worker) => worker.id);
  }

  private async cancelWorkers(workerIds: readonly WorkerId[]): Promise<void> {
    const owners = new Set<string>();
    const cancellations: Promise<void>[] = [];

    for (const workerId of workerIds) {
      const current = this.workers.get(workerId);
      if (!current || !isActiveWorkerStatus(current.status)) continue;
      if (current.status !== "stopping") {
        this.workers.set(workerId, {
          ...transitionWorkerStatus(current, "stopping"),
          activity: undefined,
        });
      }
      owners.add(current.ownerSessionId);
      cancellations.push(this.cancellationFor(workerId));
    }

    this.emitStateForOwners(owners);
    await Promise.all(cancellations);
  }

  private cancellationFor(workerId: WorkerId): Promise<void> {
    const existing = this.cancellationPromises.get(workerId);
    if (existing) return existing;

    const cancellation = Promise.resolve()
      .then(() => this.cancelWorker(workerId))
      .catch((error) => {
        this.settleCancellationFailure(workerId, error);
      });
    this.cancellationPromises.set(workerId, cancellation);
    void cancellation.then(() => {
      if (this.cancellationPromises.get(workerId) === cancellation) {
        this.cancellationPromises.delete(workerId);
      }
    });
    return cancellation;
  }

  private async cancelWorker(workerId: WorkerId): Promise<void> {
    const entry = this.entries.get(workerId);
    const session = entry?.session;

    try {
      if (session) {
        const abortOperation = Promise.resolve()
          .then(() => session.abort())
          .catch(() => undefined);
        await this.waitBestEffort(abortOperation, CANCELLATION_GRACE_MS);
      }
      try {
        await this.scheduler.remove(workerId);
      } catch {
        // Worker state still settles even if scheduler cleanup fails.
      }
    } finally {
      if (entry) this.disposeEntrySession(entry);
      const current = this.workers.get(workerId);
      if (current?.status === "stopping") {
        this.settleTerminalWorker(
          current,
          "aborted",
          { status: "aborted" },
          "cancellation",
        );
      }
    }
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

  private async cancelExactRun(run: RunRecord): Promise<void> {
    const worker = this.workers.get(run.workerId);
    const active = worker?.ownerSessionId === run.ownerSessionId &&
        worker.runId === run.id &&
        isActiveWorkerStatus(worker.status)
      ? [worker.id]
      : [];
    await this.cancelWorkers(active);
    this.maybeCompleteRun(run.id);
  }

  private awaitInlineRun(
    run: RunRecord,
    waiter: RunWaiter,
    signal: AbortSignal | undefined,
  ): Promise<CompletedRun> {
    if (!signal) return waiter.promise;

    return new Promise<CompletedRun>((resolve, reject) => {
      let abortClaimed = false;
      const removeAbortListener = () => signal.removeEventListener("abort", onAbort);
      const onAbort = () => {
        if (abortClaimed || waiter.settled) return;
        abortClaimed = true;
        removeAbortListener();
        const reason = abortSignalReason(signal);
        void this.cancelExactRun(run).then(
          () => reject(reason),
          () => reject(reason),
        );
      };

      waiter.onSettled = () => {
        removeAbortListener();
        if (!abortClaimed) waiter.promise.then(resolve, reject);
      };
      signal.addEventListener("abort", onAbort, { once: true });

      if (signal.aborted) onAbort();
      else if (waiter.settled) waiter.onSettled();
    });
  }

  private closeReadyWorker(current: WorkerRecord): void {
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

  private closeReadyWorkersForShutdown(): void {
    const ready = [...this.workers.values()].filter((worker) => worker.status === "ready");
    for (const worker of ready) this.closeReadyWorker(worker);
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

  private ownedWorker(ownerSessionId: string, workerId: WorkerId): WorkerRecord {
    const worker = this.workers.get(workerId);
    if (!worker || worker.ownerSessionId !== ownerSessionId) {
      throw new Error("Worker is not owned by this session");
    }
    return worker;
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
    if (this.disposedSessions.has(session)) return;
    this.disposedSessions.add(session);

    try {
      this.trackCleanup(session.dispose());
    } catch {
      // Cleanup is best-effort and cannot leave lifecycle state unsettled.
    }
  }

  private trackCleanup(operation: Promise<void>): Promise<void> {
    this.cleanupOperations.add(operation);
    const forget = () => this.cleanupOperations.delete(operation);
    void operation.then(forget, forget);
    return operation;
  }

  private async awaitTrackedCleanupBestEffort(): Promise<void> {
    const operations = [...this.cleanupOperations];
    if (operations.length === 0) return;
    await this.waitBestEffort(
      Promise.allSettled(operations).then(() => undefined),
      SHUTDOWN_CLEANUP_GRACE_MS,
    );
  }

  private async waitBestEffort(promise: Promise<unknown>, timeoutMs: number): Promise<void> {
    try {
      await this.bestEffortDeadline.wait(promise, timeoutMs);
    } catch {
      // A deadline implementation cannot prevent lifecycle settlement.
    }
  }

  private assertOpen(): void {
    if (this.shuttingDown) throw new Error("Orchestrator runtime is shutting down");
  }
}

export function createOrchestratorRuntime(
  options: OrchestratorRuntimeOptions,
): OrchestratorRuntime {
  return new DefaultOrchestratorRuntime(options);
}

function validateContextOwner(ownerSessionId: string): void {
  if (typeof ownerSessionId !== "string" || ownerSessionId.trim() === "") {
    throw new Error("ownerSessionId must not be blank");
  }
}

function validateMode(mode: RunMode): void {
  if (mode !== "async" && mode !== "inline") throw new Error("Invalid orchestration mode");
}

function validateText(name: string, value: string, maximumLength: number): void {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${name} must not be blank`);
  }
  if (value.length > maximumLength) {
    throw new Error(`${name} must be at most ${maximumLength} characters`);
  }
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
): status is CompletedResult["status"] {
  return status === "completed" || status === "ready" || status === "failed" || status === "aborted";
}

function safelyCall(callback: (() => void) | undefined): void {
  if (!callback) return;
  try {
    callback();
  } catch {
    // Session cleanup is idempotent best-effort and must not strand lifecycle state.
  }
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw abortSignalReason(signal);
}

function abortSignalReason(signal: AbortSignal): unknown {
  if ("reason" in signal) return signal.reason;
  return new DOMException("This operation was aborted", "AbortError");
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

function makeRunWaiter(): RunWaiter {
  let complete!: (run: CompletedRun) => void;
  const waiter: RunWaiter = {
    promise: new Promise<CompletedRun>((resolve) => {
      complete = resolve;
    }),
    settled: false,
    resolve(run) {
      if (waiter.settled) return;
      waiter.settled = true;
      waiter.onSettled?.();
      complete(run);
    },
  };
  return waiter;
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
  const result: CompletedResult = Object.freeze({
    workerId: record.id,
    worker: record.worker,
    title: record.title,
    status: record.status as CompletedResult["status"],
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
