import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import {
  CANCELLATION_GRACE_MS,
  EMPTY_WORKER_USAGE,
  MAX_TASKS_PER_WAVE,
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  createRandomIdFactories,
  findWorkerByName,
  isTerminalWorkerStatus,
  isWorkerCompleteForWave,
  transitionWorkerStatus,
  type OrchestrateIdFactories,
  type OrchestrateTaskInput,
  type WaveCompleteWorkerStatus,
  type WaveId,
  type WaveMode,
  type WaveRecord,
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

export const MAX_TERMINAL_WORKER_HISTORY = 100;
export const MAX_COMPLETED_WAVE_HISTORY = 100;
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
}

export interface AcceptedWave {
  readonly id: WaveId;
  readonly workerIds: readonly WorkerId[];
}

export interface CompletedResult {
  readonly workerId: WorkerId;
  readonly worker: string;
  readonly title: string;
  readonly status: WaveCompleteWorkerStatus;
  readonly outcome: WorkerOutcome;
  readonly usage: WorkerUsage;
  readonly sessionFile: string | undefined;
}

export interface CompletedWave {
  readonly id: WaveId;
  readonly ownerSessionId: string;
  readonly mode: WaveMode;
  readonly results: readonly CompletedResult[];
}

export type SettlementFailureStage = "startup" | "prompt" | "workflow" | "cancellation";

export interface WorkerSettlement {
  readonly eventId: string;
  readonly sequence: number;
  readonly ownerSessionId: string;
  readonly waveId: WaveId;
  readonly workerId: WorkerId;
  readonly generation: number;
  readonly mode: WaveMode;
  readonly worker: string;
  readonly title: string;
  readonly lifecycle: WorkerRecord["lifecycle"];
  readonly status: Exclude<WaveCompleteWorkerStatus, "closed">;
  readonly outcome: WorkerOutcome;
  readonly failureStage?: SettlementFailureStage;
  readonly usage: WorkerUsage;
  readonly startedAt: number;
  readonly settledAt: number;
  readonly remainingActive: number;
  readonly waveSize: number;
  readonly waveComplete: boolean;
  readonly sessionFile: string | undefined;
}

export interface RuntimeSnapshot {
  readonly waves: readonly WaveRecord[];
  readonly workers: readonly WorkerRecord[];
}

export type CompletionListener = (wave: CompletedWave) => void;
export type UnsubscribeCompletion = () => void;
export type SettlementListener = (settlement: WorkerSettlement) => void;
export type UnsubscribeSettlement = () => void;
export type StateListener = (ownerSessionId: string) => void;

export type AbortTarget =
  | {
      readonly workerIds: readonly WorkerId[];
      readonly waveId?: never;
      readonly all?: never;
    }
  | {
      readonly waveId: WaveId;
      readonly workerIds?: never;
      readonly all?: never;
    }
  | {
      readonly all: true;
      readonly workerIds?: never;
      readonly waveId?: never;
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
    tasks: readonly OrchestrateTaskInput[],
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave>;
  orchestrate(
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedWave>;
  orchestrate(
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: WaveMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave | CompletedWave>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedWave>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: WaveMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave | CompletedWave>;
  abort(ownerSessionId: string, target: AbortTarget): Promise<void>;
  close(ownerSessionId: string, workerId: WorkerId): Promise<void>;
  snapshot(ownerSessionId: string): Promise<RuntimeSnapshot>;
  subscribeCompletion(listener: CompletionListener): UnsubscribeCompletion;
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
}

interface WaveWaiter {
  readonly promise: Promise<CompletedWave>;
  settled: boolean;
  onSettled?: () => void;
  resolve(wave: CompletedWave): void;
}

const defaultBestEffortDeadline: BestEffortDeadline = {
  wait(promise, timeoutMs) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const settled = promise.then(
      () => "settled" as const,
      () => "settled" as const,
    );
    const timedOut = new Promise<DeadlineResult>((resolve) => {
      timer = setTimeout(() => resolve("timed-out"), timeoutMs);
    });
    return Promise.race([settled, timedOut]).finally(() => {
      if (timer !== undefined) clearTimeout(timer);
    });
  },
};

class DefaultOrchestratorRuntime implements OrchestratorRuntime {
  private readonly workerSessionFactory: WorkerSessionFactory;
  private readonly idFactories: OrchestrateIdFactories;
  private readonly clock: () => number;
  private readonly scheduler: WorkflowScheduler<WorkerId>;
  private readonly bestEffortDeadline: BestEffortDeadline;
  private readonly workers = new Map<WorkerId, WorkerRecord>();
  private readonly waves = new Map<WaveId, WaveRecord>();
  private readonly entries = new Map<WorkerId, RuntimeEntry>();
  private readonly waveWaiters = new Map<WaveId, WaveWaiter>();
  private readonly completedWaves = new Map<WaveId, CompletedWave>();
  private readonly cancellationPromises = new Map<WorkerId, Promise<void>>();
  private readonly cleanupOperations = new Set<Promise<void>>();
  private readonly terminalWorkerOrder: WorkerId[] = [];
  private readonly completedWaveOrder: WaveId[] = [];
  private readonly completionListeners = new Set<CompletionListener>();
  private readonly settlementListeners = new Set<SettlementListener>();
  private readonly waveSettlementListeners = new Map<WaveId, SettlementListener>();
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
    tasks: readonly OrchestrateTaskInput[],
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave>;
  orchestrate(
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedWave>;
  orchestrate(
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: WaveMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave | CompletedWave>;
  async orchestrate(
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: WaveMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave | CompletedWave> {
    this.assertOpen();
    throwIfAborted(signal);
    const definitions = this.validateTasks(context, tasks, mode);

    const waveId = this.idFactories.waveId();
    const workerIds = tasks.map(() => this.idFactories.workerId());
    this.assertFreshIds(waveId, workerIds);

    const wave: WaveRecord = {
      id: waveId,
      ownerSessionId: context.ownerSessionId,
      workerIds: [...workerIds],
      mode,
      state: "running",
      createdAt: this.clock(),
    };
    const records = tasks.map<WorkerRecord>((task, index) => {
      const definition = definitions[index];
      const id = workerIds[index];
      if (!definition || !id) throw new Error("Validated orchestration input became inconsistent");
      return {
        id,
        worker: definition.name,
        ownerSessionId: context.ownerSessionId,
        waveId,
        title: task.title,
        instructions: task.instructions,
        lifecycle: definition.lifecycle,
        status: "starting",
        usage: copyUsage(EMPTY_WORKER_USAGE),
        startedAt: this.clock(),
      };
    });

    const waiter = makeWaveWaiter();
    this.waves.set(waveId, wave);
    this.waveWaiters.set(waveId, waiter);
    if (onSettlement) this.waveSettlementListeners.set(waveId, onSettlement);
    for (let index = 0; index < records.length; index += 1) {
      const record = records[index];
      const definition = definitions[index];
      if (!record || !definition) throw new Error("Validated orchestration input became inconsistent");
      this.workers.set(record.id, record);
      this.entries.set(record.id, { context, definition, generation: 1 });
    }
    this.emitState(context.ownerSessionId);

    for (const record of records) this.launchBootstrap(record.id, 1);

    if (mode === "inline") {
      return this.awaitInlineWave(wave, waiter, signal);
    }
    return freezeAcceptedWave(waveId, workerIds);
  }

  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedWave>;
  send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: WaveMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave | CompletedWave>;
  async send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: WaveMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedWave | CompletedWave> {
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

    const waveId = this.idFactories.waveId();
    if (this.waves.has(waveId)) throw new Error(`Duplicate wave ID: ${waveId}`);
    const wave: WaveRecord = {
      id: waveId,
      ownerSessionId: context.ownerSessionId,
      workerIds: [workerId],
      mode,
      state: "running",
      createdAt: this.clock(),
    };
    const running = {
      ...transitionWorkerStatus(current, "running"),
      waveId,
      instructions,
      activity: undefined,
      startedAt: this.clock(),
      settledAt: undefined,
    };
    const waiter = makeWaveWaiter();

    entry.generation += 1;
    const generation = entry.generation;
    this.waves.set(waveId, wave);
    this.waveWaiters.set(waveId, waiter);
    if (onSettlement) this.waveSettlementListeners.set(waveId, onSettlement);
    this.workers.set(workerId, running);
    this.subscribeEntryObservability(workerId, entry, entry.session, generation);
    this.emitState(context.ownerSessionId);
    this.launchPrompt(workerId, generation, entry.session, instructions);

    if (mode === "inline") {
      return this.awaitInlineWave(wave, waiter, signal);
    }
    return freezeAcceptedWave(waveId, [workerId]);
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
    const waves = [...this.waves.values()]
      .filter((wave) => wave.ownerSessionId === ownerSessionId)
      .map(copyWaveRecord);
    const workers = [...this.workers.values()]
      .filter((worker) => worker.ownerSessionId === ownerSessionId)
      .map(copyWorkerRecord);
    return Object.freeze({
      waves: Object.freeze(waves),
      workers: Object.freeze(workers),
    });
  }

  subscribeCompletion(listener: CompletionListener): UnsubscribeCompletion {
    if (typeof listener !== "function") throw new Error("Completion listener must be a function");
    this.completionListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.completionListeners.delete(listener);
    };
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
        this.waveSettlementListeners.clear();
        this.completionListeners.clear();
        this.settlementListeners.clear();
        this.stateListeners.clear();
      }
    }
  }

  private validateTasks(
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: WaveMode,
  ): WorkerDefinition[] {
    validateContextOwner(context.ownerSessionId);
    validateMode(mode);
    if (!Array.isArray(tasks) || tasks.length < 1 || tasks.length > MAX_TASKS_PER_WAVE) {
      throw new Error(`orchestrate requires 1 to ${MAX_TASKS_PER_WAVE} tasks`);
    }

    const definitions: WorkerDefinition[] = [];
    for (const task of tasks) {
      if (!task || typeof task !== "object") throw new Error("Each task must be an object");
      validateText("worker", task.worker, MAX_WORKER_TITLE_LENGTH);
      validateText("title", task.title, MAX_WORKER_TITLE_LENGTH);
      validateText("instructions", task.instructions, MAX_WORKER_INSTRUCTIONS_LENGTH);
      const definition = findWorkerByName(context.catalog, task.worker);
      if (!definition) throw new Error(`Unknown worker: ${task.worker}`);
      definitions.push(definition);
    }

    for (const definition of definitions) {
      resolveWorkerModel(definition, context.parentModel, context.modelRegistry);
    }
    return definitions;
  }

  private assertFreshIds(waveId: WaveId, workerIds: readonly WorkerId[]): void {
    if (this.waves.has(waveId)) throw new Error(`Duplicate wave ID: ${waveId}`);
    const unique = new Set<WorkerId>();
    for (const workerId of workerIds) {
      if (unique.has(workerId) || this.workers.has(workerId)) {
        throw new Error(`Duplicate worker ID: ${workerId}`);
      }
      unique.add(workerId);
    }
  }

  private launchBootstrap(workerId: WorkerId, generation: number): void {
    try {
      this.scheduler.start(
        workerId,
        () => this.trackCleanup(this.bootstrapAndPrompt(workerId, generation)),
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
        () => this.trackCleanup(this.executePrompt(workerId, generation, session, instructions)),
        (error) => this.settleWorkflowDefect(workerId, generation, error),
      );
    } catch (error) {
      this.settleWorkflowDefect(workerId, generation, error);
    }
  }

  private async bootstrapAndPrompt(workerId: WorkerId, generation: number): Promise<void> {
    const session = await this.bootstrap(workerId, generation);
    if (!session) return;
    const current = this.workers.get(workerId);
    if (!current) return;
    await this.executePrompt(workerId, generation, session, current.instructions);
  }

  private async bootstrap(
    workerId: WorkerId,
    generation: number,
  ): Promise<WorkerSessionHandle | undefined> {
    const entry = this.entries.get(workerId);
    if (!entry) return undefined;

    let session: WorkerSessionHandle;
    try {
      session = await this.workerSessionFactory.create({
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
      return undefined;
    }

    const current = this.workers.get(workerId);
    if (
      this.shuttingDown ||
      !current ||
      current.status !== "starting" ||
      entry.generation !== generation
    ) {
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

  private async executePrompt(
    workerId: WorkerId,
    generation: number,
    session: WorkerSessionHandle,
    instructions: string,
  ): Promise<void> {
    const before = this.workers.get(workerId);
    const entry = this.entries.get(workerId);
    if (
      !before ||
      before.status !== "running" ||
      !entry ||
      entry.generation !== generation ||
      entry.session !== session
    ) {
      return;
    }

    let outcome: WorkerOutcome;
    try {
      outcome = await session.prompt(instructions);
    } catch (error) {
      outcome = { status: "failed", message: describeError(error, "Worker prompt failed") };
    }
    this.settleOutcome(workerId, generation, session, outcome);
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
      this.maybeCompleteWave(current.waveId);
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
    const affectedOwners = this.maybeCompleteWave(current.waveId);
    affectedOwners.add(current.ownerSessionId);
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
    const affectedOwners = this.maybeCompleteWave(current.waveId);
    affectedOwners.add(current.ownerSessionId);
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
    const wave = worker ? this.waves.get(worker.waveId) : undefined;
    if (!worker || !wave || !worker.outcome || !isWorkerCompleteForWave(worker.status)) return;
    if (worker.status === "closed") return;

    let remainingActive = 0;
    let waveComplete = true;
    for (const waveWorkerId of wave.workerIds) {
      const waveWorker = this.workers.get(waveWorkerId);
      if (!waveWorker || waveWorker.waveId !== wave.id || !isWorkerCompleteForWave(waveWorker.status)) {
        waveComplete = false;
        if (waveWorker && isActiveWorkerStatus(waveWorker.status)) remainingActive += 1;
      }
    }

    const sequence = ++this.settlementSequence;
    const settlement: WorkerSettlement = Object.freeze({
      eventId: `${sequence}:${wave.id}:${workerId}:${generation}`,
      sequence,
      ownerSessionId: worker.ownerSessionId,
      waveId: wave.id,
      workerId,
      generation,
      mode: wave.mode,
      worker: worker.worker,
      title: worker.title,
      lifecycle: worker.lifecycle,
      status: worker.status,
      outcome: Object.freeze(copyOutcome(worker.outcome)),
      ...(failureStage ? { failureStage } : {}),
      usage: Object.freeze(copyUsage(worker.usage)),
      startedAt: worker.startedAt,
      settledAt,
      remainingActive,
      waveSize: wave.workerIds.length,
      waveComplete,
      sessionFile: worker.sessionFile,
    });

    const localListener = this.waveSettlementListeners.get(wave.id);
    if (waveComplete) this.waveSettlementListeners.delete(wave.id);
    notifySettlementListener(localListener, settlement);
    for (const listener of [...this.settlementListeners]) {
      notifySettlementListener(listener, settlement);
    }
    if (waveComplete) this.emitCompletedWave(wave.id);
  }

  private emitCompletedWave(waveId: WaveId): void {
    const completed = this.completedWaves.get(waveId);
    if (!completed || completed.mode !== "async") return;
    for (const listener of [...this.completionListeners]) {
      try {
        listener(completed);
      } catch {
        // One subscriber cannot prevent other subscribers from receiving completion.
      }
    }
  }

  private maybeCompleteWave(waveId: WaveId): Set<string> {
    const affectedOwners = new Set<string>();
    if (this.completedWaves.has(waveId)) return affectedOwners;
    const wave = this.waves.get(waveId);
    if (!wave) return affectedOwners;
    const records: WorkerRecord[] = [];
    for (const workerId of wave.workerIds) {
      const worker = this.workers.get(workerId);
      if (
        !worker ||
        worker.waveId !== waveId ||
        !isWorkerCompleteForWave(worker.status) ||
        !worker.outcome
      ) {
        return affectedOwners;
      }
      records.push(worker);
    }

    const completed = freezeCompletedWave(wave, records);
    this.completedWaves.set(waveId, completed);
    this.completedWaveOrder.push(waveId);
    this.waves.set(waveId, { ...wave, state: "complete" });
    this.waveWaiters.get(waveId)?.resolve(completed);
    this.waveWaiters.delete(waveId);

    return affectedOwners;
  }

  private rememberTerminalWorker(workerId: WorkerId): Set<string> {
    if (!this.terminalWorkerOrder.includes(workerId)) {
      this.terminalWorkerOrder.push(workerId);
    }
    return this.pruneHistory();
  }

  private pruneHistory(): Set<string> {
    const affectedOwners = new Set<string>();
    while (this.completedWaveOrder.length > MAX_COMPLETED_WAVE_HISTORY) {
      const waveId = this.completedWaveOrder.shift();
      if (!waveId) break;
      this.completedWaves.delete(waveId);
      const wave = this.waves.get(waveId);
      if (wave) affectedOwners.add(wave.ownerSessionId);
      this.waves.delete(waveId);
    }

    while (this.terminalWorkerOrder.length > MAX_TERMINAL_WORKER_HISTORY) {
      const removableIndex = this.terminalWorkerOrder.findIndex((workerId) => {
        const worker = this.workers.get(workerId);
        if (!worker || !isTerminalWorkerStatus(worker.status)) return true;
        return this.waves.get(worker.waveId)?.state !== "running";
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
      waveId?: WaveId;
      all?: boolean;
    };
    const selected = [
      candidate.workerIds !== undefined,
      candidate.waveId !== undefined,
      candidate.all !== undefined,
    ].filter(Boolean).length;
    if (selected !== 1 || (candidate.all !== undefined && candidate.all !== true)) {
      throw new Error("Abort target must specify exactly one of workerIds, waveId, or all: true");
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

    if (candidate.waveId !== undefined) {
      const wave = this.waves.get(candidate.waveId);
      if (!wave || wave.ownerSessionId !== ownerSessionId) {
        throw new Error("Wave is not owned by this session");
      }
      return wave.workerIds.filter((workerId) => {
        const worker = this.workers.get(workerId);
        return (
          worker?.ownerSessionId === ownerSessionId &&
          worker.waveId === wave.id &&
          isActiveWorkerStatus(worker.status)
        );
      });
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

  private async cancelExactWave(wave: WaveRecord): Promise<void> {
    const active = wave.workerIds.filter((workerId) => {
      const worker = this.workers.get(workerId);
      return (
        worker?.ownerSessionId === wave.ownerSessionId &&
        worker.waveId === wave.id &&
        isActiveWorkerStatus(worker.status)
      );
    });
    await this.cancelWorkers(active);
    this.maybeCompleteWave(wave.id);
  }

  private awaitInlineWave(
    wave: WaveRecord,
    waiter: WaveWaiter,
    signal: AbortSignal | undefined,
  ): Promise<CompletedWave> {
    if (!signal) return waiter.promise;

    return new Promise<CompletedWave>((resolve, reject) => {
      let abortClaimed = false;
      const removeAbortListener = () => signal.removeEventListener("abort", onAbort);
      const onAbort = () => {
        if (abortClaimed || waiter.settled) return;
        abortClaimed = true;
        removeAbortListener();
        const reason = abortSignalReason(signal);
        void this.cancelExactWave(wave).then(
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
    const affectedOwners = this.maybeCompleteWave(current.waveId);
    addAll(affectedOwners, this.rememberTerminalWorker(current.id));
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
  }

  private disposeEntrySession(entry: RuntimeEntry): void {
    this.unsubscribeEntryObservability(entry);
    if (entry.session) this.disposeSession(entry.session);
    entry.session = undefined;
  }

  private disposeSession(session: WorkerSessionHandle): void {
    if (this.disposedSessions.has(session)) return;
    this.disposedSessions.add(session);
    safelyCall(() => session.dispose());
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

function validateMode(mode: WaveMode): void {
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
  return signal.reason ?? new DOMException("This operation was aborted", "AbortError");
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

function makeWaveWaiter(): WaveWaiter {
  let complete!: (wave: CompletedWave) => void;
  const waiter: WaveWaiter = {
    promise: new Promise<CompletedWave>((resolve) => {
      complete = resolve;
    }),
    settled: false,
    resolve(wave) {
      if (waiter.settled) return;
      waiter.settled = true;
      waiter.onSettled?.();
      complete(wave);
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

function copyOutcome(outcome: WorkerOutcome): WorkerOutcome {
  return { ...outcome };
}

function copyWaveRecord(wave: WaveRecord): WaveRecord {
  return Object.freeze({
    ...wave,
    workerIds: Object.freeze([...wave.workerIds]),
  });
}

function copyWorkerRecord(worker: WorkerRecord): WorkerRecord {
  return Object.freeze({
    ...worker,
    usage: Object.freeze(copyUsage(worker.usage)),
    ...(worker.outcome ? { outcome: Object.freeze(copyOutcome(worker.outcome)) } : {}),
  });
}

function freezeAcceptedWave(
  id: WaveId,
  workerIds: readonly WorkerId[],
): AcceptedWave {
  return Object.freeze({ id, workerIds: Object.freeze([...workerIds]) });
}

function freezeCompletedWave(
  wave: WaveRecord,
  records: readonly WorkerRecord[],
): CompletedWave {
  const results = records.map<CompletedResult>((record) =>
    Object.freeze({
      workerId: record.id,
      worker: record.worker,
      title: record.title,
      status: record.status as WaveCompleteWorkerStatus,
      outcome: Object.freeze(copyOutcome(record.outcome!)),
      usage: Object.freeze(copyUsage(record.usage)),
      sessionFile: record.sessionFile,
    }),
  );
  return Object.freeze({
    id: wave.id,
    ownerSessionId: wave.ownerSessionId,
    mode: wave.mode,
    results: Object.freeze(results),
  });
}
