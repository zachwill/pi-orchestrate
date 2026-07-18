import type { ThinkingLevel } from "@earendil-works/pi-agent-core";

export const SUPPORTED_TOOL_NAMES = [
  "read",
  "bash",
  "edit",
  "write",
  "grep",
  "find",
  "ls",
] as const;

export type SupportedToolName = (typeof SUPPORTED_TOOL_NAMES)[number];

const supportedToolNames: ReadonlySet<string> = new Set(SUPPORTED_TOOL_NAMES);

export function isSupportedToolName(value: unknown): value is SupportedToolName {
  return typeof value === "string" && supportedToolNames.has(value);
}

export type WorkerSourceKind = "package" | "user" | "project";

export interface WorkerSource {
  readonly kind: WorkerSourceKind;
  readonly filePath: string;
}

export interface WorkerModel {
  readonly provider: string;
  readonly modelId: string;
}

export interface WorkerCompaction {
  readonly enabled?: boolean;
  readonly reserveTokens?: number;
  readonly keepRecentTokens?: number;
}

export type WorkerLifecycle = "one-shot" | "reusable";

export interface WorkerDefinition {
  readonly name: string;
  readonly source: WorkerSource;
  readonly description: string;
  readonly systemPrompt: string;
  readonly lifecycle: WorkerLifecycle;
  readonly tools: readonly SupportedToolName[];
  readonly skills?: readonly string[];
  readonly model?: WorkerModel;
  readonly thinking?: ThinkingLevel;
  readonly compaction?: WorkerCompaction;
}

export type CatalogDiagnosticSeverity = "warning" | "error";

export interface CatalogDiagnostic {
  readonly severity: CatalogDiagnosticSeverity;
  readonly source: WorkerSourceKind;
  readonly message: string;
  readonly filePath?: string;
}

export interface WorkerCatalog {
  readonly workers: readonly WorkerDefinition[];
  readonly diagnostics: readonly CatalogDiagnostic[];
}

export function createWorkerCatalog(
  workers: readonly WorkerDefinition[],
  diagnostics: readonly CatalogDiagnostic[] = [],
): WorkerCatalog {
  return {
    workers: [...workers].sort(compareWorkersByName),
    diagnostics: [...diagnostics],
  };
}

export function findWorkerByName(
  catalog: WorkerCatalog,
  name: string,
): WorkerDefinition | undefined {
  return catalog.workers.find((worker) => worker.name === name);
}

function compareWorkersByName(left: WorkerDefinition, right: WorkerDefinition): number {
  if (left.name < right.name) return -1;
  if (left.name > right.name) return 1;
  return 0;
}

export interface OrchestrateTaskInput {
  readonly worker: string;
  readonly title: string;
  readonly instructions: string;
}

declare const workerIdBrand: unique symbol;
declare const waveIdBrand: unique symbol;

export type WorkerId = string & { readonly [workerIdBrand]: "WorkerId" };
export type WaveId = string & { readonly [waveIdBrand]: "WaveId" };

export type WorkerIdFactory = () => WorkerId;
export type WaveIdFactory = () => WaveId;

export interface OrchestrateIdFactories {
  readonly workerId: WorkerIdFactory;
  readonly waveId: WaveIdFactory;
}

export function createRandomWorkerIdFactory(
  randomId: () => string = defaultRandomId,
): WorkerIdFactory {
  return () => `worker-${randomId()}` as WorkerId;
}

export function createRandomWaveIdFactory(
  randomId: () => string = defaultRandomId,
): WaveIdFactory {
  return () => `wave-${randomId()}` as WaveId;
}

export function createRandomIdFactories(
  randomId: () => string = defaultRandomId,
): OrchestrateIdFactories {
  return {
    workerId: createRandomWorkerIdFactory(randomId),
    waveId: createRandomWaveIdFactory(randomId),
  };
}

export function createSequentialWorkerIdFactory(startAt = 1): WorkerIdFactory {
  let next = startAt;
  return () => `worker-${next++}` as WorkerId;
}

export function createSequentialWaveIdFactory(startAt = 1): WaveIdFactory {
  let next = startAt;
  return () => `wave-${next++}` as WaveId;
}

export function createSequentialIdFactories(startAt = 1): OrchestrateIdFactories {
  return {
    workerId: createSequentialWorkerIdFactory(startAt),
    waveId: createSequentialWaveIdFactory(startAt),
  };
}

function defaultRandomId(): string {
  return globalThis.crypto.randomUUID();
}

export interface WorkerUsage {
  readonly input: number;
  readonly output: number;
  readonly cacheRead: number;
  readonly cacheWrite: number;
  readonly cost: number;
  readonly contextTokens: number;
  readonly turns: number;
}

/** Direction of the most recent message across the worker/model boundary. */
export type WorkerMessageDirection = "to-model" | "from-model";

export const EMPTY_WORKER_USAGE: WorkerUsage = Object.freeze({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  cost: 0,
  contextTokens: 0,
  turns: 0,
});

export interface WorkerCompletedOutcome {
  readonly status: "completed";
  readonly assistantText: string;
}

export interface WorkerReadyOutcome {
  readonly status: "ready";
  readonly assistantText: string;
}

export interface WorkerFailedOutcome {
  readonly status: "failed";
  readonly message: string;
  readonly assistantText?: string;
}

export interface WorkerAbortedOutcome {
  readonly status: "aborted";
  readonly message?: string;
  readonly assistantText?: string;
}

export interface WorkerClosedOutcome {
  readonly status: "closed";
}

export type WorkerOutcome =
  | WorkerCompletedOutcome
  | WorkerReadyOutcome
  | WorkerFailedOutcome
  | WorkerAbortedOutcome
  | WorkerClosedOutcome;
export type TerminalWorkerOutcome = Exclude<WorkerOutcome, WorkerReadyOutcome>;
export type WorkerStatus =
  | "starting"
  | "running"
  | "ready"
  | "stopping"
  | "completed"
  | "failed"
  | "aborted"
  | "closed";
export type TerminalWorkerStatus = Extract<
  WorkerStatus,
  "completed" | "failed" | "aborted" | "closed"
>;
export type WaveCompleteWorkerStatus = TerminalWorkerStatus | "ready";

export interface WorkerRecord {
  readonly id: WorkerId;
  readonly worker: string;
  readonly ownerSessionId: string;
  readonly waveId: WaveId;
  readonly title: string;
  readonly instructions: string;
  readonly lifecycle: WorkerLifecycle;
  readonly status: WorkerStatus;
  readonly usage: WorkerUsage;
  readonly startedAt: number;
  readonly settledAt?: number;
  readonly activity?: string;
  readonly messageDirection?: WorkerMessageDirection;
  readonly outcome?: WorkerOutcome;
  readonly sessionFile?: string;
}

export type WaveMode = "async" | "inline";
export type WaveState = "running" | "complete";

export interface WaveRecord {
  readonly id: WaveId;
  readonly ownerSessionId: string;
  readonly workerIds: readonly WorkerId[];
  readonly mode: WaveMode;
  readonly state: WaveState;
  readonly createdAt: number;
}

export class InvalidTransitionError extends Error {
  readonly from: WorkerStatus;
  readonly to: WorkerStatus;

  constructor(from: WorkerStatus, to: WorkerStatus) {
    super(`Invalid worker status transition: ${from} -> ${to}`);
    this.name = "InvalidTransitionError";
    this.from = from;
    this.to = to;
  }
}

export function isTerminalWorkerStatus(status: WorkerStatus): status is TerminalWorkerStatus {
  return status === "completed" || status === "failed" || status === "aborted" || status === "closed";
}

export function isTerminalWorkerOutcome(outcome: WorkerOutcome): outcome is TerminalWorkerOutcome {
  return outcome.status !== "ready";
}

export function canTransitionWorkerStatus(
  from: WorkerStatus,
  to: WorkerStatus,
  lifecycle: WorkerLifecycle,
): boolean {
  if (from === to || isTerminalWorkerStatus(from)) return false;

  switch (from) {
    case "starting":
      return to === "running" || to === "stopping" || to === "failed" || to === "aborted";
    case "running":
      if (to === "ready") return lifecycle === "reusable";
      if (to === "completed") return lifecycle === "one-shot";
      return to === "stopping" || to === "failed" || to === "aborted";
    case "ready":
      if (lifecycle !== "reusable") return false;
      return to === "running" || to === "stopping" || to === "closed";
    case "stopping":
      return to === "aborted" || to === "failed";
    default:
      return false;
  }
}

export function transitionWorkerStatus(
  worker: WorkerRecord,
  status: WorkerStatus,
): WorkerRecord {
  if (!canTransitionWorkerStatus(worker.status, status, worker.lifecycle)) {
    throw new InvalidTransitionError(worker.status, status);
  }

  return { ...worker, status, outcome: undefined };
}

export function isWorkerCompleteForWave(
  status: WorkerStatus,
): status is WaveCompleteWorkerStatus {
  return status === "ready" || isTerminalWorkerStatus(status);
}

export function getWaveWorkersInOrder(
  wave: WaveRecord,
  workersById: ReadonlyMap<WorkerId, WorkerRecord>,
): readonly WorkerRecord[] | undefined {
  const workers: WorkerRecord[] = [];

  for (const workerId of wave.workerIds) {
    const worker = workersById.get(workerId);
    if (!worker) return undefined;
    workers.push(worker);
  }

  return workers;
}

export function isWaveComplete(
  wave: WaveRecord,
  workersById: ReadonlyMap<WorkerId, WorkerRecord>,
): boolean {
  const workers = getWaveWorkersInOrder(wave, workersById);
  return workers !== undefined && workers.every((worker) => isWorkerCompleteForWave(worker.status));
}

export const MAX_TASKS_PER_WAVE = 12;
export const MAX_WORKER_TITLE_LENGTH = 200;
export const MAX_WORKER_INSTRUCTIONS_LENGTH = 100_000;
export const CANCELLATION_GRACE_MS = 5_000;
