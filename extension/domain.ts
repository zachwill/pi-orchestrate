import type { ThinkingLevel } from "@earendil-works/pi-agent-core";
import { Schema } from "effect";

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

export type WorkerLifecycle = "one-shot" | "interactive";

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

export const MAX_WORKER_TITLE_LENGTH = 200;
export const MAX_WORKER_INSTRUCTIONS_LENGTH = 100_000;
export const CANCELLATION_GRACE_MS = 5_000;

const NonBlankString = Schema.String.check(Schema.isPattern(/\S/));
const WorkerLabel = NonBlankString.check(
  Schema.isMaxLength(MAX_WORKER_TITLE_LENGTH),
);
const WorkerInstructions = NonBlankString.check(
  Schema.isMaxLength(MAX_WORKER_INSTRUCTIONS_LENGTH),
);

export const OrchestrateTaskInput = Schema.Struct({
  worker: WorkerLabel,
  title: WorkerLabel,
  instructions: WorkerInstructions,
});
export interface OrchestrateTaskInput extends Schema.Schema.Type<typeof OrchestrateTaskInput> {}

/** A validated worker identity. Worker IDs are stable across interactive generations. */
export const WorkerId = Schema.String.check(
  Schema.isPattern(/^worker-\S+$/),
).pipe(Schema.brand("WorkerId"));
export type WorkerId = typeof WorkerId.Type;

/** A validated identity for exactly one worker generation. */
export const RunId = Schema.String.check(
  Schema.isPattern(/^run-\S+$/),
).pipe(Schema.brand("RunId"));
export type RunId = typeof RunId.Type;

export type WorkerIdFactory = () => WorkerId;
export type RunIdFactory = () => RunId;

export interface OrchestrateIdFactories {
  readonly workerId: WorkerIdFactory;
  readonly runId: RunIdFactory;
}

export function createRandomWorkerIdFactory(
  randomId: () => string = defaultRandomId,
): WorkerIdFactory {
  return () => WorkerId.make(`worker-${randomId()}`);
}

export function createRandomRunIdFactory(
  randomId: () => string = defaultRandomId,
): RunIdFactory {
  return () => RunId.make(`run-${randomId()}`);
}

export function createRandomIdFactories(
  randomId: () => string = defaultRandomId,
): OrchestrateIdFactories {
  return {
    workerId: createRandomWorkerIdFactory(randomId),
    runId: createRandomRunIdFactory(randomId),
  };
}

export function createSequentialWorkerIdFactory(startAt = 1): WorkerIdFactory {
  let next = startAt;
  return () => WorkerId.make(`worker-${next++}`);
}

export function createSequentialRunIdFactory(startAt = 1): RunIdFactory {
  let next = startAt;
  return () => RunId.make(`run-${next++}`);
}

export function createSequentialIdFactories(startAt = 1): OrchestrateIdFactories {
  return {
    workerId: createSequentialWorkerIdFactory(startAt),
    runId: createSequentialRunIdFactory(startAt),
  };
}

function defaultRandomId(): string {
  return globalThis.crypto.randomUUID();
}

const NonnegativeFinite = Schema.Finite.check(
  Schema.isGreaterThanOrEqualTo(0),
);
const NonnegativeInteger = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
);

export const WorkerUsage = Schema.Struct({
  input: NonnegativeFinite,
  output: NonnegativeFinite,
  cacheRead: NonnegativeFinite,
  cacheWrite: NonnegativeFinite,
  cost: NonnegativeFinite,
  contextTokens: NonnegativeFinite,
  turns: NonnegativeInteger,
});
export interface WorkerUsage extends Schema.Schema.Type<typeof WorkerUsage> {}

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

export const WorkerCompletedOutcome = Schema.Struct({
  status: Schema.Literal("completed"),
  assistantText: Schema.String,
});
export interface WorkerCompletedOutcome
  extends Schema.Schema.Type<typeof WorkerCompletedOutcome> {}

export const WorkerReadyOutcome = Schema.Struct({
  status: Schema.Literal("ready"),
  assistantText: Schema.String,
});
export interface WorkerReadyOutcome
  extends Schema.Schema.Type<typeof WorkerReadyOutcome> {}

export const WorkerFailedOutcome = Schema.Struct({
  status: Schema.Literal("failed"),
  message: Schema.String,
  assistantText: Schema.optionalKey(Schema.String),
});
export interface WorkerFailedOutcome
  extends Schema.Schema.Type<typeof WorkerFailedOutcome> {}

export const WorkerAbortedOutcome = Schema.Struct({
  status: Schema.Literal("aborted"),
  message: Schema.optionalKey(Schema.String),
  assistantText: Schema.optionalKey(Schema.String),
});
export interface WorkerAbortedOutcome
  extends Schema.Schema.Type<typeof WorkerAbortedOutcome> {}

/** Outcomes emitted in response to a worker generation. */
export const WorkerResponseOutcome = Schema.Union([
  WorkerCompletedOutcome,
  WorkerReadyOutcome,
  WorkerFailedOutcome,
  WorkerAbortedOutcome,
]);
export type WorkerResponseOutcome = typeof WorkerResponseOutcome.Type;

export const WorkerClosedOutcome = Schema.Struct({
  status: Schema.Literal("closed"),
});
export interface WorkerClosedOutcome
  extends Schema.Schema.Type<typeof WorkerClosedOutcome> {}

/** Domain outcomes include closure, which is not a generation response. */
export const WorkerOutcome = Schema.Union([
  WorkerResponseOutcome,
  WorkerClosedOutcome,
]);
export type WorkerOutcome = typeof WorkerOutcome.Type;
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

export interface WorkerRecord {
  readonly id: WorkerId;
  readonly worker: string;
  readonly ownerSessionId: string;
  readonly runId: RunId;
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

export type RunMode = "async" | "inline";
export type RunState = "running" | "complete";

export interface RunRecord {
  readonly id: RunId;
  readonly ownerSessionId: string;
  readonly workerId: WorkerId;
  readonly mode: RunMode;
  readonly state: RunState;
  readonly createdAt: number;
  readonly synthesisGroupId?: string;
  readonly synthesisGroupSize?: number;
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
      if (to === "ready") return lifecycle === "interactive";
      if (to === "completed") return lifecycle === "one-shot";
      return to === "stopping" || to === "failed" || to === "aborted";
    case "ready":
      if (lifecycle !== "interactive") return false;
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
