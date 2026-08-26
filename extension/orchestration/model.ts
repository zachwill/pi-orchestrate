import { Schema } from "effect";
import type { WorkerLifecycle } from "../catalog/definition.js";

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
export const WorkerReadyOutcome = Schema.Struct({
  status: Schema.Literal("ready"),
  assistantText: Schema.String,
});
export const WorkerFailedOutcome = Schema.Struct({
  status: Schema.Literal("failed"),
  message: Schema.String,
  assistantText: Schema.optionalKey(Schema.String),
});
export const WorkerAbortedOutcome = Schema.Struct({
  status: Schema.Literal("aborted"),
  message: Schema.optionalKey(Schema.String),
  assistantText: Schema.optionalKey(Schema.String),
});
/** Outcomes emitted in response to a worker generation. */
export const WorkerResponseOutcome = Schema.Union([
  WorkerCompletedOutcome,
  WorkerReadyOutcome,
  WorkerFailedOutcome,
  WorkerAbortedOutcome,
]);
export type WorkerResponseOutcome = typeof WorkerResponseOutcome.Type;

/** Domain outcomes include closure, which is not a generation response. */
export const WorkerOutcome = Schema.Union([
  WorkerResponseOutcome,
  Schema.Struct({ status: Schema.Literal("closed") }),
]);
export type WorkerOutcome = typeof WorkerOutcome.Type;

/** Outcomes that can settle a generation; closure is not one of them. */
export type SettledWorkerOutcome = Exclude<
  WorkerOutcome,
  { readonly status: "closed" }
>;

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

/** Status of a worker whose generation produced a response outcome. */
export type SettledWorkerStatus = Extract<
  WorkerStatus,
  "completed" | "ready" | "failed" | "aborted"
>;

/** A worker record whose current generation has settled with a response outcome. */
export interface SettledWorkerRecord extends WorkerRecord {
  readonly status: SettledWorkerStatus;
  readonly outcome: SettledWorkerOutcome;
  readonly settledAt: number;
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
