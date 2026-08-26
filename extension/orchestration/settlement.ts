import { Schema } from "effect";
import {
  RunId,
  WorkerAbortedOutcome,
  WorkerCompletedOutcome,
  WorkerFailedOutcome,
  WorkerId,
  WorkerReadyOutcome,
  WorkerResponseOutcome,
  WorkerUsage,
  type RunRecord,
  type SettledWorkerRecord,
} from "./model.ts";

const NonnegativeInteger = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const PositiveInteger = Schema.Int.check(Schema.isGreaterThan(0));

const FailureStage = Schema.Literals([
  "startup",
  "prompt",
  "workflow",
  "cancellation",
]);

/** Canonical schema for settlements written and persisted by orchestration. */
export const WorkerSettlement = Schema.Struct({
  eventId: Schema.String,
  sequence: PositiveInteger,
  ownerSessionId: Schema.String,
  runId: RunId,
  workerId: WorkerId,
  generation: PositiveInteger,
  mode: Schema.Literals(["async", "inline"]),
  worker: Schema.String,
  title: Schema.String,
  lifecycle: Schema.Literals(["one-shot", "interactive"]),
  status: Schema.Literals(["completed", "ready", "failed", "aborted"]),
  outcome: WorkerResponseOutcome,
  usage: WorkerUsage,
  startedAt: NonnegativeInteger,
  settledAt: NonnegativeInteger,
  synthesisGroupId: Schema.optionalKey(Schema.String),
  synthesisGroupSize: Schema.optionalKey(PositiveInteger),
  sessionFile: Schema.optionalKey(Schema.String),
  failureStage: Schema.optionalKey(FailureStage),
}).check(
  Schema.makeFilter((settlement) => {
    if (settlement.outcome.status !== settlement.status) {
      return "outcome status must match settlement status";
    }
    if (settlement.status === "ready" && settlement.lifecycle !== "interactive") {
      return "ready settlement requires an interactive lifecycle";
    }
    if (settlement.status === "completed" && settlement.lifecycle !== "one-shot") {
      return "completed settlement requires a one-shot lifecycle";
    }
    if (
      (settlement.synthesisGroupId === undefined) !==
      (settlement.synthesisGroupSize === undefined)
    ) {
      return "synthesis group ID and size must occur together";
    }
    if (
      settlement.synthesisGroupSize !== undefined &&
      settlement.synthesisGroupSize < 2
    ) {
      return "synthesis group size must be at least 2";
    }
    if (settlement.settledAt < settlement.startedAt) {
      return "settlement timestamp must not precede start timestamp";
    }
    if (
      settlement.failureStage !== undefined &&
      settlement.status !== "failed" &&
      settlement.status !== "aborted"
    ) {
      return "failure stage requires a failed or aborted settlement";
    }
  }),
);

export interface WorkerSettlement
  extends Schema.Schema.Type<typeof WorkerSettlement> {}

export type SettlementFailureStage = NonNullable<
  WorkerSettlement["failureStage"]
>;

const decodeCurrentWorkerSettlement = Schema.decodeUnknownResult(
  WorkerSettlement,
);

export function decodePersistedWorkerSettlement(value: unknown) {
  return decodeCurrentWorkerSettlement(value);
}

/** Every input a settlement needs; orchestration state entries stay private. */
export interface WorkerSettlementInput {
  readonly sequence: number;
  readonly generation: number;
  readonly run: RunRecord;
  readonly worker: SettledWorkerRecord;
  readonly settledAt: number;
  readonly failureStage?: SettlementFailureStage;
}

/** Builds the canonical settlement published to listeners and persisted by Pi. */
export function createWorkerSettlement({
  sequence,
  generation,
  run,
  worker,
  settledAt,
  failureStage,
}: WorkerSettlementInput): WorkerSettlement {
  return Object.freeze({
    eventId: `${sequence}:${run.id}:${worker.id}:${generation}`,
    sequence,
    ownerSessionId: worker.ownerSessionId,
    runId: run.id,
    workerId: worker.id,
    generation,
    mode: run.mode,
    worker: worker.worker,
    title: worker.title,
    lifecycle: worker.lifecycle,
    status: worker.status,
    outcome: Object.freeze({ ...worker.outcome }),
    ...(failureStage ? { failureStage } : {}),
    usage: Object.freeze({ ...worker.usage }),
    startedAt: worker.startedAt,
    settledAt,
    ...(run.synthesisGroupId && run.synthesisGroupSize
      ? {
          synthesisGroupId: run.synthesisGroupId,
          synthesisGroupSize: run.synthesisGroupSize,
        }
      : {}),
    ...(worker.sessionFile !== undefined
      ? { sessionFile: worker.sessionFile }
      : {}),
  });
}

const InlineWorkerOutcome = Schema.Union([
  WorkerCompletedOutcome.pipe(
    Schema.encodeKeys({ assistantText: "assistant_text" }),
  ),
  WorkerReadyOutcome.pipe(
    Schema.encodeKeys({ assistantText: "assistant_text" }),
  ),
  WorkerFailedOutcome.pipe(
    Schema.encodeKeys({ assistantText: "assistant_text" }),
  ),
  WorkerAbortedOutcome.pipe(
    Schema.encodeKeys({ assistantText: "assistant_text" }),
  ),
]);

const InlineWorkerUsage = WorkerUsage.pipe(
  Schema.encodeKeys({
    cacheRead: "cache_read",
    cacheWrite: "cache_write",
    contextTokens: "context_tokens",
  }),
);

/** Tool transport projection derived from the canonical settlement field schemas. */
export const InlineWorkerSettlementDetails = Schema.Struct({
  workerId: WorkerSettlement.fields.workerId,
  worker: WorkerSettlement.fields.worker,
  title: WorkerSettlement.fields.title,
  status: WorkerSettlement.fields.status,
  outcome: InlineWorkerOutcome,
  usage: InlineWorkerUsage,
  startedAt: WorkerSettlement.fields.startedAt,
  settledAt: WorkerSettlement.fields.settledAt,
  sessionFile: WorkerSettlement.fields.sessionFile,
}).pipe(
  Schema.encodeKeys({
    workerId: "worker_id",
    startedAt: "started_at",
    settledAt: "settled_at",
    sessionFile: "session_file",
  }),
).check(
  Schema.makeFilter((settlement) => {
    if (settlement.outcome.status !== settlement.status) {
      return "outcome status must match settlement status";
    }
    if (settlement.settledAt < settlement.startedAt) {
      return "settlement timestamp must not precede start timestamp";
    }
  }),
);

export interface InlineWorkerSettlementDetails
  extends Schema.Schema.Type<typeof InlineWorkerSettlementDetails> {}

export const InlineWorkerToolDetails = Schema.Struct({
  mode: Schema.Literal("inline"),
  runId: Schema.optionalKey(WorkerSettlement.fields.runId),
  ownerSessionId: Schema.optionalKey(
    WorkerSettlement.fields.ownerSessionId,
  ),
  result: InlineWorkerSettlementDetails,
}).pipe(
  Schema.encodeKeys({
    runId: "run_id",
    ownerSessionId: "owner_session_id",
  }),
);

export interface InlineWorkerToolDetails
  extends Schema.Schema.Type<typeof InlineWorkerToolDetails> {}

const decodeInlineWorkerToolDetailsResult = Schema.decodeUnknownResult(
  InlineWorkerToolDetails,
);
const encodeInlineWorkerToolDetailsSync = Schema.encodeSync(
  InlineWorkerToolDetails,
);

export function decodeInlineWorkerToolDetails(value: unknown) {
  return decodeInlineWorkerToolDetailsResult(value);
}

export function encodeInlineWorkerToolDetails(
  value: InlineWorkerToolDetails,
) {
  return encodeInlineWorkerToolDetailsSync(value);
}
