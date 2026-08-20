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
} from "./domain.js";

const NonnegativeInteger = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const PositiveInteger = Schema.Int.check(Schema.isGreaterThan(0));

const FailureStage = Schema.Literals([
  "startup",
  "prompt",
  "workflow",
  "cancellation",
]);

/** Canonical schema for settlement details written by the current runtime. */
export const WorkerSettlementDetails = Schema.Struct({
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

export interface WorkerSettlementDetails
  extends Schema.Schema.Type<typeof WorkerSettlementDetails> {}

export type SettlementFailureStage = NonNullable<
  WorkerSettlementDetails["failureStage"]
>;

export type WorkerSettlement = Schema.Schema.Type<
  typeof WorkerSettlementDetails
>;

const decodeCurrentWorkerSettlement = Schema.decodeUnknownResult(
  WorkerSettlementDetails,
);

export function decodePersistedWorkerSettlementDetails(value: unknown) {
  return decodeCurrentWorkerSettlement(value);
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
  workerId: WorkerSettlementDetails.fields.workerId,
  worker: WorkerSettlementDetails.fields.worker,
  title: WorkerSettlementDetails.fields.title,
  status: WorkerSettlementDetails.fields.status,
  outcome: InlineWorkerOutcome,
  usage: InlineWorkerUsage,
  startedAt: WorkerSettlementDetails.fields.startedAt,
  settledAt: WorkerSettlementDetails.fields.settledAt,
  sessionFile: WorkerSettlementDetails.fields.sessionFile,
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
  runId: Schema.optionalKey(WorkerSettlementDetails.fields.runId),
  ownerSessionId: Schema.optionalKey(
    WorkerSettlementDetails.fields.ownerSessionId,
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
