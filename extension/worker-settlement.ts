import { Schema } from "effect";
import type { RunId, WorkerId } from "./domain.js";

const NonnegativeFinite = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const NonnegativeInteger = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const WorkerUsage = Schema.Struct({
  input: NonnegativeFinite,
  output: NonnegativeFinite,
  cacheRead: NonnegativeFinite,
  cacheWrite: NonnegativeFinite,
  cost: NonnegativeFinite,
  contextTokens: NonnegativeFinite,
  turns: NonnegativeInteger,
});

const WorkerOutcome = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("completed"),
    assistantText: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literal("ready"),
    assistantText: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    message: Schema.String,
    assistantText: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    status: Schema.Literal("aborted"),
    message: Schema.optionalKey(Schema.String),
    assistantText: Schema.optionalKey(Schema.String),
  }),
]);

const FailureStage = Schema.Literals([
  "startup",
  "prompt",
  "workflow",
  "cancellation",
]);

/** Canonical schema for settlement details written by the current runtime. */
export const WorkerSettlementDetails = Schema.Struct({
  eventId: Schema.String,
  sequence: NonnegativeInteger,
  ownerSessionId: Schema.String,
  runId: Schema.String,
  workerId: Schema.String,
  generation: NonnegativeInteger,
  mode: Schema.Literals(["async", "inline"]),
  worker: Schema.String,
  title: Schema.String,
  lifecycle: Schema.Literals(["one-shot", "reusable"]),
  status: Schema.Literals(["completed", "ready", "failed", "aborted"]),
  outcome: WorkerOutcome,
  usage: WorkerUsage,
  startedAt: NonnegativeInteger,
  settledAt: NonnegativeInteger,
  synthesisGroupId: Schema.optionalKey(Schema.String),
  synthesisGroupSize: Schema.optionalKey(NonnegativeInteger),
  sessionFile: Schema.optionalKey(Schema.String),
  failureStage: Schema.optionalKey(FailureStage),
}).check(
  Schema.makeFilter((settlement) => {
    if (settlement.outcome.status !== settlement.status) {
      return "outcome status must match settlement status";
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

/** Runtime refinement preserves the nominal IDs already allocated by the domain. */
export type WorkerSettlement = Omit<
  WorkerSettlementDetails,
  "runId" | "workerId"
> & {
  readonly runId: RunId;
  readonly workerId: WorkerId;
};

const decodeCurrentWorkerSettlement = Schema.decodeUnknownResult(
  WorkerSettlementDetails,
);

export function decodePersistedWorkerSettlementDetails(value: unknown) {
  return decodeCurrentWorkerSettlement(value);
}
