import { Result, Schema } from "effect";
import type { WaveId, WorkerId } from "./domain.js";

const NonnegativeFinite = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const NonnegativeInteger = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const legacyOptionalKey = <S extends Schema.Constraint>(schema: S) =>
  Schema.optionalKey(Schema.UndefinedOr(schema));

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

const LegacyWorkerOutcome = Schema.Union([
  WorkerOutcome.members[0],
  WorkerOutcome.members[1],
  Schema.Struct({
    status: Schema.Literal("failed"),
    message: Schema.String,
    assistantText: legacyOptionalKey(Schema.String),
  }),
  Schema.Struct({
    status: Schema.Literal("aborted"),
    message: legacyOptionalKey(Schema.String),
    assistantText: legacyOptionalKey(Schema.String),
  }),
]);

const FailureStage = Schema.Literals([
  "startup",
  "prompt",
  "workflow",
  "cancellation",
]);

const CurrentWorkerSettlementStruct = Schema.Struct({
  eventId: Schema.String,
  sequence: NonnegativeInteger,
  ownerSessionId: Schema.String,
  waveId: Schema.String,
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
  remainingActive: NonnegativeInteger,
  waveSize: NonnegativeInteger,
  waveComplete: Schema.Boolean,
  dispatchGroupId: Schema.optionalKey(Schema.String),
  dispatchGroupSize: Schema.optionalKey(NonnegativeInteger),
  sessionFile: Schema.UndefinedOr(Schema.String),
  failureStage: Schema.optionalKey(FailureStage),
});

/** Canonical schema for settlement details written by the current runtime. */
export const WorkerSettlementDetails = CurrentWorkerSettlementStruct.check(
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
  "waveId" | "workerId"
> & {
  readonly waveId: WaveId;
  readonly workerId: WorkerId;
};

/** Historical persisted input. Only fields absent from older records are weakened. */
const LegacyWorkerSettlementDetails = CurrentWorkerSettlementStruct.mapFields((fields) => ({
  ...fields,
  eventId: legacyOptionalKey(Schema.String),
  sequence: legacyOptionalKey(NonnegativeInteger),
  remainingActive: legacyOptionalKey(NonnegativeInteger),
  waveSize: legacyOptionalKey(NonnegativeInteger),
  waveComplete: legacyOptionalKey(Schema.Boolean),
  outcome: LegacyWorkerOutcome,
  dispatchGroupId: legacyOptionalKey(Schema.String),
  dispatchGroupSize: legacyOptionalKey(NonnegativeInteger),
  sessionFile: legacyOptionalKey(Schema.String),
  failureStage: legacyOptionalKey(FailureStage),
}));

const decodeLegacyWorkerSettlement = Schema.decodeUnknownResult(
  LegacyWorkerSettlementDetails,
);
const decodeCurrentWorkerSettlement = Schema.decodeUnknownResult(
  WorkerSettlementDetails,
);

export function decodePersistedWorkerSettlementDetails(value: unknown) {
  const decoded = decodeLegacyWorkerSettlement(value);
  if (Result.isFailure(decoded)) return decoded;

  const legacy = decoded.success;
  const remainingActive = legacy.remainingActive ?? 0;
  const normalized = {
    eventId:
      legacy.eventId ??
      `legacy:${legacy.waveId}:${legacy.workerId}:${legacy.generation}`,
    sequence: legacy.sequence ?? 0,
    ownerSessionId: legacy.ownerSessionId,
    waveId: legacy.waveId,
    workerId: legacy.workerId,
    generation: legacy.generation,
    mode: legacy.mode,
    worker: legacy.worker,
    title: legacy.title,
    lifecycle: legacy.lifecycle,
    status: legacy.status,
    outcome: normalizeLegacyOutcome(legacy.outcome),
    usage: legacy.usage,
    startedAt: legacy.startedAt,
    settledAt: legacy.settledAt,
    remainingActive,
    waveSize: legacy.waveSize ?? Math.max(1, remainingActive + 1),
    waveComplete: legacy.waveComplete ?? remainingActive === 0,
    ...(legacy.dispatchGroupId !== undefined
      ? { dispatchGroupId: legacy.dispatchGroupId }
      : {}),
    ...(legacy.dispatchGroupSize !== undefined
      ? { dispatchGroupSize: legacy.dispatchGroupSize }
      : {}),
    sessionFile: legacy.sessionFile,
    ...(legacy.failureStage !== undefined
      ? { failureStage: legacy.failureStage }
      : {}),
  };
  return decodeCurrentWorkerSettlement(normalized);
}

function normalizeLegacyOutcome(
  outcome: Schema.Schema.Type<typeof LegacyWorkerOutcome>,
): WorkerSettlementDetails["outcome"] {
  if (outcome.status === "completed" || outcome.status === "ready") return outcome;
  if (outcome.status === "failed") {
    return {
      status: "failed",
      message: outcome.message,
      ...(outcome.assistantText !== undefined
        ? { assistantText: outcome.assistantText }
        : {}),
    };
  }
  return {
    status: "aborted",
    ...(outcome.message !== undefined ? { message: outcome.message } : {}),
    ...(outcome.assistantText !== undefined
      ? { assistantText: outcome.assistantText }
      : {}),
  };
}
