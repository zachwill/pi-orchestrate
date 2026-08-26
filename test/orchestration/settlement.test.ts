import { describe, expect, test } from "bun:test";
import { Result, Schema } from "effect";
import {
  RunId,
  WorkerId,
  type RunRecord,
  type SettledWorkerRecord,
  type WorkerUsage,
} from "../../extension/orchestration/model.ts";
import {
  InlineWorkerToolDetails,
  WorkerSettlement,
  createWorkerSettlement,
  decodeInlineWorkerToolDetails,
  decodePersistedWorkerSettlement,
  encodeInlineWorkerToolDetails,
  type InlineWorkerToolDetails as InlineWorkerToolDetailsValue,
} from "../../extension/orchestration/settlement.ts";

const usage: WorkerUsage = {
  input: 11,
  output: 12,
  cacheRead: 13,
  cacheWrite: 14,
  cost: 0.15,
  contextTokens: 16,
  turns: 2,
};

function settlement(status: "completed" | "ready" | "failed" | "aborted" = "completed") {
  return {
    eventId: "event",
    sequence: 1,
    ownerSessionId: "owner",
    runId: "run-1",
    workerId: "worker-1",
    generation: 2,
    mode: "async" as const,
    worker: "scout",
    title: "Inspect code",
    lifecycle: status === "ready" ? "interactive" as const : "one-shot" as const,
    status,
    outcome: status === "completed" || status === "ready"
      ? { status, assistantText: `${status} response.` }
      : status === "failed"
        ? { status, message: "Failed.", assistantText: "Partial evidence." }
        : { status, message: "Aborted.", assistantText: "Partial evidence." },
    usage,
    startedAt: 1_000,
    settledAt: 6_200,
    sessionFile: "/sessions/worker.jsonl",
  };
}

function inlineDetails() {
  return {
    mode: "inline" as const,
    run_id: "run-inline",
    owner_session_id: "owner-session",
    result: {
      worker_id: "worker-inline",
      worker: "scout",
      title: "Inspect",
      status: "completed" as const,
      outcome: { status: "completed" as const, assistant_text: "Complete." },
      usage: {
        input: 11,
        output: 12,
        cache_read: 13,
        cache_write: 14,
        cost: 0.15,
        context_tokens: 16,
        turns: 2,
      },
      started_at: 1_000,
      settled_at: 6_000,
      session_file: "/sessions/worker-inline.jsonl",
    },
  };
}

describe("canonical worker settlements", () => {
  test("constructs a frozen settlement from the owning run and worker records", () => {
    const run: RunRecord = {
      id: Schema.decodeUnknownSync(RunId)("run-1"),
      ownerSessionId: "owner",
      workerId: Schema.decodeUnknownSync(WorkerId)("worker-1"),
      mode: "async",
      state: "complete",
      createdAt: 900,
      synthesisGroupId: "group-1",
      synthesisGroupSize: 2,
    };
    const worker: SettledWorkerRecord = {
      id: run.workerId,
      worker: "scout",
      ownerSessionId: "owner",
      runId: run.id,
      title: "Inspect",
      instructions: "Inspect code.",
      lifecycle: "one-shot",
      status: "completed",
      outcome: { status: "completed", assistantText: "Complete." },
      usage,
      startedAt: 1_000,
      settledAt: 6_000,
      sessionFile: "/sessions/worker.jsonl",
    };

    const result = createWorkerSettlement({
      sequence: 3,
      generation: 2,
      run,
      worker,
      settledAt: 6_000,
    });

    expect(result).toMatchObject({
      eventId: "3:run-1:worker-1:2",
      sequence: 3,
      generation: 2,
      synthesisGroupId: "group-1",
      synthesisGroupSize: 2,
      sessionFile: "/sessions/worker.jsonl",
      outcome: worker.outcome,
      usage,
    });
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.outcome)).toBe(true);
    expect(Object.isFrozen(result.usage)).toBe(true);
  });

  test("round-trips every persisted outcome and canonical field", () => {
    for (const status of ["completed", "ready", "failed", "aborted"] as const) {
      const input = {
        ...settlement(status),
        ...(status === "failed"
          ? {
              synthesisGroupId: "synthesis-1",
              synthesisGroupSize: 2,
              failureStage: "workflow" as const,
            }
          : {}),
      };
      const decoded = decodePersistedWorkerSettlement(input);
      expect(Result.isSuccess(decoded)).toBe(true);
      if (!Result.isSuccess(decoded)) throw new Error("Expected settlement to decode");
      const persisted = JSON.parse(JSON.stringify(
        Schema.encodeSync(WorkerSettlement)(decoded.success),
      ));
      expect(persisted).toEqual(input);
      const roundTrip = decodePersistedWorkerSettlement(persisted);
      expect(Result.isSuccess(roundTrip)).toBe(true);
      if (Result.isSuccess(roundTrip)) expect(roundTrip.success).toEqual(decoded.success);
    }
  });

  test("rejects cross-field contradictions and invalid ordinal combinations", () => {
    const invalid = [
      { ...settlement(), status: "completed", outcome: { status: "failed", message: "no" } },
      { ...settlement("ready"), lifecycle: "one-shot" },
      { ...settlement("completed"), lifecycle: "interactive" },
      { ...settlement(), synthesisGroupId: "synthesis-1" },
      { ...settlement(), synthesisGroupSize: 2 },
      { ...settlement(), synthesisGroupId: "synthesis-1", synthesisGroupSize: 1 },
      { ...settlement(), sequence: 0 },
      { ...settlement(), generation: 0 },
      { ...settlement(), startedAt: 6_201 },
      { ...settlement(), failureStage: "workflow" },
    ];
    for (const value of invalid) {
      expect(Result.isFailure(decodePersistedWorkerSettlement(value))).toBe(true);
    }
  });

  test("rejects invalid usage values and every missing required persisted field", () => {
    for (const field of Object.keys(usage)) {
      for (const invalid of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
        expect(Result.isFailure(decodePersistedWorkerSettlement({
          ...settlement(),
          usage: { ...usage, [field]: invalid },
        }))).toBe(true);
      }
    }
    for (const field of Object.keys(settlement())) {
      if (field === "sessionFile") continue;
      const missing = { ...settlement() };
      Reflect.deleteProperty(missing, field);
      expect(Result.isFailure(decodePersistedWorkerSettlement(missing))).toBe(true);
    }
  });

  test("accepts omitted optional persistence fields and strips unknown fields", () => {
    const { sessionFile: _sessionFile, ...withoutSession } = settlement();
    const decoded = decodePersistedWorkerSettlement({
      ...withoutSession,
      currentExtra: "ignored",
    });
    expect(Result.isSuccess(decoded)).toBe(true);
    if (!Result.isSuccess(decoded)) throw new Error("Expected settlement to decode");
    expect(decoded.success).not.toHaveProperty("sessionFile");
    expect(decoded.success).not.toHaveProperty("currentExtra");
    expect(Result.isFailure(decodePersistedWorkerSettlement({ settlement: withoutSession }))).toBe(true);
    expect(Result.isFailure(decodePersistedWorkerSettlement({
      ...withoutSession,
      sessionFile: undefined,
    }))).toBe(true);
  });
});

describe("inline tool transport projection", () => {
  test("round-trips every response outcome and snake-case transport field", () => {
    const outcomes: InlineWorkerToolDetailsValue["result"]["outcome"][] = [
      { status: "completed", assistantText: "Complete." },
      { status: "ready", assistantText: "Ready." },
      { status: "failed", message: "Failed." },
      { status: "aborted" },
    ];
    for (const outcome of outcomes) {
      const value: InlineWorkerToolDetailsValue = {
        mode: "inline",
        runId: Schema.decodeUnknownSync(RunId)("run-inline"),
        ownerSessionId: "owner-session",
        result: {
          workerId: Schema.decodeUnknownSync(WorkerId)("worker-inline"),
          worker: "scout",
          title: "Inspect",
          status: outcome.status,
          outcome,
          usage,
          startedAt: 1_000,
          settledAt: 6_000,
        },
      };
      const encoded = JSON.parse(JSON.stringify(encodeInlineWorkerToolDetails(value)));
      expect(encoded.mode).toBe("inline");
      expect(encoded.run_id).toBe("run-inline");
      expect(encoded.owner_session_id).toBe("owner-session");
      expect(encoded.result).toMatchObject({
        worker_id: "worker-inline",
        status: outcome.status,
        usage: { cache_read: 13, cache_write: 14, context_tokens: 16 },
      });
      const decoded = decodeInlineWorkerToolDetails(encoded);
      expect(Result.isSuccess(decoded)).toBe(true);
      if (Result.isSuccess(decoded)) expect(decoded.success).toEqual(value);
    }
  });

  test("rejects missing, contradictory, and invalid transport fields", () => {
    const valid = inlineDetails();
    for (const field of ["mode", "result"] as const) {
      const details = { ...valid };
      Reflect.deleteProperty(details, field);
      expect(Result.isFailure(decodeInlineWorkerToolDetails(details))).toBe(true);
    }
    for (const field of [
      "worker_id", "worker", "title", "status", "outcome", "usage", "started_at", "settled_at",
    ] as const) {
      const result = { ...valid.result };
      Reflect.deleteProperty(result, field);
      expect(Result.isFailure(decodeInlineWorkerToolDetails({ ...valid, result }))).toBe(true);
    }
    expect(Result.isFailure(decodeInlineWorkerToolDetails({
      ...valid,
      result: { ...valid.result, outcome: { status: "failed", message: "no" } },
    }))).toBe(true);
    expect(Result.isFailure(decodeInlineWorkerToolDetails({
      ...valid,
      result: { ...valid.result, started_at: 6_001 },
    }))).toBe(true);
    for (const field of Object.keys(valid.result.usage)) {
      for (const invalid of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
        expect(Result.isFailure(decodeInlineWorkerToolDetails({
          ...valid,
          result: {
            ...valid.result,
            usage: { ...valid.result.usage, [field]: invalid },
          },
        }))).toBe(true);
      }
    }
  });

  test("strips excess transport fields and permits omitted projection optionals", () => {
    const valid = inlineDetails();
    const decoded = decodeInlineWorkerToolDetails({
      ...valid,
      future_top_level: true,
      result: {
        ...valid.result,
        future_result_field: true,
        session_file: undefined,
      },
    });
    expect(Result.isFailure(decoded)).toBe(true);

    const { session_file: _sessionFile, ...withoutSession } = valid.result;
    const clean = decodeInlineWorkerToolDetails({
      ...valid,
      future_top_level: true,
      result: { ...withoutSession, future_result_field: true },
    });
    expect(Result.isSuccess(clean)).toBe(true);
    if (Result.isSuccess(clean)) {
      expect(clean.success).not.toHaveProperty("future_top_level");
      expect(clean.success.result).not.toHaveProperty("future_result_field");
    }
    expect(Schema.decodeUnknownResult(InlineWorkerToolDetails)({
      mode: "inline",
      result: withoutSession,
    })._tag).toBe("Success");
  });
});
