import { describe, expect, test } from "bun:test";
import { Result, Schema } from "effect";
import {
  RunId,
  WorkerId,
  type WorkerUsage,
} from "../../extension/orchestration/model.ts";
import {
  WorkerSettlement,
  decodeInlineWorkerToolDetails,
  decodePersistedWorkerSettlement,
  encodeInlineWorkerToolDetails,
  type InlineWorkerToolDetails,
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

describe("persisted worker settlements", () => {
  test("round-trips one representative settlement with optional metadata", () => {
    const input = {
      ...settlement("failed"),
      synthesisGroupId: "synthesis-1",
      synthesisGroupSize: 2,
      failureStage: "workflow" as const,
    };
    const decoded = decodePersistedWorkerSettlement(input);
    expect(Result.isSuccess(decoded)).toBe(true);
    if (!Result.isSuccess(decoded)) throw new Error("Expected settlement to decode");

    const persisted = JSON.parse(JSON.stringify(
      Schema.encodeSync(WorkerSettlement)(decoded.success),
    ));
    expect(persisted).toEqual(input);
    expect(decodePersistedWorkerSettlement(persisted)).toEqual(decoded);
  });

  test("rejects custom cross-field contradictions", () => {
    const invalid = [
      { ...settlement(), outcome: { status: "failed", message: "no" } },
      { ...settlement("ready"), lifecycle: "one-shot" },
      { ...settlement(), lifecycle: "interactive" },
      { ...settlement(), synthesisGroupId: "synthesis-1" },
      { ...settlement(), synthesisGroupSize: 2 },
      { ...settlement(), synthesisGroupId: "synthesis-1", synthesisGroupSize: 1 },
      { ...settlement(), startedAt: 6_201 },
      { ...settlement(), failureStage: "workflow" },
    ];
    for (const value of invalid) {
      expect(Result.isFailure(decodePersistedWorkerSettlement(value))).toBe(true);
    }
  });

  test("accepts omitted optionals, strips unknown fields, and rejects explicit undefined", () => {
    const { sessionFile: _sessionFile, ...withoutSession } = settlement();
    const decoded = decodePersistedWorkerSettlement({
      ...withoutSession,
      currentExtra: "ignored",
    });
    expect(Result.isSuccess(decoded)).toBe(true);
    if (!Result.isSuccess(decoded)) throw new Error("Expected settlement to decode");
    expect(decoded.success).not.toHaveProperty("sessionFile");
    expect(decoded.success).not.toHaveProperty("currentExtra");
    expect(Result.isFailure(decodePersistedWorkerSettlement({
      ...withoutSession,
      sessionFile: undefined,
    }))).toBe(true);
  });
});

describe("inline tool transport projection", () => {
  test("projects representative details to snake case and decodes them", () => {
    const value: InlineWorkerToolDetails = {
      mode: "inline",
      runId: Schema.decodeUnknownSync(RunId)("run-inline"),
      ownerSessionId: "owner-session",
      result: {
        workerId: Schema.decodeUnknownSync(WorkerId)("worker-inline"),
        worker: "scout",
        title: "Inspect",
        status: "completed",
        outcome: { status: "completed", assistantText: "Complete." },
        usage,
        startedAt: 1_000,
        settledAt: 6_000,
        sessionFile: "/sessions/worker-inline.jsonl",
      },
    };
    const encoded = JSON.parse(JSON.stringify(encodeInlineWorkerToolDetails(value)));
    expect(encoded).toEqual(inlineDetails());

    const decoded = decodeInlineWorkerToolDetails(encoded);
    expect(Result.isSuccess(decoded)).toBe(true);
    if (Result.isSuccess(decoded)) expect(decoded.success).toEqual(value);
  });

  test("retains transport contradictions and optional/unknown compatibility", () => {
    const valid = inlineDetails();
    expect(Result.isFailure(decodeInlineWorkerToolDetails({
      ...valid,
      result: { ...valid.result, outcome: { status: "failed", message: "no" } },
    }))).toBe(true);
    expect(Result.isFailure(decodeInlineWorkerToolDetails({
      ...valid,
      result: { ...valid.result, started_at: 6_001 },
    }))).toBe(true);

    const { session_file: _sessionFile, ...withoutSession } = valid.result;
    const decoded = decodeInlineWorkerToolDetails({
      ...valid,
      future_top_level: true,
      result: { ...withoutSession, future_result_field: true },
    });
    expect(Result.isSuccess(decoded)).toBe(true);
    if (Result.isSuccess(decoded)) {
      expect(decoded.success).not.toHaveProperty("future_top_level");
      expect(decoded.success.result).not.toHaveProperty("future_result_field");
      expect(decoded.success.result).not.toHaveProperty("sessionFile");
    }
  });
});
