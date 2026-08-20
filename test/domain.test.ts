import { describe, expect, test } from "bun:test";
import { Schema } from "effect";
import {
  EMPTY_WORKER_USAGE,
  OrchestrateTaskInput,
  RunId,
  InvalidTransitionError,
  SUPPORTED_TOOL_NAMES,
  WorkerId,
  WorkerOutcome,
  WorkerResponseOutcome,
  WorkerUsage,
  canTransitionWorkerStatus,
  createRandomIdFactories,
  createSequentialIdFactories,
  createWorkerCatalog,
  findWorkerByName,
  isSupportedToolName,
  isTerminalWorkerStatus,
  transitionWorkerStatus,
  type WorkerDefinition,
  type WorkerLifecycle,
  type WorkerRecord,
  type WorkerStatus,
} from "../extension/domain.ts";

const workerDefinition = (name: string): WorkerDefinition => ({
  name,
  source: { kind: "package", filePath: `/workers/${name}.md` },
  description: `${name} worker`,
  systemPrompt: `You are ${name}.`,
  lifecycle: "one-shot",
  tools: ["read", "grep"],
  skills: [],
});

const workerRecord = (
  id: WorkerId,
  runId: RunId,
  status: WorkerStatus,
  lifecycle: WorkerLifecycle = "one-shot",
): WorkerRecord => ({
  id,
  worker: "scout",
  ownerSessionId: "owner-session",
  runId,
  title: "Inspect domain",
  instructions: "Check the requested domain behavior.",
  lifecycle,
  status,
  usage: EMPTY_WORKER_USAGE,
  startedAt: 1,
});

describe("supported tools and worker catalog", () => {
  test("accepts every exact built-in tool name", () => {
    for (const toolName of SUPPORTED_TOOL_NAMES) expect(isSupportedToolName(toolName)).toBe(true);
    expect(isSupportedToolName("Read")).toBe(false);
    expect(isSupportedToolName("shell")).toBe(false);
  });

  test("copies and sorts catalog workers", () => {
    const input = [workerDefinition("worker"), workerDefinition("investigator"), workerDefinition("scout")];
    const catalog = createWorkerCatalog(input);

    expect(catalog.workers.map((worker) => worker.name)).toEqual([
      "investigator",
      "scout",
      "worker",
    ]);
    expect(input.map((worker) => worker.name)).toEqual(["worker", "investigator", "scout"]);
    expect(findWorkerByName(catalog, "scout")?.description).toBe("scout worker");
    expect(findWorkerByName(catalog, "missing")).toBeUndefined();
  });
});

describe("validated orchestration ingress", () => {
  test("Effect Schema decodes task input and brands operational IDs", () => {
    expect(Schema.decodeUnknownSync(OrchestrateTaskInput)({
      worker: "scout",
      title: "Inspect",
      instructions: "Read the implementation.",
    })).toEqual({
      worker: "scout",
      title: "Inspect",
      instructions: "Read the implementation.",
    });
    expect(String(Schema.decodeUnknownSync(WorkerId)("worker-1"))).toBe("worker-1");
    expect(String(Schema.decodeUnknownSync(RunId)("run-1"))).toBe("run-1");
    expect(() => Schema.decodeUnknownSync(OrchestrateTaskInput)({
      worker: "scout",
      title: "Inspect",
    })).toThrow();
    expect(() => Schema.decodeUnknownSync(OrchestrateTaskInput)({
      worker: "   ",
      title: "Inspect",
      instructions: "Read the implementation.",
    })).toThrow();
    expect(() => Schema.decodeUnknownSync(WorkerId)("run-1")).toThrow();
    expect(() => Schema.decodeUnknownSync(WorkerId)("worker- ")).toThrow();
    expect(() => Schema.decodeUnknownSync(RunId)("worker-1")).toThrow();
    expect(() => Schema.decodeUnknownSync(RunId)("run- ")).toThrow();
  });
});

describe("canonical worker result schemas", () => {
  test("round-trips usage and keeps closure out of response outcomes", () => {
    const usage = {
      input: 1,
      output: 2,
      cacheRead: 3,
      cacheWrite: 4,
      cost: 0.5,
      contextTokens: 6,
      turns: 7,
    };
    const persisted = JSON.parse(JSON.stringify(
      Schema.encodeSync(WorkerUsage)(usage),
    ));
    expect(Schema.decodeUnknownSync(WorkerUsage)(persisted)).toEqual(usage);

    expect(Schema.decodeUnknownSync(WorkerOutcome)({ status: "closed" })).toEqual({
      status: "closed",
    });
    expect(() => Schema.decodeUnknownSync(WorkerResponseOutcome)({
      status: "closed",
    })).toThrow();
  });
});

describe("ID factories", () => {
  test("worker and run IDs have canonical prefixes and independent sequences", () => {
    const ids = createSequentialIdFactories(3);
    expect(String(ids.workerId())).toBe("worker-3");
    expect(String(ids.workerId())).toBe("worker-4");
    expect(String(ids.runId())).toBe("run-3");
    expect(String(ids.runId())).toBe("run-4");

    const values = ["alpha", "beta"];
    const random = createRandomIdFactories(() => values.shift() ?? "exhausted");
    expect(String(random.workerId())).toBe("worker-alpha");
    expect(String(random.runId())).toBe("run-beta");
  });
});

describe("worker status transitions", () => {
  test("supports one-shot completion and interactive readiness", () => {
    const ids = createSequentialIdFactories();
    const starting = workerRecord(ids.workerId(), ids.runId(), "starting");
    const running = transitionWorkerStatus(starting, "running");
    const completed = transitionWorkerStatus(running, "completed");

    expect(starting.status).toBe("starting");
    expect(completed.status).toBe("completed");
    expect(canTransitionWorkerStatus("running", "ready", "interactive")).toBe(true);
    expect(canTransitionWorkerStatus("ready", "running", "interactive")).toBe(true);
    expect(canTransitionWorkerStatus("ready", "closed", "interactive")).toBe(true);
    expect(canTransitionWorkerStatus("running", "ready", "one-shot")).toBe(false);
    expect(canTransitionWorkerStatus("running", "completed", "interactive")).toBe(false);
  });

  test("clears the prior ready outcome and rejects invalid transitions", () => {
    const ids = createSequentialIdFactories();
    const ready: WorkerRecord = {
      ...workerRecord(ids.workerId(), ids.runId(), "ready", "interactive"),
      outcome: { status: "ready", assistantText: "Ready for follow-up." },
    };
    expect(transitionWorkerStatus(ready, "running").outcome).toBeUndefined();

    const completed = workerRecord(ids.workerId(), ids.runId(), "completed");
    expect(() => transitionWorkerStatus(completed, "running")).toThrow(InvalidTransitionError);
  });
});

describe("terminal and run guards", () => {
  test("distinguishes ready from terminal states", () => {
    for (const status of ["completed", "failed", "aborted", "closed"] as const) {
      expect(isTerminalWorkerStatus(status)).toBe(true);
    }
    expect(isTerminalWorkerStatus("ready")).toBe(false);
  });
});
