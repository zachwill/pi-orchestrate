import { describe, expect, test } from "bun:test";
import {
  EMPTY_WORKER_USAGE,
  InvalidTransitionError,
  SUPPORTED_TOOL_NAMES,
  canTransitionWorkerStatus,
  createRandomIdFactories,
  createSequentialIdFactories,
  createWorkerCatalog,
  findWorkerByName,
  isSupportedToolName,
  isTerminalWorkerStatus,
  transitionWorkerStatus,
  type RunId,
  type WorkerDefinition,
  type WorkerId,
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
  test("supports one-shot completion and reusable readiness", () => {
    const ids = createSequentialIdFactories();
    const starting = workerRecord(ids.workerId(), ids.runId(), "starting");
    const running = transitionWorkerStatus(starting, "running");
    const completed = transitionWorkerStatus(running, "completed");

    expect(starting.status).toBe("starting");
    expect(completed.status).toBe("completed");
    expect(canTransitionWorkerStatus("running", "ready", "reusable")).toBe(true);
    expect(canTransitionWorkerStatus("ready", "running", "reusable")).toBe(true);
    expect(canTransitionWorkerStatus("ready", "closed", "reusable")).toBe(true);
    expect(canTransitionWorkerStatus("running", "ready", "one-shot")).toBe(false);
    expect(canTransitionWorkerStatus("running", "completed", "reusable")).toBe(false);
  });

  test("clears the prior ready outcome and rejects invalid transitions", () => {
    const ids = createSequentialIdFactories();
    const ready: WorkerRecord = {
      ...workerRecord(ids.workerId(), ids.runId(), "ready", "reusable"),
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
