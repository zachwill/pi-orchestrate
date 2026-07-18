import { describe, expect, test } from "bun:test";
import { join } from "node:path";
import {
  CANCELLATION_GRACE_MS,
  EMPTY_WORKER_USAGE,
  InvalidTransitionError,
  MAX_TASKS_PER_WAVE,
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  SUPPORTED_TOOL_NAMES,
  canTransitionWorkerStatus,
  createRandomIdFactories,
  createSequentialIdFactories,
  createWorkerCatalog,
  findWorkerByName,
  getWaveWorkersInOrder,
  isSupportedToolName,
  isTerminalWorkerOutcome,
  isTerminalWorkerStatus,
  isWaveComplete,
  isWorkerCompleteForWave,
  transitionWorkerStatus,
  type WaveId,
  type WaveRecord,
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
  waveId: WaveId,
  status: WorkerStatus,
  lifecycle: WorkerLifecycle = "one-shot",
): WorkerRecord => ({
  id,
  worker: "scout",
  ownerSessionId: "owner-session",
  waveId,
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
  test("worker and wave IDs have canonical prefixes and independent sequences", () => {
    const ids = createSequentialIdFactories(3);
    expect(ids.workerId()).toBe("worker-3");
    expect(ids.workerId()).toBe("worker-4");
    expect(ids.waveId()).toBe("wave-3");
    expect(ids.waveId()).toBe("wave-4");

    const values = ["alpha", "beta"];
    const random = createRandomIdFactories(() => values.shift() ?? "exhausted");
    expect(random.workerId()).toBe("worker-alpha");
    expect(random.waveId()).toBe("wave-beta");
  });
});

describe("worker status transitions", () => {
  test("supports one-shot completion and reusable readiness", () => {
    const ids = createSequentialIdFactories();
    const starting = workerRecord(ids.workerId(), ids.waveId(), "starting");
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
      ...workerRecord(ids.workerId(), ids.waveId(), "ready", "reusable"),
      outcome: { status: "ready", assistantText: "Ready for follow-up." },
    };
    expect(transitionWorkerStatus(ready, "running").outcome).toBeUndefined();

    const completed = workerRecord(ids.workerId(), ids.waveId(), "completed");
    expect(() => transitionWorkerStatus(completed, "running")).toThrow(InvalidTransitionError);
  });
});

describe("terminal and wave guards", () => {
  test("distinguishes ready from terminal states", () => {
    for (const status of ["completed", "failed", "aborted", "closed"] as const) {
      expect(isTerminalWorkerStatus(status)).toBe(true);
    }
    expect(isTerminalWorkerStatus("ready")).toBe(false);
    expect(isTerminalWorkerOutcome({ status: "completed", assistantText: "complete" })).toBe(true);
    expect(isTerminalWorkerOutcome({ status: "failed", message: "failed" })).toBe(true);
    expect(isTerminalWorkerOutcome({ status: "ready", assistantText: "ready" })).toBe(false);
    expect(isWorkerCompleteForWave("ready")).toBe(true);
  });

  test("checks a wave's worker IDs in declared order", () => {
    const ids = createSequentialIdFactories();
    const waveId = ids.waveId();
    const firstId = ids.workerId();
    const secondId = ids.workerId();
    const wave: WaveRecord = {
      id: waveId,
      ownerSessionId: "owner-session",
      workerIds: [firstId, secondId],
      mode: "async",
      state: "running",
      createdAt: 1,
    };
    const workers = new Map<WorkerId, WorkerRecord>([
      [secondId, workerRecord(secondId, waveId, "ready", "reusable")],
      [firstId, workerRecord(firstId, waveId, "completed")],
    ]);

    expect(getWaveWorkersInOrder(wave, workers)?.map((worker) => worker.id)).toEqual([
      firstId,
      secondId,
    ]);
    expect(isWaveComplete(wave, workers)).toBe(true);
    workers.set(secondId, workerRecord(secondId, waveId, "running", "reusable"));
    expect(isWaveComplete(wave, workers)).toBe(false);
  });
});

test("orchestration limits are explicit domain constants", () => {
  expect(MAX_TASKS_PER_WAVE).toBe(12);
  expect(MAX_WORKER_TITLE_LENGTH).toBe(200);
  expect(MAX_WORKER_INSTRUCTIONS_LENGTH).toBe(100_000);
  expect(CANCELLATION_GRACE_MS).toBe(5_000);
});

test("owned extension files expose none of the superseded public vocabulary", async () => {
  const extensionDirectory = join(import.meta.dir, "..", "extension");
  const files = ["domain.ts", "catalog.ts", "contract.ts", "worker-session.ts", "runtime.ts", "delivery.ts"];
  const source = (await Promise.all(files.map((file) => Bun.file(join(extensionDirectory, file)).text()))).join("\n");

  for (const superseded of [
    "RosterSnapshot",
    "RosterDiagnostic",
    "createRosterSnapshot",
    "discoverRoster",
    "roster:",
    "WorkerRunId",
    "workerRunId",
    "OrchestrateRunInput",
    "worker_respond",
    "respond(",
    "runIds",
    "persistent:",
    'status: "waiting"',
    'status: "done"',
    'status: "error"',
  ]) {
    expect(source).not.toContain(superseded);
  }
});
