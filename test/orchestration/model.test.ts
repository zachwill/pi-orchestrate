import { describe, expect, test } from "bun:test";
import type { WorkerLifecycle } from "../../extension/catalog/definition.ts";
import {
  EMPTY_WORKER_USAGE,
  InvalidTransitionError,
  canTransitionWorkerStatus,
  createSequentialIdFactories,
  transitionWorkerStatus,
  type WorkerRecord,
  type WorkerStatus,
} from "../../extension/orchestration/model.ts";

const statuses: readonly WorkerStatus[] = [
  "starting",
  "running",
  "ready",
  "stopping",
  "completed",
  "failed",
  "aborted",
  "closed",
];

const allowedTransitions: Record<
  WorkerLifecycle,
  Partial<Record<WorkerStatus, readonly WorkerStatus[]>>
> = {
  "one-shot": {
    starting: ["running", "stopping", "failed", "aborted"],
    running: ["stopping", "completed", "failed", "aborted"],
    stopping: ["failed", "aborted"],
  },
  interactive: {
    starting: ["running", "stopping", "failed", "aborted"],
    running: ["ready", "stopping", "failed", "aborted"],
    ready: ["running", "stopping", "closed"],
    stopping: ["failed", "aborted"],
  },
};

function workerRecord(
  status: WorkerStatus,
  lifecycle: WorkerLifecycle,
): WorkerRecord {
  const ids = createSequentialIdFactories();
  return {
    id: ids.workerId(),
    worker: "scout",
    ownerSessionId: "owner-session",
    runId: ids.runId(),
    title: "Inspect domain",
    instructions: "Check the requested domain behavior.",
    lifecycle,
    status,
    usage: EMPTY_WORKER_USAGE,
    startedAt: 1,
  };
}

describe("worker status transitions", () => {
  test("enforces the complete lifecycle-specific transition matrix", () => {
    for (const lifecycle of ["one-shot", "interactive"] as const) {
      for (const from of statuses) {
        for (const to of statuses) {
          expect(canTransitionWorkerStatus(from, to, lifecycle)).toBe(
            allowedTransitions[lifecycle][from]?.includes(to) ?? false,
          );
        }
      }
    }
  });

  test("clears stale ready outcomes and rejects transitions outside the matrix", () => {
    const ready: WorkerRecord = {
      ...workerRecord("ready", "interactive"),
      outcome: { status: "ready", assistantText: "Ready for follow-up." },
    };
    expect(transitionWorkerStatus(ready, "running")).toMatchObject({
      status: "running",
      outcome: undefined,
    });

    expect(() => transitionWorkerStatus(
      workerRecord("completed", "one-shot"),
      "running",
    )).toThrow(InvalidTransitionError);
  });
});
