import { describe, expect, test } from "bun:test";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type {} from "@earendil-works/pi-coding-agent";
import {
  RunId,
  WorkerId,
  type WorkerRecord,
  type WorkerStatus,
} from "../../extension/orchestration/model.ts";
import type { OwnerSnapshot } from "../../extension/orchestration/service.ts";
import {
  LIVE_WORKER_CONTEXT_TYPE,
  MAX_LIVE_WORKER_CONTEXT_BYTES,
  MAX_LIVE_WORKER_ITEM_BYTES,
  projectLiveWorkerContext,
  replaceLiveWorkerContext,
} from "../../extension/parent/worker-context.ts";

type CustomMessage = Extract<AgentMessage, { readonly role: "custom" }>;

const usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  cost: 0,
  contextTokens: 0,
  turns: 0,
};

function worker(
  id: number,
  status: WorkerStatus,
  overrides: Partial<WorkerRecord> = {},
): WorkerRecord {
  return {
    id: WorkerId.make(`worker-${id}`),
    runId: RunId.make(`run-${id}`),
    ownerSessionId: "owner-a",
    worker: "scout",
    title: `Inspect scope ${id}`,
    instructions: `Inspect assignment ${id} without changing files.`,
    lifecycle: status === "ready" ? "interactive" : "one-shot",
    status,
    usage,
    startedAt: id,
    ...overrides,
  };
}

function snapshot(workers: readonly WorkerRecord[]): OwnerSnapshot {
  return { workers, runs: [] };
}

function contentOf(message: CustomMessage | undefined): string {
  expect(message).toBeDefined();
  const content = message?.content;
  expect(typeof content).toBe("string");
  return typeof content === "string" ? content : "";
}

describe("live worker context projection", () => {
  test("includes only owned active workers and ready interactive sessions", () => {
    const message = projectLiveWorkerContext(
      snapshot([
        worker(1, "running", { worker: "investigator", title: "Map API ownership" }),
        worker(2, "ready", { title: "Retained reviewer" }),
        worker(3, "completed", {
          outcome: { status: "completed", assistantText: "completed outcome must not replay" },
          settledAt: 30,
        }),
        worker(4, "failed", {
          outcome: { status: "failed", message: "failed outcome must not replay" },
          settledAt: 40,
        }),
        worker(5, "running", { ownerSessionId: "owner-b", title: "Foreign work" }),
      ]),
      { ownerSessionId: "owner-a", pendingResultCount: 2, timestamp: 123 },
    );

    const content = contentOf(message);
    expect(message).toMatchObject({
      role: "custom",
      customType: LIVE_WORKER_CONTEXT_TYPE,
      display: false,
      timestamp: 123,
    });
    expect(content).toContain('worker_id="worker-1"');
    expect(content).toContain('run_id="run-1"');
    expect(content).toContain('definition="investigator"');
    expect(content).toContain('title="Map API ownership"');
    expect(content).toContain("lifecycle=one-shot | status=running");
    expect(content).toContain('worker_id="worker-2"');
    expect(content).toContain("lifecycle=interactive | status=ready");
    expect(content).toContain("Pending delivery: 2 settled worker results await automatic delivery.");
    expect(content).toContain("Do not duplicate active assignments.");
    expect(content).toContain("use interactive_send with the worker_id");
    expect(content).not.toContain("worker-3");
    expect(content).not.toContain("worker-4");
    expect(content).not.toContain("worker-5");
    expect(content).not.toContain("completed outcome must not replay");
    expect(content).not.toContain("failed outcome must not replay");
  });

  test("removes a previous transient snapshot when only completed history remains", () => {
    const previous: CustomMessage = {
      role: "custom",
      customType: LIVE_WORKER_CONTEXT_TYPE,
      content: "stale running worker",
      display: false,
      timestamp: 1,
    };
    const unrelated: CustomMessage = {
      role: "custom",
      customType: "another-extension",
      content: "keep this",
      display: false,
      timestamp: 2,
    };
    const user: AgentMessage = { role: "user", content: "Continue", timestamp: 3 };
    const projection = projectLiveWorkerContext(
      snapshot([worker(1, "completed")]),
      { ownerSessionId: "owner-a", pendingResultCount: 0 },
    );

    expect(projection).toBeUndefined();
    expect(replaceLiveWorkerContext([user, previous, unrelated], projection)).toEqual([
      user,
      unrelated,
    ]);
  });

  test("retains a ready interactive worker without active workers", () => {
    const content = contentOf(projectLiveWorkerContext(
      snapshot([worker(7, "ready", { instructions: "Review the revised patch." })]),
      { ownerSessionId: "owner-a", pendingResultCount: 0 },
    ));

    expect(content).toContain("Relevant owned workers: 1 (0 active, 1 ready interactive).");
    expect(content).toContain('worker_id="worker-7"');
    expect(content).toContain('assignment="Review the revised patch."');
    expect(content).toContain("interactive_close");
    expect(content).not.toContain("Do not duplicate active assignments.");
  });

  test("bounds each worker entry and marks a truncated assignment", () => {
    const content = contentOf(projectLiveWorkerContext(
      snapshot([worker(1, "running", { instructions: "界".repeat(2_000) })]),
      { ownerSessionId: "owner-a", pendingResultCount: 0 },
    ));
    const item = content.split("\n\n").find((section) => section.startsWith("- worker_id="));

    expect(item).toBeDefined();
    expect(Buffer.byteLength(item ?? "", "utf8")).toBeLessThanOrEqual(
      MAX_LIVE_WORKER_ITEM_BYTES,
    );
    expect(item).toContain("assignment excerpt truncated");
    expect(item).not.toContain("�");
  });

  test("bounds the whole message and reports exactly how many workers were omitted", () => {
    const workers = Array.from({ length: 100 }, (_, index) =>
      worker(index + 1, "running", {
        instructions: `Scope ${index + 1}: ${"detail ".repeat(200)}`,
      })
    );
    const content = contentOf(projectLiveWorkerContext(
      snapshot(workers),
      { ownerSessionId: "owner-a", pendingResultCount: 0 },
    ));
    const shown = [...content.matchAll(/^- worker_id=/gm)].length;
    const overflow = content.match(
      /Snapshot overflow: showing (\d+) of 100; (\d+) workers omitted/,
    );

    expect(Buffer.byteLength(content, "utf8")).toBeLessThanOrEqual(
      MAX_LIVE_WORKER_CONTEXT_BYTES,
    );
    expect(overflow).not.toBeNull();
    expect(Number(overflow?.[1])).toBe(shown);
    expect(Number(overflow?.[2])).toBe(100 - shown);
    expect(content).toContain("worker_status once for diagnostics/recovery");
    expect(content).toContain("do not poll");
  });

  test("replaces all prior own projections without disturbing unrelated messages", () => {
    const user: AgentMessage = { role: "user", content: "Keep", timestamp: 1 };
    const unrelated: CustomMessage = {
      role: "custom",
      customType: "other",
      content: "Keep custom",
      display: false,
      timestamp: 2,
    };
    const stale = (timestamp: number): CustomMessage => ({
      role: "custom",
      customType: LIVE_WORKER_CONTEXT_TYPE,
      content: "stale",
      display: false,
      timestamp,
    });
    const current = projectLiveWorkerContext(
      snapshot([worker(9, "starting")]),
      { ownerSessionId: "owner-a", pendingResultCount: 0, timestamp: 5 },
    );
    if (!current) throw new Error("Expected a live worker projection");

    expect(replaceLiveWorkerContext(
      [stale(3), user, unrelated, stale(4)],
      current,
    )).toEqual([user, unrelated, current]);
  });
});
