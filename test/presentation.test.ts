import { beforeAll, describe, expect, test } from "bun:test";
import {
  initTheme,
  type ExtensionAPI,
  type ExtensionContext,
  type MessageRenderer,
  type Theme,
} from "@earendil-works/pi-coding-agent";
import { visibleWidth, type Component } from "@earendil-works/pi-tui";
import { readFileSync } from "node:fs";
import type { RuntimeSnapshot } from "../extension/runtime.ts";
import type { WaveRecord, WorkerRecord, WorkerStatus } from "../extension/domain.ts";
import {
  MAX_RESULT_PREVIEW_LINES,
  MAX_WIDGET_WORKERS,
  ORCHESTRATION_PRESENTATION_KEY,
  StatusController,
  formatFooterStatus,
  formatResultPreviews,
  formatResultStatusSummary,
  formatWorkerStatusLine,
  formatWorkerUsage,
  registerOrchestrationPresentation,
  type PresentationRuntime,
} from "../extension/presentation.ts";

beforeAll(() => initTheme("dark", false));

const theme = {
  fg: (_color: string, text: string) => text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
  italic: (text: string) => text,
  underline: (text: string) => text,
  inverse: (text: string) => text,
  strikethrough: (text: string) => text,
} as Theme;

const usage = {
  input: 1_200,
  output: 345,
  cacheRead: 0,
  cacheWrite: 0,
  cost: 0.0123,
  contextTokens: 12_345,
  turns: 2,
};

function worker(
  id: string,
  status: WorkerStatus,
  overrides: Partial<WorkerRecord> = {},
): WorkerRecord {
  return {
    id,
    worker: "scout",
    ownerSessionId: "owner",
    waveId: "wave-1",
    title: `Human title ${id}`,
    instructions: "Inspect the repository",
    lifecycle: status === "ready" ? "reusable" : "one-shot",
    status,
    usage,
    ...overrides,
  } as WorkerRecord;
}

function wave(
  id = "wave-1",
  workerIds: readonly string[] = ["worker-1"],
  state: WaveRecord["state"] = "running",
  createdAt = 10_000,
): WaveRecord {
  return {
    id,
    ownerSessionId: "owner",
    workerIds,
    mode: "async",
    state,
    createdAt,
  } as WaveRecord;
}

function snapshot(
  workers: readonly WorkerRecord[],
  waves: readonly WaveRecord[] = [wave()],
): RuntimeSnapshot {
  return { workers, waves };
}

function resultDetails(count = 2): unknown {
  const statuses = ["completed", "failed", "aborted", "ready", "completed", "completed"];
  return {
    id: "wave-results",
    ownerSessionId: "owner",
    mode: "async",
    results: Array.from({ length: count }, (_, index) => {
      const status = statuses[index] ?? "completed";
      return {
        workerId: `worker-${index + 1}`,
        worker: index % 2 === 0 ? "scout" : "worker",
        title: `Task ${index + 1}`,
        status,
        outcome: index === 1
          ? { status: "failed", message: "Tests failed", assistantText: "Partial result" }
          : index === 2
            ? { status: "aborted", message: "Stopped" }
            : index === 3
              ? { status: "ready", assistantText: "Waiting for follow-up" }
              : { status: "completed", assistantText: `Result body ${index + 1}` },
        usage,
        sessionFile: `/sessions/worker-${index + 1}.jsonl`,
      };
    }),
  };
}

function registeredRenderer(): MessageRenderer {
  let renderer: MessageRenderer | undefined;
  const pi = {
    registerMessageRenderer(customType: string, candidate: MessageRenderer) {
      expect(customType).toBe("pi-orchestrate-wave");
      renderer = candidate;
    },
  } as unknown as ExtensionAPI;
  registerOrchestrationPresentation(pi);
  return renderer!;
}

function renderMessage(
  expanded: boolean,
  details: unknown = resultDetails(),
  content = "## Capped worker results\n\nDelivery preview only.",
  width = 60,
): string[] {
  const component = registeredRenderer()(
    {
      role: "custom",
      customType: "pi-orchestrate-wave",
      content,
      display: true,
      details,
      timestamp: Date.now(),
    },
    { expanded },
    theme,
  );
  expect(component).toBeDefined();
  return component!.render(width);
}

class Deferred<T> {
  readonly promise: Promise<T>;
  private resolvePromise!: (value: T) => void;

  constructor() {
    this.promise = new Promise((resolve) => {
      this.resolvePromise = resolve;
    });
  }

  resolve(value: T): void {
    this.resolvePromise(value);
  }
}

class RuntimeHarness implements PresentationRuntime {
  readonly listeners = new Set<(ownerSessionId: string) => void>();
  readonly snapshotOwners: string[] = [];
  subscribeCalls = 0;
  unsubscribeCalls = 0;
  snapshotImpl: (ownerSessionId: string) => Promise<RuntimeSnapshot> = async () => snapshot([], []);

  snapshot(ownerSessionId: string): Promise<RuntimeSnapshot> {
    this.snapshotOwners.push(ownerSessionId);
    return this.snapshotImpl(ownerSessionId);
  }

  subscribeState(listener: (ownerSessionId: string) => void): () => void {
    this.subscribeCalls += 1;
    this.listeners.add(listener);
    return () => {
      if (!this.listeners.delete(listener)) return;
      this.unsubscribeCalls += 1;
    };
  }

  emit(ownerSessionId: string): void {
    for (const listener of this.listeners) listener(ownerSessionId);
  }
}

interface UiHarness {
  readonly ctx: ExtensionContext;
  readonly statuses: Array<string | undefined>;
  readonly widgets: Array<unknown>;
}

function context(mode: ExtensionContext["mode"] = "tui"): UiHarness {
  const statuses: Array<string | undefined> = [];
  const widgets: Array<unknown> = [];
  const ui = {
    setStatus(key: string, value: string | undefined) {
      expect(key).toBe(ORCHESTRATION_PRESENTATION_KEY);
      statuses.push(value);
    },
    setWidget(key: string, value: unknown) {
      expect(key).toBe(ORCHESTRATION_PRESENTATION_KEY);
      widgets.push(value);
    },
  };
  return {
    ctx: { mode, ui } as unknown as ExtensionContext,
    statuses,
    widgets,
  };
}

function latestWidget(ui: UiHarness): Component {
  const factory = ui.widgets.at(-1) as (tui: unknown, theme: Theme) => Component;
  expect(typeof factory).toBe("function");
  return factory({ requestRender() {} }, theme);
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("pure result formatting", () => {
  test("summarizes statuses and keeps ordered previews to five lines", () => {
    expect(formatResultStatusSummary(resultDetails(4))).toBe(
      "4 results · 1 completed · 1 ready · 1 failed · 1 aborted",
    );

    const previews = formatResultPreviews(resultDetails(7));
    expect(previews).toHaveLength(MAX_RESULT_PREVIEW_LINES);
    expect(previews[0]).toContain("✓ scout — Task 1 · completed: Result body 1");
    expect(previews[1]).toContain("✗ worker — Task 2 · failed: Tests failed");
    expect(previews[2]).toContain("■ scout — Task 3 · aborted: Stopped");
    expect(previews[3]).toContain("○ worker — Task 4 · ready: Waiting for follow-up");
    expect(previews[4]).toBe("… 3 more results");
  });

  test("handles malformed details with a bounded content fallback", () => {
    const fallback = `${"first line ".repeat(1_000)}\nsecond\nthird`;
    expect(formatResultStatusSummary(undefined)).toBe("Result details unavailable");
    expect(formatResultPreviews({ id: 42, results: "bad" }, fallback, 2)).toEqual([
      "first line ".repeat(1_000).trim(),
      "second",
    ]);
  });

  test("formats compact usage and tolerates missing usage", () => {
    expect(formatWorkerUsage(undefined)).toBeUndefined();
    expect(formatWorkerUsage({})).toBeUndefined();
    expect(formatWorkerUsage(usage)).toBe("12.3k ctx · 2 turns · ↑1.2k · ↓345 · $0.0123");
  });
});

describe("worker activity presentation", () => {
  test.each([
    ["read", "reading"],
    ["grep", "searching"],
    ["find", "finding files"],
    ["ls", "listing"],
    ["bash", "running command"],
    ["edit", "editing"],
    ["write", "writing"],
    [undefined, "thinking"],
  ] as const)("maps %s to %s", (activity, expected) => {
    const line = formatWorkerStatusLine(
      worker("worker-activity", "running", { activity }),
    );
    expect(line).toStartWith(`Human title worker-activity · scout · ${expected}`);
    expect(line).toContain("· 12.3k ctx · $0.0123");
    expect(line).not.toMatch(/ · \d+[smh](?: | ·)/);
    expect(line).toEndWith("· worker-activity");
  });

  test("starting and stopping override tool activity", () => {
    expect(formatWorkerStatusLine(worker("start", "starting", { activity: "bash" })))
      .toContain("· scout · starting ·");
    expect(formatWorkerStatusLine(worker("stop", "stopping", { activity: "read" })))
      .toContain("· scout · stopping ·");
  });

  test("footer has only active and ready counts and clears when both are zero", () => {
    expect(formatFooterStatus(snapshot([
      worker("running", "running"),
      worker("starting", "starting"),
      worker("ready", "ready"),
      worker("done", "completed"),
    ]))).toBe("Orchestrate: 2 active · 1 ready");
    expect(formatFooterStatus(snapshot([worker("ready", "ready")]))).toBe(
      "Orchestrate: 0 active · 1 ready",
    );
    expect(formatFooterStatus(snapshot([worker("done", "completed")]))).toBeUndefined();
  });
});

describe("wave result renderer", () => {
  test("collapsed rendering is neutral, ordered, bounded, and uses Pi's expansion hint", () => {
    const plain = renderMessage(false, resultDetails(7)).map((line) => Bun.stripANSI(line));
    expect(plain[0]).toStartWith("Worker results · wave-results");
    expect(plain[0]).not.toMatch(/^[✓✗■●]/);
    expect(plain[1]).toContain("7 results");
    expect(plain).toHaveLength(2 + MAX_RESULT_PREVIEW_LINES + 1);
    expect(plain[2]).toContain("Task 1 · completed");
    expect(plain[3]).toContain("Task 2 · failed: Tests failed");
    expect(plain[4]).toContain("Task 3 · aborted: Stopped");
    expect(plain.at(-1)).toContain("to expand results");
  });

  test("expanded rendering reconstructs full outcome Markdown instead of capped content", () => {
    const fullAssistantText = `# Full structured result\n\n${"complete detail ".repeat(4_500)}\n\nTAIL-ONLY-IN-DETAILS`;
    const details = {
      id: "wave-large",
      results: [{
        workerId: "worker-full-id",
        worker: "investigator",
        title: "Deep review",
        status: "completed",
        outcome: { status: "completed", assistantText: fullAssistantText },
        usage,
        sessionFile: "/sessions/full.jsonl",
      }],
    };
    const cappedContent = `${fullAssistantText.slice(0, 50 * 1024)}\n[truncated at 50KB]`;
    const expanded = Bun.stripANSI(renderMessage(true, details, cappedContent, 80).join("\n"));

    expect(expanded).toContain("Full structured result");
    expect(expanded).toContain("TAIL-ONLY-IN-DETAILS");
    expect(expanded).toContain("investigator — Deep review");
    expect(expanded).toContain("ID worker-full-id · status completed");
    expect(expanded).toContain("usage 12.3k ctx");
    expect(expanded).toContain("session /sessions/full.jsonl");
  });

  test("expanded failures and aborts retain full text and visible reasons", () => {
    const plain = Bun.stripANSI(renderMessage(true, resultDetails(3), "capped fallback", 80).join("\n"));
    expect(plain).toContain("Failed: Tests failed");
    expect(plain).toContain("Partial result");
    expect(plain).toContain("Aborted: Stopped");
    expect(plain).toContain("ID worker-2 · status failed");
    expect(plain).toContain("ID worker-3 · status aborted");
  });

  test("malformed details use capped content as the safe fallback", () => {
    const malformed = {
      id: "wave-bad",
      results: [{ workerId: "worker-1", worker: "scout", title: "Bad", status: "completed" }],
    };
    const collapsed = Bun.stripANSI(renderMessage(false, malformed, "fallback result").join("\n"));
    const expanded = Bun.stripANSI(renderMessage(true, malformed, "fallback **Markdown**").join("\n"));
    expect(collapsed).toContain("fallback result");
    expect(expanded).toContain("fallback Markdown");
    expect(expanded).toContain("Structured worker metadata unavailable");
  });

  test("collapsed and expanded renderers are safe at 120/80/50/32 columns", () => {
    const longDetails = resultDetails(4) as { results: Array<Record<string, unknown>> };
    longDetails.results[0]!.title = "A very long human title ".repeat(20);
    longDetails.results[0]!.sessionFile = `/sessions/${"deep/".repeat(30)}worker.jsonl`;

    for (const width of [120, 80, 50, 32]) {
      for (const expanded of [false, true]) {
        for (const line of renderMessage(expanded, longDetails, "Long markdown content ".repeat(50), width)) {
          expect(visibleWidth(line)).toBeLessThanOrEqual(width);
        }
      }
    }
  });
});

describe("StatusController", () => {
  test("subscribes only while bound and can rebind after unbinding", async () => {
    const runtime = new RuntimeHarness();
    const first = context();
    const second = context();
    const controller = new StatusController(runtime);

    expect(runtime.subscribeCalls).toBe(0);
    expect(runtime.listeners.size).toBe(0);

    controller.bind("owner-a", first.ctx);
    await settle();
    expect(runtime.subscribeCalls).toBe(1);
    expect(runtime.listeners.size).toBe(1);

    controller.unbind("owner-a");
    expect(runtime.unsubscribeCalls).toBe(1);
    expect(runtime.listeners.size).toBe(0);

    controller.bind("owner-b", second.ctx);
    await settle();
    expect(runtime.subscribeCalls).toBe(2);
    expect(runtime.listeners.size).toBe(1);

    controller.dispose();
    expect(runtime.unsubscribeCalls).toBe(2);
    expect(runtime.listeners.size).toBe(0);
  });

  test("groups running waves with progress and includes their settled rows", async () => {
    const runtime = new RuntimeHarness();
    runtime.snapshotImpl = async () => snapshot(
      [
        worker("worker-alpha", "running", { waveId: "wave-a", title: "Inspect code", activity: "read" }),
        worker("worker-beta", "completed", { waveId: "wave-a", title: "Review tests" }),
        worker("worker-gamma", "ready", {
          waveId: "wave-a",
          title: "Await follow-up",
          lifecycle: "reusable",
        }),
        worker("worker-delta", "running", { waveId: "wave-b", title: "Run checks", activity: "bash" }),
      ],
      [
        wave("wave-a", ["worker-alpha", "worker-beta", "worker-gamma"], "running", 10_000),
        wave("wave-b", ["worker-delta"], "running", 70_000),
      ],
    );
    const ui = context();
    const controller = new StatusController(runtime);

    controller.bind("owner", ui.ctx);
    await settle();
    const plain = Bun.stripANSI(latestWidget(ui).render(120).join("\n"));
    expect(plain).toContain("Wave wave-a · 2/3 settled");
    expect(plain).toContain("● Inspect code · scout · reading");
    expect(plain).toContain("✓ Review tests · scout · completed");
    expect(plain).toContain("○ Await follow-up · scout · ready");
    expect(plain).toContain("Wave wave-b · 0/1 settled");
    expect(plain).not.toMatch(/settled · \d+[smh]/);
    expect(plain).toContain("running command");
    expect(plain).not.toContain("Ready ·");
    controller.dispose();
  });

  test("moves reusable workers to Ready and removes terminal rows after wave completion", async () => {
    const runtime = new RuntimeHarness();
    let current = snapshot(
      [
        worker("worker-terminal", "completed", { waveId: "wave-a", title: "Finished task" }),
        worker("worker-ready", "ready", {
          waveId: "wave-a",
          title: "Reusable reviewer",
          lifecycle: "reusable",
        }),
        worker("worker-live", "running", { waveId: "wave-a", title: "Still working" }),
      ],
      [wave("wave-a", ["worker-terminal", "worker-ready", "worker-live"], "running")],
    );
    runtime.snapshotImpl = async () => current;
    const ui = context();
    const controller = new StatusController(runtime);

    controller.bind("owner", ui.ctx);
    await settle();
    expect(Bun.stripANSI(latestWidget(ui).render(80).join("\n"))).toContain("Finished task");

    current = snapshot(
      [
        worker("worker-terminal", "completed", { waveId: "wave-a", title: "Finished task" }),
        worker("worker-ready", "ready", {
          waveId: "wave-a",
          title: "Reusable reviewer",
          lifecycle: "reusable",
        }),
        worker("worker-live", "completed", { waveId: "wave-a", title: "Last result" }),
      ],
      [wave("wave-a", ["worker-terminal", "worker-ready", "worker-live"], "complete")],
    );
    runtime.emit("owner");
    await settle();
    const ready = Bun.stripANSI(latestWidget(ui).render(80).join("\n"));
    expect(ready).toContain("Ready · 1");
    expect(ready).toContain("Reusable reviewer");
    expect(ready).toContain("worker-ready");
    expect(ready).not.toContain("Finished task");
    expect(ready).not.toContain("Last result");
    expect(ui.statuses.at(-1)).toBe("Orchestrate: 0 active · 1 ready");

    current = snapshot([worker("worker-terminal", "completed")], [wave("wave-a", ["worker-terminal"], "complete")]);
    runtime.emit("owner");
    await settle();
    expect(ui.statuses.at(-1)).toBeUndefined();
    expect(ui.widgets.at(-1)).toBeUndefined();
    controller.dispose();
  });

  test("keeps worker IDs and state visible while adapting detail at 120/80/50/32 columns", async () => {
    const runtime = new RuntimeHarness();
    runtime.snapshotImpl = async () => snapshot(
      [worker("worker-alpha", "running", {
        waveId: "wave-a",
        title: "A long human-readable investigation title",
        activity: "grep",
      })],
      [wave("wave-a", ["worker-alpha"], "running")],
    );
    const ui = context();
    const controller = new StatusController(runtime);
    controller.bind("owner", ui.ctx);
    await settle();
    const component = latestWidget(ui);

    for (const width of [120, 80, 50, 32]) {
      const lines = component.render(width);
      expect(lines).toEqual(component.render(width));
      expect(lines.every((line) => visibleWidth(line) <= width)).toBe(true);
      const workerLine = Bun.stripANSI(lines.find((line) => line.includes("worker-alpha")) ?? "");
      expect(workerLine).toContain("worker-alpha");
      expect(workerLine).toContain("searching");
      expect(workerLine.indexOf("A long")).toBeLessThan(workerLine.indexOf("worker-alpha"));
    }
    controller.dispose();
  });

  test("limits worker rows to eight and adds a clear overflow line", async () => {
    const runtime = new RuntimeHarness();
    const workers = Array.from({ length: 10 }, (_, index) => worker(`ready-${index + 1}`, "ready"));
    runtime.snapshotImpl = async () => snapshot(workers, [wave("wave-complete", [], "complete")]);
    const ui = context();
    const controller = new StatusController(runtime);
    controller.bind("owner", ui.ctx);
    await settle();

    const plainLines = latestWidget(ui).render(80).map((line) => Bun.stripANSI(line));
    expect(plainLines.filter((line) => line.startsWith("○ "))).toHaveLength(MAX_WIDGET_WORKERS);
    expect(plainLines.at(-1)).toBe("… 2 more workers");
    controller.dispose();
  });

  test("has no timer-driven refresh or animation", () => {
    const source = readFileSync(new URL("../extension/presentation.ts", import.meta.url), "utf8");
    expect(source).not.toMatch(/setInterval|setTimeout|requestAnimationFrame/);
  });

  test("ignores stale owners and out-of-order refreshes, then cleans up once", async () => {
    const runtime = new RuntimeHarness();
    const ownerA = new Deferred<RuntimeSnapshot>();
    const ownerB = new Deferred<RuntimeSnapshot>();
    runtime.snapshotImpl = (owner) => owner === "owner-a" ? ownerA.promise : ownerB.promise;
    const first = context();
    const second = context();
    const controller = new StatusController(runtime);

    controller.bind("owner-a", first.ctx);
    controller.bind("owner-b", second.ctx);
    ownerB.resolve(snapshot([worker("ready", "ready")], []));
    await settle();
    ownerA.resolve(snapshot([worker("stale", "running")], [wave("wave-1", ["stale"])]));
    await settle();

    expect(second.statuses).toEqual(["Orchestrate: 0 active · 1 ready"]);
    expect(first.statuses).toEqual([undefined]);
    controller.unbind("owner-a");
    expect(second.statuses.at(-1)).toBe("Orchestrate: 0 active · 1 ready");
    controller.dispose();
    controller.dispose();
    expect(second.statuses.at(-1)).toBeUndefined();
    expect(second.widgets.at(-1)).toBeUndefined();
    expect(runtime.subscribeCalls).toBe(2);
    expect(runtime.unsubscribeCalls).toBe(2);
  });

  test("ignores an older same-owner response", async () => {
    const runtime = new RuntimeHarness();
    const older = new Deferred<RuntimeSnapshot>();
    const newer = new Deferred<RuntimeSnapshot>();
    let call = 0;
    runtime.snapshotImpl = () => (++call === 1 ? older.promise : newer.promise);
    const ui = context();
    const controller = new StatusController(runtime);

    controller.bind("owner", ui.ctx);
    runtime.emit("owner");
    newer.resolve(snapshot([worker("ready", "ready")], []));
    await settle();
    older.resolve(snapshot([worker("stale", "running")], [wave("wave-1", ["stale"])]));
    await settle();

    expect(ui.statuses).toEqual(["Orchestrate: 0 active · 1 ready"]);
    controller.dispose();
  });

  test("does not install widgets outside TUI mode", async () => {
    const runtime = new RuntimeHarness();
    runtime.snapshotImpl = async () => snapshot(
      [worker("worker-1", "running")],
      [wave("wave-1", ["worker-1"])],
    );
    const ui = context("rpc");
    const controller = new StatusController(runtime);

    controller.bind("owner", ui.ctx);
    await settle();
    expect(ui.statuses).toContain("Orchestrate: 1 active · 0 ready");
    expect(ui.widgets).toEqual([]);
    controller.dispose();
  });
});
