import { beforeAll, describe, expect, test } from "bun:test";
import { initTheme, type ExtensionAPI, type ExtensionContext, type MessageRenderer, type Theme } from "@earendil-works/pi-coding-agent";
import { visibleWidth, type Component } from "@earendil-works/pi-tui";
import type {
  RunId,
  WorkerId,
  WorkerRecord,
  WorkerStatus,
} from "../../extension/orchestration/model.ts";
import type { OwnerSnapshot } from "../../extension/orchestration/service.ts";
import {
  MAX_RESULT_PREVIEW_LINES,
  MAX_WIDGET_WORKERS,
  ORCHESTRATION_PRESENTATION_KEY,
  StatusController,
  WorkerStatusComponent,
  formatFooterStatus,
  registerOrchestrationPresentation,
  type WorkerStateSource,
} from "../../extension/pi/presentation.ts";

beforeAll(() => initTheme("dark", false));
const theme = { fg: (_: string, text: string) => text, bg: (_: string, text: string) => text, bold: (text: string) => text, italic: (text: string) => text, underline: (text: string) => text, inverse: (text: string) => text, strikethrough: (text: string) => text } as Theme;
const usage = { input: 1200, output: 345, cacheRead: 12, cacheWrite: 3, cost: 0.0123, contextTokens: 12345, turns: 2 };

function worker(id: string, status: WorkerStatus, overrides: Partial<WorkerRecord> = {}): WorkerRecord {
  return { id: id as WorkerId, worker: "scout", ownerSessionId: "owner", runId: "run" as RunId, title: `Task ${id}`, instructions: "Do it", lifecycle: status === "ready" ? "interactive" : "one-shot", status, usage, messageDirection: status === "starting" ? "to-model" : "from-model", startedAt: Date.now() - 78_000, ...overrides };
}
function snapshot(workers: readonly WorkerRecord[]): OwnerSnapshot {
  return {
    workers,
    runs: workers.length === 0 ? [] : [{
      id: "run" as RunId,
      ownerSessionId: "owner",
      workerId: workers[0]!.id,
      mode: "async",
      state: "running",
      createdAt: Date.now() - 78_000,
    }],
  };
}
function settlement(status: "completed" | "ready" | "failed" | "aborted" = "completed", text = "A useful worker response.") {
  return {
    eventId: "event", sequence: 1, ownerSessionId: "owner", runId: "run-1", workerId: "worker-1", generation: 2,
    mode: "async" as const, worker: "scout", title: "Inspect code", lifecycle: status === "ready" ? "interactive" : "one-shot", status,
    outcome: status === "completed" || status === "ready" ? { status, assistantText: text } : { status, message: text, assistantText: "Partial evidence." },
    usage, startedAt: 1000, settledAt: 6200, sessionFile: "/sessions/worker.jsonl",
  };
}
function renderer(): MessageRenderer {
  let result: MessageRenderer | undefined;
  registerOrchestrationPresentation({ registerMessageRenderer(type: string, candidate: MessageRenderer) { expect(type).toBe("pi-orchestrate-worker-result"); result = candidate; } } as unknown as ExtensionAPI);
  return result!;
}
function renderResult(details: unknown, expanded: boolean, width: number, content = "fallback content"): string[] {
  return renderer()({ role: "custom", customType: "pi-orchestrate-worker-result", content, display: true, details, timestamp: 1 }, { expanded, outputPad: 1 }, theme)!.render(width);
}

describe("per-worker result messages", () => {
  test("does not present failed work as successful", () => {
    const output = Bun.stripANSI(renderResult(settlement("failed", "Worker failed."), false, 80).join("\n"));
    expect(output).toContain("Worker failed.");
    expect(output).not.toContain("✓");
  });

  test("bounds collapsed output and preserves the full expanded response and reconstruction facts", () => {
    const text = `BEGIN\n${"detail ".repeat(1_000)}\nINTERIOR_RESPONSE_SENTINEL\n${"more ".repeat(1_000)}\nTAIL`;
    const collapsed = renderResult(settlement("completed", text), false, 32);
    expect(collapsed.length).toBeLessThanOrEqual(MAX_RESULT_PREVIEW_LINES + 7);
    expect(collapsed.every((line) => visibleWidth(line) <= 32)).toBe(true);

    const expanded = renderResult(settlement("completed", text), true, 80);
    const output = Bun.stripANSI(expanded.join("\n"));
    expect(output).toContain("BEGIN");
    expect(output).toContain("INTERIOR_RESPONSE_SENTINEL");
    expect(output).toContain("TAIL");
    expect(output).toContain("worker ID worker-1");
    expect(output).toContain("run ID run-1");
    expect(output).toContain("status completed");
    expect(output).toContain("generation 2");
    expect(output).toContain("turns 2");
    expect(output).toContain("input 1200");
    expect(output).toContain("session /sessions/worker.jsonl");
    expect(expanded.every((line) => visibleWidth(line) <= 80)).toBe(true);
    expect(renderResult(settlement("completed", "x".repeat(10_000)), true, 1)
      .every((line) => visibleWidth(line) <= 1)).toBe(true);
  });

  test("renders one malformed settlement neutrally without dropping fallback content", () => {
    const content = Array.from({ length: 150 }, (_, index) => `fallback line ${index}`).join("\n");
    const output = Bun.stripANSI(renderResult({ bad: true }, true, 40, content).join("\n"));
    expect(output).toContain("details unavailable");
    expect(output).toContain("fallback line 0");
    expect(output).toContain("fallback line 149");
    expect(output).not.toContain("✓");
  });

  test("rebuilds themed worker-result children on invalidation", () => {
    let marker = "old";
    const mutableTheme = { ...theme, fg: (_: string, text: string) => `${marker}:${text}` } as Theme;
    const component = renderer()({ role: "custom", customType: "pi-orchestrate-worker-result", content: "fallback", display: true, details: settlement(), timestamp: 1 }, { expanded: false, outputPad: 1 }, mutableTheme)!;
    expect(Bun.stripANSI(component.render(80).join("\n"))).toContain("old:");
    marker = "new";
    component.invalidate();
    const refreshed = Bun.stripANSI(component.render(80).join("\n"));
    expect(refreshed).toContain("new:");
    expect(refreshed).not.toContain("old:");
  });
});

describe("active widget", () => {
  test("renders turn count with the latest message direction", () => {
    const component = new WorkerStatusComponent(snapshot([
      worker("receiving", "running", { usage: { ...usage, turns: 2 }, messageDirection: "from-model" }),
      worker("sending", "running", { usage: { ...usage, turns: 7 }, messageDirection: "to-model" }),
    ]), theme);
    const output = Bun.stripANSI(component.render(80).join("\n"));
    expect(output).toContain("2↓");
    expect(output).toContain("7↑");
    expect(output).not.toMatch(/\d+t\b/);
    component.dispose();
  });

  test("shows active rows only and remains width-safe", () => {
    const component = new WorkerStatusComponent(snapshot([
      worker("run", "running"), worker("start", "starting", { usage: undefined }),
      worker("done", "completed"), worker("ready", "ready"), worker("failed", "failed"),
      worker("aborted", "aborted"),
    ]), theme);
    const output = Bun.stripANSI(component.render(80).join("\n"));
    expect(output).toContain("Task run");
    expect(output).toContain("Task start");
    expect(output).not.toContain("Task done");
    expect(output).not.toContain("Task ready");
    expect(output).not.toContain("Task failed");
    expect(output).not.toContain("Task aborted");
    expect(component.render(32).every((line) => visibleWidth(line) <= 32)).toBe(true);
    component.dispose();
  });

  test("caps active rows", () => {
    const workers = Array.from({ length: 10 }, (_, index) => worker(String(index), "running"));
    const component = new WorkerStatusComponent(snapshot(workers), theme);
    expect(component.render(80)).toHaveLength(MAX_WIDGET_WORKERS + 2);
    component.dispose();
  });

  test("disposes the animation timer", async () => {
    let requests = 0;
    const component = new WorkerStatusComponent(snapshot([worker("run", "running")]), theme, { requestRender: () => { requests += 1; } });
    await Bun.sleep(155);
    expect(requests).toBeGreaterThan(0);
    component.dispose();
    const stopped = requests;
    await Bun.sleep(155);
    expect(requests).toBe(stopped);
  });

  test("footer contains interactive-ready facts only", () => {
    expect(formatFooterStatus(snapshot([worker("run", "running"), worker("ready", "ready")]))).toBe("1 interactive ready");
    expect(formatFooterStatus(snapshot([worker("run", "running")]))).toBeUndefined();
  });
});

type StateListener = (snapshot: OwnerSnapshot) => void;

class WorkerStateHarness implements WorkerStateSource {
  listeners = new Map<string, Set<StateListener>>();
  initialSnapshots = new Map<string, OwnerSnapshot>();
  subscribeCalls = 0;
  unsubscribeCalls = 0;

  subscribeState(ownerSessionId: string, listener: StateListener): () => void {
    this.subscribeCalls += 1;
    const listeners = this.listeners.get(ownerSessionId) ?? new Set<StateListener>();
    listeners.add(listener);
    this.listeners.set(ownerSessionId, listeners);
    listener(this.initialSnapshots.get(ownerSessionId) ?? snapshot([]));
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.unsubscribeCalls += 1;
      listeners.delete(listener);
      if (listeners.size === 0) this.listeners.delete(ownerSessionId);
    };
  }

  emit(value: OwnerSnapshot, ownerSessionId = "owner"): void {
    for (const listener of this.listeners.get(ownerSessionId) ?? []) listener(value);
  }

  listenerCount(): number {
    return [...this.listeners.values()].reduce((total, listeners) => total + listeners.size, 0);
  }
}

test("controller restores the synchronous initial owner snapshot and applies updates in order", () => {
  const runtime = new WorkerStateHarness();
  const statuses: unknown[] = [];
  const context = { mode: "non-interactive", ui: {
    setStatus(_key: string, value: unknown) { statuses.push(value); },
    setWidget() { throw new Error("non-TUI must not install widgets"); },
  } } as unknown as ExtensionContext;
  runtime.initialSnapshots.set("new", snapshot([worker("retained", "ready")]));
  const controller = new StatusController(runtime);
  controller.bind("new", context);
  expect(runtime.listenerCount()).toBe(1);
  expect(statuses.at(-1)).toBe("1 interactive ready");

  runtime.emit(snapshot([worker("stale", "ready")]), "old");
  runtime.emit(snapshot([]), "new");
  runtime.emit(snapshot([worker("latest", "ready")]), "new");
  runtime.emit(snapshot([]), "new");

  expect(statuses.slice(-3)).toEqual([undefined, "1 interactive ready", undefined]);
  controller.dispose();
  controller.dispose();
  expect(runtime.listenerCount()).toBe(0);
});

test("controller rebind synchronously replaces an active widget with retained ready status", () => {
  const runtime = new WorkerStateHarness();
  runtime.initialSnapshots.set("active-owner", snapshot([worker("active", "running")]));
  runtime.initialSnapshots.set("ready-owner", snapshot([worker("ready", "ready")]));
  let installed: WorkerStatusComponent | undefined;
  let installs = 0;
  let clears = 0;
  const statuses: unknown[] = [];
  const ctx = { mode: "tui", ui: {
    setStatus(_key: string, value: unknown) { statuses.push(value); },
    setWidget(_key: string, value: unknown) {
      installed?.dispose();
      installed = undefined;
      if (typeof value === "function") {
        installed = value({ requestRender() {} }, theme);
        installs += 1;
      } else {
        clears += 1;
      }
    },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);

  controller.bind("active-owner", ctx);
  expect(installs).toBe(1);
  expect(installed).toBeDefined();
  controller.bind("ready-owner", ctx);

  expect(runtime.listeners.has("active-owner")).toBe(false);
  expect(runtime.listeners.has("ready-owner")).toBe(true);
  expect(runtime.unsubscribeCalls).toBe(1);
  expect(clears).toBe(1);
  expect(installed).toBeUndefined();
  expect(statuses.at(-1)).toBe("1 interactive ready");
  controller.dispose();
});

test("controller bind, unbind, and rebind subscriptions are isolated", () => {
  const runtime = new WorkerStateHarness();
  const statusValues: unknown[] = [];
  const ctx = { mode: "non-interactive", ui: {
    setStatus(_key: string, value: unknown) { statusValues.push(value); },
    setWidget() { throw new Error("non-TUI must not install widgets"); },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("owner", ctx);
  expect(runtime.listenerCount()).toBe(1);
  controller.unbind("different");
  expect(runtime.listenerCount()).toBe(1);
  controller.unbind("owner");
  expect(runtime.listenerCount()).toBe(0);
  controller.bind("owner", ctx);
  expect(runtime.listenerCount()).toBe(1);
  expect(runtime.subscribeCalls).toBe(2);
  expect(runtime.unsubscribeCalls).toBe(1);
  controller.dispose();
  controller.dispose();
  expect(runtime.listenerCount()).toBe(0);
  expect(runtime.unsubscribeCalls).toBe(2);
  expect(statusValues.at(-1)).toBeUndefined();
});

test("lets Pi dispose installed widgets exactly once", () => {
  const runtime = new WorkerStateHarness();
  let installed: WorkerStatusComponent | undefined;
  let disposals = 0;
  const ctx = { mode: "tui", ui: {
    setStatus() {},
    setWidget(_key: string, value: unknown) {
      if (typeof value === "function") {
        const component: WorkerStatusComponent = value({ requestRender() {} }, theme);
        const original = component.dispose.bind(component);
        component.dispose = () => { disposals += 1; original(); };
        installed = component;
      } else if (installed !== undefined) {
        installed.dispose();
        installed = undefined;
      }
    },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("owner", ctx);
  runtime.emit(snapshot([worker("active", "running")]));
  controller.dispose();
  controller.dispose();
  expect(disposals).toBe(1);
});

test("controller updates one widget instance, removes terminal rows, and clears at zero", () => {
  const runtime = new WorkerStateHarness();
  let installed: WorkerStatusComponent | undefined;
  let installs = 0;
  let clears = 0;
  const statuses: unknown[] = [];
  const ctx = { mode: "tui", ui: {
    setStatus(key: string, value: unknown) { expect(key).toBe(ORCHESTRATION_PRESENTATION_KEY); statuses.push(value); },
    setWidget(key: string, value: unknown) {
      expect(key).toBe(ORCHESTRATION_PRESENTATION_KEY);
      if (typeof value === "function") {
        installed = value({ requestRender() {} }, theme);
        installs += 1;
      } else {
        installed?.dispose();
        installed = undefined;
        clears += 1;
      }
    },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("owner", ctx);
  runtime.emit(snapshot([worker("a", "running"), worker("b", "running")]));
  expect(installs).toBe(1);
  const component = installed;
  expect(component).toBeDefined();
  if (component === undefined) throw new Error("Expected synchronous widget installation");
  runtime.emit(snapshot([worker("a", "completed"), worker("b", "running")]));
  expect(installs).toBe(1);
  expect(Bun.stripANSI(component.render(80).join("\n"))).not.toContain("Task a");
  runtime.emit(snapshot([worker("a", "completed"), worker("b", "ready")]));
  expect(clears).toBe(1);
  expect(installed).toBeUndefined();
  expect(statuses.at(-1)).toBe("1 interactive ready");
  controller.dispose();
  expect(clears).toBe(1);
});
