import { beforeAll, describe, expect, test } from "bun:test";
import { initTheme, type ExtensionAPI, type ExtensionContext, type MessageRenderer, type Theme } from "@earendil-works/pi-coding-agent";
import { visibleWidth, type Component } from "@earendil-works/pi-tui";
import type { WorkerRecord, WorkerStatus } from "../extension/domain.ts";
import type { RuntimeSnapshot } from "../extension/runtime.ts";
import {
  MAX_RESULT_PREVIEW_LINES,
  MAX_WIDGET_WORKERS,
  ORCHESTRATION_PRESENTATION_KEY,
  StatusController,
  WorkerStatusComponent,
  formatFooterStatus,
  registerOrchestrationPresentation,
  type PresentationRuntime,
} from "../extension/presentation.ts";

beforeAll(() => initTheme("dark", false));
const theme = { fg: (_: string, text: string) => text, bg: (_: string, text: string) => text, bold: (text: string) => text, italic: (text: string) => text, underline: (text: string) => text, inverse: (text: string) => text, strikethrough: (text: string) => text } as Theme;
const usage = { input: 1200, output: 345, cacheRead: 12, cacheWrite: 3, cost: 0.0123, contextTokens: 12345, turns: 2 };

function worker(id: string, status: WorkerStatus, overrides: Partial<WorkerRecord> = {}): WorkerRecord {
  return { id, worker: "scout", ownerSessionId: "owner", waveId: "wave", title: `Task ${id}`, instructions: "Do it", lifecycle: status === "ready" ? "reusable" : "one-shot", status, usage, startedAt: Date.now() - 78_000, ...overrides } as WorkerRecord;
}
function snapshot(workers: readonly WorkerRecord[]): RuntimeSnapshot {
  return { workers, waves: [{ id: "wave", ownerSessionId: "owner", workerIds: workers.map((item) => item.id), mode: "async", state: "running", createdAt: Date.now() - 78_000 }] } as RuntimeSnapshot;
}
function settlement(status: "completed" | "ready" | "failed" | "aborted" = "completed", text = "A useful worker response.") {
  return {
    eventId: "event", sequence: 1, ownerSessionId: "owner", waveId: "wave", workerId: "worker-1", generation: 2,
    mode: "async", worker: "scout", title: "Inspect code", lifecycle: status === "ready" ? "reusable" : "one-shot", status,
    outcome: status === "completed" || status === "ready" ? { status, assistantText: text } : { status, message: text, assistantText: "Partial evidence." },
    usage, startedAt: 1000, settledAt: 6200, remainingActive: 0, waveComplete: true, sessionFile: "/sessions/worker.jsonl",
  };
}
function renderer(): MessageRenderer {
  let result: MessageRenderer | undefined;
  registerOrchestrationPresentation({ registerMessageRenderer(type: string, candidate: MessageRenderer) { expect(type).toBe("pi-orchestrate-worker-result"); result = candidate; } } as unknown as ExtensionAPI);
  return result!;
}
function renderResult(details: unknown, expanded: boolean, width: number, content = "fallback content"): string[] {
  return renderer()({ role: "custom", customType: "pi-orchestrate-worker-result", content, display: true, details, timestamp: 1 }, { expanded }, theme)!.render(width);
}

describe("per-worker result messages", () => {
  test.each([
    ["completed", "✓ completed"], ["ready", "✓ response complete"], ["failed", "✗ failed"], ["aborted", "■ aborted"],
  ] as const)("renders truthful %s styling", (status, heading) => {
    const output = Bun.stripANSI(renderResult(settlement(status), false, 80).join("\n"));
    expect(output).toContain(heading);
    expect(output).toContain("scout · Inspect code · 5s");
    expect(output).toContain("to expand");
    if (status === "failed" || status === "aborted") expect(output).not.toContain("✓");
  });

  test("expanded output reconstructs full response and adjacent metadata", () => {
    const text = `# Full response\n\n${"detail ".repeat(200)}TAIL`;
    const output = Bun.stripANSI(renderResult(settlement("completed", text), true, 50, "capped").join("\n"));
    expect(output).toContain("TAIL");
    expect(output).toContain("worker ID worker-1 · wave ID wave");
    expect(output).toContain("status completed · generation 2");
    expect(output).toContain("turns 2 · current context 12.3k");
    expect(output).toContain("session /sessions/worker.jsonl");
  });

  test("caps collapsed content by rendered visual height and safely falls back", () => {
    const long = "word ".repeat(1000);
    const collapsed = renderResult(settlement("completed", long), false, 32);
    expect(collapsed.length).toBeLessThanOrEqual(MAX_RESULT_PREVIEW_LINES + 7);
    const malformed = Bun.stripANSI(renderResult({ bad: true }, false, 32, long).join("\n"));
    expect(malformed).toContain("Worker result");
    expect(malformed).toContain("to expand");
  });

  test("expanded malformed fallback never silently omits content", () => {
    const content = Array.from({ length: 150 }, (_, index) => `fallback line ${index}`).join("\n");
    const output = Bun.stripANSI(renderResult({ bad: true }, true, 40, content).join("\n"));
    expect(output).toContain("fallback line 0");
    expect(output).toContain("fallback line 149");
  });

  test("rejects contradictory and malformed details without optimistic success", () => {
    for (const details of [
      { ...settlement(), mode: "background" },
      { ...settlement(), generation: 1.5 },
      { ...settlement(), usage: { ...usage, turns: Number.NaN } },
      { ...settlement(), status: "completed", outcome: { status: "failed", message: "no" } },
    ]) {
      const output = Bun.stripANSI(renderResult(details, false, 80).join("\n"));
      expect(output).toContain("details unavailable");
      expect(output).not.toContain("✓ completed");
    }
  });

  test("uses explicit startup failure stage and keeps ordinary zero-turn failures truthful", () => {
    const ordinary = { ...settlement("failed"), usage: { ...usage, turns: 0 }, sessionFile: undefined };
    expect(Bun.stripANSI(renderResult(ordinary, false, 80).join("\n"))).toContain("✗ failed");
    expect(Bun.stripANSI(renderResult({ ...ordinary, failureStage: "startup" }, false, 80).join("\n"))).toContain("✗ could not start");
  });

  test("keeps every line width-safe down to one column", () => {
    const details = settlement("completed", "x".repeat(100_000));
    for (const width of [120, 80, 50, 32, 3, 2, 1]) for (const expanded of [false, true]) {
      expect(renderResult(details, expanded, width).every((line) => visibleWidth(line) <= width)).toBe(true);
    }
  });

  test("rebuilds themed worker-result children on invalidation", () => {
    let marker = "old";
    const mutableTheme = { ...theme, fg: (_: string, text: string) => `${marker}:${text}` } as Theme;
    const component = renderer()({ role: "custom", customType: "pi-orchestrate-worker-result", content: "fallback", display: true, details: settlement(), timestamp: 1 }, { expanded: false }, mutableTheme)!;
    expect(Bun.stripANSI(component.render(80).join("\n"))).toContain("old:✓ completed");
    marker = "new";
    component.invalidate();
    const refreshed = Bun.stripANSI(component.render(80).join("\n"));
    expect(refreshed).toContain("new:✓ completed");
    expect(refreshed).not.toContain("old:✓ completed");
  });
});

describe("active widget", () => {
  test("reserves exact high-priority fields before long-title truncation at every width", () => {
    const component = new WorkerStatusComponent(snapshot([worker("long", "running", {
      title: "A very long worker title that must be truncated only after suffixes are reserved",
      activity: "grep",
    })]), theme);
    for (const width of [120, 80, 50, 42, 32]) {
      const row = Bun.stripANSI(component.render(width)[1]!);
      expect(visibleWidth(row)).toBeLessThanOrEqual(width);
      expect(row).toContain("2t");
      expect(row).toContain("12.3k ctx");
      expect(row).toContain("A very");
      if (width >= 72) expect(row).toContain("scout →");
      else expect(row).not.toContain("scout →");
      if (width >= 42) expect(row).toContain("searching");
      else expect(row).not.toContain("searching");
    }
    component.dispose();
  });

  test("shows active rows only with stable adaptive usage", () => {
    const component = new WorkerStatusComponent(snapshot([
      worker("run", "running", { activity: "grep" }), worker("start", "starting", { usage: undefined }),
      worker("done", "completed"), worker("ready", "ready"), worker("failed", "failed"),
    ]), theme);
    for (const width of [120, 80, 50, 32]) {
      const output = Bun.stripANSI(component.render(width).join("\n"));
      expect(output).toContain("Workers · 2 active · 1m 18s");
      expect(output).toContain("Task run");
      expect(output).toContain("2t");
      expect(output).toContain("Task start");
      expect(output).toContain("0t");
      expect(output).not.toContain("Task done");
      expect(output).not.toContain("Task ready");
      expect(component.render(width).every((line) => visibleWidth(line) <= width)).toBe(true);
    }
    component.dispose();
  });

  test("limits rows and reports active overflow", () => {
    const workers = Array.from({ length: 10 }, (_, index) => worker(String(index), "running"));
    const lines = new WorkerStatusComponent(snapshot(workers), theme).render(80).map(Bun.stripANSI);
    expect(lines).toHaveLength(MAX_WIDGET_WORKERS + 2);
    expect(lines.at(-1)).toBe("… 2 more active");
  });

  test("animates only the glyph and disposes its timer", async () => {
    let requests = 0;
    const component = new WorkerStatusComponent(snapshot([worker("run", "running")]), theme, { requestRender: () => { requests += 1; } });
    const before = Bun.stripANSI(component.render(80)[1]!);
    await Bun.sleep(155);
    const after = Bun.stripANSI(component.render(80)[1]!);
    expect(requests).toBeGreaterThan(0);
    expect(after.slice(2)).toBe(before.slice(2));
    component.dispose();
    const stopped = requests;
    await Bun.sleep(155);
    expect(requests).toBe(stopped);
  });

  test("footer contains reusable facts only", () => {
    expect(formatFooterStatus(snapshot([worker("run", "running"), worker("ready", "ready")]))).toBe("1 available for follow-up");
    expect(formatFooterStatus(snapshot([worker("run", "running")]))).toBeUndefined();
  });
});

class RuntimeHarness implements PresentationRuntime {
  listeners = new Set<(owner: string) => void>();
  value = snapshot([]);
  snapshot(): Promise<RuntimeSnapshot> { return Promise.resolve(this.value); }
  subscribeState(listener: (owner: string) => void): () => void { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; }
  emit(): void { for (const listener of this.listeners) listener("owner"); }
}

test("controller ignores stale owner and out-of-order same-owner snapshots", async () => {
  let listeners = new Set<(owner: string) => void>();
  const requests: Array<{ owner: string; resolve: (value: RuntimeSnapshot) => void }> = [];
  const runtime: PresentationRuntime = {
    snapshot(owner) { return new Promise((resolve) => requests.push({ owner, resolve })); },
    subscribeState(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
  };
  const statuses: unknown[] = [];
  const context = { mode: "non-interactive", ui: {
    setStatus(_key: string, value: unknown) { statuses.push(value); },
    setWidget() { throw new Error("non-TUI must not install widgets"); },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("old", context);
  controller.bind("new", context);
  expect(listeners.size).toBe(1);
  requests[0]!.resolve(snapshot([worker("stale", "ready")]));
  await Promise.resolve(); await Promise.resolve();
  expect(statuses).not.toContain("1 available for follow-up");
  requests[1]!.resolve(snapshot([]));
  await Promise.resolve(); await Promise.resolve();
  const listener = [...listeners][0]!;
  listener("new"); listener("new");
  requests[3]!.resolve(snapshot([worker("latest", "ready")]));
  requests[2]!.resolve(snapshot([]));
  await Promise.resolve(); await Promise.resolve();
  expect(statuses.at(-1)).toBe("1 available for follow-up");
  controller.dispose();
  controller.dispose();
  expect(listeners.size).toBe(0);
});

test("controller bind, unbind, and rebind subscriptions are isolated", async () => {
  const runtime = new RuntimeHarness();
  const statusValues: unknown[] = [];
  const ctx = { mode: "non-interactive", ui: {
    setStatus(_key: string, value: unknown) { statusValues.push(value); },
    setWidget() { throw new Error("non-TUI must not install widgets"); },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("owner", ctx);
  await Promise.resolve(); await Promise.resolve();
  expect(runtime.listeners.size).toBe(1);
  controller.unbind("different");
  expect(runtime.listeners.size).toBe(1);
  controller.unbind("owner");
  expect(runtime.listeners.size).toBe(0);
  controller.bind("owner", ctx);
  expect(runtime.listeners.size).toBe(1);
  controller.dispose();
  controller.dispose();
  expect(runtime.listeners.size).toBe(0);
  expect(statusValues.at(-1)).toBeUndefined();
});

test("lets Pi dispose installed widgets exactly once", async () => {
  const runtime = new RuntimeHarness();
  runtime.value = snapshot([worker("active", "running")]);
  let installed: Component | undefined;
  let disposals = 0;
  const ctx = { mode: "tui", ui: {
    setStatus() {},
    setWidget(_key: string, value: unknown) {
      if (typeof value === "function") {
        installed = (value as (tui: unknown, theme: Theme) => Component)({ requestRender() {} }, theme);
        const original = (installed as WorkerStatusComponent).dispose.bind(installed);
        (installed as WorkerStatusComponent).dispose = () => { disposals += 1; original(); };
      } else if (installed) {
        (installed as WorkerStatusComponent).dispose();
        installed = undefined;
      }
    },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("owner", ctx);
  await Promise.resolve(); await Promise.resolve();
  runtime.value = snapshot([]);
  runtime.emit();
  await Promise.resolve(); await Promise.resolve();
  controller.dispose();
  expect(disposals).toBe(1);
});

test("controller updates one widget instance, removes terminal rows, and clears at zero", async () => {
  const runtime = new RuntimeHarness();
  runtime.value = snapshot([worker("a", "running"), worker("b", "running")]);
  const widgets: unknown[] = [];
  const statuses: unknown[] = [];
  const ctx = { mode: "tui", ui: {
    setStatus(key: string, value: unknown) { expect(key).toBe(ORCHESTRATION_PRESENTATION_KEY); statuses.push(value); },
    setWidget(key: string, value: unknown) { expect(key).toBe(ORCHESTRATION_PRESENTATION_KEY); widgets.push(value); },
  } } as unknown as ExtensionContext;
  const controller = new StatusController(runtime);
  controller.bind("owner", ctx);
  await Promise.resolve(); await Promise.resolve();
  expect(widgets).toHaveLength(1);
  const component = (widgets[0] as (tui: unknown, theme: Theme) => Component)({ requestRender() {} }, theme);
  runtime.value = snapshot([worker("a", "completed"), worker("b", "running")]);
  runtime.emit(); await Promise.resolve(); await Promise.resolve();
  expect(widgets).toHaveLength(1);
  expect(Bun.stripANSI(component.render(80).join("\n"))).not.toContain("Task a");
  runtime.value = snapshot([worker("a", "completed"), worker("b", "ready")]);
  runtime.emit(); await Promise.resolve(); await Promise.resolve();
  expect(widgets.at(-1)).toBeUndefined();
  expect(statuses.at(-1)).toBe("1 available for follow-up");
  controller.dispose();
});
