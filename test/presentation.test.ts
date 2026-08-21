import { beforeAll, describe, expect, test } from "bun:test";
import { initTheme, type ExtensionAPI, type ExtensionContext, type MessageRenderer, type Theme } from "@earendil-works/pi-coding-agent";
import { Result, Schema } from "effect";
import { visibleWidth, type Component } from "@earendil-works/pi-tui";
import type { RunId, WorkerId, WorkerRecord, WorkerStatus } from "../extension/domain.ts";
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
import {
  WorkerSettlementDetails,
  decodePersistedWorkerSettlementDetails,
} from "../extension/worker-settlement.ts";

beforeAll(() => initTheme("dark", false));
const theme = { fg: (_: string, text: string) => text, bg: (_: string, text: string) => text, bold: (text: string) => text, italic: (text: string) => text, underline: (text: string) => text, inverse: (text: string) => text, strikethrough: (text: string) => text } as Theme;
const usage = { input: 1200, output: 345, cacheRead: 12, cacheWrite: 3, cost: 0.0123, contextTokens: 12345, turns: 2 };

function worker(id: string, status: WorkerStatus, overrides: Partial<WorkerRecord> = {}): WorkerRecord {
  return { id: id as WorkerId, worker: "scout", ownerSessionId: "owner", runId: "run" as RunId, title: `Task ${id}`, instructions: "Do it", lifecycle: status === "ready" ? "interactive" : "one-shot", status, usage, messageDirection: status === "starting" ? "to-model" : "from-model", startedAt: Date.now() - 78_000, ...overrides };
}
function snapshot(workers: readonly WorkerRecord[]): RuntimeSnapshot {
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
  return renderer()({ role: "custom", customType: "pi-orchestrate-worker-result", content, display: true, details, timestamp: 1 }, { expanded }, theme)!.render(width);
}

describe("per-worker result messages", () => {
  test.each([
    ["completed", "✓ Inspect code · scout · 5s"],
    ["ready", "✓ Inspect code · scout · interactive ready · 5s"],
    ["failed", "✗ Inspect code · scout · failed · 5s"],
    ["aborted", "■ Inspect code · scout · aborted · 5s"],
  ] as const)("renders truthful %s styling", (status, heading) => {
    const output = Bun.stripANSI(renderResult(settlement(status), false, 80).join("\n"));
    expect(output).toContain(heading);
    expect(output).toContain("to expand");
    if (status === "failed" || status === "aborted") expect(output).not.toContain("✓");
  });

  test("italicizes the worker type after the result title", () => {
    const italicTheme = {
      ...theme,
      italic: (text: string) => `<italic>${text}</italic>`,
    } as Theme;
    const component = renderer()(
      {
        role: "custom",
        customType: "pi-orchestrate-worker-result",
        content: "fallback",
        display: true,
        details: settlement(),
        timestamp: 1,
      },
      { expanded: false },
      italicTheme,
    )!;

    const output = Bun.stripANSI(component.render(80).join("\n"));
    expect(output).toContain("✓ Inspect code · <italic>scout</italic> · 5s");
  });

  test("expanded output reconstructs full response and adjacent metadata", () => {
    const text = `# Full response\n\n${"detail ".repeat(200)}TAIL`;
    const output = Bun.stripANSI(renderResult(settlement("completed", text), true, 50, "capped").join("\n"));
    expect(output).toContain("TAIL");
    expect(output).toContain("worker ID worker-1 · run ID run-1");
    expect(output).toContain("status completed · generation 2");
    expect(output).toContain("turns 2 · current context 12.3k");
    expect(output).toContain("session /sessions/worker.jsonl");
  });

  test("removes a redundant completion heading from the worker response", () => {
    const output = Bun.stripANSI(renderResult(
      settlement("completed", "## Completed\n\nChanged the worker bootstrap."),
      true,
      80,
    ).join("\n"));

    expect(output).toContain("✓ Inspect code · scout · 5s");
    expect(output).toContain("Changed the worker bootstrap.");
    expect(output).not.toContain("Completed");
  });

  test("caps collapsed content by rendered visual height and safely falls back", () => {
    const long = "word ".repeat(1000);
    const collapsed = renderResult(settlement("completed", long), false, 32);
    expect(collapsed.length).toBeLessThanOrEqual(MAX_RESULT_PREVIEW_LINES + 7);
    const malformedDetails = JSON.parse(JSON.stringify({ bad: true }));
    const malformed = Bun.stripANSI(renderResult(malformedDetails, false, 32, long).join("\n"));
    expect(malformed).toContain("Worker result");
    expect(malformed).toContain("to expand");
  });

  test("expanded malformed fallback never silently omits content", () => {
    const content = Array.from({ length: 150 }, (_, index) => `fallback line ${index}`).join("\n");
    const details = JSON.parse(JSON.stringify({ bad: true }));
    const output = Bun.stripANSI(renderResult(details, true, 40, content).join("\n"));
    expect(output).toContain("fallback line 0");
    expect(output).toContain("fallback line 149");
  });

  test("rejects contradictory and malformed details without optimistic success", () => {
    for (const details of [
      { ...settlement(), mode: "background" },
      { ...settlement(), generation: 1.5 },
      { ...settlement(), usage: { ...usage, turns: Number.NaN } },
      { ...settlement(), status: "completed", outcome: { status: "failed", message: "no" } },
      { ...settlement(), startedAt: 6201 },
      { ...settlement(), failureStage: "workflow" },
      { ...settlement(), usage: { ...usage, cost: Number.POSITIVE_INFINITY } },
      { ...settlement(), usage: { ...usage, cacheRead: -1 } },
    ]) {
      const output = Bun.stripANSI(renderResult(details, false, 80).join("\n"));
      expect(output).toContain("details unavailable");
      expect(output).not.toContain("✓ Inspect code · scout");
    }
  });

  test("round-trips every persisted outcome and usage field through the canonical schema", () => {
    const outcomes = [
      { status: "completed", assistantText: "Complete." },
      { status: "ready", assistantText: "Ready." },
      { status: "failed", message: "Failed." },
      { status: "aborted" },
    ] as const;

    for (const outcome of outcomes) {
      const lifecycle: "interactive" | "one-shot" =
        outcome.status === "ready" ? "interactive" : "one-shot";
      const { sessionFile: _sessionFile, ...withoutSessionFile } = settlement(
        outcome.status,
      );
      const input = {
        ...withoutSessionFile,
        lifecycle,
        outcome,
        usage,
        ...(outcome.status === "failed"
          ? {
              synthesisGroupId: "synthesis-1",
              synthesisGroupSize: 2,
              failureStage: "workflow" as const,
            }
          : {}),
      };
      const decoded = decodePersistedWorkerSettlementDetails(input);
      expect(Result.isSuccess(decoded)).toBe(true);
      if (!Result.isSuccess(decoded)) throw new Error("Expected settlement to decode");
      const encoded = Schema.encodeSync(WorkerSettlementDetails)(decoded.success);
      const persisted = JSON.parse(JSON.stringify(encoded));
      expect(persisted).toEqual(input);
      expect(persisted).not.toHaveProperty("sessionFile");
      if (outcome.status === "failed" || outcome.status === "aborted") {
        expect(persisted.outcome).not.toHaveProperty("assistantText");
      }
      if (outcome.status === "aborted") {
        expect(persisted.outcome).not.toHaveProperty("message");
      }
      const roundTrip = decodePersistedWorkerSettlementDetails(persisted);
      expect(Result.isSuccess(roundTrip)).toBe(true);
      if (Result.isSuccess(roundTrip)) expect(roundTrip.success).toEqual(decoded.success);
    }
  });

  test("rejects every invalid persisted usage field and missing required field", () => {
    for (const field of Object.keys(usage)) {
      for (const invalid of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
        expect(Result.isFailure(decodePersistedWorkerSettlementDetails({
          ...settlement(),
          usage: { ...usage, [field]: invalid },
        }))).toBe(true);
      }
    }

    for (const field of Object.keys(settlement())) {
      if (field === "sessionFile") continue;
      const missing = { ...settlement() };
      Reflect.deleteProperty(missing, field);
      expect(Result.isFailure(decodePersistedWorkerSettlementDetails(missing))).toBe(true);
    }
  });

  test("keeps the canonical current fields through persisted decoding", () => {
    const current = {
      ...settlement(),
      synthesisGroupId: "synthesis-1",
      synthesisGroupSize: 3,
      currentExtra: "ignored",
    };
    const decoded = decodePersistedWorkerSettlementDetails(current);

    expect(Result.isSuccess(decoded)).toBe(true);
    if (!Result.isSuccess(decoded)) throw new Error("Expected current settlement to decode");
    expect(decoded.success).toMatchObject({
      eventId: "event",
      sequence: 1,
      synthesisGroupId: "synthesis-1",
      synthesisGroupSize: 3,
      sessionFile: "/sessions/worker.jsonl",
    });
    expect(decoded.success).not.toHaveProperty("currentExtra");

    const missingCurrentField = { ...current };
    Reflect.deleteProperty(missingCurrentField, "eventId");
    expect(Result.isFailure(
      Schema.decodeUnknownResult(WorkerSettlementDetails)(missingCurrentField),
    )).toBe(true);
  });

  test("accepts only runtime-emittable lifecycle, grouping, and ordinal combinations", () => {
    const validSettlements = [
      settlement("completed"),
      settlement("ready"),
      {
        ...settlement("completed"),
        synthesisGroupId: "synthesis-1",
        synthesisGroupSize: 2,
      },
    ];
    for (const details of validSettlements) {
      expect(Result.isSuccess(decodePersistedWorkerSettlementDetails(details))).toBe(true);
    }

    const invalidSettlements = [
      { ...settlement("ready"), lifecycle: "one-shot" },
      { ...settlement("completed"), lifecycle: "interactive" },
      { ...settlement(), synthesisGroupId: "synthesis-1" },
      { ...settlement(), synthesisGroupSize: 2 },
      {
        ...settlement(),
        synthesisGroupId: "synthesis-1",
        synthesisGroupSize: 1,
      },
      { ...settlement(), sequence: 0 },
      { ...settlement(), generation: 0 },
    ];
    for (const details of invalidSettlements) {
      expect(Result.isFailure(decodePersistedWorkerSettlementDetails(details))).toBe(true);
      const output = Bun.stripANSI(
        renderResult(details, true, 80, "raw boundary fallback").join("\n"),
      );
      expect(output).toContain("details unavailable");
      expect(output).toContain("raw boundary fallback");
    }
  });

  test("decodes only direct current settlements and falls back with full raw content", () => {
    const withoutSession = settlement();
    Reflect.deleteProperty(withoutSession, "sessionFile");
    expect(Result.isSuccess(decodePersistedWorkerSettlementDetails(withoutSession))).toBe(true);
    expect(Result.isFailure(decodePersistedWorkerSettlementDetails({
      settlement: withoutSession,
    }))).toBe(true);
    expect(Result.isFailure(decodePersistedWorkerSettlementDetails({
      ...withoutSession,
      sessionFile: undefined,
    }))).toBe(true);

    const content = "raw fallback line 1\nraw fallback line 2";
    const output = Bun.stripANSI(renderResult({ settlement: withoutSession }, true, 80, content).join("\n"));
    expect(output).toContain("details unavailable");
    expect(output).toContain("raw fallback line 1");
    expect(output).toContain("raw fallback line 2");
  });

  test("uses explicit startup failure stage and keeps ordinary zero-turn failures truthful", () => {
    const { sessionFile: _sessionFile, ...failedSettlement } = settlement("failed");
    const ordinary = { ...failedSettlement, usage: { ...usage, turns: 0 } };
    expect(Bun.stripANSI(renderResult(ordinary, false, 80).join("\n"))).toContain("✗ Inspect code · scout · failed · 5s");
    expect(Bun.stripANSI(renderResult({ ...ordinary, failureStage: "startup" }, false, 80).join("\n"))).toContain("✗ Inspect code · scout · could not start · 5s");
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
    expect(Bun.stripANSI(component.render(80).join("\n"))).toContain(
      "old:✓ Inspect code · old:scout · old:5s",
    );
    marker = "new";
    component.invalidate();
    const refreshed = Bun.stripANSI(component.render(80).join("\n"));
    expect(refreshed).toContain("new:✓ Inspect code · new:scout · new:5s");
    expect(refreshed).not.toContain("old:✓ Inspect code");
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
      expect(row).toContain("2↓");
      expect(row).toContain("12k ctx");
      expect(row).toContain("A very");
      if (width >= 72) expect(row).toContain(" · scout · 2↓");
      else expect(row).not.toContain(" · scout · 2↓");
      expect(row).not.toContain("searching");
    }
    component.dispose();
  });

  test("italicizes the worker type between the title and turn count when space allows", () => {
    const italicTheme = {
      ...theme,
      italic: (text: string) => `<italic>${text}</italic>`,
    } as Theme;
    const component = new WorkerStatusComponent(
      snapshot([worker("run", "running", { worker: "investigator" })]),
      italicTheme,
    );

    const wide = Bun.stripANSI(component.render(100)[1]!);
    const narrow = Bun.stripANSI(component.render(60)[1]!);
    expect(wide).toContain("Task run · <italic>investigator</italic> · 2↓ · 12k ctx");
    expect(narrow).not.toContain("investigator");
    component.dispose();
  });

  test("omits low-value activity labels from running rows", () => {
    const component = new WorkerStatusComponent(snapshot([worker("run", "running", { activity: "bash" })]), theme);
    const row = Bun.stripANSI(component.render(80)[1]!);
    expect(row).not.toContain("working");
    expect(row).not.toContain("running command");
    expect(row).toContain("2↓");
    expect(row).toContain("12k ctx");
    component.dispose();
  });

  test.each([
    [49_499, "49k ctx"],
    [49_501, "50k ctx"],
    [192_300, "192k ctx"],
  ])("rounds %i context tokens to a whole-thousand label", (contextTokens, expected) => {
    const component = new WorkerStatusComponent(snapshot([
      worker("context", "running", { usage: { ...usage, contextTokens } }),
    ]), theme);
    const row = Bun.stripANSI(component.render(80)[1]!);
    expect(row).toContain(expected);
    expect(row).not.toMatch(/\d+\.\d+k ctx/);
    component.dispose();
  });

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

  test("uses distinct motion shapes for lifecycle states", () => {
    const component = new WorkerStatusComponent(snapshot([
      worker("start", "starting"),
      worker("run", "running"),
      worker("stop", "stopping"),
    ]), theme);
    const glyphs = component.render(80).slice(1).map((line) => Bun.stripANSI(line).slice(0, 1));
    expect(new Set(glyphs).size).toBe(3);
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
      expect(output).toContain("2↓");
      expect(output).toContain("Task start");
      expect(output).toContain("0↑");
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

  test("footer contains interactive-ready facts only", () => {
    expect(formatFooterStatus(snapshot([worker("run", "running"), worker("ready", "ready")]))).toBe("1 interactive ready");
    expect(formatFooterStatus(snapshot([worker("run", "running")]))).toBeUndefined();
  });
});

type StateListener = (snapshot: RuntimeSnapshot) => void;

class RuntimeHarness implements PresentationRuntime {
  listeners = new Map<string, Set<StateListener>>();
  initialSnapshots = new Map<string, RuntimeSnapshot>();
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

  emit(value: RuntimeSnapshot, ownerSessionId = "owner"): void {
    for (const listener of this.listeners.get(ownerSessionId) ?? []) listener(value);
  }

  listenerCount(): number {
    return [...this.listeners.values()].reduce((total, listeners) => total + listeners.size, 0);
  }
}

test("controller restores the synchronous initial owner snapshot and applies updates in order", () => {
  const runtime = new RuntimeHarness();
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
  const runtime = new RuntimeHarness();
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
  const runtime = new RuntimeHarness();
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
  const runtime = new RuntimeHarness();
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
  const runtime = new RuntimeHarness();
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
