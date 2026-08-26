import { beforeAll, describe, expect, test } from "bun:test";
import type { Api, Model } from "@earendil-works/pi-ai";
import {
  getAgentDir,
  initTheme,
  type ExtensionAPI,
  type ExtensionContext,
  type ModelRegistry,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import type { TSchema } from "typebox";
import * as Value from "typebox/value";
import {
  createWorkerCatalog,
  type WorkerCatalog,
  type WorkerDefinition,
} from "../../extension/catalog/definition.js";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  type RunId,
  type WorkerId,
  type WorkerUsage,
} from "../../extension/orchestration/model.js";
import {
  OrchestrationActionRejected,
  type AbortTarget,
  type OrchestrationContext,
} from "../../extension/orchestration/admission.js";
import type {
  AcceptedRun,
  CompletedRun,
  OwnerSnapshot,
  SettlementListener,
} from "../../extension/orchestration/service.js";
import type { OrchestrationClient } from "../../extension/parent/process-host.js";
import {
  registerOrchestrationTools,
  type OrchestrationToolDependencies,
} from "../../extension/pi/tools.js";
import type { DispatchDecision } from "../../extension/parent/dispatch-policy.js";
import type { WorkerSettlement } from "../../extension/orchestration/settlement.js";

beforeAll(() => initTheme("dark", false));

type RegisteredTool = ToolDefinition<TSchema, unknown>;
type DispatchMode = "async" | "inline";

class FakePi {
  readonly tools: RegisteredTool[] = [];

  registerTool(tool: RegisteredTool): void {
    this.tools.push(tool);
  }

  tool(name: string): RegisteredTool {
    const tool = this.tools.find((candidate) => candidate.name === name);
    if (!tool) throw new Error(`Missing registered tool: ${name}`);
    return tool;
  }
}

class FakeOrchestrationClient {
  readonly orchestrateCalls: Array<{
    context: OrchestrationContext;
    task: { worker: string; title: string; instructions: string };
    mode: DispatchMode;
    signal?: AbortSignal;
  }> = [];
  settlementToEmit: WorkerSettlement | undefined;
  readonly interactiveSendCalls: Array<{
    context: OrchestrationContext;
    workerId: string;
    instructions: string;
    mode: DispatchMode;
    signal?: AbortSignal;
  }> = [];
  readonly abortCalls: Array<{ ownerSessionId: string; target: AbortTarget }> = [];
  readonly interactiveCloseCalls: Array<{ ownerSessionId: string; workerId: string }> = [];
  readonly snapshotCalls: string[] = [];

  acceptedRun: AcceptedRun = {
    id: "run-accepted" as RunId,
    workerId: "worker-accepted" as WorkerId,
  };
  completedRun: CompletedRun = completedRun();
  snapshotResult: OwnerSnapshot = snapshot();
  failures: Partial<
    Record<"orchestrate" | "sendInteractive" | "abort" | "closeInteractive" | "snapshot", Error>
  > = {};

  async orchestrate(
    context: OrchestrationContext,
    task: { worker: string; title: string; instructions: string },
    mode: DispatchMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    if (signal?.aborted) throw signal.reason;
    this.orchestrateCalls.push({ context, task, mode, signal });
    if (this.failures.orchestrate) throw this.failures.orchestrate;
    if (mode === "inline" && this.settlementToEmit) onSettlement?.(this.settlementToEmit);
    return mode === "async" ? this.acceptedRun : this.completedRun;
  }

  async sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: DispatchMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    if (signal?.aborted) throw signal.reason;
    this.interactiveSendCalls.push({ context, workerId, instructions, mode, signal });
    if (this.failures.sendInteractive) throw this.failures.sendInteractive;
    if (mode === "inline" && this.settlementToEmit) onSettlement?.(this.settlementToEmit);
    return mode === "async" ? this.acceptedRun : this.completedRun;
  }

  async abort(ownerSessionId: string, target: AbortTarget): Promise<void> {
    this.abortCalls.push({ ownerSessionId, target });
    if (this.failures.abort) throw this.failures.abort;
  }

  async closeInteractive(ownerSessionId: string, workerId: string): Promise<void> {
    this.interactiveCloseCalls.push({ ownerSessionId, workerId });
    if (this.failures.closeInteractive) throw this.failures.closeInteractive;
  }

  async snapshot(ownerSessionId: string): Promise<OwnerSnapshot> {
    this.snapshotCalls.push(ownerSessionId);
    if (this.failures.snapshot) throw this.failures.snapshot;
    return this.snapshotResult;
  }
}

interface Harness {
  readonly pi: FakePi;
  readonly runtime: FakeOrchestrationClient;
  readonly context: ExtensionContext;
  readonly catalog: WorkerCatalog;
  readonly catalogCalls: ExtensionContext[];
  readonly dispatchCalls: string[];
  readonly modes: Map<string, DispatchMode>;
  readonly synthesisGroups: Map<string, DispatchDecision["synthesisGroup"]>;
}

function harness(): Harness {
  const pi = new FakePi();
  const runtime = new FakeOrchestrationClient();
  const catalog = createWorkerCatalog([definition("scout")], [
    {
      severity: "warning",
      source: "project",
      filePath: "/project/.pi/pi-orchestrate/workers/bad.md",
      message: "ignored invalid worker",
    },
  ]);
  const context = extensionContext();
  const catalogCalls: ExtensionContext[] = [];
  const dispatchCalls: string[] = [];
  const modes = new Map<string, DispatchMode>();
  const synthesisGroups = new Map<string, DispatchDecision["synthesisGroup"]>();
  const deps: OrchestrationToolDependencies = {
    orchestration: runtime as unknown as OrchestrationClient,
    getCatalog(ctx) {
      catalogCalls.push(ctx);
      return catalog;
    },
    getDispatchDecision(toolCallId) {
      dispatchCalls.push(toolCallId);
      const synthesisGroup = synthesisGroups.get(toolCallId);
      return {
        mode: modes.get(toolCallId) ?? "async",
        ...(synthesisGroup ? { synthesisGroup } : {}),
      };
    },
  };

  registerOrchestrationTools(pi as unknown as ExtensionAPI, deps);
  return {
    pi,
    runtime,
    context,
    catalog,
    catalogCalls,
    dispatchCalls,
    modes,
    synthesisGroups,
  };
}

async function invoke(
  pi: FakePi,
  name: string,
  toolCallId: string,
  params: unknown,
  ctx: ExtensionContext,
  signal?: AbortSignal,
  onUpdate?: (result: unknown) => void,
) {
  return pi.tool(name).execute(
    toolCallId,
    params as never,
    signal,
    onUpdate as never,
    ctx,
  );
}

function extensionContext(overrides: {
  ownerSessionId?: string;
  sessionFile?: string;
  cwd?: string;
  trusted?: boolean;
} = {}): ExtensionContext {
  const modelRegistry = { marker: "registry" } as unknown as ModelRegistry;
  const parentModel = model("parent-provider", "parent-model");
  return {
    cwd: overrides.cwd ?? "/workspace",
    sessionManager: {
      getSessionId: () => overrides.ownerSessionId ?? "owner-session",
      getSessionFile: () => overrides.sessionFile ?? "/sessions/parent.jsonl",
    },
    modelRegistry,
    model: parentModel,
    isProjectTrusted: () => overrides.trusted ?? true,
  } as unknown as ExtensionContext;
}

function themeForRendering() {
  return {
    fg: (_color: string, text: string) => text,
    bg: (_color: string, text: string) => text,
    bold: (text: string) => text,
    italic: (text: string) => text,
    underline: (text: string) => text,
    inverse: (text: string) => text,
    strikethrough: (text: string) => text,
  } as never;
}

function model(provider: string, id: string): Model<Api> {
  return {
    provider,
    id,
    name: id,
    api: "openai-responses",
    baseUrl: "https://example.test",
    reasoning: false,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 128_000,
    maxTokens: 16_000,
  };
}

function definition(name: string): WorkerDefinition {
  return {
    name,
    source: { kind: "project", filePath: `/workers/${name}.md` },
    description: `${name} description`,
    systemPrompt: `You are ${name}.`,
    lifecycle: "one-shot",
    tools: ["read"],
    skills: ["review"],
    model: { provider: "worker-provider", modelId: "worker-model" },
    thinking: "high",
    compaction: { enabled: true, reserveTokens: 100, keepRecentTokens: 50 },
  };
}

const usage: WorkerUsage = {
  input: 11,
  output: 12,
  cacheRead: 13,
  cacheWrite: 14,
  cost: 0.15,
  contextTokens: 16,
  turns: 2,
};

function completedRun(): CompletedRun {
  return {
    id: "run-inline" as RunId,
    ownerSessionId: "owner-session",
    mode: "inline",
    result: {
      workerId: "worker-inline" as WorkerId,
      worker: "scout",
      title: "Inspect",
      status: "completed",
      outcome: { status: "completed", assistantText: "Inspection complete." },
      usage,
      startedAt: 1_000,
      settledAt: 6_000,
      sessionFile: "/sessions/worker-inline.jsonl",
    },
  };
}

function snapshot(): OwnerSnapshot {
  return {
    runs: [
      {
        id: "run-owned" as RunId,
        ownerSessionId: "owner-session",
        workerId: "worker-owned" as WorkerId,
        mode: "async",
        state: "running",
        createdAt: 123,
      },
    ],
    workers: [
      {
        id: "worker-owned" as WorkerId,
        worker: "scout",
        ownerSessionId: "owner-session",
        runId: "run-owned" as RunId,
        title: "Inspect",
        instructions: "Inspect the runtime.",
        lifecycle: "interactive",
        status: "ready",
        usage,
        outcome: { status: "ready", assistantText: "Ready for follow-up." },
        sessionFile: "/sessions/worker-owned.jsonl",
        startedAt: 123,
      },
    ],
  };
}

describe("registerOrchestrationTools", () => {
  test("registers exactly the five canonical tools and renderers", () => {
    const { pi } = harness();

    expect(pi.tools.map((tool) => tool.name)).toEqual([
      "orchestrate",
      "worker_status",
      "interactive_send",
      "worker_abort",
      "interactive_close",
    ]);
    for (const tool of pi.tools) {
      expect(tool.renderCall).toBeFunction();
      expect(tool.renderResult).toBeFunction();
    }

    expect(pi.tool("orchestrate").executionMode).toBe("parallel");
    const workerStatus = pi.tool("worker_status");
    expect(workerStatus.label).toBe("Worker Status");
    const renderedCall = workerStatus.renderCall!(
      {},
      themeForRendering(),
      { expanded: false } as never,
    );
    expect(Bun.stripANSI(renderedCall.render(80).join("\n")).trimEnd()).toBe("worker_status");
    expect(() => pi.tool("orchestration_status")).toThrow(
      "Missing registered tool: orchestration_status",
    );

  });

  test("schemas accept current inputs and reject malformed or ambiguous inputs", () => {
    const { pi } = harness();
    const validTask = { worker: "scout", title: "Inspect", instructions: "Inspect." };

    expect(Value.Check(pi.tool("orchestrate").parameters, validTask)).toBe(true);
    expect(Value.Check(pi.tool("orchestrate").parameters, [validTask])).toBe(false);
    expect(Value.Check(pi.tool("orchestrate").parameters, { ...validTask, extra: true })).toBe(false);
    expect(Value.Check(pi.tool("orchestrate").parameters, {
      worker: "scout",
      title: "Inspect",
    })).toBe(false);
    for (const field of ["worker", "title", "instructions"] as const) {
      expect(Value.Check(pi.tool("orchestrate").parameters, {
        ...validTask,
        [field]: "   ",
      })).toBe(false);
    }
    for (const field of ["worker", "title"] as const) {
      expect(Value.Check(pi.tool("orchestrate").parameters, {
        ...validTask,
        [field]: "x".repeat(MAX_WORKER_TITLE_LENGTH + 1),
      })).toBe(false);
    }
    expect(Value.Check(pi.tool("orchestrate").parameters, {
      ...validTask,
      instructions: "x".repeat(MAX_WORKER_INSTRUCTIONS_LENGTH + 1),
    })).toBe(false);

    expect(Value.Check(pi.tool("worker_status").parameters, {})).toBe(true);
    expect(Value.Check(pi.tool("worker_status").parameters, { poll: true })).toBe(false);

    const sendSchema = pi.tool("interactive_send").parameters;
    expect(Value.Check(sendSchema, {
      worker_id: "worker-1",
      instructions: "Continue.",
    })).toBe(true);
    for (const workerId of ["run-1", "worker- ", "worker-"]) {
      expect(Value.Check(sendSchema, {
        worker_id: workerId,
        instructions: "Continue.",
      })).toBe(false);
    }
    expect(Value.Check(sendSchema, {
      worker_id: "worker-1",
      instructions: "   ",
    })).toBe(false);
    expect(Value.Check(sendSchema, {
      worker_id: "worker-1",
      instructions: "x".repeat(MAX_WORKER_INSTRUCTIONS_LENGTH + 1),
    })).toBe(false);
    const abortSchema = pi.tool("worker_abort").parameters;
    expect(Value.Check(abortSchema, { worker_ids: ["worker-1"] })).toBe(true);
    expect(Value.Check(abortSchema, { worker_ids: ["run-1"] })).toBe(false);
    expect(Value.Check(abortSchema, { worker_ids: ["worker- "] })).toBe(false);
    expect(Value.Check(abortSchema, { all: true })).toBe(true);
    expect(Value.Check(abortSchema, {})).toBe(false);
    expect(Value.Check(abortSchema, { worker_ids: [] })).toBe(false);
    expect(Value.Check(abortSchema, { all: false })).toBe(false);
    expect(Value.Check(abortSchema, { worker_ids: ["worker-1"], all: true })).toBe(false);

    const closeSchema = pi.tool("interactive_close").parameters;
    expect(Value.Check(closeSchema, { worker_id: "worker-1" })).toBe(true);
    expect(Value.Check(closeSchema, { worker_id: "run-1" })).toBe(false);
    expect(Value.Check(closeSchema, { worker_id: "invalid-placeholder" })).toBe(false);
    expect(Value.Check(closeSchema, { worker_id: "worker- " })).toBe(false);
  });

  test("uses concise, nonduplicated prompt guidance with the required semantics", () => {
    const { pi } = harness();
    const bullets = pi.tools.flatMap((tool) => tool.promptGuidelines ?? []);
    expect(new Set(bullets).size).toBe(bullets.length);

    const guidance = (name: string) => {
      const tool = pi.tool(name);
      expect(tool.description).toBeTruthy();
      expect(tool.promptSnippet).toBeTruthy();
      expect(tool.promptGuidelines?.length).toBeGreaterThan(0);
      return [tool.description, tool.promptSnippet, ...(tool.promptGuidelines ?? [])].join(" ");
    };

    expect(guidance("orchestrate")).toMatch(/fully briefed|fully briefed.*parallel|parallel worker scopes/);
    expect(guidance("orchestrate")).toMatch(/async|asynchronously/);
    expect(guidance("worker_status")).toMatch(/diagnostics or recovery/);
    expect(guidance("worker_status")).toMatch(/never poll/i);
    expect(guidance("interactive_send")).toMatch(/interactive worker.*ready/);
    expect(guidance("interactive_send")).toMatch(/one-shot/);
    expect(guidance("worker_abort")).toMatch(/active.*interactive_close/);
    expect(guidance("interactive_close")).toMatch(/interactive worker.*ready/);
    expect(guidance("interactive_close")).toMatch(/one-shot/);
  });

  test("constructs the complete orchestration context and selects async mode by tool call ID", async () => {
    const {
      pi,
      runtime,
      context,
      catalog,
      catalogCalls,
      dispatchCalls,
      modes,
      synthesisGroups,
    } = harness();
    modes.set("orchestrate-call", "async");
    synthesisGroups.set("orchestrate-call", { id: "synthesis-group", size: 2 });
    const task = { worker: "scout", title: "Inspect", instructions: "Inspect." };
    const controller = new AbortController();

    const result = await invoke(
      pi,
      "orchestrate",
      "orchestrate-call",
      task,
      context,
      controller.signal,
    );

    expect(dispatchCalls).toEqual(["orchestrate-call"]);
    expect(catalogCalls).toEqual([context]);
    expect(runtime.orchestrateCalls).toHaveLength(1);
    expect(runtime.orchestrateCalls[0]).toEqual({
      context: {
        ownerSessionId: "owner-session",
        cwd: "/workspace",
        agentDir: getAgentDir(),
        parentSessionFile: "/sessions/parent.jsonl",
        projectTrusted: true,
        catalog,
        parentModel: context.model,
        modelRegistry: context.modelRegistry,
        synthesisGroup: { id: "synthesis-group", size: 2 },
      },
      task,
      mode: "async",
      signal: controller.signal,
    });
    expect(result.terminate).toBe(true);
    expect(result.details).toEqual({
      mode: "async",
      run_id: runtime.acceptedRun.id,
      worker_id: runtime.acceptedRun.workerId,
    });
    expect(result.content[0]).toMatchObject({ type: "text" });
    expect(result.content[0]?.type === "text" && result.content[0].text).toContain(
      "run-accepted",
    );
  });

  test("rejects already-aborted async orchestrate admission with the exact reason", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("orchestrate-aborted", "async");
    const reason = { kind: "parent-turn-ended" };
    const controller = new AbortController();
    controller.abort(reason);

    const rejectedReason = await invoke(
      pi,
      "orchestrate",
      "orchestrate-aborted",
      { worker: "scout", title: "Inspect", instructions: "Inspect." },
      context,
      controller.signal,
    ).then(
      () => "unexpected success",
      (error: unknown) => error,
    );

    expect(rejectedReason).toBe(reason);
    expect(runtime.orchestrateCalls).toHaveLength(0);
  });

  test("returns one inline result without termination", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("inline-call", "inline");
    const controller = new AbortController();

    const result = await invoke(
      pi,
      "orchestrate",
      "inline-call",
      { worker: "scout", title: "Inspect", instructions: "Inspect." },
      context,
      controller.signal,
    );

    expect(runtime.orchestrateCalls[0]?.mode).toBe("inline");
    expect(runtime.orchestrateCalls[0]?.signal).toBe(controller.signal);
    controller.abort();
    expect(runtime.orchestrateCalls[0]?.signal?.aborted).toBe(true);
    expect(result).not.toHaveProperty("terminate");
    expect(result.details).toMatchObject({
      mode: "inline",
      run_id: runtime.completedRun.id,
      result: {
        worker_id: runtime.completedRun.result.workerId,
        worker: "scout",
        title: "Inspect",
      },
    });
    expect(result.content[0]?.type === "text" && result.content[0].text).toContain(
      "Inspection complete.",
    );
    const rendered = pi.tool("orchestrate").renderResult!(
      result,
      { isPartial: false, expanded: false },
      themeForRendering(),
      { lastComponent: undefined } as never,
    );
    expect(Bun.stripANSI(rendered.render(80).join("\n"))).toContain(
      "✓ Inspect · scout · 5s",
    );
  });

  test("interactive_send forwards plain worker IDs and follows mode termination semantics", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("send-async", "async");
    modes.set("send-inline", "inline");
    const controller = new AbortController();

    const asyncResult = await invoke(
      pi,
      "interactive_send",
      "send-async",
      { worker_id: "worker-ready", instructions: "Continue." },
      context,
      controller.signal,
    );
    const inlineResult = await invoke(
      pi,
      "interactive_send",
      "send-inline",
      { worker_id: "worker-ready", instructions: "Finish." },
      context,
      controller.signal,
    );

    expect(runtime.interactiveSendCalls.map(({ workerId, instructions, mode, signal }) => ({
      workerId,
      instructions,
      mode,
      signal,
    }))).toEqual([
      {
        workerId: "worker-ready",
        instructions: "Continue.",
        mode: "async",
        signal: controller.signal,
      },
      {
        workerId: "worker-ready",
        instructions: "Finish.",
        mode: "inline",
        signal: controller.signal,
      },
    ]);
    expect(runtime.interactiveSendCalls[0]?.context.ownerSessionId).toBe("owner-session");
    expect(asyncResult.terminate).toBe(true);
    expect(inlineResult).not.toHaveProperty("terminate");

    await invoke(
      pi,
      "interactive_send",
      "send-blank",
      { worker_id: "   ", instructions: "Continue." },
      context,
    );
    expect(runtime.interactiveSendCalls.at(-1)?.workerId).toBe("   ");
  });

  test("rejects already-aborted async interactive_send admission with the exact reason", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("send-aborted", "async");
    const reason = new Error("parent turn ended before admission");
    const controller = new AbortController();
    controller.abort(reason);

    const rejectedReason = await invoke(
      pi,
      "interactive_send",
      "send-aborted",
      { worker_id: "worker-ready", instructions: "Continue." },
      context,
      controller.signal,
    ).then(
      () => "unexpected success",
      (error: unknown) => error,
    );

    expect(rejectedReason).toBe(reason);
    expect(runtime.interactiveSendCalls).toHaveLength(0);
  });

  test("worker_status forwards only the current owner and returns catalog diagnostics plus state", async () => {
    const { pi, runtime, context, catalogCalls } = harness();

    const result = await invoke(pi, "worker_status", "status-call", {}, context);

    expect(runtime.snapshotCalls).toEqual(["owner-session"]);
    expect(catalogCalls).toEqual([context]);
    expect(result).not.toHaveProperty("terminate");
    const details = result.details as {
      catalog: { workers: Array<Record<string, unknown>>; diagnostics: unknown[] };
      state: { runs: Array<Record<string, unknown>>; workers: Array<Record<string, unknown>> };
    };
    expect(details.catalog.diagnostics).toHaveLength(1);
    expect(details.catalog.workers[0]).not.toHaveProperty("systemPrompt");
    expect(details.state.workers[0]).toMatchObject({
      worker_id: "worker-owned",
      worker: "scout",
      owner_session_id: "owner-session",
      run_id: "run-owned",
      title: "Inspect",
      lifecycle: "interactive",
      status: "ready",
      usage: expect.anything(),
    });
    expect(details.state.runs[0]).toMatchObject({ run_id: "run-owned" });
    expect(details.state.workers[0]).toHaveProperty("activity");
    expect(details.state.workers[0]).not.toHaveProperty("instructions");
    expect(JSON.stringify(result)).not.toContain("Inspect the runtime.");
    expect(result.content[0]?.type === "text" && result.content[0].text).toContain(
      "ignored invalid worker",
    );
    expect(result.content[0]?.type === "text" && result.content[0].text).toContain(
      "worker-owned",
    );
    expect(result.content[0]?.type === "text" && result.content[0].text).not.toContain(
      "Inspect the runtime.",
    );
  });

  test("normalizes abort field names without enforcing target policy", async () => {
    const { pi, runtime, context } = harness();

    await invoke(
      pi,
      "worker_abort",
      "abort-workers",
      { worker_ids: ["worker-1", "worker-2"] },
      context,
    );
    await invoke(pi, "worker_abort", "abort-all", { all: true }, context);

    await invoke(
      pi,
      "worker_abort",
      "abort-blank-worker",
      { worker_ids: [" "] },
      context,
    );
    await invoke(
      pi,
      "worker_abort",
      "abort-ambiguous",
      { worker_ids: ["worker-1"], all: true },
      context,
    );
    await invoke(pi, "worker_abort", "abort-empty", { worker_ids: [] }, context);

    expect(runtime.abortCalls).toEqual([
      { ownerSessionId: "owner-session", target: { workerIds: ["worker-1", "worker-2"] } },
      { ownerSessionId: "owner-session", target: { all: true } },
      { ownerSessionId: "owner-session", target: { workerIds: [" "] } },
      {
        ownerSessionId: "owner-session",
        target: { workerIds: ["worker-1"], all: true },
      },
      { ownerSessionId: "owner-session", target: { workerIds: [] } },
    ]);
  });

  test("closes an owner-scoped ready interactive worker", async () => {
    const { pi, runtime, context } = harness();

    const result = await invoke(
      pi,
      "interactive_close",
      "close-call",
      { worker_id: "worker-ready" },
      context,
    );

    expect(runtime.interactiveCloseCalls).toEqual([
      { ownerSessionId: "owner-session", workerId: "worker-ready" },
    ]);
    expect(result.details).toEqual({ worker_id: "worker-ready" });
    expect(result).not.toHaveProperty("terminate");

    await invoke(
      pi,
      "interactive_close",
      "close-blank",
      { worker_id: "\t" },
      context,
    );
    expect(runtime.interactiveCloseCalls.at(-1)?.workerId).toBe("\t");
  });

  test("stores exact outbound instructions through both execution adapters", async () => {
    const { pi, runtime, context } = harness();
    const instructions = `  First  exact\tline.\r\n\r\nUnicode 雪 \u001b[31mred\u0000\n${"UNBROKEN".repeat(12_500)}\nTAIL  `;
    const task = { worker: "scout", title: "Inspect", instructions };

    await invoke(pi, "orchestrate", "exact-storage", task, context);
    expect(runtime.orchestrateCalls.at(-1)?.task).toEqual(task);
    await invoke(pi, "interactive_send", "exact-send", { worker_id: "worker-1", instructions }, context);
    expect(runtime.interactiveSendCalls.at(-1)?.instructions).toBe(instructions);
  });

  test("publishes the current inline settlement update", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("inline-partial", "inline");
    runtime.settlementToEmit = {
      eventId: "settlement-inline",
      sequence: 1,
      ownerSessionId: "owner-session",
      runId: "run-inline" as RunId,
      workerId: "worker-inline" as WorkerId,
      generation: 1,
      mode: "inline",
      worker: "scout",
      title: "Inspect",
      lifecycle: "one-shot",
      status: "completed",
      outcome: { status: "completed", assistantText: "Live complete response." },
      usage,
      startedAt: 1,
      settledAt: 2,
      sessionFile: "/sessions/worker-inline.jsonl",
    };
    const updates: unknown[] = [];
    await invoke(pi, "orchestrate", "inline-partial", { worker: "scout", title: "Inspect", instructions: "Inspect." }, context, undefined, (update) => updates.push(update));
    expect(updates).toHaveLength(1);
    expect(updates[0]).toMatchObject({
      details: {
        mode: "inline",
        result: {
          worker_id: "worker-inline",
          outcome: {
            status: "completed",
            assistant_text: "Live complete response.",
          },
        },
      },
    });
  });

  test("propagates typed worker-ID validation rejections from executable actions", async () => {
    const cases = [
      {
        tool: "interactive_send",
        runtimeMethod: "sendInteractive",
        operation: "sendInteractive",
        params: { worker_id: "  ", instructions: "Continue." },
      },
      {
        tool: "worker_abort",
        runtimeMethod: "abort",
        operation: "abort",
        params: { worker_ids: ["\t"] },
      },
      {
        tool: "interactive_close",
        runtimeMethod: "closeInteractive",
        operation: "closeInteractive",
        params: { worker_id: "\n" },
      },
    ] as const;

    for (const expected of cases) {
      const { pi, runtime, context } = harness();
      const rejected = new OrchestrationActionRejected({
        operation: expected.operation,
        reason: "validation",
        message: "worker_id must not be blank",
      });
      runtime.failures[expected.runtimeMethod] = rejected;

      const observed = await invoke(
        pi,
        expected.tool,
        `${expected.tool}-blank`,
        expected.params,
        context,
      ).then(
        () => "unexpected success",
        (error: unknown) => error,
      );

      expect(observed).toBe(rejected);
      expect(observed).toMatchObject({
        operation: expected.operation,
        reason: "validation",
        message: "worker_id must not be blank",
      });
    }
  });

  test("preserves tagged orchestration rejection identity through tool execution", async () => {
    const { pi, runtime, context } = harness();
    const rejected = new OrchestrationActionRejected({
      operation: "orchestrate",
      reason: "unknown-worker",
      message: "Unknown worker: missing",
    });
    runtime.failures.orchestrate = rejected;

    await expect(
      invoke(
        pi,
        "orchestrate",
        "typed-rejection",
        { worker: "missing", title: "Inspect", instructions: "Inspect." },
        context,
      ),
    ).rejects.toBe(rejected);
    expect(rejected.message).toBe("Unknown worker: missing");
  });

  test("throws execution failures instead of returning fake error results", async () => {
    for (const name of [
      "orchestrate",
      "worker_status",
      "interactive_send",
      "worker_abort",
      "interactive_close",
    ] as const) {
      const { pi, runtime, context } = harness();
      const runtimeMethod = {
        orchestrate: "orchestrate",
        worker_status: "snapshot",
        interactive_send: "sendInteractive",
        worker_abort: "abort",
        interactive_close: "closeInteractive",
      }[name];
      const error = new Error(`${name} failed`);
      runtime.failures[runtimeMethod as keyof FakeOrchestrationClient["failures"]] = error;
      const params = {
        orchestrate: { worker: "scout", title: "Inspect", instructions: "Inspect." },
        worker_status: {},
        interactive_send: { worker_id: "worker-ready", instructions: "Continue." },
        worker_abort: { all: true },
        interactive_close: { worker_id: "worker-ready" },
      }[name];

      await expect(invoke(pi, name, `${name}-call`, params, context)).rejects.toBe(error);
    }
  });
});
