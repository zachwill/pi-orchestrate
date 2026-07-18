import { beforeAll, describe, expect, test } from "bun:test";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
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
  type RunId,
  type WorkerCatalog,
  type WorkerDefinition,
  type WorkerId,
  type WorkerUsage,
} from "../extension/domain.js";
import type {
  AbortTarget,
  AcceptedRun,
  CompletedRun,
  OrchestrationContext,
  OrchestratorRuntime,
  RuntimeSnapshot,
  SettlementListener,
  WorkerSettlement,
} from "../extension/runtime.js";
import {
  registerOrchestrationTools,
  type DispatchDecision,
  type OrchestrationToolDependencies,
} from "../extension/tools.js";

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

class FakeRuntime {
  readonly orchestrateCalls: Array<{
    context: OrchestrationContext;
    task: { worker: string; title: string; instructions: string };
    mode: DispatchMode;
    signal?: AbortSignal;
  }> = [];
  settlementToEmit: WorkerSettlement | undefined;
  readonly sendCalls: Array<{
    context: OrchestrationContext;
    workerId: WorkerId;
    instructions: string;
    mode: DispatchMode;
    signal?: AbortSignal;
  }> = [];
  readonly abortCalls: Array<{ ownerSessionId: string; target: AbortTarget }> = [];
  readonly closeCalls: Array<{ ownerSessionId: string; workerId: WorkerId }> = [];
  readonly snapshotCalls: string[] = [];

  acceptedRun: AcceptedRun = {
    id: "run-accepted" as RunId,
    workerId: "worker-accepted" as WorkerId,
  };
  completedRun: CompletedRun = completedRun();
  snapshotResult: RuntimeSnapshot = snapshot();
  failures: Partial<
    Record<"orchestrate" | "send" | "abort" | "close" | "snapshot", Error>
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

  async send(
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: DispatchMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    if (signal?.aborted) throw signal.reason;
    this.sendCalls.push({ context, workerId, instructions, mode, signal });
    if (this.failures.send) throw this.failures.send;
    if (mode === "inline" && this.settlementToEmit) onSettlement?.(this.settlementToEmit);
    return mode === "async" ? this.acceptedRun : this.completedRun;
  }

  async abort(ownerSessionId: string, target: AbortTarget): Promise<void> {
    this.abortCalls.push({ ownerSessionId, target });
    if (this.failures.abort) throw this.failures.abort;
  }

  async close(ownerSessionId: string, workerId: WorkerId): Promise<void> {
    this.closeCalls.push({ ownerSessionId, workerId });
    if (this.failures.close) throw this.failures.close;
  }

  async snapshot(ownerSessionId: string): Promise<RuntimeSnapshot> {
    this.snapshotCalls.push(ownerSessionId);
    if (this.failures.snapshot) throw this.failures.snapshot;
    return this.snapshotResult;
  }
}

interface Harness {
  readonly pi: FakePi;
  readonly runtime: FakeRuntime;
  readonly context: ExtensionContext;
  readonly catalog: WorkerCatalog;
  readonly catalogCalls: ExtensionContext[];
  readonly dispatchCalls: string[];
  readonly modes: Map<string, DispatchMode>;
  readonly synthesisGroups: Map<string, DispatchDecision["synthesisGroup"]>;
}

function harness(): Harness {
  const pi = new FakePi();
  const runtime = new FakeRuntime();
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
    runtime: runtime as unknown as OrchestratorRuntime,
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

function snapshot(): RuntimeSnapshot {
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
        lifecycle: "reusable",
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
      "orchestration_status",
      "worker_send",
      "worker_abort",
      "worker_close",
    ]);
    for (const tool of pi.tools) {
      expect(tool.renderCall).toBeFunction();
      expect(tool.renderResult).toBeFunction();
    }

    expect(pi.tool("orchestrate").executionMode).toBe("parallel");

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

    expect(Value.Check(pi.tool("orchestration_status").parameters, {})).toBe(true);
    expect(Value.Check(pi.tool("orchestration_status").parameters, { poll: true })).toBe(false);

    expect(
      Value.Check(pi.tool("worker_send").parameters, {
        worker_id: "worker-1",
        instructions: "Continue.",
      }),
    ).toBe(true);
    const abortSchema = pi.tool("worker_abort").parameters;
    expect(Value.Check(abortSchema, { worker_ids: ["worker-1"] })).toBe(true);
    expect(Value.Check(abortSchema, { all: true })).toBe(true);
    expect(Value.Check(abortSchema, {})).toBe(false);
    expect(Value.Check(abortSchema, { worker_ids: [] })).toBe(false);
    expect(Value.Check(abortSchema, { all: false })).toBe(false);
    expect(Value.Check(abortSchema, { worker_ids: ["worker-1"], all: true })).toBe(false);

    expect(Value.Check(pi.tool("worker_close").parameters, { worker_id: "worker-1" })).toBe(
      true,
    );
  });

  test("uses concise, nonduplicated prompt guidance with the required semantics", () => {
    const { pi } = harness();
    const bullets = pi.tools.flatMap((tool) => tool.promptGuidelines ?? []);

    expect(new Set(bullets).size).toBe(bullets.length);
    expect(pi.tool("orchestrate").description).toContain("one fully briefed task");
    expect(pi.tool("orchestrate").description).toContain("sibling orchestrate calls");
    expect(pi.tool("orchestrate").description).toContain("asynchronously");
    expect(pi.tool("orchestrate").promptSnippet).toContain("one fully briefed worker task");
    expect(pi.tool("orchestrate").promptGuidelines?.[0]).toContain("one complete brief per call");
    expect(pi.tool("orchestrate").promptGuidelines?.[1]).toContain(
      "dispatch every currently known independent task",
    );
    expect(pi.tool("orchestrate").promptGuidelines?.[1]).toContain(
      "never wait for one sibling's acceptance or completion",
    );
    expect(pi.tool("orchestration_status").description).toContain("Never poll");
    expect(pi.tool("orchestration_status").promptGuidelines?.[0]).toContain("never poll");
    expect(pi.tool("worker_send").promptGuidelines?.[0]).toContain("ready reusable");
    expect(pi.tool("worker_abort").description).toContain("active");
    expect(pi.tool("worker_abort").promptGuidelines?.[0]).toContain("worker_close");
    expect(pi.tool("worker_close").description).toContain("ready reusable");
  });

  test("constructs the complete runtime context and selects async mode by tool call ID", async () => {
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

  test("worker_send validates its branded boundary and follows mode termination semantics", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("send-async", "async");
    modes.set("send-inline", "inline");
    const controller = new AbortController();

    const asyncResult = await invoke(
      pi,
      "worker_send",
      "send-async",
      { worker_id: "worker-ready", instructions: "Continue." },
      context,
      controller.signal,
    );
    const inlineResult = await invoke(
      pi,
      "worker_send",
      "send-inline",
      { worker_id: "worker-ready", instructions: "Finish." },
      context,
      controller.signal,
    );

    expect(runtime.sendCalls.map(({ workerId, instructions, mode, signal }) => ({
      workerId,
      instructions,
      mode,
      signal,
    }))).toEqual([
      {
        workerId: "worker-ready" as WorkerId,
        instructions: "Continue.",
        mode: "async",
        signal: controller.signal,
      },
      {
        workerId: "worker-ready" as WorkerId,
        instructions: "Finish.",
        mode: "inline",
        signal: controller.signal,
      },
    ]);
    expect(runtime.sendCalls[0]?.context.ownerSessionId).toBe("owner-session");
    expect(asyncResult.terminate).toBe(true);
    expect(inlineResult).not.toHaveProperty("terminate");

    await expect(
      invoke(
        pi,
        "worker_send",
        "send-blank",
        { worker_id: "   ", instructions: "Continue." },
        context,
      ),
    ).rejects.toThrow("worker_id must not be blank");
    expect(runtime.sendCalls).toHaveLength(2);
  });

  test("rejects already-aborted async worker_send admission with the exact reason", async () => {
    const { pi, runtime, context, modes } = harness();
    modes.set("send-aborted", "async");
    const reason = new Error("parent turn ended before admission");
    const controller = new AbortController();
    controller.abort(reason);

    const rejectedReason = await invoke(
      pi,
      "worker_send",
      "send-aborted",
      { worker_id: "worker-ready", instructions: "Continue." },
      context,
      controller.signal,
    ).then(
      () => "unexpected success",
      (error: unknown) => error,
    );

    expect(rejectedReason).toBe(reason);
    expect(runtime.sendCalls).toHaveLength(0);
  });

  test("status forwards only the current owner and returns catalog diagnostics plus state", async () => {
    const { pi, runtime, context, catalogCalls } = harness();

    const result = await invoke(pi, "orchestration_status", "status-call", {}, context);

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
      lifecycle: "reusable",
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

  test("maps each exclusive abort target and forwards owner isolation", async () => {
    const { pi, runtime, context } = harness();

    await invoke(
      pi,
      "worker_abort",
      "abort-workers",
      { worker_ids: ["worker-1", "worker-2"] },
      context,
    );
    await invoke(pi, "worker_abort", "abort-all", { all: true }, context);

    expect(runtime.abortCalls).toEqual([
      { ownerSessionId: "owner-session", target: { workerIds: ["worker-1" as WorkerId, "worker-2" as WorkerId] } },
      { ownerSessionId: "owner-session", target: { all: true } },
    ]);
    await expect(
      invoke(pi, "worker_abort", "abort-blank-worker", { worker_ids: [" "] }, context),
    ).rejects.toThrow("worker_id must not be blank");
    await expect(
      invoke(
        pi,
        "worker_abort",
        "abort-ambiguous",
        { worker_ids: ["worker-1"], all: true },
        context,
      ),
    ).rejects.toThrow("exactly one target");
    await expect(
      invoke(pi, "worker_abort", "abort-empty", { worker_ids: [] }, context),
    ).rejects.toThrow("at least one worker ID");
    expect(runtime.abortCalls).toHaveLength(2);
  });

  test("closes an owner-scoped ready reusable worker", async () => {
    const { pi, runtime, context } = harness();

    const result = await invoke(
      pi,
      "worker_close",
      "close-call",
      { worker_id: "worker-ready" },
      context,
    );

    expect(runtime.closeCalls).toEqual([
      { ownerSessionId: "owner-session", workerId: "worker-ready" as WorkerId },
    ]);
    expect(result.details).toEqual({ worker_id: "worker-ready" });
    expect(result).not.toHaveProperty("terminate");

    await expect(
      invoke(pi, "worker_close", "close-blank", { worker_id: "\t" }, context),
    ).rejects.toThrow("worker_id must not be blank");
    expect(runtime.closeCalls).toHaveLength(1);
  });

  test("renders incomplete streaming tool arguments without crashing", () => {
    const { pi } = harness();
    const renderContext = (expanded: boolean) => ({ expanded, argsComplete: false, cwd: "/workspace", state: {}, invalidate() {} }) as never;

    const partialOrchestrate = pi.tool("orchestrate").renderCall!(
      { worker: "scout" } as never,
      themeForRendering(),
      renderContext(false),
    ).render(40);
    expect(partialOrchestrate.every((line) => Bun.stringWidth(line) <= 40)).toBe(true);
    expect(Bun.stripANSI(partialOrchestrate.join("\n"))).toContain("orchestrate scout");

    const missingTask = pi.tool("orchestrate").renderCall!(
      {} as never,
      themeForRendering(),
      renderContext(true),
    ).render(40);
    expect(Bun.stripANSI(missingTask.join("\n"))).toContain("orchestrate");

    const partialSend = pi.tool("worker_send").renderCall!(
      { worker_id: "worker-ready" } as never,
      themeForRendering(),
      renderContext(false),
    ).render(40);
    expect(partialSend.every((line) => Bun.stringWidth(line) <= 40)).toBe(true);
    expect(Bun.stripANSI(partialSend.join("\n"))).toContain("worker_send worker-ready");
  });

  test("renders and stores one exact expandable outbound message with a safe bounded preview", async () => {
    const { pi, runtime, context } = harness();
    const instructions = `  First  exact\tline.\r\n\r\nUnicode 雪 \u001b[31mred\u0000\n${"UNBROKEN".repeat(12_500)}\nTAIL  `;
    const task = { worker: "scout", title: "Inspect", instructions };
    const renderContext = (expanded: boolean) => ({ expanded, argsComplete: true, cwd: "/workspace", state: {}, invalidate() {} }) as never;
    const collapsed = pi.tool("orchestrate").renderCall!(task, themeForRendering(), renderContext(false)).render(32);
    expect(collapsed.every((line) => Bun.stringWidth(line) <= 32)).toBe(true);
    const collapsedText = Bun.stripANSI(collapsed.join("\n"));
    expect(collapsedText).toContain("orchestrate scout");
    expect(collapsedText).toContain("Inspect");
    expect(collapsedText).toContain("First exact line.");
    expect(collapsedText).toContain("…");
    expect(collapsedText).toContain("to inspect full instructions");
    const expanded = Bun.stripANSI(pi.tool("orchestrate").renderCall!(task, themeForRendering(), renderContext(true)).render(120_000).join("\n"));
    expect(expanded).toContain("  First  exact    line.");
    expect(expanded).toContain("Unicode 雪 ␛[31mred␀");
    expect(expanded).toContain("UNBROKEN".repeat(12_500));
    expect(expanded).toContain("TAIL  ");

    const sendExpanded = Bun.stripANSI(pi.tool("worker_send").renderCall!({ worker_id: "worker-1", instructions } as never, themeForRendering(), renderContext(true)).render(120_000).join("\n"));
    expect(sendExpanded).toContain("UNBROKEN".repeat(12_500));
    expect(sendExpanded).toContain("TAIL  ");

    await invoke(pi, "orchestrate", "exact-storage", task, context);
    expect(runtime.orchestrateCalls.at(-1)?.task).toEqual(task);
    await invoke(pi, "worker_send", "exact-send", { worker_id: "worker-1", instructions }, context);
    expect(runtime.sendCalls.at(-1)?.instructions).toBe(instructions);
  });

  test("renders malformed inline details neutrally rather than as success", () => {
    const { pi } = harness();
    const result = { content: [{ type: "text", text: "completed" }], details: { result: { worker: "scout", title: "Bad", status: "completed", outcome: { status: "failed", message: "no" } } } } as AgentToolResult<unknown>;
    const rendered = pi.tool("orchestrate").renderResult!(result, { isPartial: false, expanded: false }, themeForRendering(), { lastComponent: undefined } as never);
    const output = Bun.stripANSI(rendered.render(80).join("\n"));
    expect(output).toContain("details unavailable");
    expect(output).not.toContain("✓");
  });

  test("publishes and renders the current inline settlement update", async () => {
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
    const partial = updates[0] as AgentToolResult<unknown>;
    const rendered = pi.tool("orchestrate").renderResult!(partial, { isPartial: true, expanded: false }, themeForRendering(), { lastComponent: undefined } as never);
    const output = Bun.stripANSI(rendered.render(80).join("\n"));
    expect(output).toContain("✓ Inspect · scout · 0s");
    expect(output).toContain("Live complete response.");
    expect(output).toContain("Receiving worker response");
  });

  test("renders concrete neutral diagnostics", async () => {
    const { pi, runtime, context } = harness();
    runtime.snapshotResult = { runs: [], workers: [] };
    const result = await invoke(pi, "orchestration_status", "status-render", {}, context);
    const rendered = pi.tool("orchestration_status").renderResult!(result, { isPartial: false, expanded: false }, themeForRendering(), {} as never);
    expect(Bun.stripANSI(rendered.render(80).join("\n"))).toContain("No active workers");
    expect(Bun.stripANSI(rendered.render(80).join("\n"))).not.toContain("state ready");
  });

  test("throws execution failures instead of returning fake error results", async () => {
    for (const name of [
      "orchestrate",
      "orchestration_status",
      "worker_send",
      "worker_abort",
      "worker_close",
    ] as const) {
      const { pi, runtime, context } = harness();
      const runtimeMethod =
        name === "orchestration_status" ? "snapshot" : name.replace("worker_", "");
      const error = new Error(`${name} failed`);
      runtime.failures[runtimeMethod as keyof FakeRuntime["failures"]] = error;
      const params = {
        orchestrate: { worker: "scout", title: "Inspect", instructions: "Inspect." },
        orchestration_status: {},
        worker_send: { worker_id: "worker-ready", instructions: "Continue." },
        worker_abort: { all: true },
        worker_close: { worker_id: "worker-ready" },
      }[name];

      await expect(invoke(pi, name, `${name}-call`, params, context)).rejects.toBe(error);
    }
  });
});
