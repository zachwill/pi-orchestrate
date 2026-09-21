import { describe, expect, test } from "bun:test";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type {
  ExtensionAPI,
  ExtensionContext,
  ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { DeliveryCoordinator } from "../extension/parent/delivery.ts";
import {
  createWorkerCatalog,
  type WorkerCatalog,
  type WorkerDefinition,
} from "../extension/catalog/definition.ts";
import type { RunId, WorkerId, WorkerRecord } from "../extension/orchestration/model.ts";
import { LIVE_WORKER_CONTEXT_TYPE } from "../extension/parent/worker-context.ts";
import {
  attachProcessHost,
  createProcessHost,
  destroyProcessHost,
  detachProcessHost,
  getProcessHost,
  quitProcessHost,
  type ProcessHost,
} from "../extension/parent/process-host.ts";
import {
  createOrchestrationExtension,
  type OrchestrationExtensionDependencies,
} from "../extension/index.ts";
import type { OrchestrationContext } from "../extension/orchestration/admission.ts";
import type { OwnerSnapshot } from "../extension/orchestration/service.ts";
import type { WorkerSettlement } from "../extension/orchestration/settlement.ts";

type Handler = (event: any, ctx: ExtensionContext) => unknown;

class FakePi {
  readonly handlers = new Map<string, Handler[]>();
  readonly renderers: string[] = [];
  readonly sent: Array<{ message: unknown; options: unknown }> = [];
  private readonly toolsByName = new Map<string, ToolDefinition>();

  get tools(): ToolDefinition[] {
    return [...this.toolsByName.values()];
  }

  readonly sendMessage = (message: unknown, options?: unknown): void => {
    this.sent.push({ message, options });
  };

  on(event: string, handler: Handler): void {
    const handlers = this.handlers.get(event) ?? [];
    handlers.push(handler);
    this.handlers.set(event, handlers);
  }

  registerTool(tool: ToolDefinition): void {
    this.toolsByName.set(tool.name, tool);
  }

  registerMessageRenderer(customType: string): void {
    this.renderers.push(customType);
  }

  tool(name: string): ToolDefinition {
    const tool = this.toolsByName.get(name);
    if (!tool) throw new Error(`Missing tool: ${name}`);
    return tool;
  }

  async emit(event: string, value: Record<string, unknown>, ctx: ExtensionContext): Promise<unknown[]> {
    const results: unknown[] = [];
    for (const handler of this.handlers.get(event) ?? []) {
      results.push(await handler({ type: event, ...value }, ctx));
    }
    return results;
  }
}

class FakeOrchestrationClient {
  readonly orchestrateCalls: Array<{
    context: OrchestrationContext;
    task: unknown;
    mode: string;
  }> = [];
  readonly interactiveSendModes: Array<"async" | "inline"> = [];
  readonly stateListeners = new Map<string, Set<(snapshot: OwnerSnapshot) => void>>();
  readonly snapshots = new Map<string, OwnerSnapshot>();
  shutdownCalls = 0;
  unsubscribeStateCalls = 0;

  async orchestrate(
    context: OrchestrationContext,
    task: unknown,
    mode: "async" | "inline",
    _signal?: AbortSignal,
    onSettlement?: (settlement: WorkerSettlement) => void,
  ): Promise<unknown> {
    this.orchestrateCalls.push({ context, task, mode });
    if (mode === "async") {
      return { id: "run-accepted", workerId: "worker-accepted" };
    }
    onSettlement?.(workerSettlement(context.ownerSessionId, {
      eventId: "inline-result",
      mode: "inline",
    }));
    return inlineCompletedRun(context.ownerSessionId);
  }

  async sendInteractive(
    context: OrchestrationContext,
    _workerId: WorkerId,
    _instructions: string,
    mode: "async" | "inline",
  ): Promise<unknown> {
    this.interactiveSendModes.push(mode);
    return mode === "async"
      ? { id: "run-send", workerId: "worker-ready" }
      : inlineCompletedRun(context.ownerSessionId);
  }

  async abort(): Promise<void> {}
  async closeInteractive(): Promise<void> {}

  async snapshot(ownerSessionId: string): Promise<OwnerSnapshot> {
    return this.snapshots.get(ownerSessionId) ?? { runs: [], workers: [] };
  }

  subscribeState(
    ownerSessionId: string,
    listener: (snapshot: OwnerSnapshot) => void,
  ): () => void {
    const listeners = this.stateListeners.get(ownerSessionId) ?? new Set();
    listeners.add(listener);
    this.stateListeners.set(ownerSessionId, listeners);
    listener(this.snapshots.get(ownerSessionId) ?? { runs: [], workers: [] });
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      listeners.delete(listener);
      if (listeners.size === 0) this.stateListeners.delete(ownerSessionId);
      this.unsubscribeStateCalls += 1;
    };
  }

  async shutdown(): Promise<void> {
    this.shutdownCalls += 1;
  }
}

function definition(name = "scout"): WorkerDefinition {
  return {
    name,
    source: { kind: "project", filePath: `/workers/${name}.md` },
    description: `${name} worker`,
    systemPrompt: `You are ${name}.`,
    lifecycle: "one-shot",
    tools: ["read"],
    skills: [],
  };
}

function inlineCompletedRun(ownerSessionId: string) {
  const settlement = workerSettlement(ownerSessionId, { mode: "inline" });
  return {
    id: settlement.runId,
    ownerSessionId,
    mode: "inline" as const,
    result: {
      workerId: settlement.workerId,
      worker: settlement.worker,
      title: settlement.title,
      status: settlement.status,
      outcome: settlement.outcome,
      usage: settlement.usage,
      startedAt: settlement.startedAt,
      settledAt: settlement.settledAt,
      sessionFile: settlement.sessionFile,
    },
  };
}

function workerSettlement(
  ownerSessionId: string,
  overrides: Partial<WorkerSettlement> = {},
): WorkerSettlement {
  const workerId = overrides.workerId ?? "worker-complete" as WorkerId;
  return {
    eventId: overrides.eventId ?? `${ownerSessionId}:${workerId}:1`,
    sequence: overrides.sequence ?? 1,
    ownerSessionId,
    runId: overrides.runId ?? "run-complete" as RunId,
    workerId,
    generation: overrides.generation ?? 1,
    mode: overrides.mode ?? "async",
    worker: overrides.worker ?? "scout",
    title: overrides.title ?? "Inspect",
    lifecycle: overrides.lifecycle ?? "one-shot",
    status: overrides.status ?? "completed",
    outcome: overrides.outcome ?? {
      status: "completed",
      assistantText: "Inspection complete.",
    },
    usage: overrides.usage ?? {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      cost: 0,
      contextTokens: 2,
      turns: 1,
    },
    startedAt: overrides.startedAt ?? 1,
    settledAt: overrides.settledAt ?? 2,
    sessionFile: overrides.sessionFile ?? "/sessions/worker.jsonl",
  };
}

function createContext(
  ownerSessionId = "owner-session",
  options: { cwd?: string; trusted?: boolean; idle?: boolean } = {},
): { ctx: ExtensionContext; setIdle(idle: boolean): void } {
  let idle = options.idle ?? true;
  const ui = {
    setStatus() {},
    setWidget() {},
  };
  const ctx = {
    cwd: options.cwd ?? "/workspace",
    mode: "tui",
    ui,
    sessionManager: {
      getSessionId: () => ownerSessionId,
      getSessionFile: () => `/sessions/${ownerSessionId}.jsonl`,
    },
    modelRegistry: {},
    model: undefined,
    isIdle: () => idle,
    isProjectTrusted: () => options.trusted ?? true,
  } as unknown as ExtensionContext;
  return { ctx, setIdle: (nextIdle) => (idle = nextIdle) };
}

function fakeHost(runtime = new FakeOrchestrationClient()): { host: ProcessHost; runtime: FakeOrchestrationClient } {
  return {
    host: {
      orchestration: runtime as unknown as ProcessHost["orchestration"],
      delivery: new DeliveryCoordinator(),
    },
    runtime,
  };
}

function install(
  pi: FakePi,
  host: ProcessHost,
  overrides: Partial<OrchestrationExtensionDependencies> = {},
): void {
  createOrchestrationExtension({
    getHost: () => host,
    destroyHost: async () => {},
    discoverCatalog: () => createWorkerCatalog([definition()]),
    ...overrides,
  })(pi as unknown as ExtensionAPI);
}

function assistantToolCalls(
  calls: Array<{ id: string; name: string }>,
  text?: string,
) {
  const toolCalls = calls.map((call) => ({
    type: "toolCall" as const,
    id: call.id,
    name: call.name,
    arguments: {},
  }));
  return {
    role: "assistant",
    content: text === undefined
      ? toolCalls
      : [{ type: "text" as const, text }, ...toolCalls],
  };
}

async function invoke(
  pi: FakePi,
  name: string,
  toolCallId: string,
  params: unknown,
  ctx: ExtensionContext,
) {
  return pi.tool(name).execute(
    toolCallId,
    params as never,
    undefined,
    undefined,
    ctx,
  );
}

const orchestrationParams = {
  worker: "scout",
  title: "Inspect",
  instructions: "Inspect the project.",
};

const publicToolNames = [
  "orchestrate",
  "worker_status",
  "interactive_send",
  "worker_abort",
  "interactive_close",
];

function runningWorker(ownerSessionId: string): WorkerRecord {
  const settlement = workerSettlement(ownerSessionId);
  return {
    id: `worker-${ownerSessionId}` as WorkerId,
    runId: settlement.runId,
    worker: "scout",
    title: `Assignment for ${ownerSessionId}`,
    instructions: "Inspect the assigned files.",
    ownerSessionId,
    lifecycle: "one-shot",
    status: "running",
    startedAt: 1,
    usage: settlement.usage,
  };
}

const staleWorkerContext: AgentMessage = {
  role: "custom", customType: LIVE_WORKER_CONTEXT_TYPE,
  content: "stale worker snapshot", display: false, timestamp: 1,
};

describe("Pi Orchestrate extension integration", () => {
  test("projects current owned workers on each provider request without persisting messages", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    const { ctx } = createContext("owner-a");
    const worker = runningWorker("owner-a");
    runtime.snapshots.set("owner-a", { runs: [], workers: [worker, runningWorker("owner-b")] });
    install(pi, host);
    await pi.emit("session_start", {}, ctx);
    const original: AgentMessage[] = [{ role: "user", content: "Continue after compaction.", timestamp: 1 }];
    const [projected] = await pi.emit("context", { messages: [...original, staleWorkerContext] }, ctx);
    expect(projected).toEqual({ messages: [original[0], expect.objectContaining({
      customType: LIVE_WORKER_CONTEXT_TYPE,
      display: false,
      content: expect.stringContaining("worker-owner-a"),
    })] });
    expect(JSON.stringify(projected)).not.toContain("worker-owner-b");
    expect(JSON.stringify(projected)).not.toContain("stale worker snapshot");
    expect(original).toHaveLength(1);
    expect(pi.sent).toEqual([]);
    runtime.snapshots.set("owner-a", { runs: [], workers: [{ ...worker, status: "completed" }] });
    expect(await pi.emit("context", { messages: original }, ctx)).toEqual([{ messages: original }]);
    await pi.emit("session_shutdown", { reason: "quit" }, ctx);
  });

  for (const reason of ["reload", "resume"] as const) {
    test(`discards a snapshot resolved after ${reason} and binds fresh context to the replacement`, async () => {
      const pi = new FakePi();
      const { host, runtime } = fakeHost();
      const { ctx } = createContext("owner-a");
      install(pi, host);
      await pi.emit("session_start", {}, ctx);
      let resolveSnapshot!: (snapshot: OwnerSnapshot) => void;
      runtime.snapshot = () => new Promise((resolve) => { resolveSnapshot = resolve; });
      const pending = pi.emit("context", { messages: [staleWorkerContext] }, ctx);
      await pi.emit("session_shutdown", { reason }, ctx);
      const replacement = new FakePi();
      const owner = reason === "reload" ? "owner-a" : "owner-b";
      const next = createContext(owner);
      install(replacement, host);
      await replacement.emit("session_start", {}, next.ctx);
      resolveSnapshot({ runs: [], workers: [runningWorker("owner-a")] });
      expect(await pending).toEqual([{ messages: [] }]);
      runtime.snapshot = async () => ({ runs: [], workers: [runningWorker(owner)] });
      expect(await replacement.emit("context", { messages: [] }, next.ctx)).toEqual([
        { messages: [expect.objectContaining({ content: expect.stringContaining(`worker-${owner}`) })] },
      ]);
      expect(await pi.emit("context", { messages: [staleWorkerContext] }, ctx)).toEqual([{ messages: [] }]);
      await replacement.emit("session_shutdown", { reason: "quit" }, next.ctx);
    });
  }

  test("removes stale projections when the snapshot fails or the context belongs to another owner", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    const { ctx } = createContext("owner-a");
    install(pi, host);
    await pi.emit("session_start", {}, ctx);
    runtime.snapshot = async () => { throw new Error("snapshot unavailable"); };
    expect(await pi.emit("context", { messages: [staleWorkerContext] }, ctx)).toEqual([{ messages: [] }]);
    runtime.snapshot = async () => ({ runs: [], workers: [runningWorker("owner-a")] });
    expect(await pi.emit("context", { messages: [] }, createContext("owner-b").ctx)).toEqual([{ messages: [] }]);
    await pi.emit("session_shutdown", { reason: "quit" }, ctx);
  });

  test("a factory that never starts a session acquires no lifecycle resources", async () => {
    const pi = new FakePi();
    const shared = fakeHost();
    let getHostCalls = 0;
    let createControllerCalls = 0;
    let destroyCalls = 0;
    createOrchestrationExtension({
      getHost() {
        getHostCalls += 1;
        return shared.host;
      },
      createStatusController() {
        createControllerCalls += 1;
        throw new Error("status controller must not be created");
      },
      async destroyHost() {
        destroyCalls += 1;
      },
    })(pi as unknown as ExtensionAPI);
    const { ctx } = createContext();

    expect(pi.renderers).toEqual(["pi-orchestrate-worker-result"]);
    expect(pi.tools).toEqual([]);
    await pi.emit("session_shutdown", { reason: "quit" }, ctx);

    expect(getHostCalls).toBe(0);
    expect(createControllerCalls).toBe(0);
    expect(destroyCalls).toBe(0);
  });

  test("registers public tools only after session start", async () => {
    const pi = new FakePi();
    const shared = fakeHost();
    install(pi, shared.host);
    const { ctx } = createContext();

    expect(pi.tools).toEqual([]);
    expect(pi.renderers).toEqual(["pi-orchestrate-worker-result"]);
    expect(shared.runtime.stateListeners.size).toBe(0);

    await pi.emit("session_start", { reason: "startup" }, ctx);

    expect(pi.tools.map((tool) => tool.name)).toEqual(publicToolNames);
    expect(shared.runtime.stateListeners.size).toBe(1);

    await pi.emit("session_start", { reason: "reload" }, ctx);

    expect(pi.tools.map((tool) => tool.name)).toEqual(publicToolNames);
  });

  test("rebinds repeated starts for the same session and replaces registered tools", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    install(pi, host);
    const parent = createContext("owner-repeated");

    await pi.emit("session_start", { reason: "startup" }, parent.ctx);
    const firstOrchestrateTool = pi.tool("orchestrate");
    await pi.emit("session_start", { reason: "reload" }, parent.ctx);

    expect(pi.tool("orchestrate")).not.toBe(firstOrchestrateTool);
    expect(runtime.unsubscribeStateCalls).toBe(1);
    expect(runtime.stateListeners.has("owner-repeated")).toBe(true);
    expect(host.delivery.accept(workerSettlement("owner-repeated"))).toBe(true);
    expect(pi.sent).toHaveLength(1);
    expect(pi.sent[0]).toMatchObject({
      message: { details: { ownerSessionId: "owner-repeated" } },
    });
  });

  test("repeated session shutdown detaches, disposes, and destroys a started session once", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    let destroyCalls = 0;
    install(pi, host, {
      async destroyHost(destroyedHost) {
        expect(destroyedHost).toBe(host);
        destroyCalls += 1;
        await destroyedHost.orchestration.shutdown();
      },
    });
    const { ctx } = createContext("owner-shutdown");

    await pi.emit("session_start", { reason: "startup" }, ctx);
    await pi.emit("session_shutdown", { reason: "quit" }, ctx);
    await pi.emit("session_shutdown", { reason: "quit" }, ctx);

    expect(runtime.unsubscribeStateCalls).toBe(1);
    expect(runtime.stateListeners.size).toBe(0);
    expect(destroyCalls).toBe(1);
    expect(runtime.shutdownCalls).toBe(1);
  });

  test("discovers the trusted parent catalog before each run, appends the contract, and reuses that exact catalog in tools", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    const catalog = createWorkerCatalog([definition("trusted-scout")]);
    const discoveries: unknown[] = [];
    install(pi, host, {
      discoverCatalog(options) {
        discoveries.push(options);
        return catalog;
      },
    });
    const { ctx } = createContext("owner", { cwd: "/trusted/project", trusted: true });

    await pi.emit("session_start", { reason: "startup" }, ctx);
    const [promptResult] = await pi.emit(
      "before_agent_start",
      { prompt: "Inspect", systemPrompt: "Parent prompt", systemPromptOptions: {} },
      ctx,
    );
    await pi.emit(
      "message_end",
      { message: assistantToolCalls([{ id: "dispatch", name: "orchestrate" }]) },
      ctx,
    );
    await invoke(pi, "orchestrate", "dispatch", orchestrationParams, ctx);

    expect(discoveries).toEqual([{ cwd: "/trusted/project", projectTrusted: true }]);
    const injectedPrompt = (promptResult as { systemPrompt: string }).systemPrompt;
    expect(injectedPrompt).not.toBe("Parent prompt");
    expect(injectedPrompt).toContain("trusted-scout");
    expect(runtime.orchestrateCalls[0]?.context.catalog).toBe(catalog);
    expect(runtime.orchestrateCalls[0]?.context.projectTrusted).toBe(true);
  });

  test("propagates a pure dispatch mode from the hook through the registered tool adapter", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    install(pi, host);
    const { ctx } = createContext();
    await pi.emit("session_start", { reason: "startup" }, ctx);
    await pi.emit(
      "message_end",
      { message: assistantToolCalls([{ id: "pure", name: "orchestrate" }]) },
      ctx,
    );

    const result = await invoke(pi, "orchestrate", "pure", orchestrationParams, ctx);

    expect(runtime.orchestrateCalls[0]?.mode).toBe("async");
    expect(result.terminate).toBe(true);
  });

  test("classifies interactive sends and clears their dispatch decision after execution", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    install(pi, host);
    const { ctx } = createContext();
    await pi.emit("session_start", { reason: "startup" }, ctx);

    await pi.emit(
      "message_end",
      { message: assistantToolCalls([{ id: "send", name: "interactive_send" }]) },
      ctx,
    );
    await invoke(pi, "interactive_send", "send", {
      worker_id: "worker-ready",
      instructions: "Continue.",
    }, ctx);
    await pi.emit("tool_execution_end", {
      toolCallId: "send",
      toolName: "interactive_send",
      result: {},
      isError: false,
    }, ctx);
    await invoke(pi, "interactive_send", "send", {
      worker_id: "worker-ready",
      instructions: "Continue again.",
    }, ctx);

    await pi.emit(
      "message_end",
      {
        message: assistantToolCalls([
          { id: "mixed-send", name: "interactive_send" },
          { id: "ordinary", name: "read" },
        ]),
      },
      ctx,
    );
    await invoke(pi, "interactive_send", "mixed-send", {
      worker_id: "worker-ready",
      instructions: "Finish.",
    }, ctx);

    expect(runtime.interactiveSendModes).toEqual([
      "async",
      "inline",
      "inline",
    ]);
  });

  test("keeps a mixed call inline and marks a sibling dispatch group for synthesis", async () => {
    const pi = new FakePi();
    const { host, runtime } = fakeHost();
    install(pi, host);
    const { ctx } = createContext();
    await pi.emit("session_start", { reason: "startup" }, ctx);

    await pi.emit(
      "message_end",
      {
        message: assistantToolCalls([
          { id: "mixed-dispatch", name: "orchestrate" },
          { id: "ordinary", name: "read" },
        ]),
      },
      ctx,
    );
    const inlineResult = await invoke(
      pi,
      "orchestrate",
      "mixed-dispatch",
      orchestrationParams,
      ctx,
    );

    await pi.emit(
      "message_end",
      {
        message: assistantToolCalls([
          { id: "first-dispatch", name: "orchestrate" },
          { id: "second-dispatch", name: "orchestrate" },
        ]),
      },
      ctx,
    );
    const groupedResults = await Promise.all([
      invoke(pi, "orchestrate", "first-dispatch", orchestrationParams, ctx),
      invoke(pi, "orchestrate", "second-dispatch", orchestrationParams, ctx),
    ]);
    const synthesisGroups = runtime.orchestrateCalls
      .slice(-2)
      .map((call) => call.context.synthesisGroup);

    expect(inlineResult).not.toHaveProperty("terminate");
    expect(synthesisGroups[0]).toEqual(synthesisGroups[1]);
    expect(synthesisGroups[0]?.size).toBe(2);
    expect(groupedResults.every((result) => "terminate" in result && result.terminate === true))
      .toBe(true);
  });

  test("finishes an async sibling group when one call fails before admission", async () => {
    const pi = new FakePi();
    const { host } = fakeHost();
    install(pi, host);
    const { ctx } = createContext("owner-group");
    await pi.emit("session_start", { reason: "startup" }, ctx);
    await pi.emit("agent_start", {}, ctx);
    await pi.emit(
      "message_end",
      {
        message: assistantToolCalls([
          { id: "group-valid", name: "orchestrate" },
          { id: "group-invalid", name: "orchestrate" },
        ]),
      },
      ctx,
    );

    const accepted = await invoke(pi, "orchestrate", "group-valid", orchestrationParams, ctx);
    host.delivery.accept(workerSettlement("owner-group", {
      eventId: "group-result",
      sequence: 50,
      synthesisGroupId: "orchestrate:group-valid",
      synthesisGroupSize: 2,
    }));
    await pi.emit("tool_execution_end", {
      toolCallId: "group-valid",
      toolName: "orchestrate",
      result: accepted,
      isError: false,
    }, ctx);
    await pi.emit("tool_execution_end", {
      toolCallId: "group-invalid",
      toolName: "orchestrate",
      result: {},
      isError: true,
    }, ctx);
    await pi.emit("agent_settled", {}, ctx);

    expect(pi.sent).toHaveLength(1);
    expect(pi.sent[0]).toMatchObject({
      options: { triggerTurn: true },
      message: { details: { eventId: "group-result" } },
    });
  });

  test("queues per-worker results for a busy owner and flushes them in settlement order with one synthesis turn", async () => {
    const pi = new FakePi();
    const { host } = fakeHost();
    install(pi, host);
    const parent = createContext("owner-delivery", { idle: false });

    await pi.emit("session_start", { reason: "startup" }, parent.ctx);
    await pi.emit("agent_start", {}, parent.ctx);
    expect(host.delivery.accept(workerSettlement("owner-delivery", {
      eventId: "first",
      sequence: 1,
      workerId: "worker-first" as WorkerId,
      title: "First",
    }))).toBe(true);
    expect(host.delivery.accept(workerSettlement("owner-delivery", {
      eventId: "second",
      sequence: 2,
      workerId: "worker-second" as WorkerId,
      title: "Second",
    }))).toBe(true);
    expect(pi.sent).toEqual([]);

    parent.setIdle(true);
    await pi.emit("agent_settled", {}, parent.ctx);

    expect(pi.sent).toHaveLength(2);
    expect(pi.sent.map(({ options }) => options)).toEqual([
      { triggerTurn: false },
      { triggerTurn: true },
    ]);
    expect(pi.sent.map(({ message }) => message)).toEqual([
      expect.objectContaining({
        customType: "pi-orchestrate-worker-result",
        details: expect.objectContaining({ eventId: "first", workerId: "worker-first" }),
      }),
      expect.objectContaining({
        customType: "pi-orchestrate-worker-result",
        details: expect.objectContaining({ eventId: "second", workerId: "worker-second" }),
      }),
    ]);
  });

  test("keeps two SDK owners bound and quitting one leaves the other host attachment live", async () => {
    const shared = fakeHost();
    const ownerPi = new FakePi();
    const otherPi = new FakePi();
    let destroyCalls = 0;
    const overrides = {
      async destroyHost(host: ProcessHost) {
        destroyCalls += 1;
        await host.orchestration.shutdown();
      },
    };
    install(ownerPi, shared.host, overrides);
    install(otherPi, shared.host, overrides);
    const owner = createContext("owner-a");
    const other = createContext("owner-b");

    await ownerPi.emit("session_start", { reason: "startup" }, owner.ctx);
    await otherPi.emit("session_start", { reason: "startup" }, other.ctx);
    shared.host.delivery.accept(workerSettlement("owner-a", { sequence: 1 }));
    shared.host.delivery.accept(workerSettlement("owner-b", { sequence: 2 }));

    expect(ownerPi.sent).toHaveLength(1);
    expect(otherPi.sent).toHaveLength(1);

    await ownerPi.emit("session_shutdown", { reason: "quit" }, owner.ctx);
    expect(destroyCalls).toBe(0);
    expect(shared.runtime.shutdownCalls).toBe(0);

    await otherPi.emit("agent_settled", {}, other.ctx);
    shared.host.delivery.accept(workerSettlement("owner-b", {
      eventId: "owner-b:second",
      sequence: 3,
    }));
    expect(ownerPi.sent).toHaveLength(1);
    expect(otherPi.sent).toHaveLength(2);

    await otherPi.emit("session_shutdown", { reason: "quit" }, other.ctx);
    expect(destroyCalls).toBe(1);
    expect(shared.runtime.shutdownCalls).toBe(1);
  });

  test("keeps A's pending worker result isolated while B is active and delivers it when A resumes", async () => {
    const shared = fakeHost();
    const firstA = new FakePi();
    const ownerB = new FakePi();
    const resumedA = new FakePi();
    let destroyCalls = 0;
    const overrides = {
      async destroyHost() {
        destroyCalls += 1;
      },
    };
    install(firstA, shared.host, overrides);
    const sessionA = createContext("owner-a");
    await firstA.emit("session_start", { reason: "startup" }, sessionA.ctx);
    await firstA.emit("session_shutdown", { reason: "resume" }, sessionA.ctx);

    install(ownerB, shared.host, overrides);
    const sessionB = createContext("owner-b");
    await ownerB.emit("session_start", { reason: "resume" }, sessionB.ctx);
    shared.host.delivery.accept(workerSettlement("owner-a"));

    expect(firstA.sent).toEqual([]);
    expect(ownerB.sent).toEqual([]);
    expect(shared.host.delivery.pendingCount("owner-a")).toBe(1);

    await ownerB.emit("session_shutdown", { reason: "resume" }, sessionB.ctx);
    install(resumedA, shared.host, overrides);
    const resumedSessionA = createContext("owner-a");
    await resumedA.emit("session_start", { reason: "resume" }, resumedSessionA.ctx);

    expect(firstA.sent).toEqual([]);
    expect(ownerB.sent).toEqual([]);
    expect(resumedA.sent).toHaveLength(1);
    expect(resumedA.sent[0]).toMatchObject({
      options: { triggerTurn: true },
      message: { details: { ownerSessionId: "owner-a" } },
    });
    expect(shared.host.delivery.pendingCount("owner-a")).toBe(0);
    expect(destroyCalls).toBe(0);
  });

  test("reload preserves the process runtime and stale shutdown cannot detach the newer generation", async () => {
    const shared = fakeHost();
    const oldPi = new FakePi();
    const newPi = new FakePi();
    install(oldPi, shared.host);
    install(newPi, shared.host);
    const oldParent = createContext("same-owner", { idle: false });
    const newParent = createContext("same-owner", { idle: false });

    await oldPi.emit("session_start", { reason: "startup" }, oldParent.ctx);
    await newPi.emit("session_start", { reason: "reload" }, newParent.ctx);
    await newPi.emit("agent_start", {}, newParent.ctx);
    shared.host.delivery.accept(workerSettlement("same-owner"));
    await oldPi.emit("session_shutdown", { reason: "reload" }, oldParent.ctx);

    newParent.setIdle(true);
    await newPi.emit("agent_settled", {}, newParent.ctx);

    expect(oldPi.sent).toEqual([]);
    expect(newPi.sent).toHaveLength(1);
    expect(shared.runtime.shutdownCalls).toBe(0);
    expect(shared.runtime.unsubscribeStateCalls).toBe(1);
  });

  test("concurrent host destruction closes its runtime and root Effect lifetime exactly once", async () => {
    const shared = fakeHost();
    let effectRuntimeDisposeCalls = 0;
    Object.assign(shared.host, {
      effectRuntime: {
        async dispose() {
          effectRuntimeDisposeCalls += 1;
        },
      },
    });
    let releaseShutdown: (() => void) | undefined;
    const shutdownGate = new Promise<void>((resolve) => {
      releaseShutdown = resolve;
    });
    shared.runtime.shutdown = async () => {
      shared.runtime.shutdownCalls += 1;
      await shutdownGate;
    };

    const first = destroyProcessHost(shared.host);
    const second = destroyProcessHost(shared.host);
    expect(second).toBe(first);
    await Promise.resolve();
    expect(shared.runtime.shutdownCalls).toBe(1);

    releaseShutdown?.();
    await Promise.all([first, second]);
    expect(shared.runtime.shutdownCalls).toBe(1);
    expect(effectRuntimeDisposeCalls).toBe(1);
  });

  test("destruction gates new attachments until its shared failure settles", async () => {
    const shared = fakeHost();
    let rejectShutdown!: (error: Error) => void;
    const shutdownFailure = new Error("shutdown failed");
    const shutdownGate = new Promise<void>((_resolve, reject) => {
      rejectShutdown = reject;
    });
    shared.runtime.shutdown = async () => {
      shared.runtime.shutdownCalls += 1;
      await shutdownGate;
    };

    const first = destroyProcessHost(shared.host);
    expect(() => attachProcessHost(shared.host)).toThrow("Cannot attach to a destroying process host");
    const second = destroyProcessHost(shared.host);
    expect(second).toBe(first);

    rejectShutdown(shutdownFailure);
    await expect(first).rejects.toBe(shutdownFailure);
    await expect(second).rejects.toBe(shutdownFailure);
    expect(() => attachProcessHost(shared.host)).toThrow("Cannot attach to a destroyed process host");
    expect(shared.runtime.shutdownCalls).toBe(1);
  });

  test("quit awaits extension cleanup and host destruction", async () => {
    const pi = new FakePi();
    const shared = fakeHost();
    let releaseDestroy!: () => void;
    let destroyStarted = false;
    let destroyFinished = false;
    const destroyGate = new Promise<void>((resolve) => (releaseDestroy = resolve));
    install(pi, shared.host, {
      async destroyHost(host) {
        expect(host).toBe(shared.host);
        destroyStarted = true;
        await destroyGate;
        destroyFinished = true;
      },
    });
    const { ctx } = createContext("owner-quit");
    await pi.emit("session_start", { reason: "startup" }, ctx);

    const shutdown = pi.emit("session_shutdown", { reason: "quit" }, ctx);
    await Promise.resolve();
    expect(destroyStarted).toBe(true);
    expect(destroyFinished).toBe(false);
    expect(shared.runtime.unsubscribeStateCalls).toBe(1);

    releaseDestroy();
    await shutdown;
    expect(destroyFinished).toBe(true);
  });

  test("the versioned process host survives reloads and refuses quit while attached", async () => {
    await quitProcessHost();
    const first = createProcessHost();
    const second = createProcessHost();
    const firstAttachment = attachProcessHost(first);
    const secondAttachment = attachProcessHost(second);

    expect(second).toBe(first);
    expect(getProcessHost()).toBe(first);

    await quitProcessHost();
    expect(getProcessHost()).toBe(first);
    expect(detachProcessHost(first, firstAttachment)).toBe(false);

    await quitProcessHost();
    expect(getProcessHost()).toBe(first);
    expect(detachProcessHost(second, secondAttachment)).toBe(true);

    let releaseShutdown!: () => void;
    const shutdownGate = new Promise<void>((resolve) => {
      releaseShutdown = resolve;
    });
    Object.assign(first.orchestration, {
      shutdown: () => shutdownGate,
    });
    const destruction = quitProcessHost();
    expect(() => createProcessHost()).toThrow(
      "Cannot create a process host while the current host is being destroyed",
    );
    expect(() => attachProcessHost(first)).toThrow(
      "Cannot attach to a destroying process host",
    );

    releaseShutdown();
    await destruction;
    expect(getProcessHost()).toBeUndefined();
  });

  test("late disposal settlement cannot delete a replacement process host", async () => {
    await quitProcessHost();
    const staleHost = createProcessHost();
    const root = (staleHost as unknown as {
      effectRuntime: { dispose(): Promise<void> };
    }).effectRuntime;
    const disposeRoot = root.dispose.bind(root);
    let rejectLate!: (error: Error) => void;
    const lateDisposal = new Promise<void>((_resolve, reject) => {
      rejectLate = reject;
    });
    root.dispose = () => lateDisposal;

    await destroyProcessHost(staleHost, {
      awaitRootDisposal: async () => {},
    });
    const replacement = createProcessHost();
    rejectLate(new Error("stale disposal failed"));
    await Promise.resolve();
    await Promise.resolve();

    expect(getProcessHost()).toBe(replacement);

    await disposeRoot();
    await destroyProcessHost(replacement);
    expect(getProcessHost()).toBeUndefined();
  });
});
