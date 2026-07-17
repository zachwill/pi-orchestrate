import { describe, expect, test } from "bun:test";
import type {
  ExtensionAPI,
  ExtensionContext,
  ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { DeliveryCoordinator } from "../extension/delivery.js";
import {
  createWorkerCatalog,
  type WaveId,
  type WorkerCatalog,
  type WorkerDefinition,
  type WorkerId,
} from "../extension/domain.js";
import {
  attachProcessHost,
  createProcessHost,
  detachProcessHost,
  getProcessHost,
  quitProcessHost,
  type ProcessHost,
} from "../extension/host.js";
import {
  createOrchestrationExtension,
  type OrchestrationExtensionDependencies,
} from "../extension/index.js";
import type {
  CompletedWave,
  OrchestrationContext,
  RuntimeSnapshot,
} from "../extension/runtime.js";

const TOOL_NAMES = [
  "orchestrate",
  "orchestration_status",
  "worker_send",
  "worker_abort",
  "worker_close",
] as const;

type Handler = (event: any, ctx: ExtensionContext) => unknown;

class FakePi {
  readonly handlers = new Map<string, Handler[]>();
  readonly tools: ToolDefinition[] = [];
  readonly renderers: string[] = [];
  readonly sent: Array<{ message: unknown; options: unknown }> = [];

  readonly sendMessage = (message: unknown, options?: unknown): void => {
    this.sent.push({ message, options });
  };

  on(event: string, handler: Handler): void {
    const handlers = this.handlers.get(event) ?? [];
    handlers.push(handler);
    this.handlers.set(event, handlers);
  }

  registerTool(tool: ToolDefinition): void {
    this.tools.push(tool);
  }

  registerMessageRenderer(customType: string): void {
    this.renderers.push(customType);
  }

  tool(name: string): ToolDefinition {
    const tool = this.tools.find((candidate) => candidate.name === name);
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

class FakeRuntime {
  readonly orchestrateCalls: Array<{ context: OrchestrationContext; mode: string }> = [];
  readonly sendCalls: Array<{ context: OrchestrationContext; mode: string }> = [];
  readonly stateListeners = new Set<(ownerSessionId: string) => void>();
  readonly snapshotOwners: string[] = [];
  shutdownCalls = 0;
  unsubscribeStateCalls = 0;

  async orchestrate(
    context: OrchestrationContext,
    _tasks: readonly unknown[],
    mode: "async" | "inline",
  ): Promise<unknown> {
    this.orchestrateCalls.push({ context, mode });
    return mode === "async"
      ? { id: "wave-accepted", workerIds: ["worker-accepted"] }
      : completedWave(context.ownerSessionId, "inline");
  }

  async send(
    context: OrchestrationContext,
    _workerId: WorkerId,
    _instructions: string,
    mode: "async" | "inline",
  ): Promise<unknown> {
    this.sendCalls.push({ context, mode });
    return mode === "async"
      ? { id: "wave-send", workerIds: ["worker-ready"] }
      : completedWave(context.ownerSessionId, "inline");
  }

  async abort(): Promise<void> {}
  async close(): Promise<void> {}

  async snapshot(ownerSessionId: string): Promise<RuntimeSnapshot> {
    this.snapshotOwners.push(ownerSessionId);
    return { waves: [], workers: [] };
  }

  subscribeState(listener: (ownerSessionId: string) => void): () => void {
    this.stateListeners.add(listener);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.stateListeners.delete(listener);
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

function completedWave(
  ownerSessionId: string,
  mode: "async" | "inline" = "async",
): CompletedWave {
  return {
    id: "wave-complete" as WaveId,
    ownerSessionId,
    mode,
    results: [
      {
        workerId: "worker-complete" as WorkerId,
        worker: "scout",
        title: "Inspect",
        status: "completed",
        outcome: { status: "completed", assistantText: "Inspection complete." },
        usage: {
          input: 1,
          output: 1,
          cacheRead: 0,
          cacheWrite: 0,
          cost: 0,
          contextTokens: 2,
          turns: 1,
        },
        sessionFile: "/sessions/worker.jsonl",
      },
    ],
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

function fakeHost(runtime = new FakeRuntime()): { host: ProcessHost; runtime: FakeRuntime } {
  return {
    host: {
      runtime: runtime as unknown as ProcessHost["runtime"],
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

function assistantToolCalls(calls: Array<{ id: string; name: string }>) {
  return {
    role: "assistant",
    content: calls.map((call) => ({
      type: "toolCall" as const,
      id: call.id,
      name: call.name,
      arguments: {},
    })),
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
  tasks: [{ worker: "scout", title: "Inspect", instructions: "Inspect the project." }],
};

describe("Pi Orchestrate extension integration", () => {
  test("registers exactly five public tools without subscribing before session start", () => {
    const pi = new FakePi();
    const shared = fakeHost();
    install(pi, shared.host);

    expect(pi.tools.map((tool) => tool.name)).toEqual(TOOL_NAMES);
    expect(pi.renderers).toEqual(["pi-orchestrate-wave"]);
    expect(pi.tools.map((tool) => tool.name)).not.toContain("worker_status");
    expect(pi.tools.map((tool) => tool.name)).not.toContain("worker_respond");
    expect(shared.runtime.stateListeners.size).toBe(0);
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
    expect((promptResult as { systemPrompt: string }).systemPrompt).toStartWith("Parent prompt");
    expect((promptResult as { systemPrompt: string }).systemPrompt).toContain("trusted-scout");
    expect(runtime.orchestrateCalls[0]?.context.catalog).toBe(catalog);
    expect(runtime.orchestrateCalls[0]?.context.projectTrusted).toBe(true);
  });

  test("classifies a pure dispatch async and clears its mode after tool execution", async () => {
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

    const asyncResult = await invoke(pi, "orchestrate", "pure", orchestrationParams, ctx);
    await pi.emit(
      "tool_execution_end",
      { toolCallId: "pure", toolName: "orchestrate", result: asyncResult, isError: false },
      ctx,
    );
    await invoke(pi, "orchestrate", "pure", orchestrationParams, ctx);

    expect(runtime.orchestrateCalls.map((call) => call.mode)).toEqual(["async", "inline"]);
    expect(asyncResult.terminate).toBe(true);
  });

  test("classifies dispatches inline when their assistant message contains mixed or multiple calls", async () => {
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
    await invoke(pi, "orchestrate", "mixed-dispatch", orchestrationParams, ctx);

    await pi.emit(
      "message_end",
      {
        message: assistantToolCalls([
          { id: "first-dispatch", name: "orchestrate" },
          { id: "second-dispatch", name: "worker_send" },
        ]),
      },
      ctx,
    );
    await Promise.all([
      invoke(pi, "orchestrate", "first-dispatch", orchestrationParams, ctx),
      invoke(
        pi,
        "worker_send",
        "second-dispatch",
        { worker_id: "worker-ready", instructions: "Continue." },
        ctx,
      ),
    ]);

    expect(runtime.orchestrateCalls.map((call) => call.mode)).toEqual(["inline", "inline"]);
    expect(runtime.sendCalls.map((call) => call.mode)).toEqual(["inline"]);
  });

  test("binds the exact session owner and generation, then forwards delivery only after agent settlement", async () => {
    const pi = new FakePi();
    const { host } = fakeHost();
    install(pi, host);
    const parent = createContext("owner-delivery", { idle: false });

    await pi.emit("session_start", { reason: "startup" }, parent.ctx);
    await pi.emit("agent_start", {}, parent.ctx);
    expect(host.delivery.accept(completedWave("owner-delivery"))).toBe(true);
    expect(pi.sent).toEqual([]);

    parent.setIdle(true);
    await pi.emit("agent_settled", {}, parent.ctx);

    expect(pi.sent).toHaveLength(1);
    expect(pi.sent[0]?.options).toEqual({ triggerTurn: true });
    expect(pi.sent[0]?.message).toMatchObject({
      customType: "pi-orchestrate-wave",
      details: { ownerSessionId: "owner-delivery" },
    });
  });

  test("keeps two SDK owners bound and quitting one leaves the other host attachment live", async () => {
    const shared = fakeHost();
    const ownerPi = new FakePi();
    const otherPi = new FakePi();
    let destroyCalls = 0;
    const overrides = {
      async destroyHost(host: ProcessHost) {
        destroyCalls += 1;
        await host.runtime.shutdown();
      },
    };
    install(ownerPi, shared.host, overrides);
    install(otherPi, shared.host, overrides);
    const owner = createContext("owner-a");
    const other = createContext("owner-b");

    await ownerPi.emit("session_start", { reason: "startup" }, owner.ctx);
    await otherPi.emit("session_start", { reason: "startup" }, other.ctx);
    shared.host.delivery.accept(completedWave("owner-a"));
    shared.host.delivery.accept(completedWave("owner-b"));

    expect(ownerPi.sent).toHaveLength(1);
    expect(otherPi.sent).toHaveLength(1);

    await ownerPi.emit("session_shutdown", { reason: "quit" }, owner.ctx);
    expect(destroyCalls).toBe(0);
    expect(shared.runtime.shutdownCalls).toBe(0);

    shared.host.delivery.accept(completedWave("owner-b"));
    expect(ownerPi.sent).toHaveLength(1);
    expect(otherPi.sent).toHaveLength(2);

    await otherPi.emit("session_shutdown", { reason: "quit" }, other.ctx);
    expect(destroyCalls).toBe(1);
    expect(shared.runtime.shutdownCalls).toBe(1);
  });

  test("queues A's completion while B is active and delivers it once when A resumes", async () => {
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
    shared.host.delivery.accept(completedWave("owner-a"));

    expect(firstA.sent).toEqual([]);
    expect(ownerB.sent).toEqual([]);
    expect(shared.host.delivery.pendingCount("owner-a")).toBe(1);

    shared.runtime.snapshotOwners.length = 0;
    await invoke(ownerB, "orchestration_status", "status-b", {}, sessionB.ctx);
    expect(shared.runtime.snapshotOwners).toEqual(["owner-b"]);

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
    shared.host.delivery.accept(completedWave("same-owner"));
    await oldPi.emit("session_shutdown", { reason: "reload" }, oldParent.ctx);

    newParent.setIdle(true);
    await newPi.emit("agent_settled", {}, newParent.ctx);

    expect(oldPi.sent).toEqual([]);
    expect(newPi.sent).toHaveLength(1);
    expect(shared.runtime.shutdownCalls).toBe(0);
    expect(shared.runtime.unsubscribeStateCalls).toBe(1);
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

    await quitProcessHost();
    expect(getProcessHost()).toBeUndefined();
  });
});
