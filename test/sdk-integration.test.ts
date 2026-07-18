import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import {
  createAssistantMessageEventStream,
  InMemoryCredentialStore,
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  type Model,
  type SimpleStreamOptions,
  type ToolCall,
} from "@earendil-works/pi-ai";
import {
  createAgentSession,
  createAgentSessionRuntime,
  DefaultResourceLoader,
  type ExtensionFactory,
  type InlineExtension,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type CreateAgentSessionRuntimeFactory,
} from "@earendil-works/pi-coding-agent";
import { getProcessHost, quitProcessHost } from "../extension/host.js";
import { createOrchestrationExtension } from "../extension/index.js";

const PROVIDER_ID = "pi-orchestrate-sdk-smoke";
const MODEL_ID = "deterministic-agent";
const API_ID = "pi-orchestrate-memory";
const BASE_SYSTEM_PROMPT = "PARENT_BASE_PROMPT";

type Scenario = "async" | "mixed" | "failure";
type ProviderConfig = Parameters<ModelRuntime["registerProvider"]>[1];

interface ProviderRequest {
  readonly kind: "parent-initial" | "parent-synthesis" | "child-alpha" | "child-beta" | "child-inline" | "child-failure";
  readonly provider: string;
  readonly model: string;
  readonly systemPrompt: string;
  readonly transcript: string;
}

class Deferred {
  readonly promise: Promise<void>;
  private resolvePromise!: () => void;

  constructor(resolved = false) {
    this.promise = new Promise<void>((resolve) => {
      this.resolvePromise = resolve;
    });
    if (resolved) this.resolve();
  }

  resolve(): void {
    this.resolvePromise();
  }
}

class Counter {
  value = 0;
  private readonly waiters: Array<{ target: number; resolve: () => void }> = [];

  increment(): void {
    this.value += 1;
    for (const waiter of this.waiters.splice(0)) {
      if (this.value >= waiter.target) waiter.resolve();
      else this.waiters.push(waiter);
    }
  }

  waitFor(target: number): Promise<void> {
    if (this.value >= target) return Promise.resolve();
    return new Promise((resolve) => this.waiters.push({ target, resolve }));
  }
}

interface SmokeHarness {
  readonly root: string;
  readonly runtime: Awaited<ReturnType<typeof createAgentSessionRuntime>>;
  readonly requests: ProviderRequest[];
  readonly events: string[];
  readonly customMessagesStarted: Counter;
  readonly childGates: Readonly<Record<"child-alpha" | "child-beta" | "child-inline" | "child-failure", Deferred>>;
  readonly previousAgentDir: string | undefined;
  readonly previousOffline: string | undefined;
  disposed: boolean;
}

const model: Model<Api> = {
  provider: PROVIDER_ID,
  id: MODEL_ID,
  name: "Deterministic SDK Agent",
  api: API_ID,
  baseUrl: "memory://pi-orchestrate-sdk-smoke",
  reasoning: false,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 32_000,
  maxTokens: 2_000,
};

function emptyUsage(): AssistantMessage["usage"] {
  return {
    input: 1,
    output: 1,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 2,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
}

function assistantMessage(content: AssistantMessage["content"], stopReason: AssistantMessage["stopReason"]): AssistantMessage {
  return {
    role: "assistant",
    content,
    api: API_ID,
    provider: PROVIDER_ID,
    model: MODEL_ID,
    usage: emptyUsage(),
    stopReason,
    timestamp: Date.now(),
  };
}

function streamText(text: string): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output = assistantMessage([], "stop");
  const block = { type: "text" as const, text: "" };

  stream.push({ type: "start", partial: output });
  output.content.push(block);
  stream.push({ type: "text_start", contentIndex: 0, partial: output });
  block.text = text;
  stream.push({ type: "text_delta", contentIndex: 0, delta: text, partial: output });
  stream.push({ type: "text_end", contentIndex: 0, content: text, partial: output });
  stream.push({ type: "done", reason: "stop", message: output });
  stream.end(output);
  return stream;
}

function streamToolCalls(toolCalls: readonly ToolCall[]): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output = assistantMessage([], "toolUse");

  stream.push({ type: "start", partial: output });
  for (const toolCall of toolCalls) {
    const contentIndex = output.content.length;
    const streamedCall: ToolCall = {
      type: "toolCall",
      id: toolCall.id,
      name: toolCall.name,
      arguments: {},
    };
    output.content.push(streamedCall);
    stream.push({ type: "toolcall_start", contentIndex, partial: output });
    const argumentsJson = JSON.stringify(toolCall.arguments);
    streamedCall.arguments = toolCall.arguments;
    stream.push({
      type: "toolcall_delta",
      contentIndex,
      delta: argumentsJson,
      partial: output,
    });
    stream.push({
      type: "toolcall_end",
      contentIndex,
      toolCall: streamedCall,
      partial: output,
    });
  }
  stream.push({ type: "done", reason: "toolUse", message: output });
  stream.end(output);
  return stream;
}

function textContent(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((part) => {
      if (!part || typeof part !== "object") return "";
      if ("text" in part && typeof part.text === "string") return part.text;
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

function transcriptText(context: Context): string {
  return context.messages
    .map((message) => textContent(message.content))
    .filter(Boolean)
    .join("\n");
}

function providerKind(systemPrompt: string, transcript: string): ProviderRequest["kind"] {
  if (!systemPrompt.includes("direct child worker session")) {
    return transcript.includes("RESULT_ALPHA") ||
      transcript.includes("RESULT_BETA") ||
      transcript.includes("DETERMINISTIC_PROVIDER_FAILURE") ||
      transcript.includes("Completed inline wave")
      ? "parent-synthesis"
      : "parent-initial";
  }
  if (transcript.includes("ALPHA_TASK")) return "child-alpha";
  if (transcript.includes("BETA_TASK")) return "child-beta";
  if (transcript.includes("FAIL_TASK")) return "child-failure";
  return "child-inline";
}

function createProviderExtension(
  scenario: Scenario,
  requests: ProviderRequest[],
  events: string[],
  childGates: SmokeHarness["childGates"],
): InlineExtension {
  const streamSimple = (
    requestedModel: Model<Api>,
    context: Context,
    _options?: SimpleStreamOptions,
  ): AssistantMessageEventStream => {
    const systemPrompt = context.systemPrompt ?? "";
    const transcript = transcriptText(context);
    const kind = providerKind(systemPrompt, transcript);
    requests.push({
      kind,
      provider: requestedModel.provider,
      model: requestedModel.id,
      systemPrompt,
      transcript,
    });
    events.push(`provider:${kind}:start`);

    if (kind === "parent-initial") {
      if (scenario === "async") {
        return streamToolCalls([
          {
            type: "toolCall",
            id: "dispatch-wave",
            name: "orchestrate",
            arguments: {
              tasks: [
                {
                  worker: "scout",
                  title: "Alpha task",
                  instructions: "ALPHA_TASK: return deterministic alpha evidence.",
                },
                {
                  worker: "scout",
                  title: "Beta task",
                  instructions: "BETA_TASK: return deterministic beta evidence.",
                },
              ],
            },
          },
        ]);
      }

      if (scenario === "failure") {
        return streamToolCalls([{
          type: "toolCall",
          id: "dispatch-failure",
          name: "orchestrate",
          arguments: {
            tasks: [{
              worker: "scout",
              title: "Failing provider task",
              instructions: "FAIL_TASK: exercise deterministic provider failure.",
            }],
          },
        }]);
      }

      return streamToolCalls([
        {
          type: "toolCall",
          id: "dispatch-inline",
          name: "orchestrate",
          arguments: {
            tasks: [
              {
                worker: "scout",
                title: "Inline task",
                instructions: "INLINE_TASK: return deterministic inline evidence.",
              },
            ],
          },
        },
        {
          type: "toolCall",
          id: "read-fixture",
          name: "read",
          arguments: { path: "fixture.txt" },
        },
      ]);
    }

    if (kind === "parent-synthesis") {
      const synthesis = scenario === "async"
        ? `SYNTHESIS:${transcript.includes("RESULT_ALPHA")}:${transcript.includes("RESULT_BETA")}`
        : scenario === "failure"
          ? `FAILURE_SYNTHESIS:${transcript.includes("DETERMINISTIC_PROVIDER_FAILURE")}`
          : `INLINE_SYNTHESIS:${transcript.includes("RESULT_INLINE")}:${transcript.includes("fixture-content")}`;
      events.push("provider:parent-synthesis:done");
      return streamText(synthesis);
    }

    if (kind === "child-failure") {
      throw new Error("DETERMINISTIC_PROVIDER_FAILURE");
    }

    const response = kind === "child-alpha"
      ? "RESULT_ALPHA"
      : kind === "child-beta"
        ? "RESULT_BETA"
        : "RESULT_INLINE";
    const stream = createAssistantMessageEventStream();
    void childGates[kind].promise.then(() => {
      events.push(`provider:${kind}:done`);
      const output = assistantMessage([], "stop");
      const block = { type: "text" as const, text: "" };
      stream.push({ type: "start", partial: output });
      output.content.push(block);
      stream.push({ type: "text_start", contentIndex: 0, partial: output });
      block.text = response;
      stream.push({ type: "text_delta", contentIndex: 0, delta: response, partial: output });
      stream.push({ type: "text_end", contentIndex: 0, content: response, partial: output });
      stream.push({ type: "done", reason: "stop", message: output });
      stream.end(output);
    });
    return stream;
  };

  const config: ProviderConfig = {
    name: "Pi Orchestrate SDK Smoke Provider",
    api: API_ID,
    baseUrl: "memory://pi-orchestrate-sdk-smoke",
    apiKey: "in-memory-test-key",
    models: [{
      id: MODEL_ID,
      name: model.name,
      reasoning: model.reasoning,
      input: [...model.input],
      cost: { ...model.cost },
      contextWindow: model.contextWindow,
      maxTokens: model.maxTokens,
    }],
    streamSimple,
  };

  return {
    name: "sdk-smoke-provider",
    factory(pi) {
      pi.registerProvider(PROVIDER_ID, config);
    },
  };
}

function createObserverExtension(
  events: string[],
  effectivePrompts: string[],
  customMessagesStarted: Counter,
): ExtensionFactory {
  return (pi) => {
    pi.on("before_agent_start", (event) => {
      effectivePrompts.push(event.systemPrompt);
      events.push("parent:before_agent_start");
    });
    pi.on("agent_start", () => {
      events.push("parent:agent_start");
    });
    pi.on("agent_end", () => {
      events.push("parent:agent_end");
    });
    pi.on("agent_settled", () => {
      events.push("parent:agent_settled");
    });
    pi.on("message_start", (event) => {
      const message = event.message as AgentMessage;
      if (message.role !== "custom") return;
      events.push(`parent:custom:${message.customType}`);
      customMessagesStarted.increment();
    });
    pi.on("tool_execution_start", (event) => {
      events.push(`parent:tool_start:${event.toolName}`);
    });
    pi.on("tool_execution_end", (event) => {
      events.push(`parent:tool_end:${event.toolName}`);
    });
    pi.on("session_shutdown", (event) => {
      events.push(`parent:session_shutdown:${event.reason}`);
    });
  };
}

async function createHarness(scenario: Scenario): Promise<SmokeHarness & { effectivePrompts: string[] }> {
  await quitProcessHost();
  expect(getProcessHost()).toBeUndefined();

  const root = await mkdtemp(join(tmpdir(), "pi-orchestrate-sdk-"));
  const cwd = join(root, "project");
  const agentDir = join(root, "agent");
  await mkdir(cwd, { recursive: true });
  await mkdir(agentDir, { recursive: true });
  await Bun.write(join(cwd, "fixture.txt"), "fixture-content\n");

  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  const previousOffline = process.env.PI_OFFLINE;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.env.PI_OFFLINE = "1";

  const requests: ProviderRequest[] = [];
  const events: string[] = [];
  const effectivePrompts: string[] = [];
  const customMessagesStarted = new Counter();
  const childGates = {
    "child-alpha": new Deferred(),
    "child-beta": new Deferred(),
    "child-inline": new Deferred(true),
    "child-failure": new Deferred(true),
  };
  const settingsManager = SettingsManager.inMemory(
    {
      compaction: { enabled: false },
      retry: { enabled: false },
    },
    { projectTrusted: true },
  );
  const modelRuntime = await ModelRuntime.create({
    credentials: new InMemoryCredentialStore(),
    modelsPath: null,
    allowModelNetwork: false,
  });

  const createRuntime: CreateAgentSessionRuntimeFactory = async (options) => {
    const resourceLoader = new DefaultResourceLoader({
      cwd: options.cwd,
      agentDir: options.agentDir,
      settingsManager,
      noSkills: true,
      noPromptTemplates: true,
      noThemes: true,
      noContextFiles: true,
      systemPromptOverride: () => BASE_SYSTEM_PROMPT,
      extensionFactories: [
        createProviderExtension(scenario, requests, events, childGates),
        { name: "pi-orchestrate", factory: createOrchestrationExtension() },
        {
          name: "sdk-smoke-observer",
          factory: createObserverExtension(events, effectivePrompts, customMessagesStarted),
        },
      ],
    });
    await resourceLoader.reload();
    const result = await createAgentSession({
      cwd: options.cwd,
      agentDir: options.agentDir,
      model,
      thinkingLevel: "off",
      tools: scenario === "mixed" ? ["orchestrate", "read"] : ["orchestrate"],
      modelRuntime,
      resourceLoader,
      sessionManager: options.sessionManager,
      settingsManager,
      sessionStartEvent: options.sessionStartEvent,
    });
    await result.session.bindExtensions({ mode: "print" });
    return {
      ...result,
      services: {
        cwd: options.cwd,
        agentDir: options.agentDir,
        modelRuntime,
        settingsManager,
        resourceLoader,
        diagnostics: [],
      },
      diagnostics: [],
    };
  };

  try {
    const runtime = await createAgentSessionRuntime(createRuntime, {
      cwd,
      agentDir,
      sessionManager: SessionManager.create(cwd, join(root, "parent-sessions")),
    });
    return {
      root,
      runtime,
      requests,
      events,
      effectivePrompts,
      customMessagesStarted,
      childGates,
      previousAgentDir,
      previousOffline,
      disposed: false,
    };
  } catch (error) {
    if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
    if (previousOffline === undefined) delete process.env.PI_OFFLINE;
    else process.env.PI_OFFLINE = previousOffline;
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function disposeHarness(harness: SmokeHarness): Promise<void> {
  if (!harness.disposed) {
    await harness.runtime.dispose();
    harness.disposed = true;
  }
  const loader = harness.runtime.services.resourceLoader;
  if ("dispose" in loader && typeof loader.dispose === "function") loader.dispose();

  if (harness.previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
  else process.env.PI_CODING_AGENT_DIR = harness.previousAgentDir;
  if (harness.previousOffline === undefined) delete process.env.PI_OFFLINE;
  else process.env.PI_OFFLINE = harness.previousOffline;
  await rm(harness.root, { recursive: true, force: true });
}

function assistantTexts(messages: readonly AgentMessage[]): string[] {
  return messages
    .filter((message): message is AssistantMessage => message.role === "assistant")
    .map((message) => textContent(message.content));
}

function indexOfEvent(events: readonly string[], event: string): number {
  const index = events.indexOf(event);
  expect(index).toBeGreaterThanOrEqual(0);
  return index;
}

describe("Pi 0.80.10 SDK integration", () => {
  test.serial("runs a pure async orchestration wave through real parent and child AgentSessions", async () => {
    const harness = await createHarness("async");
    try {
      const session = harness.runtime.session;
      await session.prompt("Dispatch the deterministic smoke-test wave.");

      expect(harness.requests.filter((request) => request.kind === "parent-initial")).toHaveLength(1);
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis")).toHaveLength(0);
      expect(session.isStreaming).toBe(false);

      const betaSettled = new Deferred();
      let ownerSessionId = "";
      const host = getProcessHost()!;
      const unsubscribeSettlement = host.runtime.subscribeSettlement((settlement) => {
        if (settlement.title !== "Beta task") return;
        ownerSessionId = settlement.ownerSessionId;
        betaSettled.resolve();
      });
      harness.childGates["child-beta"].resolve();
      await betaSettled.promise;
      unsubscribeSettlement();

      let workerMessages = session.messages.filter(
        (message) => message.role === "custom" &&
          message.customType === "pi-orchestrate-worker-result",
      );
      expect(workerMessages).toHaveLength(1);
      expect(textContent(workerMessages[0]?.content)).toContain("RESULT_BETA");
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis")).toHaveLength(0);
      const intermediateSnapshot = await host.runtime.snapshot(ownerSessionId);
      expect(intermediateSnapshot.workers
        .filter((worker) => worker.status === "starting" || worker.status === "running")
        .map((worker) => worker.title)).toEqual(["Alpha task"]);

      harness.childGates["child-alpha"].resolve();
      await harness.customMessagesStarted.waitFor(1);
      await session.agent.waitForIdle();

      const initialParent = harness.requests.find((request) => request.kind === "parent-initial")!;
      expect(harness.effectivePrompts).toHaveLength(1);
      expect(initialParent.systemPrompt).toBe(harness.effectivePrompts[0]);
      expect(initialParent.systemPrompt).toStartWith(BASE_SYSTEM_PROMPT);
      expect(initialParent.systemPrompt).toContain("## Pi Orchestrate Contract");
      expect(initialParent.systemPrompt).toContain("Trusted worker catalog");
      expect(initialParent.systemPrompt).toContain("`scout` [package]");
      expect(initialParent.systemPrompt).toContain("`investigator` [package]");
      expect(initialParent.systemPrompt).toContain("`worker` [package]");

      const childRequests = harness.requests.filter((request) => request.kind.startsWith("child-"));
      expect(childRequests.map((request) => request.kind).sort()).toEqual([
        "child-alpha",
        "child-beta",
      ]);
      expect(childRequests.every((request) => request.provider === PROVIDER_ID)).toBe(true);
      expect(childRequests.every((request) => request.model === MODEL_ID)).toBe(true);
      expect(childRequests.every((request) => request.systemPrompt.includes("direct child worker session")))
        .toBe(true);

      const firstAssistant = session.messages.find(
        (message): message is AssistantMessage => message.role === "assistant",
      );
      expect(firstAssistant?.content.filter((part) => part.type === "toolCall")).toEqual([
        {
          type: "toolCall",
          id: "dispatch-wave",
          name: "orchestrate",
          arguments: {
            tasks: [
              {
                worker: "scout",
                title: "Alpha task",
                instructions: "ALPHA_TASK: return deterministic alpha evidence.",
              },
              {
                worker: "scout",
                title: "Beta task",
                instructions: "BETA_TASK: return deterministic beta evidence.",
              },
            ],
          },
        },
      ]);
      const acceptedResult = session.messages.find(
        (message) => message.role === "toolResult" && message.toolName === "orchestrate",
      );
      expect(textContent(acceptedResult?.content)).toContain("Accepted async wave");
      expect(acceptedResult?.details.workerIds).toHaveLength(2);

      workerMessages = session.messages.filter(
        (message) => message.role === "custom" &&
          message.customType === "pi-orchestrate-worker-result",
      );
      expect(workerMessages).toHaveLength(2);
      expect(workerMessages.map((message) => textContent(message.content))).toEqual([
        expect.stringContaining("RESULT_BETA"),
        expect.stringContaining("RESULT_ALPHA"),
      ]);
      expect(workerMessages.map((message) => message.details.title)).toEqual([
        "Beta task",
        "Alpha task",
      ]);
      expect(workerMessages.map((message) => message.details.outcome.assistantText)).toEqual([
        "RESULT_BETA",
        "RESULT_ALPHA",
      ]);
      expect(workerMessages.map((message) => message.details.waveComplete)).toEqual([false, true]);
      expect(workerMessages.every((message) =>
        typeof message.details.sessionFile === "string" &&
        message.details.sessionFile.includes(harness.root))).toBe(true);

      const synthesisRequests = harness.requests.filter((request) => request.kind === "parent-synthesis");
      expect(synthesisRequests).toHaveLength(1);
      expect(synthesisRequests[0]!.transcript).toContain("RESULT_ALPHA");
      expect(synthesisRequests[0]!.transcript).toContain("RESULT_BETA");
      expect(assistantTexts(session.messages).at(-1)).toBe("SYNTHESIS:true:true");
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(2);

      const betaDone = indexOfEvent(harness.events, "provider:child-beta:done");
      const alphaDone = indexOfEvent(harness.events, "provider:child-alpha:done");
      const initialSettled = indexOfEvent(harness.events, "parent:agent_settled");
      const resultDeliveries = harness.events
        .map((event, index) => ({ event, index }))
        .filter(({ event }) => event === "parent:custom:pi-orchestrate-worker-result")
        .map(({ index }) => index);
      const synthesisStarted = indexOfEvent(harness.events, "provider:parent-synthesis:start");
      expect(betaDone).toBeLessThan(alphaDone);
      expect(initialSettled).toBeLessThan(alphaDone);
      expect(alphaDone).toBeLessThan(resultDeliveries[0]!);
      expect(resultDeliveries[0]!).toBeLessThan(synthesisStarted);
      expect(session.messages.some((message) =>
        message.role === "assistant" && message.content.some((part) =>
          part.type === "toolCall" && part.name === "orchestration_status"))).toBe(false);

      await harness.runtime.dispose();
      harness.disposed = true;
      expect(harness.events.at(-1)).toBe("parent:session_shutdown:quit");
      expect(getProcessHost()).toBeUndefined();
    } finally {
      await disposeHarness(harness);
    }
  }, 4_000);

  test.serial("delivers a provider failure as its own worker result and continues once", async () => {
    const harness = await createHarness("failure");
    try {
      const session = harness.runtime.session;
      await session.prompt("Dispatch the deterministic failing worker.");
      await harness.customMessagesStarted.waitFor(1);
      await session.agent.waitForIdle();

      const workerMessages = session.messages.filter(
        (message) => message.role === "custom" &&
          message.customType === "pi-orchestrate-worker-result",
      );
      expect(workerMessages).toHaveLength(1);
      expect(workerMessages[0]?.details).toMatchObject({
        title: "Failing provider task",
        status: "failed",
        outcome: {
          status: "failed",
          message: "DETERMINISTIC_PROVIDER_FAILURE",
        },
        remainingActive: 0,
        waveComplete: true,
      });
      expect(textContent(workerMessages[0]?.content)).toContain("DETERMINISTIC_PROVIDER_FAILURE");
      expect(assistantTexts(session.messages).at(-1)).toBe("FAILURE_SYNTHESIS:true");
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis"))
        .toHaveLength(1);
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(2);
    } finally {
      await disposeHarness(harness);
    }
  }, 4_000);

  test.serial("keeps mixed orchestrate and read calls inline without detached delivery", async () => {
    const harness = await createHarness("mixed");
    try {
      const session = harness.runtime.session;
      await session.prompt("Run the mixed-call SDK smoke test.");
      await session.agent.waitForIdle();

      expect(assistantTexts(session.messages).at(-1)).toBe("INLINE_SYNTHESIS:true:true");
      expect(harness.requests.filter((request) => request.kind === "parent-initial")).toHaveLength(1);
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis")).toHaveLength(1);
      expect(harness.requests.filter((request) => request.kind === "child-inline")).toHaveLength(1);
      expect(session.messages.filter((message) => message.role === "custom")).toHaveLength(0);
      expect(harness.events.filter((event) =>
        event === "parent:custom:pi-orchestrate-worker-result"))
        .toHaveLength(0);

      const toolResults = session.messages.filter((message) => message.role === "toolResult");
      const orchestrationResult = toolResults.find((message) => message.toolName === "orchestrate");
      const readResult = toolResults.find((message) => message.toolName === "read");
      expect(textContent(orchestrationResult?.content)).toContain("Completed inline wave");
      expect(textContent(orchestrationResult?.content)).toContain("RESULT_INLINE");
      expect(textContent(readResult?.content)).toContain("fixture-content");
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(1);

      await harness.runtime.dispose();
      harness.disposed = true;
      expect(getProcessHost()).toBeUndefined();
    } finally {
      await disposeHarness(harness);
    }
  }, 4_000);
});
