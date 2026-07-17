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

type Scenario = "async" | "mixed";
type ProviderConfig = Parameters<ModelRuntime["registerProvider"]>[1];

interface ProviderRequest {
  readonly kind: "parent-initial" | "parent-synthesis" | "child-alpha" | "child-beta" | "child-inline";
  readonly provider: string;
  readonly model: string;
  readonly systemPrompt: string;
  readonly transcript: string;
}

interface SmokeHarness {
  readonly root: string;
  readonly runtime: Awaited<ReturnType<typeof createAgentSessionRuntime>>;
  readonly requests: ProviderRequest[];
  readonly events: string[];
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
    return transcript.includes("Worker results — wave") || transcript.includes("Completed inline wave")
      ? "parent-synthesis"
      : "parent-initial";
  }
  if (transcript.includes("ALPHA_TASK")) return "child-alpha";
  if (transcript.includes("BETA_TASK")) return "child-beta";
  return "child-inline";
}

function createProviderExtension(
  scenario: Scenario,
  requests: ProviderRequest[],
  events: string[],
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
        : `INLINE_SYNTHESIS:${transcript.includes("RESULT_INLINE")}:${transcript.includes("fixture-content")}`;
      events.push("provider:parent-synthesis:done");
      return streamText(synthesis);
    }

    const response = kind === "child-alpha"
      ? "RESULT_ALPHA"
      : kind === "child-beta"
        ? "RESULT_BETA"
        : "RESULT_INLINE";
    const delayMs = kind === "child-alpha" ? 40 : kind === "child-beta" ? 5 : 10;
    const stream = createAssistantMessageEventStream();
    void Bun.sleep(delayMs).then(() => {
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

function createObserverExtension(events: string[], effectivePrompts: string[]): ExtensionFactory {
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
      if (message.role === "custom") events.push(`parent:custom:${message.customType}`);
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
        createProviderExtension(scenario, requests, events),
        { name: "pi-orchestrate", factory: createOrchestrationExtension() },
        { name: "sdk-smoke-observer", factory: createObserverExtension(events, effectivePrompts) },
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

async function waitFor(description: string, condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${description}`);
    await Bun.sleep(5);
  }
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

      await waitFor(
        "automatic parent synthesis",
        () => assistantTexts(session.messages).includes("SYNTHESIS:true:true"),
      );
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
        expect.objectContaining({ id: "dispatch-wave", name: "orchestrate" }),
      ]);
      const acceptedResult = session.messages.find(
        (message) => message.role === "toolResult" && message.toolName === "orchestrate",
      );
      expect(textContent(acceptedResult?.content)).toContain("Accepted async wave");
      expect(acceptedResult?.details.workerIds).toHaveLength(2);

      const customMessages = session.messages.filter(
        (message) => message.role === "custom" && message.customType === "pi-orchestrate-wave",
      );
      expect(customMessages).toHaveLength(1);
      const aggregate = customMessages[0]!;
      expect(textContent(aggregate.content)).toContain("RESULT_ALPHA");
      expect(textContent(aggregate.content)).toContain("RESULT_BETA");
      expect(aggregate.details.results.map((result: { title: string }) => result.title)).toEqual([
        "Alpha task",
        "Beta task",
      ]);
      expect(aggregate.details.results.map((result: { outcome: { assistantText: string } }) =>
        result.outcome.assistantText)).toEqual(["RESULT_ALPHA", "RESULT_BETA"]);
      expect(aggregate.details.results.every((result: { sessionFile?: string }) =>
        typeof result.sessionFile === "string" && result.sessionFile.includes(harness.root))).toBe(true);

      const synthesisRequests = harness.requests.filter((request) => request.kind === "parent-synthesis");
      expect(synthesisRequests).toHaveLength(1);
      expect(synthesisRequests[0]!.transcript).toContain("RESULT_ALPHA");
      expect(synthesisRequests[0]!.transcript).toContain("RESULT_BETA");
      expect(assistantTexts(session.messages).at(-1)).toBe("SYNTHESIS:true:true");
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(2);

      const betaDone = indexOfEvent(harness.events, "provider:child-beta:done");
      const alphaDone = indexOfEvent(harness.events, "provider:child-alpha:done");
      const initialSettled = indexOfEvent(harness.events, "parent:agent_settled");
      const aggregateDelivered = indexOfEvent(harness.events, "parent:custom:pi-orchestrate-wave");
      const synthesisStarted = indexOfEvent(harness.events, "provider:parent-synthesis:start");
      expect(betaDone).toBeLessThan(alphaDone);
      expect(initialSettled).toBeLessThan(aggregateDelivered);
      expect(alphaDone).toBeLessThan(aggregateDelivered);
      expect(aggregateDelivered).toBeLessThan(synthesisStarted);

      await harness.runtime.dispose();
      harness.disposed = true;
      expect(harness.events.at(-1)).toBe("parent:session_shutdown:quit");
      expect(getProcessHost()).toBeUndefined();
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
      expect(harness.events.filter((event) => event === "parent:custom:pi-orchestrate-wave"))
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
