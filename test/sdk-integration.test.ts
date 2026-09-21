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
import { getProcessHost, quitProcessHost } from "../extension/parent/process-host.ts";
import { createOrchestrationExtension } from "../extension/index.ts";
import { LIVE_WORKER_CONTEXT_TYPE } from "../extension/parent/worker-context.ts";

interface WorkerResultDetails {
  title: string;
  outcome: { assistantText?: string };
  synthesisGroupId?: string;
  synthesisGroupSize?: number;
  sessionFile?: string;
}

type WorkerResultMessage = Extract<AgentMessage, { role: "custom" }> & {
  details: WorkerResultDetails;
};
const PROVIDER_ID = "pi-orchestrate-sdk-smoke";
const MODEL_ID = "deterministic-agent";
const API_ID = "pi-orchestrate-memory";
const BASE_SYSTEM_PROMPT = "PARENT_BASE_PROMPT";

type Scenario = "async" | "parallel" | "mixed" | "failure" | "shutdown";
type ProviderConfig = Parameters<ModelRuntime["registerProvider"]>[1];

interface ProviderRequest {
  readonly kind: "parent-initial" | "parent-synthesis" | "parent-checkpoint" | "compaction" | "child-alpha" | "child-beta" | "child-inline" | "child-failure";
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

interface CompactionControl {
  readonly started: Counter;
  readonly release: Deferred;
  readonly outcome: "success" | "failure";
}

interface SmokeHarness {
  readonly root: string;
  readonly runtime: Awaited<ReturnType<typeof createAgentSessionRuntime>>;
  readonly requests: ProviderRequest[];
  readonly events: string[];
  readonly customMessagesStarted: Counter;
  readonly childRequestsStarted: Counter;
  readonly childProviderSignals: AbortSignal[];
  readonly childProviderAborted: Counter;
  readonly childSessionShutdowns: Counter;
  readonly childExtensionStateKey: string;
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

function streamText(text: string, inputTokens = 1): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output = assistantMessage([], "stop");
  output.usage.input = inputTokens;
  output.usage.totalTokens = inputTokens + output.usage.output;
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

function streamError(message: string): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output = assistantMessage([], "error");
  output.errorMessage = message;
  stream.push({ type: "start", partial: output });
  stream.push({ type: "error", reason: "error", error: output });
  stream.end(output);
  return stream;
}

function streamCompaction(control: CompactionControl, signal?: AbortSignal): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  let finished = false;
  const finish = (aborted: boolean) => {
    if (finished) return;
    finished = true;
    signal?.removeEventListener("abort", abort);
    const reason = aborted ? "aborted" : control.outcome === "failure" ? "error" : "stop";
    const output = assistantMessage(
      reason === "stop" ? [{ type: "text", text: "COMPACTED_CHECKPOINT: continue the task." }] : [],
      reason,
    );
    stream.push({ type: "start", partial: output });
    if (reason === "stop") stream.push({ type: "done", reason, message: output });
    else {
      output.errorMessage = aborted ? "Compaction cancelled" : "DETERMINISTIC_COMPACTION_FAILURE";
      stream.push({ type: "error", reason, error: output });
    }
    stream.end(output);
  };
  const abort = () => finish(true);
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  void control.release.promise.then(() => finish(false));
  control.started.increment();
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

function isWorkerResultMessage(message: AgentMessage): message is WorkerResultMessage {
  if (message.role !== "custom" || message.customType !== "pi-orchestrate-worker-result") return false;
  const details = message.details;
  if (!details || typeof details !== "object") return false;
  if (!("title" in details) || typeof details.title !== "string") return false;
  if (!("outcome" in details) || !details.outcome || typeof details.outcome !== "object") return false;
  return "workerId" in details && typeof details.workerId === "string";
}

function transcriptText(context: Context): string {
  return context.messages
    .map((message) => textContent(message.content))
    .filter(Boolean)
    .join("\n");
}

function providerKind(systemPrompt: string, transcript: string): ProviderRequest["kind"] {
  if (systemPrompt.includes("You are a context summarization assistant")) return "compaction";
  if (!systemPrompt.includes("direct child worker session")) {
    return transcript.includes("RESULT_ALPHA") ||
      transcript.includes("RESULT_BETA") ||
      transcript.includes("DETERMINISTIC_PROVIDER_FAILURE") ||
      transcript.includes("Completed inline run")
      ? "parent-synthesis"
      : transcript.includes("CONTEXT_CHECKPOINT") || transcript.includes("COMPACTED_CHECKPOINT")
        ? "parent-checkpoint"
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
  childRequestsStarted: Counter,
  childProviderSignals: AbortSignal[],
  childProviderAborted: Counter,
  compaction?: CompactionControl,
): InlineExtension {
  const streamSimple = (
    requestedModel: Model<Api>,
    context: Context,
    options?: SimpleStreamOptions,
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

    if (kind === "compaction") {
      if (!compaction) throw new Error("Unexpected compaction request");
      return streamCompaction(compaction, options?.signal);
    }

    if (kind === "parent-checkpoint") {
      if (compaction?.started.value === 0 && transcript.includes("OVERFLOW_REQUEST")) {
        return streamError("maximum context length exceeded");
      }
      const highUsage = compaction?.started.value === 0 && transcript.includes("THRESHOLD_REQUEST");
      // Real usage-based threshold detection, without changing Pi's lifecycle.
      return streamText("Checkpoint acknowledged.", highUsage ? 20_000 : 1);
    }

    if (kind === "parent-initial") {
      if (scenario === "async" || scenario === "shutdown") {
        return streamToolCalls([
          {
            type: "toolCall",
            id: "dispatch-run",
            name: "orchestrate",
            arguments: {
              worker: "scout",
              title: "Alpha task",
              instructions: "ALPHA_TASK: return deterministic alpha evidence.",
            },
          },
        ]);
      }

      if (scenario === "parallel") {
        return streamToolCalls([
          {
            type: "toolCall",
            id: "dispatch-alpha",
            name: "orchestrate",
            arguments: {
              worker: "scout",
              title: "Alpha task",
              instructions: "ALPHA_TASK: return deterministic alpha evidence.",
            },
          },
          {
            type: "toolCall",
            id: "dispatch-beta",
            name: "orchestrate",
            arguments: {
              worker: "scout",
              title: "Beta task",
              instructions: "BETA_TASK: return deterministic beta evidence.",
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
            worker: "scout",
            title: "Failing provider task",
            instructions: "FAIL_TASK: exercise deterministic provider failure.",
          },
        }]);
      }

      return streamToolCalls([
        {
          type: "toolCall",
          id: "dispatch-inline",
          name: "orchestrate",
          arguments: {
            worker: "scout",
            title: "Inline task",
            instructions: "INLINE_TASK: return deterministic inline evidence.",
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
        ? `SYNTHESIS:${transcript.includes("RESULT_ALPHA")}`
        : scenario === "parallel"
          ? `PARALLEL_SYNTHESIS:${transcript.includes("RESULT_ALPHA")}:${transcript.includes("RESULT_BETA")}`
          : scenario === "failure"
          ? `FAILURE_SYNTHESIS:${transcript.includes("DETERMINISTIC_PROVIDER_FAILURE")}`
          : `INLINE_SYNTHESIS:${transcript.includes("RESULT_INLINE")}:${transcript.includes("fixture-content")}`;
      events.push("provider:parent-synthesis:done");
      return streamText(synthesis);
    }

    if (kind === "child-failure") {
      childRequestsStarted.increment();
      throw new Error("DETERMINISTIC_PROVIDER_FAILURE");
    }

    childRequestsStarted.increment();
    const response = kind === "child-alpha"
      ? "RESULT_ALPHA"
      : kind === "child-beta"
        ? "RESULT_BETA"
        : "RESULT_INLINE";
    const stream = createAssistantMessageEventStream();
    if (options?.signal) {
      childProviderSignals.push(options.signal);
      options.signal.addEventListener("abort", () => {
        childProviderAborted.increment();
        if (scenario !== "shutdown") return;
        const output = assistantMessage([], "aborted");
        output.errorMessage = "Provider request aborted";
        stream.push({ type: "start", partial: output });
        stream.push({ type: "error", reason: "aborted", error: output });
        stream.end(output);
      }, { once: true });
    }
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

async function createHarness(
  scenario: Scenario,
  compaction?: CompactionControl,
): Promise<SmokeHarness & { effectivePrompts: string[] }> {
  await quitProcessHost();
  expect(getProcessHost()).toBeUndefined();

  const root = await mkdtemp(join(tmpdir(), "pi-orchestrate-sdk-"));
  const cwd = join(root, "project");
  const agentDir = join(root, "agent");
  await mkdir(cwd, { recursive: true });
  await mkdir(agentDir, { recursive: true });
  await Bun.write(join(cwd, "fixture.txt"), "fixture-content\n");

  const childSessionShutdowns = new Counter();
  const childExtensionStateKey = `__pi_orchestrate_sdk_${crypto.randomUUID().replaceAll("-", "_")}`;
  Object.assign(globalThis, { [childExtensionStateKey]: childSessionShutdowns });
  if (scenario === "shutdown") {
    const childExtensionPath = join(root, "child-observer.js");
    await Bun.write(
      childExtensionPath,
      `export default function childObserver(pi) {
  pi.on("session_shutdown", () => globalThis[${JSON.stringify(childExtensionStateKey)}].increment());
}\n`,
    );
    await Bun.write(
      join(agentDir, "settings.json"),
      JSON.stringify({ extensions: [childExtensionPath] }),
    );
  }

  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  const previousOffline = process.env.PI_OFFLINE;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.env.PI_OFFLINE = "1";

  const requests: ProviderRequest[] = [];
  const events: string[] = [];
  const effectivePrompts: string[] = [];
  const customMessagesStarted = new Counter();
  const childRequestsStarted = new Counter();
  const childProviderSignals: AbortSignal[] = [];
  const childProviderAborted = new Counter();
  const childGates = {
    "child-alpha": new Deferred(),
    "child-beta": new Deferred(),
    "child-inline": new Deferred(true),
    "child-failure": new Deferred(true),
  };
  const settingsManager = SettingsManager.inMemory(
    {
      compaction: { enabled: false, ...(compaction ? { keepRecentTokens: 1 } : {}) },
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
        createProviderExtension(
          scenario,
          requests,
          events,
          childGates,
          childRequestsStarted,
          childProviderSignals,
          childProviderAborted,
          compaction,
        ),
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
      childRequestsStarted,
      childProviderSignals,
      childProviderAborted,
      childSessionShutdowns,
      childExtensionStateKey,
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
    Reflect.deleteProperty(globalThis, childExtensionStateKey);
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function disposeHarness(harness: SmokeHarness): Promise<void> {
  for (const gate of Object.values(harness.childGates)) gate.resolve();
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
  Reflect.deleteProperty(globalThis, harness.childExtensionStateKey);
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

describe("Pi 0.85.0 SDK integration", () => {
  for (const outcome of ["success", "failure", "abort"] as const) {
    test.serial(`delivers a worker group after manual compaction ${outcome} without another prompt`, async () => {
      const control: CompactionControl = {
        started: new Counter(), release: new Deferred(),
        outcome: outcome === "failure" ? "failure" : "success",
      };
      const harness = await createHarness("parallel", control);
      try {
        const session = harness.runtime.session;
        await session.prompt("Dispatch two workers.");
        await harness.childRequestsStarted.waitFor(2);
        await session.prompt("CONTEXT_CHECKPOINT: leave both workers running.");
        const host = getProcessHost()!;
        const owner = session.sessionId;
        const settled = new Counter();
        const unsubscribe = host.orchestration.subscribeSettlement(() => settled.increment());
        const ended = new Deferred();
        const unsubscribeSession = session.subscribe((event) => {
          if (event.type === "compaction_end") {
            harness.events.push(`compaction:end:${event.reason}`);
            ended.resolve();
          }
        });
        const compacting = session.compact().then(
          () => undefined,
          (error: unknown) => error,
        );
        await control.started.waitFor(1);
        expect(session.isIdle).toBe(false);
        harness.childGates["child-beta"].resolve();
        await settled.waitFor(1);
        harness.childGates["child-alpha"].resolve();
        await settled.waitFor(2);
        expect(host.delivery.pendingCount(owner)).toBe(2);
        expect(session.messages.filter(isWorkerResultMessage)).toHaveLength(0);
        expect(harness.requests.filter((r) => r.kind === "parent-synthesis")).toHaveLength(0);

        if (outcome === "abort") session.abortCompaction();
        else control.release.resolve();
        const error = await compacting;
        if (outcome === "success") expect(error).toBeUndefined();
        else {
          expect(error).toBeInstanceOf(Error);
          if (outcome === "abort") expect((error as Error).message).toMatch(/cancelled|aborted/i);
          else expect((error as Error).message).toContain("DETERMINISTIC_COMPACTION_FAILURE");
        }
        await ended.promise;
        // No prompt or synthetic agent_settled: the production idle recheck must wake delivery.
        await harness.customMessagesStarted.waitFor(1);
        await session.waitForIdle();
        const messages = session.messages.filter(isWorkerResultMessage);
        expect(messages.map((m) => m.details.title)).toEqual(["Beta task", "Alpha task"]);
        expect(new Set(messages.map((m) => m.details.synthesisGroupId)).size).toBe(1);
        expect(host.delivery.pendingCount(owner)).toBe(0);
        expect(harness.requests.filter((r) => r.kind === "parent-synthesis")).toHaveLength(1);
        expect(assistantTexts(session.messages).at(-1)).toBe("PARALLEL_SYNTHESIS:true:true");
        expect(indexOfEvent(harness.events, "compaction:end:manual")).toBeLessThan(
          indexOfEvent(harness.events, "parent:custom:pi-orchestrate-worker-result"),
        );
        const persisted = SessionManager.open(session.sessionFile!).buildSessionContext().messages;
        expect(persisted.filter(isWorkerResultMessage)).toHaveLength(2);
        expect(persisted.some((m) => m.role === "custom" && m.customType === LIVE_WORKER_CONTEXT_TYPE)).toBe(false);
        unsubscribe();
        unsubscribeSession();
      } finally {
        control.release.resolve();
        await disposeHarness(harness);
      }
    }, 5_000);
  }

  for (const reason of ["threshold", "overflow"] as const) {
    test.serial(`restores live worker context after ${reason} compaction and preserves grouped delivery`, async () => {
      const control: CompactionControl = {
        started: new Counter(), release: new Deferred(), outcome: "success",
      };
      const harness = await createHarness("parallel", control);
      try {
        const session = harness.runtime.session;
        await session.prompt("Dispatch two workers.");
        await harness.childRequestsStarted.waitFor(2);
        await session.prompt("CONTEXT_CHECKPOINT: leave both workers running.");
        const host = getProcessHost()!;
        const before = await host.orchestration.snapshot(session.sessionId);
        const alpha = before.workers.find((w) => w.title === "Alpha task")!;
        const beta = before.workers.find((w) => w.title === "Beta task")!;
        const betaSettled = new Deferred();
        const unsubscribe = host.orchestration.subscribeSettlement((s) => {
          if (s.workerId === beta.id) betaSettled.resolve();
        });
        const compactionReasons: string[] = [];
        const unsubscribeSession = session.subscribe((event) => {
          if (event.type === "compaction_end") compactionReasons.push(event.reason);
        });
        session.setAutoCompactionEnabled(true);
        const prompting = session.prompt(`CONTEXT_CHECKPOINT ${reason.toUpperCase()}_REQUEST`);
        await control.started.waitFor(1);
        harness.childGates["child-beta"].resolve();
        await betaSettled.promise;
        expect(host.delivery.pendingCount(session.sessionId)).toBe(1);
        expect(session.messages.filter(isWorkerResultMessage)).toHaveLength(0);
        const requestCountDuringCompaction = harness.requests.length;
        control.release.resolve();
        await prompting;
        expect(compactionReasons).toEqual([reason]);
        expect(harness.requests.filter((r) => r.kind === "parent-synthesis")).toHaveLength(0);
        // Threshold compaction does not retry a completed answer; inspect the next request.
        if (reason === "threshold") await session.prompt("CONTEXT_CHECKPOINT: report live workers.");
        const afterCompaction = harness.requests.slice(requestCountDuringCompaction)
          .find((r) => r.kind.startsWith("parent-"));
        expect(afterCompaction).toBeDefined();
        expect(afterCompaction!.transcript).toContain("Pi Orchestrate live worker context");
        expect(afterCompaction!.transcript).toContain(alpha.id);
        expect(afterCompaction!.transcript).toContain("ALPHA_TASK");
        expect(afterCompaction!.transcript).toContain("status=running");
        expect(afterCompaction!.transcript).not.toContain(`worker_id="${beta.id}"`);
        if (reason === "overflow") expect(afterCompaction!.transcript).toContain("Pending delivery: 1");
        expect(session.messages.some((m) => m.role === "toolResult" && m.toolName === "orchestrate")).toBe(false);
        expect(session.messages.some((m) => m.role === "custom" && m.customType === LIVE_WORKER_CONTEXT_TYPE)).toBe(false);

        harness.childGates["child-alpha"].resolve();
        await harness.customMessagesStarted.waitFor(1);
        await session.waitForIdle();
        expect(session.messages.filter(isWorkerResultMessage)).toHaveLength(2);
        expect(assistantTexts(session.messages).at(-1)).toBe("PARALLEL_SYNTHESIS:true:true");
        expect(harness.requests.filter((r) => r.kind === "parent-synthesis")).toHaveLength(reason === "threshold" ? 2 : 1);
        expect(harness.childRequestsStarted.value).toBe(2);
        expect(await Bun.file(session.sessionFile!).text()).not.toContain(LIVE_WORKER_CONTEXT_TYPE);
        unsubscribe();
        unsubscribeSession();
      } finally {
        control.release.resolve();
        await disposeHarness(harness);
      }
    }, 5_000);
  }

  test.serial("runs a sole orchestrate call asynchronously through real parent and child AgentSessions", async () => {
    const harness = await createHarness("async");
    try {
      const session = harness.runtime.session;
      await session.prompt("Dispatch the deterministic smoke-test task.");

      expect(harness.requests.filter((request) => request.kind === "parent-initial")).toHaveLength(1);
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis")).toHaveLength(0);
      expect(session.isStreaming).toBe(false);
      await harness.childRequestsStarted.waitFor(1);

      harness.childGates["child-alpha"].resolve();
      await harness.customMessagesStarted.waitFor(1);
      await session.agent.waitForIdle();

      const initialParent = harness.requests.find((request) => request.kind === "parent-initial")!;
      expect(harness.effectivePrompts).toHaveLength(1);
      expect(initialParent.systemPrompt).toBe(harness.effectivePrompts[0]!);
      expect(initialParent.systemPrompt).toStartWith(BASE_SYSTEM_PROMPT);
      expect(initialParent.systemPrompt).toContain("## Pi Orchestrate Contract");
      expect(initialParent.systemPrompt).toContain("Trusted worker catalog");
      expect(initialParent.systemPrompt).toContain("`scout` [package]");
      expect(initialParent.systemPrompt).toContain("`investigator` [package]");
      expect(initialParent.systemPrompt).toContain("`worker` [package]");

      const childRequests = harness.requests.filter((request) => request.kind.startsWith("child-"));
      expect(childRequests.map((request) => request.kind)).toEqual(["child-alpha"]);
      expect(childRequests[0]).toMatchObject({ provider: PROVIDER_ID, model: MODEL_ID });
      expect(childRequests[0]?.systemPrompt).toContain("direct child worker session");

      const workerMessages = session.messages.filter(isWorkerResultMessage);
      expect(workerMessages).toHaveLength(1);
      expect(workerMessages[0]?.details).toMatchObject({
        title: "Alpha task",
        outcome: { assistantText: "RESULT_ALPHA" },
      });
      const sessionFile = workerMessages[0]?.details.sessionFile;
      expect(sessionFile).toContain(harness.root);
      if (typeof sessionFile !== "string") throw new Error("Worker settlement omitted its session file");
      const durableSessionFile = Bun.file(sessionFile);
      expect(await durableSessionFile.exists()).toBe(true);
      expect((await durableSessionFile.text()).trim().length).toBeGreaterThan(0);

      const synthesisRequests = harness.requests.filter((request) => request.kind === "parent-synthesis");
      expect(synthesisRequests).toHaveLength(1);
      expect(assistantTexts(session.messages).at(-1)).toBe("SYNTHESIS:true");
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(2);

      const alphaDone = indexOfEvent(harness.events, "provider:child-alpha:done");
      const initialSettled = indexOfEvent(harness.events, "parent:agent_settled");
      const resultDelivered = indexOfEvent(
        harness.events,
        "parent:custom:pi-orchestrate-worker-result",
      );
      const synthesisStarted = indexOfEvent(harness.events, "provider:parent-synthesis:start");
      expect(initialSettled).toBeLessThan(alphaDone);
      expect(alphaDone).toBeLessThan(resultDelivered);
      expect(resultDelivered).toBeLessThan(synthesisStarted);

      await harness.runtime.dispose();
      harness.disposed = true;
      expect(harness.events.at(-1)).toBe("parent:session_shutdown:quit");
      expect(getProcessHost()).toBeUndefined();
    } finally {
      await disposeHarness(harness);
    }
  }, 4_000);

  test.serial("aborts an active child provider request when the final process-host attachment quits", async () => {
    const harness = await createHarness("shutdown");
    try {
      const session = harness.runtime.session;
      await session.prompt("Dispatch the worker that remains active until process shutdown.");
      await harness.childRequestsStarted.waitFor(1);

      const host = getProcessHost()!;
      const ownerSessionId = session.sessionManager.getSessionId();
      expect(harness.childProviderSignals).toHaveLength(1);
      expect(harness.childProviderSignals[0]?.aborted).toBe(false);

      await harness.runtime.dispose();
      harness.disposed = true;
      await harness.childProviderAborted.waitFor(1);

      expect(harness.childProviderSignals[0]?.aborted).toBe(true);
      expect(harness.childProviderAborted.value).toBe(1);
      expect(harness.childSessionShutdowns.value).toBe(1);
      expect(getProcessHost()).toBeUndefined();

      const snapshot = await host.orchestration.snapshot(ownerSessionId);
      expect(snapshot.runs).toHaveLength(1);
      expect(snapshot.runs[0]?.state).toBe("complete");
      expect(snapshot.workers).toHaveLength(1);
      expect(snapshot.workers[0]).toMatchObject({
        title: "Alpha task",
        status: "aborted",
        outcome: { status: "aborted" },
      });

      await quitProcessHost();
      await quitProcessHost();
      expect(harness.childProviderAborted.value).toBe(1);
      expect(harness.childSessionShutdowns.value).toBe(1);
      expect(getProcessHost()).toBeUndefined();
    } finally {
      await disposeHarness(harness);
    }
  }, 4_000);

  test.serial("accepts sibling orchestrate calls concurrently and synthesizes after the group", async () => {
    const harness = await createHarness("parallel");
    try {
      const session = harness.runtime.session;
      await session.prompt("Dispatch two independent smoke-test tasks.");

      await harness.childRequestsStarted.waitFor(2);
      expect(harness.requests.filter((request) => request.kind.startsWith("child-")).map(
        (request) => request.kind,
      ).sort()).toEqual(["child-alpha", "child-beta"]);
      expect(session.isStreaming).toBe(false);

      const betaSettled = new Deferred();
      const unsubscribeSettlement = getProcessHost()!.orchestration.subscribeSettlement((settlement) => {
        if (settlement.title === "Beta task") betaSettled.resolve();
      });
      harness.childGates["child-beta"].resolve();
      await betaSettled.promise;
      expect(session.messages.filter(isWorkerResultMessage)).toHaveLength(1);
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis"))
        .toHaveLength(0);

      harness.childGates["child-alpha"].resolve();
      await harness.customMessagesStarted.waitFor(1);
      unsubscribeSettlement();
      await session.agent.waitForIdle();

      const workerMessages = session.messages.filter(isWorkerResultMessage);
      expect(workerMessages).toHaveLength(2);
      expect(workerMessages.map((message) => message.details.title)).toEqual([
        "Beta task",
        "Alpha task",
      ]);
      const synthesisGroupIds = workerMessages.map((message) => message.details.synthesisGroupId);
      expect(synthesisGroupIds.every((id) => typeof id === "string" && id.length > 0)).toBe(true);
      expect(new Set(synthesisGroupIds).size).toBe(1);
      expect(workerMessages.map((message) => message.details.synthesisGroupSize)).toEqual([2, 2]);
      expect(assistantTexts(session.messages).at(-1)).toBe("PARALLEL_SYNTHESIS:true:true");
      expect(harness.requests.filter((request) => request.kind === "parent-synthesis"))
        .toHaveLength(1);
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(2);
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

      const workerMessages = session.messages.filter(isWorkerResultMessage);
      expect(workerMessages).toHaveLength(1);
      expect(workerMessages[0]?.details).toMatchObject({
        title: "Failing provider task",
        status: "failed",
        outcome: {
          status: "failed",
          message: "DETERMINISTIC_PROVIDER_FAILURE",
        },
      });
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
      expect(orchestrationResult).toBeDefined();
      expect(readResult).toBeDefined();
      expect(harness.events.filter((event) => event === "parent:agent_start")).toHaveLength(1);

      await harness.runtime.dispose();
      harness.disposed = true;
      expect(getProcessHost()).toBeUndefined();
    } finally {
      await disposeHarness(harness);
    }
  }, 4_000);
});
