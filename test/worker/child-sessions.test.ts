import { describe, expect, mock, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import {
  createAssistantMessageEventStream,
  type Api,
  type AssistantMessage,
  type Model,
} from "@earendil-works/pi-ai";
import {
  DefaultResourceLoader,
  ModelRegistry,
  ModelRuntime,
  SessionManager,
  type AgentSessionEvent,
  type PromptOptions,
  type ResourceLoader,
  SettingsManager,
  type Skill,
} from "@earendil-works/pi-coding-agent";
import type { WorkerDefinition } from "../../extension/catalog/definition.ts";
import { Effect, Fiber, ManagedRuntime } from "effect";
import { ChildSessions, createChildSessionsLayer } from "../../extension/worker/child-sessions.ts";
import {
  isOrchestrationExtensionPath,
  WorkerAgentSessionAcquisitionError,
  WorkerModelAcquisitionError,
  WorkerResourceAcquisitionError,
  WorkerSessionAbortError,
  type WorkerSessionDependencies,
  type ChildSessionOptions,
  type WorkerSessionHandle,
  type WorkerSessionObservation,
} from "../../extension/worker/session.ts";

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

function streamedAssistant(selectedModel: Model<Api>, text: string) {
  const stream = createAssistantMessageEventStream();
  const output: AssistantMessage = {
    role: "assistant",
    content: [{ type: "text", text }],
    api: selectedModel.api,
    provider: selectedModel.provider,
    model: selectedModel.id,
    stopReason: "stop",
    timestamp: Date.now(),
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
  stream.push({ type: "start", partial: output });
  stream.push({ type: "done", reason: "stop", message: output });
  stream.end(output);
  return stream;
}

function assistant(
  text: string,
  stopReason: AssistantMessage["stopReason"] = "stop",
  usage: Partial<AssistantMessage["usage"]> = {},
  errorMessage?: string,
): AssistantMessage {
  const { cost, ...usageFields } = usage;
  return {
    role: "assistant",
    content: [
      { type: "text", text },
      { type: "thinking", thinking: "not returned" },
      { type: "text", text: "second block" },
    ],
    api: "openai-responses",
    provider: "test",
    model: "worker-model",
    stopReason,
    errorMessage,
    timestamp: Date.now(),
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      ...usageFields,
      cost: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        total: 0,
        ...cost,
      },
    },
  };
}

class FakeSession {
  readonly messages: AgentMessage[] = [];
  readonly prompt = mock(async (_task: string, _options?: PromptOptions) => {});
  readonly abortCompaction = mock(() => {});
  readonly abort = mock(async () => {});
  readonly dispose = mock(() => {});
  readonly bindExtensions = mock(async (_bindings: { mode: "print" }) => {});

  private readonly listeners = new Set<(event: AgentSessionEvent) => void>();
  private readonly historicalListeners: Array<(event: AgentSessionEvent) => void> = [];
  readonly unsubscribe = mock(() => {});

  constructor(readonly sessionFile: string | undefined = "/sessions/child.jsonl") {}

  subscribe(listener: (event: AgentSessionEvent) => void): () => void {
    this.listeners.add(listener);
    this.historicalListeners.push(listener);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.listeners.delete(listener);
      this.unsubscribe();
    };
  }

  startMessage(message: AgentMessage): void {
    this.emit({ type: "message_start", message });
  }

  finishTurn(message: AssistantMessage): void {
    this.messages.push(message);
    const event: Extract<AgentSessionEvent, { type: "turn_end" }> = {
      type: "turn_end",
      message,
      toolResults: [],
    };
    this.emit(event);
  }

  startTool(toolCallId: string, toolName: string): void {
    this.emit({
      type: "tool_execution_start",
      toolCallId,
      toolName,
      args: {},
    });
  }

  endTool(toolCallId: string, toolName: string): void {
    this.emit({
      type: "tool_execution_end",
      toolCallId,
      toolName,
      result: {},
      isError: false,
    });
  }

  replayToHistoricalListeners(event: AgentSessionEvent): void {
    for (const listener of this.historicalListeners) listener(event);
  }

  private emit(event: AgentSessionEvent): void {
    for (const listener of this.listeners) listener(event);
  }
}

class SubscriptionFailureSession extends FakeSession {
  override subscribe(_listener: (event: AgentSessionEvent) => void): () => void {
    throw new Error("subscription failed");
  }
}

type ServicesInput = Parameters<WorkerSessionDependencies["createServices"]>[0];
type ResourceLoaderInput = ServicesInput["resourceLoaderOptions"];
type SessionManagerInput = Parameters<WorkerSessionDependencies["createSessionManager"]>[0];
type SettingsInput = Parameters<WorkerSessionDependencies["createSettingsManager"]>[0];
type ModelRuntimeInput = Parameters<WorkerSessionDependencies["createModelRuntime"]>[0];
type AgentSessionInput = Parameters<WorkerSessionDependencies["createAgentSession"]>[0];
type ProviderConfig = NonNullable<ReturnType<ModelRegistry["getRegisteredProviderConfig"]>>;

class FakeModelRuntime {
  readonly registrations: Array<{ providerId: string; config: ProviderConfig }> = [];
  readonly runtimeApiKeys: Array<{ providerId: string; apiKey: string }> = [];
  readonly refresh = mock(async (_options?: { allowNetwork?: boolean }) => ({
    aborted: false,
    errors: new Map<string, Error>(),
  }));

  constructor(readonly models: Model<Api>[]) {}

  getModel(providerId: string, modelId: string): Model<Api> | undefined {
    return this.models.find((candidate) =>
      candidate.provider === providerId && candidate.id === modelId
    );
  }

  registerProvider(providerId: string, config: ProviderConfig): void {
    this.registrations.push({ providerId, config });
    for (const configuredModel of config.models ?? []) {
      const existing = this.getModel(providerId, configuredModel.id);
      if (existing) continue;
      this.models.push({
        ...configuredModel,
        provider: providerId,
        api: configuredModel.api ?? config.api ?? "openai-responses",
        baseUrl: configuredModel.baseUrl ?? config.baseUrl ?? "https://example.test",
      });
    }
  }

  async setRuntimeApiKey(providerId: string, apiKey: string): Promise<void> {
    this.runtimeApiKeys.push({ providerId, apiKey });
  }
}

interface Harness {
  session: FakeSession;
  runtime: FakeModelRuntime;
  loaderOptions: ResourceLoaderInput[];
  sessionManagerInputs: SessionManagerInput[];
  settingsInputs: SettingsInput[];
  modelRuntimeInputs: ModelRuntimeInput[];
  agentInputs: AgentSessionInput[];
  reload: ReturnType<typeof mock>;
  loaderDispose: ReturnType<typeof mock>;
  dependencies: WorkerSessionDependencies;
}

function skill(name: string): Skill {
  const filePath = `/skills/${name}/SKILL.md`;
  return {
    name,
    description: `${name} skill`,
    filePath,
    baseDir: `/skills/${name}`,
    sourceInfo: {
      path: filePath,
      source: "test",
      scope: "user",
      origin: "top-level",
    },
    disableModelInvocation: false,
  };
}

function harness(
  session = new FakeSession(),
  loadedSkillNames: readonly string[] = ["alpha", "beta"],
  runtime = new FakeModelRuntime([model("parent", "selected")]),
): Harness {
  const loaderOptions: ResourceLoaderInput[] = [];
  const sessionManagerInputs: SessionManagerInput[] = [];
  const settingsInputs: SettingsInput[] = [];
  const modelRuntimeInputs: ModelRuntimeInput[] = [];
  const agentInputs: AgentSessionInput[] = [];
  const reload = mock(async () => {});
  const loaderDispose = mock(() => {});

  const loader = {
    reload,
    dispose: loaderDispose,
    getExtensions: () => ({ extensions: [], errors: [], runtime: {} }),
    getSkills: () => ({ skills: loadedSkillNames.map(skill), diagnostics: [] }),
    getPrompts: () => ({ prompts: [], diagnostics: [] }),
    getThemes: () => ({ themes: [], diagnostics: [] }),
    getAgentsFiles: () => ({ agentsFiles: [] }),
    getSystemPrompt: () => undefined,
    getAppendSystemPrompt: () => [],
    extendResources: () => {},
  } as unknown as ResourceLoader;

  const dependencies = {
    createSettingsManager(input: {
      cwd: string;
      agentDir: string;
      projectTrusted: boolean;
      compaction: WorkerDefinition["compaction"];
    }) {
      settingsInputs.push(input);
      return { kind: "settings-manager" } as unknown as SettingsManager;
    },
    async resolveExtensionPaths() {
      return [];
    },
    createSessionManager(input: SessionManagerInput) {
      sessionManagerInputs.push(input);
      return { kind: "session-manager", entries: [] } as unknown as SessionManager;
    },
    async createModelRuntime(input: ModelRuntimeInput) {
      modelRuntimeInputs.push(input);
      return runtime as unknown as ModelRuntime;
    },
    async createServices(input: {
      cwd: string;
      agentDir: string;
      settingsManager: SettingsManager;
      modelRuntime: ModelRuntime;
      resourceLoaderOptions: ResourceLoaderInput;
    }) {
      loaderOptions.push(input.resourceLoaderOptions);
      try {
        await loader.reload();
      } catch (error) {
        loaderDispose();
        throw error;
      }
      await input.modelRuntime.refresh({ allowNetwork: false });
      return {
        cwd: input.cwd,
        agentDir: input.agentDir,
        settingsManager: input.settingsManager,
        modelRuntime: input.modelRuntime,
        resourceLoader: loader,
        diagnostics: [],
      };
    },
    async createAgentSession(input: AgentSessionInput) {
      agentInputs.push(input);
      return { session };
    },
    createRuntime(input: { session: FakeSession }) {
      return {
        session: input.session,
        async dispose() {
          input.session.dispose();
        },
      };
    },
  } as unknown as WorkerSessionDependencies;

  return {
    session,
    runtime,
    loaderOptions,
    sessionManagerInputs,
    settingsInputs,
    modelRuntimeInputs,
    agentInputs,
    reload,
    loaderDispose,
    dependencies,
  };
}

function definition(overrides: Partial<WorkerDefinition> = {}): WorkerDefinition {
  return {
    name: "scout",
    source: { kind: "package", filePath: "/workers/scout.md" },
    description: "Scout",
    thinking: "medium",
    tools: ["read", "grep", "find", "ls"],
    skills: ["alpha", "beta"],
    compaction: { enabled: false },
    lifecycle: "one-shot",
    systemPrompt: "Worker system prompt.",
    ...overrides,
  };
}

function registry(overrides: Partial<ModelRegistry> = {}): ModelRegistry {
  return {
    find: mock(() => undefined),
    getRegisteredProviderIds: mock(() => []),
    getRegisteredProviderConfig: mock(() => undefined),
    getApiKeyAndHeaders: mock(async () => ({ ok: true } as const)),
    isUsingOAuth: mock(() => false),
    ...overrides,
  } as unknown as ModelRegistry;
}

function options(
  overrides: Partial<ChildSessionOptions> = {},
): ChildSessionOptions {
  return {
    cwd: "/project",
    agentDir: "/agent",
    parentSessionFile: "/sessions/parent.jsonl",
    projectTrusted: true,
    definition: definition(),
    parentModel: model("parent", "selected"),
    modelRegistry: registry(),
    ...overrides,
  };
}

function createChildSessionTestClient(
  dependencies: Partial<WorkerSessionDependencies> = {},
) {
  return {
    acquire(sessionOptions: ChildSessionOptions) {
      return Effect.runPromise(
        Effect.gen(function* () {
          const sessions = yield* ChildSessions;
          const session = yield* sessions.acquire(sessionOptions, (handle) => handle);
          if (!session) return yield* Effect.die(new Error("Expected session adoption"));
          return session;
        }).pipe(Effect.provide(createChildSessionsLayer(dependencies))),
      );
    },
  };
}

class PromiseGate<T = void> {
  readonly promise: Promise<T>;
  private resolvePromise!: (value: T | PromiseLike<T>) => void;

  constructor() {
    this.promise = new Promise<T>((resolve) => {
      this.resolvePromise = resolve;
    });
  }

  resolve(value: T extends void ? undefined : T): void {
    this.resolvePromise(value as T);
  }
}

describe("child session acquisition handoff", () => {
  test("interruption abandons waiting while late success is disposed exactly once", async () => {
    const h = harness();
    const started = new PromiseGate();
    const release = new PromiseGate();
    const disposed = new PromiseGate();
    const finalized = new PromiseGate();
    h.dependencies.createModelRuntime = async () => {
      started.resolve(undefined);
      await release.promise;
      return h.runtime as unknown as ModelRuntime;
    };
    h.session.dispose.mockImplementation(() => disposed.resolve(undefined));
    h.loaderDispose.mockImplementation(() => finalized.resolve(undefined));
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);

    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), (session) => session),
    );
    await started.promise;
    await effectRuntime.runPromise(Fiber.interrupt(acquisition));
    release.resolve(undefined);
    await disposed.promise;
    await finalized.promise;

    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("shutdown abandons pending acquisition without blocking and reclaims late success", async () => {
    const h = harness();
    const started = new PromiseGate();
    const release = new PromiseGate();
    const disposed = new PromiseGate();
    h.dependencies.createModelRuntime = async () => {
      started.resolve(undefined);
      await release.promise;
      return h.runtime as unknown as ModelRuntime;
    };
    h.session.dispose.mockImplementation(() => disposed.resolve(undefined));
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);

    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), (session) => session),
    );
    await started.promise;
    await effectRuntime.runPromise(sessions.shutdown());
    await expect(effectRuntime.runPromise(Fiber.join(acquisition))).rejects.toMatchObject({
      _tag: "WorkerSession.AcquisitionClosedError",
    });
    expect(h.session.dispose).toHaveBeenCalledTimes(0);

    // The process scope still owns physical acquisition and reclamation, but
    // service shutdown does not wait for the uncancellable Pi Promise.
    const disposingRuntime = effectRuntime.dispose();
    let runtimeDisposed = false;
    void disposingRuntime.then(() => (runtimeDisposed = true));
    await Promise.resolve();
    expect(runtimeDisposed).toBe(false);

    release.resolve(undefined);
    await disposed.promise;
    await disposingRuntime;
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
  });

  test("interrupted shutdown transfers offered cleanup ownership before committing interruption", async () => {
    const h = harness();
    const reservationReached = new PromiseGate();
    const releaseReservation = new PromiseGate();
    const reclamationObserved = new PromiseGate();
    const releaseReclamation = new PromiseGate();
    h.dependencies.beforeAdoptionReservation = () => Effect.yieldNow.pipe(
      Effect.andThen(Effect.promise(() => {
        reservationReached.resolve(undefined);
        return releaseReservation.promise;
      })),
    );
    h.dependencies.onReclamationOpenObserved = mock(() => Effect.promise(() => {
      reclamationObserved.resolve(undefined);
      return releaseReclamation.promise;
    }));
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), (session) => session),
    );
    await reservationReached.promise;

    const shutdownFiber = effectRuntime.runFork(sessions.shutdown());
    await reclamationObserved.promise;
    shutdownFiber.interruptUnsafe();
    releaseReclamation.resolve(undefined);
    expect((await effectRuntime.runPromise(Fiber.await(shutdownFiber)))._tag).toBe("Failure");
    releaseReservation.resolve(undefined);
    await expect(effectRuntime.runPromise(Fiber.join(acquisition))).rejects.toMatchObject({
      _tag: "WorkerSession.AcquisitionClosedError",
    });

    await effectRuntime.runPromise(sessions.shutdown());
    await effectRuntime.runPromise(sessions.shutdown());
    await effectRuntime.dispose();

    expect(h.dependencies.onReclamationOpenObserved).toHaveBeenCalledTimes(1);
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
  });

  test("falls back when root closes between observing open and reclamation admission", async () => {
    const h = harness();
    const runtimeDispose = mock(async () => h.session.dispose());
    h.dependencies.createRuntime = (input) => ({
      session: input.session,
      dispose: runtimeDispose,
    });
    let disposeRoot: Promise<void> | undefined;
    let closeRoot: () => void = () => {
      throw new Error("Runtime root was not installed");
    };
    h.dependencies.onReclamationOpenObserved = mock(() => Effect.sync(() => closeRoot()));
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    closeRoot = () => {
      disposeRoot ??= effectRuntime.dispose();
    };

    await expect(effectRuntime.runPromise(
      sessions.acquire(options(), () => undefined),
    )).rejects.toThrow("All fibers interrupted without error");
    await disposeRoot;

    expect(h.dependencies.onReclamationOpenObserved).toHaveBeenCalledTimes(1);
    expect(runtimeDispose).toHaveBeenCalledTimes(1);
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
  });

  test("late acquisition failure after interruption remains typed and unobserved by the abandoned caller", async () => {
    const h = harness();
    const started = new PromiseGate();
    const release = new PromiseGate();
    h.dependencies.createModelRuntime = async () => {
      started.resolve(undefined);
      await release.promise;
      throw new Error("late model failure");
    };
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);

    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), (session) => session),
    );
    await started.promise;
    await effectRuntime.runPromise(Fiber.interrupt(acquisition));
    release.resolve(undefined);
    await Promise.resolve();
    await Promise.resolve();

    expect(h.loaderOptions).toHaveLength(0);
    expect(h.session.dispose).toHaveBeenCalledTimes(0);
    await effectRuntime.dispose();
  });

  test("interruption requested inside adoption cannot orphan the offered session", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    let adopted: WorkerSessionHandle | undefined;
    let interruptAcquisition: () => void = () => {
      throw new Error("Acquisition fiber was not installed");
    };

    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), (session) => {
        adopted = session;
        interruptAcquisition();
        return session;
      }),
    );
    interruptAcquisition = () => acquisition.interruptUnsafe();

    const acquisitionExit = await effectRuntime.runPromise(Fiber.await(acquisition));
    expect(acquisitionExit._tag).toBe("Failure");
    if (!adopted) throw new Error("Expected session adoption");
    await effectRuntime.runPromise(sessions.shutdown());
    expect(h.session.dispose).toHaveBeenCalledTimes(0);
    await Effect.runPromise(adopted.dispose());
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("interruption requested during rejected adoption cannot skip reclamation", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    let interruptAcquisition: () => void = () => {
      throw new Error("Acquisition fiber was not installed");
    };

    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), () => {
        interruptAcquisition();
        return undefined;
      }),
    );
    interruptAcquisition = () => acquisition.interruptUnsafe();

    expect((await effectRuntime.runPromise(Fiber.await(acquisition)))._tag).toBe("Failure");
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("interruption requested during throwing adoption cannot skip reclamation", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    let interruptAcquisition: () => void = () => {
      throw new Error("Acquisition fiber was not installed");
    };

    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), () => {
        interruptAcquisition();
        throw new Error("adopter failed");
      }),
    );
    interruptAcquisition = () => acquisition.interruptUnsafe();

    expect((await effectRuntime.runPromise(Fiber.await(acquisition)))._tag).toBe("Failure");
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("shutdown during rejecting adoption preserves undefined and reclaims exactly once", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);

    expect(await effectRuntime.runPromise(
      sessions.acquire(options(), () => {
        effectRuntime.runSync(sessions.shutdown());
        return undefined;
      }),
    )).toBeUndefined();
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
  });

  test("shutdown during throwing adoption preserves the defect and reclaims exactly once", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    const adopterFailure = new Error("adopter failed during shutdown");

    await expect(effectRuntime.runPromise(
      sessions.acquire(options(), () => {
        effectRuntime.runSync(sessions.shutdown());
        throw adopterFailure;
      }),
    )).rejects.toBe(adopterFailure);
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
  });

  test("a typed producer failure settled before shutdown remains the acquisition result", async () => {
    const h = harness();
    const producerFailure = new Error("model failed before shutdown");
    h.dependencies.createModelRuntime = async () => {
      throw producerFailure;
    };
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    const acquisition = effectRuntime.runFork(
      sessions.acquire(options(), (session) => session),
    );
    const exit = await effectRuntime.runPromise(Fiber.await(acquisition));

    await effectRuntime.runPromise(sessions.shutdown());
    expect(exit._tag).toBe("Failure");
    await expect(effectRuntime.runPromise(Fiber.join(acquisition))).rejects.toMatchObject({
      _tag: "WorkerSession.ModelAcquisitionError",
      operation: "create-model-runtime",
      cause: producerFailure,
    });
    await effectRuntime.dispose();
  });

  test("adopted sessions transfer out of the process handoff and survive service shutdown", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    const handle = await effectRuntime.runPromise(
      sessions.acquire(options(), (session) => session),
    );
    if (!handle) throw new Error("Expected session adoption");

    await effectRuntime.runPromise(sessions.shutdown());
    expect(h.session.dispose).toHaveBeenCalledTimes(0);
    await Effect.runPromise(handle.dispose());
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("serializes shutdown behind a synchronous adopter and transfers ownership atomically", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);
    let shutdownFiber: ReturnType<typeof effectRuntime.runFork> | undefined;

    const handle = await effectRuntime.runPromise(
      sessions.acquire(options(), (session) => {
        shutdownFiber = effectRuntime.runFork(sessions.shutdown());
        return session;
      }),
    );
    if (!handle || !shutdownFiber) throw new Error("Expected atomic session adoption");
    await effectRuntime.runPromise(Fiber.join(shutdownFiber));

    expect(h.session.dispose).toHaveBeenCalledTimes(0);
    await Effect.runPromise(handle.dispose());
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("allows synchronous adopter reentrancy into shutdown without stealing ownership", async () => {
    const h = harness();
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);

    const handle = await effectRuntime.runPromise(
      sessions.acquire(options(), (session) => {
        effectRuntime.runSync(sessions.shutdown());
        return session;
      }),
    );

    if (!handle) throw new Error("Expected session adoption");
    expect(h.session.dispose).toHaveBeenCalledTimes(0);
    await Effect.runPromise(handle.dispose());
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });

  test("rejected adoption schedules one process-owned reclamation", async () => {
    const h = harness();
    const disposed = new PromiseGate();
    h.session.dispose.mockImplementation(() => disposed.resolve(undefined));
    const effectRuntime = ManagedRuntime.make(createChildSessionsLayer(h.dependencies));
    const sessions = effectRuntime.runSync(ChildSessions);

    expect(await effectRuntime.runPromise(
      sessions.acquire(options(), () => undefined),
    )).toBeUndefined();
    await disposed.promise;
    expect(h.session.dispose).toHaveBeenCalledTimes(1);

    await effectRuntime.runPromise(sessions.shutdown());
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    await effectRuntime.dispose();
  });
});
