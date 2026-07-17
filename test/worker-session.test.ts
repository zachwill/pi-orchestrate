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
  type AgentSessionEvent,
  type ResourceLoader,
  SettingsManager,
  type Skill,
} from "@earendil-works/pi-coding-agent";
import type { WorkerDefinition } from "../extension/domain.js";
import {
  createWorkerSessionFactory,
  type WorkerSessionDependencies,
  type WorkerSessionFactoryOptions,
} from "../extension/worker-session.js";

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
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      ...usage,
      cost: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        total: 0,
        ...usage.cost,
      },
    },
  };
}

class FakeSession {
  readonly messages: AgentMessage[] = [];
  readonly prompt = mock(async (_task: string) => {});
  readonly abort = mock(async () => {});
  readonly dispose = mock(() => {});

  private readonly listeners = new Set<(event: AgentSessionEvent) => void>();
  readonly unsubscribe = mock(() => {});

  constructor(readonly sessionFile: string | undefined = "/sessions/child.jsonl") {}

  subscribe(listener: (event: AgentSessionEvent) => void): () => void {
    this.listeners.add(listener);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.listeners.delete(listener);
      this.unsubscribe();
    };
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

  private emit(event: AgentSessionEvent): void {
    for (const listener of this.listeners) listener(event);
  }
}

type ResourceLoaderInput = Parameters<WorkerSessionDependencies["createResourceLoader"]>[0];
type SessionManagerInput = Parameters<WorkerSessionDependencies["createSessionManager"]>[0];
type SettingsInput = Parameters<WorkerSessionDependencies["createSettingsManager"]>[0];
type SettingsOptions = Parameters<WorkerSessionDependencies["createSettingsManager"]>[1];
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
  settingsInputs: Array<{ settings: SettingsInput; options: SettingsOptions }>;
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
  const settingsInputs: Array<{ settings: SettingsInput; options: SettingsOptions }> = [];
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

  const dependencies: WorkerSessionDependencies = {
    createResourceLoader(options) {
      loaderOptions.push(options);
      return loader;
    },
    createSessionManager(input) {
      sessionManagerInputs.push(input);
      return { kind: "session-manager", entries: [] };
    },
    createSettingsManager(settings, settingsOptions) {
      settingsInputs.push({ settings, options: settingsOptions });
      return { kind: "settings-manager" };
    },
    async createModelRuntime(input) {
      modelRuntimeInputs.push(input);
      return runtime as unknown as ModelRuntime;
    },
    async createAgentSession(input) {
      agentInputs.push(input);
      return { session };
    },
  };

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
    ...overrides,
  } as unknown as ModelRegistry;
}

function options(
  overrides: Partial<WorkerSessionFactoryOptions> = {},
): WorkerSessionFactoryOptions {
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

async function writeSkill(filePath: string, name: string, description: string): Promise<void> {
  await mkdir(join(filePath, ".."), { recursive: true });
  await Bun.write(filePath, `---\nname: ${name}\ndescription: ${description}\n---\n\n# ${name}\n`);
}

describe("worker session factory", () => {
  test("builds isolated resources, exact skills/tools, settings, and a direct-child prompt", async () => {
    const h = harness();
    const factory = createWorkerSessionFactory(h.dependencies);
    const factoryOptions = options();
    await factory.create(factoryOptions);

    expect(h.reload).toHaveBeenCalledTimes(1);
    const loader = h.loaderOptions[0]!;
    expect(loader).toMatchObject({
      cwd: "/project",
      agentDir: "/agent",
      noExtensions: true,
      noSkills: false,
      noPromptTemplates: true,
      noThemes: true,
      noContextFiles: false,
    });
    expect(loader.appendSystemPrompt[0]).toBe("Worker system prompt.");
    expect(loader.appendSystemPrompt[1]).toContain("direct child worker session");
    expect(loader.appendSystemPrompt[1]).toContain("Do not spawn");

    const filtered = loader.skillsOverride!({
      skills: [skill("alpha"), skill("other"), skill("beta")],
      diagnostics: [],
    });
    expect(filtered.skills.map((loadedSkill) => loadedSkill.name)).toEqual(["alpha", "beta"]);
    expect(filtered.diagnostics).toEqual([]);

    expect(h.settingsInputs).toEqual([{
      settings: { compaction: { enabled: false } },
      options: { projectTrusted: true },
    }]);
    expect(h.modelRuntimeInputs).toEqual([{
      authPath: "/agent/auth.json",
      modelsPath: "/agent/models.json",
    }]);
    expect(h.runtime.refresh).toHaveBeenCalledTimes(2);
    expect(h.runtime.refresh).toHaveBeenNthCalledWith(1, { allowNetwork: false });
    expect(h.runtime.refresh).toHaveBeenNthCalledWith(2, { allowNetwork: false });
    expect(h.agentInputs[0]!.model).toBe(h.runtime.models[0]);
    expect(h.agentInputs[0]!.modelRuntime).toBe(h.runtime);
    expect(h.agentInputs[0]!.tools).toEqual(["read", "grep", "find", "ls"]);
    expect(h.agentInputs[0]!.resourceLoader).toBeDefined();
    expect(h.agentInputs[0]!.sessionManager).toBeDefined();
    expect(h.agentInputs[0]!.settingsManager).toBeDefined();
    expect("authStorage" in h.agentInputs[0]!).toBe(false);
    expect("modelRegistry" in h.agentInputs[0]!).toBe(false);
  });

  test("loads no skills when none are selected and excludes context in untrusted projects", async () => {
    const h = harness();
    await createWorkerSessionFactory(h.dependencies).create(
      options({ definition: definition({ skills: [] }), projectTrusted: false }),
    );

    expect(h.settingsInputs[0]!.options).toEqual({ projectTrusted: false });
    expect(h.loaderOptions[0]).toMatchObject({ noSkills: true, noContextFiles: true });
    const filtered = h.loaderOptions[0]!.skillsOverride!({
      skills: [skill("alpha")],
      diagnostics: [],
    });
    expect(filtered.skills).toEqual([]);
  });

  test("copies provider/auth state and resolves the strict selection from the child runtime", async () => {
    const configuredProvider: ProviderConfig = {
      name: "Custom Provider",
      api: "openai-responses",
      baseUrl: "https://custom.test/v1",
      apiKey: "$CUSTOM_KEY",
      headers: { "X-Configured": "$CUSTOM_HEADER" },
      models: [{
        id: "configured/model",
        name: "Configured Model",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 10_000,
        maxTokens: 1_000,
      }],
    };
    const parentRegistry = registry({
      getRegisteredProviderIds: mock(() => ["provider"]),
      getRegisteredProviderConfig: mock((providerId: string) =>
        providerId === "provider" ? configuredProvider : undefined
      ),
      getApiKeyAndHeaders: mock(async () => ({
        ok: true as const,
        apiKey: "runtime-secret",
        headers: { "X-Configured": "resolved-header", "X-Runtime": "runtime-header" },
      })),
    });
    const runtime = new FakeModelRuntime([]);
    const h = harness(new FakeSession(), ["alpha", "beta"], runtime);

    await createWorkerSessionFactory(h.dependencies).create(
      options({
        definition: definition({
          model: { provider: "provider", modelId: "configured/model" },
        }),
        modelRegistry: parentRegistry,
      }),
    );

    expect(runtime.registrations[0]).toEqual({
      providerId: "provider",
      config: configuredProvider,
    });
    expect(runtime.registrations[1]).toEqual({
      providerId: "provider",
      config: {
        headers: { "X-Configured": "resolved-header", "X-Runtime": "runtime-header" },
      },
    });
    expect(runtime.runtimeApiKeys).toEqual([
      { providerId: "provider", apiKey: "runtime-secret" },
    ]);
    expect(parentRegistry.getApiKeyAndHeaders).toHaveBeenCalledWith(runtime.models[0]);
    expect(h.agentInputs[0]!.model).toBe(runtime.models[0]);
    expect(h.agentInputs[0]!.modelRuntime).toBe(runtime);

    const missingHarness = harness(new FakeSession(), ["alpha", "beta"], new FakeModelRuntime([]));
    const missingFactory = createWorkerSessionFactory(missingHarness.dependencies);
    await expect(
      missingFactory.create(
        options({
          definition: definition({
            model: { provider: "provider", modelId: "missing" },
          }),
        }),
      ),
    ).rejects.toThrow('configured model "provider/missing" was not found');
    expect(missingHarness.loaderOptions).toHaveLength(0);
    expect(missingHarness.agentInputs).toHaveLength(0);
  });

  test("copies dynamic providers and selected auth through real Pi ModelRuntimes", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-orchestrate-model-runtime-"));
    try {
      const selectedProvider: ProviderConfig = {
        name: "Actual Custom Provider",
        api: "openai-responses",
        baseUrl: "https://actual-custom.test/v1",
        apiKey: "configured-fallback",
        headers: { "X-Registered": "registered-header" },
        models: [{
          id: "actual-model",
          name: "Actual Model",
          reasoning: false,
          input: ["text"],
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          contextWindow: 10_000,
          maxTokens: 1_000,
        }],
      };
      const unrelatedProvider: ProviderConfig = {
        api: "openai-responses",
        baseUrl: "https://unrelated.test/v1",
        apiKey: "unrelated-key",
        models: [{
          id: "unrelated-model",
          name: "Unrelated Model",
          reasoning: false,
          input: ["text"],
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          contextWindow: 10_000,
          maxTokens: 1_000,
        }],
      };
      const parentRuntime = await ModelRuntime.create({
        authPath: join(root, "parent-auth.json"),
        modelsPath: null,
        allowModelNetwork: false,
      });
      parentRuntime.registerProvider("actual-custom", selectedProvider);
      parentRuntime.registerProvider("unrelated-custom", unrelatedProvider);
      await parentRuntime.setRuntimeApiKey("actual-custom", "parent-runtime-key");

      const h = harness(new FakeSession(), ["alpha", "beta"]);
      let childRuntime: ModelRuntime | undefined;
      h.dependencies.createModelRuntime = async (input) => {
        childRuntime = await ModelRuntime.create({ ...input, allowModelNetwork: false });
        return childRuntime;
      };
      await createWorkerSessionFactory(h.dependencies).create(options({
        agentDir: root,
        definition: definition({
          model: { provider: "actual-custom", modelId: "actual-model" },
        }),
        modelRegistry: new ModelRegistry(parentRuntime),
      }));

      expect(childRuntime).toBeDefined();
      expect(childRuntime!.getRegisteredProviderIds()).toEqual([
        "actual-custom",
        "unrelated-custom",
      ]);
      expect(childRuntime!.getRegisteredProviderConfig("actual-custom")).toEqual(
        selectedProvider,
      );
      expect(childRuntime!.getRegisteredProviderConfig("unrelated-custom")).toEqual(
        unrelatedProvider,
      );
      const childModel = childRuntime!.getModel("actual-custom", "actual-model")!;
      expect(h.agentInputs[0]!.modelRuntime).toBe(childRuntime);
      expect(h.agentInputs[0]!.model).toEqual(childModel);
      const childAuth = await childRuntime!.getAuth(childModel);
      expect(childAuth?.auth.apiKey).toBe("parent-runtime-key");
      expect(childAuth?.auth.headers).toEqual({ "X-Registered": "registered-header" });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("refreshes and executes a real refreshModels-only provider in the child runtime", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-orchestrate-refresh-model-runtime-"));
    const refreshNetworkModes: boolean[] = [];
    const executedModels: Array<{ provider: string; modelId: string }> = [];
    const dynamicModel = {
      id: "refresh-only-model",
      name: "Refresh Only Model",
      reasoning: false,
      input: ["text" as const],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 10_000,
      maxTokens: 1_000,
    };
    const dynamicProvider: ProviderConfig = {
      name: "Refresh Only Provider",
      api: "openai-responses",
      baseUrl: "https://refresh-only.test/v1",
      apiKey: "refresh-only-key",
      headers: { "X-Refresh-Provider": "registered-header" },
      async refreshModels({ allowNetwork }) {
        refreshNetworkModes.push(allowNetwork);
        return [dynamicModel];
      },
      streamSimple(selectedModel) {
        executedModels.push({
          provider: selectedModel.provider,
          modelId: selectedModel.id,
        });
        return streamedAssistant(selectedModel, "refresh-only response");
      },
    };

    try {
      expect(dynamicProvider.models).toBeUndefined();
      const parentRuntime = await ModelRuntime.create({
        authPath: join(root, "parent-auth.json"),
        modelsPath: null,
        allowModelNetwork: false,
      });
      parentRuntime.registerProvider("refresh-only", dynamicProvider);
      const parentRefresh = await parentRuntime.refresh({ allowNetwork: false });
      expect(parentRefresh.errors.size).toBe(0);
      expect(parentRuntime.getModel("refresh-only", dynamicModel.id)).toBeDefined();

      const h = harness(new FakeSession(), ["alpha", "beta"]);
      let childRuntime: ModelRuntime | undefined;
      h.dependencies.createModelRuntime = async (input) => {
        childRuntime = await ModelRuntime.create({ ...input, allowModelNetwork: false });
        return childRuntime;
      };
      const handle = await createWorkerSessionFactory(h.dependencies).create(options({
        agentDir: root,
        definition: definition({
          model: { provider: "refresh-only", modelId: dynamicModel.id },
        }),
        modelRegistry: new ModelRegistry(parentRuntime),
      }));

      const selectedModel = h.agentInputs[0]!.model;
      expect(h.agentInputs[0]!.modelRuntime).toBe(childRuntime);
      expect(selectedModel).toEqual(
        childRuntime!.getModel("refresh-only", dynamicModel.id),
      );
      const response = await childRuntime!.completeSimple(selectedModel, { messages: [] });
      expect(response.content).toEqual([{ type: "text", text: "refresh-only response" }]);
      expect(executedModels).toEqual([{
        provider: "refresh-only",
        modelId: dynamicModel.id,
      }]);
      expect(refreshNetworkModes.length).toBeGreaterThan(0);
      expect(refreshNetworkModes.every((allowNetwork) => allowNetwork === false)).toBe(true);
      handle.dispose();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("fails clearly when child model refresh reports provider errors", async () => {
    const h = harness();
    h.runtime.refresh.mockResolvedValueOnce({
      aborted: false,
      errors: new Map([["broken-provider", new Error("catalog unavailable")]]),
    });

    await expect(
      createWorkerSessionFactory(h.dependencies).create(options()),
    ).rejects.toThrow(
      'Worker "scout" failed to refresh child model providers: broken-provider: catalog unavailable',
    );
    expect(h.loaderOptions).toHaveLength(0);
    expect(h.agentInputs).toHaveLength(0);
  });

  test("fails before resource creation when neither worker nor parent supplies a model", async () => {
    const h = harness();

    await expect(
      createWorkerSessionFactory(h.dependencies).create(options({ parentModel: undefined })),
    ).rejects.toThrow("no configured model and no parent model is available");

    expect(h.modelRuntimeInputs).toHaveLength(0);
    expect(h.settingsInputs).toHaveLength(0);
    expect(h.loaderOptions).toHaveLength(0);
    expect(h.sessionManagerInputs).toHaveLength(0);
    expect(h.agentInputs).toHaveLength(0);
  });

  test("fails and disposes resources when an explicitly selected skill was not loaded", async () => {
    const h = harness(new FakeSession(), ["alpha"]);

    await expect(
      createWorkerSessionFactory(h.dependencies).create(options()),
    ).rejects.toThrow('Worker "scout" selected skills were not loaded: beta');

    expect(h.reload).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
    expect(h.sessionManagerInputs).toHaveLength(0);
    expect(h.agentInputs).toHaveLength(0);
  });

  test("propagates trust to a real loader and excludes untrusted project context and skills", async () => {
    const root = await mkdtemp(join(tmpdir(), "pi-orchestrate-trust-"));
    const agentDir = join(root, "agent");
    const projectRoot = join(root, "project");
    const cwd = join(projectRoot, "nested");
    const globalShared = join(agentDir, "skills", "shared", "SKILL.md");
    const projectShared = join(cwd, ".pi", "skills", "shared", "SKILL.md");
    const ancestorSkill = join(projectRoot, ".agents", "skills", "ancestor", "SKILL.md");

    try {
      await mkdir(join(projectRoot, ".git"), { recursive: true });
      await writeSkill(globalShared, "trust-shared", "global shared skill");
      await writeSkill(
        join(agentDir, "skills", "unselected", "SKILL.md"),
        "unselected",
        "must be filtered",
      );
      await writeSkill(projectShared, "trust-shared", "project shared skill");
      await writeSkill(ancestorSkill, "ancestor-skill", "ancestor project skill");
      await Bun.write(join(agentDir, "AGENTS.md"), "global context");
      await Bun.write(join(projectRoot, "AGENTS.md"), "ancestor project context");
      await Bun.write(join(cwd, "AGENTS.md"), "local project context");

      const runtime = new FakeModelRuntime([model("parent", "selected")]);
      const sessions: FakeSession[] = [];
      const loaders: DefaultResourceLoader[] = [];
      const dependencies: WorkerSessionDependencies = {
        createResourceLoader(loaderOptions) {
          const loader = new DefaultResourceLoader(loaderOptions);
          loaders.push(loader);
          return loader;
        },
        createSessionManager: () => ({ entries: [] }),
        createSettingsManager: (settings, settingsOptions) =>
          SettingsManager.inMemory(settings, {
            projectTrusted: settingsOptions.projectTrusted,
          }),
        createModelRuntime: async () => runtime as unknown as ModelRuntime,
        createAgentSession: async () => {
          const session = new FakeSession();
          sessions.push(session);
          return { session };
        },
      };
      const factory = createWorkerSessionFactory(dependencies);
      const selectedDefinition = definition({
        skills: ["trust-shared", "ancestor-skill"],
      });

      await expect(factory.create(options({
        cwd,
        agentDir,
        projectTrusted: false,
        definition: selectedDefinition,
      }))).rejects.toThrow("selected skills were not loaded: ancestor-skill");

      expect(loaders[0]!.getSkills().skills.map((loadedSkill) => loadedSkill.name)).toEqual([
        "trust-shared",
      ]);
      expect(loaders[0]!.getSkills().skills[0]!.filePath).toBe(globalShared);
      expect(loaders[0]!.getAgentsFiles().agentsFiles).toEqual([]);

      const handle = await factory.create(options({
        cwd,
        agentDir,
        projectTrusted: true,
        definition: selectedDefinition,
      }));
      const trustedSkills = loaders[1]!.getSkills().skills;
      expect(trustedSkills.map((loadedSkill) => loadedSkill.name)).toEqual([
        "trust-shared",
        "ancestor-skill",
      ]);
      expect(trustedSkills.find((loadedSkill) => loadedSkill.name === "trust-shared")!.filePath)
        .toBe(projectShared);
      expect(trustedSkills.find((loadedSkill) => loadedSkill.name === "ancestor-skill")!.filePath)
        .toBe(ancestorSkill);
      expect(loaders[1]!.getAgentsFiles().agentsFiles.map((file) => file.path)).toEqual(
        expect.arrayContaining([join(projectRoot, "AGENTS.md"), join(cwd, "AGENTS.md")]),
      );
      handle.dispose();
      expect(sessions[0]!.dispose).toHaveBeenCalledTimes(1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("disposes the loader on reload, session creation, and durability failures", async () => {
    const reloadFailure = harness();
    reloadFailure.reload.mockImplementation(async () => {
      throw new Error("reload failed");
    });
    await expect(
      createWorkerSessionFactory(reloadFailure.dependencies).create(options()),
    ).rejects.toThrow("reload failed");
    expect(reloadFailure.loaderDispose).toHaveBeenCalledTimes(1);

    const creationFailure = harness();
    creationFailure.dependencies.createAgentSession = async () => {
      throw new Error("session failed");
    };
    await expect(
      createWorkerSessionFactory(creationFailure.dependencies).create(options()),
    ).rejects.toThrow("session failed");
    expect(creationFailure.loaderDispose).toHaveBeenCalledTimes(1);

    const noDurability = harness(new FakeSession(""));
    await expect(
      createWorkerSessionFactory(noDurability.dependencies).create(options()),
    ).rejects.toThrow("durable storage");
    expect(noDurability.session.dispose).toHaveBeenCalledTimes(1);
    expect(noDurability.loaderDispose).toHaveBeenCalledTimes(1);
  });

  test("creates fresh session lineage without importing a parent transcript", async () => {
    const h = harness();
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());

    expect(handle.sessionFile).toBe("/sessions/child.jsonl");
    expect(h.sessionManagerInputs).toEqual([
      { cwd: "/project", parentSessionFile: "/sessions/parent.jsonl" },
    ]);
    expect((h.agentInputs[0]!.sessionManager as { entries: unknown[] }).entries).toEqual([]);
  });
});

describe("worker session handle", () => {
  test("returns assistant text with one-shot or reusable success status", async () => {
    const oneShotHarness = harness();
    oneShotHarness.session.prompt.mockImplementation(async () => {
      oneShotHarness.session.finishTurn(assistant("first block"));
    });
    const oneShot = await createWorkerSessionFactory(oneShotHarness.dependencies).create(options());
    expect(await oneShot.prompt("task")).toEqual({
      status: "completed",
      assistantText: "first block\nsecond block",
    });

    const reusableHarness = harness();
    reusableHarness.session.prompt.mockImplementation(async () => {
      reusableHarness.session.finishTurn(assistant("continue"));
    });
    const reusable = await createWorkerSessionFactory(reusableHarness.dependencies).create(
      options({ definition: definition({ lifecycle: "reusable" }) }),
    );
    expect(await reusable.prompt("instructions")).toEqual({
      status: "ready",
      assistantText: "continue\nsecond block",
    });
  });

  test("classifies assistant errors, assistant aborts, thrown errors, and requested aborts", async () => {
    for (const [stopReason, expectedStatus] of [
      ["error", "failed"],
      ["aborted", "aborted"],
    ] as const) {
      const h = harness();
      h.session.prompt.mockImplementation(async () => {
        h.session.finishTurn(assistant("partial", stopReason, {}, `${stopReason} detail`));
      });
      const handle = await createWorkerSessionFactory(h.dependencies).create(options());
      expect(await handle.prompt("task")).toEqual({
        status: expectedStatus,
        message: `${stopReason} detail`,
        assistantText: "partial\nsecond block",
      });
    }

    const failed = harness();
    failed.session.prompt.mockImplementation(async () => {
      throw new Error("request failed");
    });
    const failedHandle = await createWorkerSessionFactory(failed.dependencies).create(options());
    expect(await failedHandle.prompt("task")).toEqual({
      status: "failed",
      message: "request failed",
    });

    let rejectPrompt!: (error: Error) => void;
    const aborting = harness();
    aborting.session.prompt.mockImplementation(
      () => new Promise<void>((_resolve, reject) => (rejectPrompt = reject)),
    );
    aborting.session.abort.mockImplementation(async () => rejectPrompt(new Error("cancelled")));
    const abortingHandle = await createWorkerSessionFactory(aborting.dependencies).create(options());
    const prompt = abortingHandle.prompt("task");
    await abortingHandle.abort();
    expect(await prompt).toEqual({ status: "aborted", message: "cancelled" });
  });

  test("accumulates turn usage and removes usage subscriptions", async () => {
    const h = harness();
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());
    const updates: unknown[] = [];
    const unsubscribe = handle.subscribeUsage((usage) => updates.push(usage));

    h.session.finishTurn(
      assistant("one", "stop", {
        input: 10,
        output: 4,
        cacheRead: 2,
        cacheWrite: 1,
        totalTokens: 17,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.25 },
      }),
    );
    h.session.finishTurn(
      assistant("two", "stop", {
        input: 5,
        output: 3,
        cacheRead: 1,
        cacheWrite: 2,
        totalTokens: 11,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.5 },
      }),
    );

    expect(updates).toEqual([
      {
        input: 10,
        output: 4,
        cacheRead: 2,
        cacheWrite: 1,
        cost: 0.25,
        contextTokens: 17,
        turns: 1,
      },
      {
        input: 15,
        output: 7,
        cacheRead: 3,
        cacheWrite: 3,
        cost: 0.75,
        contextTokens: 11,
        turns: 2,
      },
    ]);

    unsubscribe();
    unsubscribe();
    h.session.finishTurn(assistant("ignored"));
    expect(updates).toHaveLength(2);
  });

  test("isolates throwing usage and activity listeners", async () => {
    const h = harness();
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());
    const usageUpdates: number[] = [];
    const activityUpdates: Array<string | undefined> = [];

    handle.subscribeUsage(() => {
      throw new Error("usage listener failed");
    });
    handle.subscribeUsage((usage) => usageUpdates.push(usage.turns));
    handle.subscribeActivity(() => {
      throw new Error("activity listener failed");
    });
    handle.subscribeActivity((activity) => activityUpdates.push(activity));

    h.session.finishTurn(assistant("one"));
    h.session.startTool("call-1", "read");

    expect(usageUpdates).toEqual([1]);
    expect(activityUpdates).toEqual(["read"]);
  });

  test("tracks overlapping tools in start order and falls back to the latest active tool", async () => {
    const h = harness();
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());
    const updates: Array<string | undefined> = [];
    const unsubscribe = handle.subscribeActivity((activity) => updates.push(activity));

    h.session.startTool("call-1", "read");
    h.session.startTool("call-2", "grep");
    h.session.startTool("call-3", "bash");
    h.session.endTool("call-2", "grep");
    h.session.endTool("stale-call", "write");
    expect(updates).toEqual(["read", "grep", "bash"]);

    h.session.endTool("call-3", "bash");
    expect(updates).toEqual(["read", "grep", "bash", "read"]);
    h.session.endTool("call-1", "read");
    expect(updates).toEqual(["read", "grep", "bash", "read", undefined]);

    unsubscribe();
    unsubscribe();
    h.session.startTool("call-4", "edit");
    expect(updates).toHaveLength(5);
  });

  test("preserves same-tool overlap, ignores stale ends, and stops activity after dispose", async () => {
    const h = harness();
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());
    const updates: Array<string | undefined> = [];
    handle.subscribeActivity((activity) => updates.push(activity));

    h.session.startTool("read-1", "read");
    h.session.startTool("read-2", "read");
    h.session.endTool("read-2", "wrong-name");
    expect(updates).toEqual(["read"]);
    h.session.endTool("read-1", "read");
    expect(updates).toEqual(["read"]);
    h.session.endTool("read-2", "read");
    expect(updates).toEqual(["read", undefined]);

    h.session.startTool("find-1", "find");
    handle.dispose();
    h.session.endTool("find-1", "find");
    h.session.startTool("after-dispose", "write");
    expect(updates).toEqual(["read", undefined, "find"]);
    expect(h.session.unsubscribe).toHaveBeenCalledTimes(1);
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
  });

  test("dispose cleanup continues when unsubscribe and session disposal throw", async () => {
    const h = harness();
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());
    const activityUpdates: Array<string | undefined> = [];
    handle.subscribeActivity((activity) => activityUpdates.push(activity));
    h.session.unsubscribe.mockImplementation(() => {
      throw new Error("unsubscribe failed");
    });
    h.session.dispose.mockImplementation(() => {
      throw new Error("session dispose failed");
    });

    expect(() => handle.dispose()).not.toThrow();
    h.session.startTool("after-dispose", "read");

    expect(activityUpdates).toEqual([]);
    expect(h.session.unsubscribe).toHaveBeenCalledTimes(1);
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
  });

  test("awaits abort and disposes the session and subscription exactly once", async () => {
    const h = harness();
    let releaseAbort!: () => void;
    h.session.abort.mockImplementation(() => new Promise<void>((resolve) => (releaseAbort = resolve)));
    const handle = await createWorkerSessionFactory(h.dependencies).create(options());

    let abortSettled = false;
    const abort = handle.abort().then(() => (abortSettled = true));
    await Promise.resolve();
    expect(abortSettled).toBe(false);
    releaseAbort();
    await abort;
    expect(abortSettled).toBe(true);

    handle.dispose();
    handle.dispose();
    expect(h.session.unsubscribe).toHaveBeenCalledTimes(1);
    expect(h.session.dispose).toHaveBeenCalledTimes(1);
    expect(h.loaderDispose).toHaveBeenCalledTimes(1);
  });
});
