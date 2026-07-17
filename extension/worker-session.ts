import { join } from "node:path";
import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Model } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  type ModelRegistry,
  type ResourceLoader,
  SessionManager,
  SettingsManager,
  type AgentSessionEvent,
  type CreateAgentSessionOptions,
} from "@earendil-works/pi-coding-agent";
import type { WorkerDefinition, WorkerOutcome, WorkerUsage } from "./domain.js";

const DIRECT_CHILD_BOUNDARY =
  "You are a direct child worker session. Do not spawn, delegate to, or orchestrate other workers or child sessions. Complete the assigned task yourself and return the result directly to the parent orchestrator.";

interface WorkerAgentSession {
  readonly sessionFile: string | undefined;
  readonly messages: AgentMessage[];
  prompt(instructions: string): Promise<void>;
  abort(): Promise<void>;
  dispose(): void;
  subscribe(listener: (event: AgentSessionEvent) => void): () => void;
}

type ResourceLoaderOptions = ConstructorParameters<typeof DefaultResourceLoader>[0];
type InMemorySettings = Parameters<typeof SettingsManager.inMemory>[0];

export interface WorkerSessionFactoryOptions {
  cwd: string;
  agentDir: string;
  parentSessionFile: string | undefined;
  projectTrusted: boolean;
  definition: WorkerDefinition;
  /** The model selected by the parent, used only when the worker omits a model. */
  parentModel?: Model<Api>;
  modelRegistry: ModelRegistry;
}

export interface WorkerSessionHandle {
  readonly sessionFile: string;
  prompt(instructions: string): Promise<WorkerOutcome>;
  abort(): Promise<void>;
  dispose(): void;
  subscribeUsage(listener: (usage: WorkerUsage) => void): () => void;
  subscribeActivity(listener: (activity: string | undefined) => void): () => void;
}

export interface WorkerSessionFactory {
  create(options: WorkerSessionFactoryOptions): Promise<WorkerSessionHandle>;
}

interface SessionManagerInput {
  cwd: string;
  parentSessionFile: string | undefined;
}

interface ModelRuntimeInput {
  authPath: string;
  modelsPath: string;
}

interface AgentSessionInput {
  cwd: string;
  agentDir: string;
  model: Model<Api>;
  modelRuntime: ModelRuntime;
  thinkingLevel: ThinkingLevel | undefined;
  tools: string[];
  resourceLoader: ResourceLoader;
  sessionManager: unknown;
  settingsManager: unknown;
}

export interface WorkerSessionDependencies {
  createResourceLoader(options: ResourceLoaderOptions): ResourceLoader;
  createSessionManager(input: SessionManagerInput): unknown;
  createSettingsManager(
    settings: InMemorySettings,
    options: { projectTrusted: boolean },
  ): unknown;
  createModelRuntime(input: ModelRuntimeInput): Promise<ModelRuntime>;
  createAgentSession(input: AgentSessionInput): Promise<{ session: WorkerAgentSession }>;
}

const defaultDependencies: WorkerSessionDependencies = {
  createResourceLoader: (options) => new DefaultResourceLoader(options),
  createSessionManager: ({ cwd, parentSessionFile }) =>
    SessionManager.create(cwd, undefined, { parentSession: parentSessionFile }),
  createSettingsManager: (settings, options) =>
    SettingsManager.inMemory(settings, { projectTrusted: options.projectTrusted }),
  createModelRuntime: (input) => ModelRuntime.create(input),
  createAgentSession: (input) =>
    createAgentSession(input as CreateAgentSessionOptions) as Promise<{ session: WorkerAgentSession }>,
};

export function resolveWorkerModel(
  definition: WorkerDefinition,
  parentModel: Model<Api> | undefined,
  modelRegistry: ModelRegistry,
): Model<Api> {
  const configured = definition.model;
  if (!configured) {
    if (parentModel) return parentModel;
    throw new Error(
      `Worker "${definition.name}" has no configured model and no parent model is available`,
    );
  }

  const model = modelRegistry.find(configured.provider, configured.modelId);
  if (!model) {
    throw new Error(
      `Worker "${definition.name}" configured model "${configured.provider}/${configured.modelId}" was not found`,
    );
  }
  return model;
}

interface DisposableResource {
  dispose(): void;
}

function isDisposableResource(resource: ResourceLoader): resource is ResourceLoader & DisposableResource {
  return "dispose" in resource && typeof resource.dispose === "function";
}

function disposeResourceLoader(resourceLoader: ResourceLoader): void {
  if (!isDisposableResource(resourceLoader)) return;
  try {
    resourceLoader.dispose();
  } catch {
    // Cleanup is best-effort and must not hide the original failure.
  }
}

function disposeSession(session: WorkerAgentSession): void {
  try {
    session.dispose();
  } catch {
    // Other owned resources still need to be released.
  }
}

function describePromptError(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string" && error !== "") return error;
  return "Worker prompt failed";
}

interface MutableWorkerUsage {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  cost: number;
  contextTokens: number;
  turns: number;
}

function emptyUsage(): MutableWorkerUsage {
  return {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    cost: 0,
    contextTokens: 0,
    turns: 0,
  };
}

function assistantText(message: AssistantMessage | undefined): string | undefined {
  if (!message) return undefined;
  const text = message.content
    .filter((part) => part.type === "text")
    .map((part) => part.text);
  return text.length > 0 ? text.join("\n") : undefined;
}

function lastAssistant(messages: AgentMessage[]): AssistantMessage | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role === "assistant") return message;
  }
  return undefined;
}

class DefaultWorkerSessionHandle implements WorkerSessionHandle {
  readonly sessionFile: string;

  private readonly usage = emptyUsage();
  private readonly usageListeners = new Set<(usage: WorkerUsage) => void>();
  private readonly activityListeners = new Set<
    (activity: string | undefined) => void
  >();
  private readonly unsubscribeSession: () => void;
  private disposed = false;
  private activity: string | undefined;
  private readonly activeToolCalls = new Map<string, string>();
  private prompting = false;
  private abortRequested = false;
  private promptAssistant: AssistantMessage | undefined;

  constructor(
    private readonly session: WorkerAgentSession,
    private readonly resourceLoader: ResourceLoader,
    private readonly reusable: boolean,
  ) {
    this.sessionFile = session.sessionFile!;
    this.unsubscribeSession = session.subscribe((event) => this.onSessionEvent(event));
  }

  private onSessionEvent(event: AgentSessionEvent): void {
    if (event.type === "tool_execution_start") {
      this.activeToolCalls.delete(event.toolCallId);
      this.activeToolCalls.set(event.toolCallId, event.toolName);
      this.setActivity(event.toolName);
      return;
    }

    if (event.type === "tool_execution_end") {
      if (this.activeToolCalls.get(event.toolCallId) !== event.toolName) return;
      this.activeToolCalls.delete(event.toolCallId);
      this.setActivity(this.mostRecentActiveTool());
      return;
    }

    if (event.type !== "turn_end" || event.message.role !== "assistant") return;

    const message = event.message;
    this.promptAssistant = message;
    this.usage.input += message.usage.input ?? 0;
    this.usage.output += message.usage.output ?? 0;
    this.usage.cacheRead += message.usage.cacheRead ?? 0;
    this.usage.cacheWrite += message.usage.cacheWrite ?? 0;
    this.usage.cost += message.usage.cost?.total ?? 0;
    this.usage.contextTokens = message.usage.totalTokens ?? 0;
    this.usage.turns += 1;

    for (const listener of [...this.usageListeners]) {
      try {
        listener({ ...this.usage });
      } catch {
        // One subscriber cannot prevent other subscribers from receiving usage.
      }
    }
  }

  private mostRecentActiveTool(): string | undefined {
    let latest: string | undefined;
    for (const toolName of this.activeToolCalls.values()) latest = toolName;
    return latest;
  }

  private setActivity(activity: string | undefined): void {
    if (this.activity === activity) return;
    this.activity = activity;
    for (const listener of [...this.activityListeners]) {
      try {
        listener(activity);
      } catch {
        // One subscriber cannot prevent other subscribers from receiving activity.
      }
    }
  }

  async prompt(instructions: string): Promise<WorkerOutcome> {
    if (this.disposed) throw new Error("Worker session has been disposed");
    if (this.prompting) throw new Error("Worker session is already processing a prompt");

    this.prompting = true;
    this.abortRequested = false;
    this.promptAssistant = undefined;
    const previousAssistant = lastAssistant(this.session.messages);

    try {
      await this.session.prompt(instructions);
      const latestAssistant = lastAssistant(this.session.messages);
      const message = this.promptAssistant ?? (latestAssistant !== previousAssistant ? latestAssistant : undefined);
      return this.outcomeFrom(message);
    } catch (error) {
      const latestAssistant = lastAssistant(this.session.messages);
      const message = this.promptAssistant ?? (latestAssistant !== previousAssistant ? latestAssistant : undefined);
      return this.outcomeFrom(message, describePromptError(error));
    } finally {
      this.prompting = false;
    }
  }

  private outcomeFrom(
    message: AssistantMessage | undefined,
    failureMessage?: string,
  ): WorkerOutcome {
    const text = assistantText(message);
    const assistantPayload = text === undefined ? {} : { assistantText: text };

    if (this.abortRequested || message?.stopReason === "aborted") {
      const abortMessage = message?.errorMessage ?? failureMessage;
      return {
        status: "aborted",
        ...(abortMessage === undefined ? {} : { message: abortMessage }),
        ...assistantPayload,
      };
    }

    if (message?.stopReason === "error") {
      return {
        status: "failed",
        message: message.errorMessage ?? failureMessage ?? "Worker assistant reported a failure",
        ...assistantPayload,
      };
    }

    if (failureMessage !== undefined) {
      return { status: "failed", message: failureMessage, ...assistantPayload };
    }

    return {
      status: this.reusable ? "ready" : "completed",
      assistantText: text ?? "",
    };
  }

  async abort(): Promise<void> {
    if (this.prompting) this.abortRequested = true;
    await this.session.abort();
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    try {
      this.unsubscribeSession();
    } catch {
      // Continue releasing independently owned resources.
    }
    this.activeToolCalls.clear();
    this.usageListeners.clear();
    this.activityListeners.clear();
    disposeSession(this.session);
    disposeResourceLoader(this.resourceLoader);
  }

  subscribeUsage(listener: (usage: WorkerUsage) => void): () => void {
    this.usageListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.usageListeners.delete(listener);
    };
  }

  subscribeActivity(
    listener: (activity: string | undefined) => void,
  ): () => void {
    this.activityListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.activityListeners.delete(listener);
    };
  }
}

function selectedModelCoordinates(
  definition: WorkerDefinition,
  parentModel: Model<Api> | undefined,
): { provider: string; modelId: string } {
  if (definition.model) return definition.model;
  if (parentModel) return { provider: parentModel.provider, modelId: parentModel.id };
  throw new Error(
    `Worker "${definition.name}" has no configured model and no parent model is available`,
  );
}

async function refreshChildModelRuntime(
  modelRuntime: ModelRuntime,
  definition: WorkerDefinition,
): Promise<void> {
  const result = await modelRuntime.refresh({ allowNetwork: false });
  if (result.errors.size > 0) {
    const errors = [...result.errors]
      .map(([providerId, error]) => `${providerId}: ${error.message}`)
      .join("; ");
    throw new Error(
      `Worker "${definition.name}" failed to refresh child model providers: ${errors}`,
    );
  }
  if (result.aborted) {
    throw new Error(`Worker "${definition.name}" child model refresh was aborted`);
  }
}

async function prepareChildModelRuntime(
  options: WorkerSessionFactoryOptions,
  dependencies: WorkerSessionDependencies,
): Promise<{ model: Model<Api>; modelRuntime: ModelRuntime }> {
  const selected = selectedModelCoordinates(options.definition, options.parentModel);
  const modelRuntime = await dependencies.createModelRuntime({
    authPath: join(options.agentDir, "auth.json"),
    modelsPath: join(options.agentDir, "models.json"),
  });

  for (const providerId of options.modelRegistry.getRegisteredProviderIds()) {
    const providerConfig = options.modelRegistry.getRegisteredProviderConfig(providerId);
    if (providerConfig) modelRuntime.registerProvider(providerId, { ...providerConfig });
  }
  await refreshChildModelRuntime(modelRuntime, options.definition);

  const initiallyResolvedModel = modelRuntime.getModel(selected.provider, selected.modelId);
  if (!initiallyResolvedModel) {
    throw new Error(
      `Worker "${options.definition.name}" configured model "${selected.provider}/${selected.modelId}" was not found`,
    );
  }

  const resolvedAuth = await options.modelRegistry.getApiKeyAndHeaders(initiallyResolvedModel);
  if (resolvedAuth.ok) {
    if (resolvedAuth.headers) {
      modelRuntime.registerProvider(selected.provider, {
        headers: { ...resolvedAuth.headers },
      });
    }
    if (resolvedAuth.apiKey) {
      await modelRuntime.setRuntimeApiKey(selected.provider, resolvedAuth.apiKey);
    }
    // Pi 0.80.10 exposes resolved provider env here but has no public ModelRuntime
    // runtime-env setter. Provider registrations, resolved headers, and API keys are copied.
  }

  await refreshChildModelRuntime(modelRuntime, options.definition);
  const model = modelRuntime.getModel(selected.provider, selected.modelId);
  if (!model) {
    throw new Error(
      `Worker "${options.definition.name}" configured model "${selected.provider}/${selected.modelId}" was not found`,
    );
  }
  return { model, modelRuntime };
}

export function createWorkerSessionFactory(
  dependencies: WorkerSessionDependencies = defaultDependencies,
): WorkerSessionFactory {
  return {
    async create(options) {
      const definition = options.definition;
      const { model, modelRuntime } = await prepareChildModelRuntime(options, dependencies);
      const selectedSkills = new Set(definition.skills);
      const settingsManager = dependencies.createSettingsManager(
        definition.compaction === undefined
          ? undefined
          : { compaction: { ...definition.compaction } },
        { projectTrusted: options.projectTrusted },
      );
      const resourceLoader = dependencies.createResourceLoader({
        cwd: options.cwd,
        agentDir: options.agentDir,
        settingsManager: settingsManager as SettingsManager,
        noExtensions: true,
        noSkills: selectedSkills.size === 0,
        noPromptTemplates: true,
        noThemes: true,
        noContextFiles: !options.projectTrusted,
        skillsOverride: (base) => ({
          skills: base.skills.filter((skill) => selectedSkills.has(skill.name)),
          diagnostics: base.diagnostics,
        }),
        appendSystemPrompt: [definition.systemPrompt, DIRECT_CHILD_BOUNDARY],
      });

      let session: WorkerAgentSession | undefined;
      try {
        await resourceLoader.reload();

        const loadedSkillNames = new Set(
          resourceLoader.getSkills().skills.map((skill) => skill.name),
        );
        const missingSkills = [...selectedSkills].filter(
          (skillName) => !loadedSkillNames.has(skillName),
        );
        if (missingSkills.length > 0) {
          throw new Error(
            `Worker "${definition.name}" selected skills were not loaded: ${missingSkills.join(", ")}`,
          );
        }

        const sessionManager = dependencies.createSessionManager({
          cwd: options.cwd,
          parentSessionFile: options.parentSessionFile,
        });
        ({ session } = await dependencies.createAgentSession({
          cwd: options.cwd,
          agentDir: options.agentDir,
          model,
          modelRuntime,
          thinkingLevel: definition.thinking,
          tools: [...definition.tools],
          resourceLoader,
          sessionManager,
          settingsManager,
        }));
        if (!session.sessionFile) {
          throw new Error("Worker session was not created with durable storage");
        }

        const handle = new DefaultWorkerSessionHandle(
          session,
          resourceLoader,
          definition.lifecycle === "reusable",
        );
        session = undefined;
        return handle;
      } catch (error) {
        if (session) disposeSession(session);
        disposeResourceLoader(resourceLoader);
        throw error;
      }
    },
  };
}
