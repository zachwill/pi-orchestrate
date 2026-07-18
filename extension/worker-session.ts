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
import { Effect, Exit, Schema, Scope } from "effect";
import type {
  WorkerDefinition,
  WorkerMessageDirection,
  WorkerOutcome,
  WorkerUsage,
} from "./domain.js";

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
  subscribeMessageDirection(listener: (direction: WorkerMessageDirection) => void): () => void;
}

export interface WorkerSessionFactory {
  create(options: WorkerSessionFactoryOptions): Promise<WorkerSessionHandle>;
}

export class WorkerModelAcquisitionError extends Schema.TaggedErrorClass<WorkerModelAcquisitionError>()(
  "WorkerSession.ModelAcquisitionError",
  { message: Schema.String, cause: Schema.Defect() },
) {}

export class WorkerResourceAcquisitionError extends Schema.TaggedErrorClass<WorkerResourceAcquisitionError>()(
  "WorkerSession.ResourceAcquisitionError",
  { message: Schema.String, cause: Schema.Defect() },
) {}

export class WorkerAgentSessionAcquisitionError extends Schema.TaggedErrorClass<WorkerAgentSessionAcquisitionError>()(
  "WorkerSession.AgentSessionAcquisitionError",
  { message: Schema.String, cause: Schema.Defect() },
) {}

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

function runCleanup(cleanup: () => void): void {
  try {
    cleanup();
  } catch {
    // Cleanup is best-effort and cannot prevent independent finalizers.
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

const promptAgentSession = Effect.fn("WorkerSession.promptAgentSession")(function* (
  session: WorkerAgentSession,
  instructions: string,
) {
  return yield* Effect.tryPromise({
    try: () => session.prompt(instructions),
    catch: (error) => error,
  });
});

const abortAgentSession = Effect.fn("WorkerSession.abortAgentSession")(function* (
  session: WorkerAgentSession,
) {
  return yield* Effect.tryPromise({
    try: () => session.abort(),
    catch: (error) => error,
  });
});

class DefaultWorkerSessionHandle implements WorkerSessionHandle {
  readonly sessionFile: string;

  private readonly usage = emptyUsage();
  private readonly usageListeners = new Set<(usage: WorkerUsage) => void>();
  private readonly activityListeners = new Set<
    (activity: string | undefined) => void
  >();
  private readonly messageDirectionListeners = new Set<
    (direction: WorkerMessageDirection) => void
  >();
  private disposed = false;
  private activity: string | undefined;
  private messageDirection: WorkerMessageDirection | undefined;
  private readonly activeToolCalls = new Map<string, string>();
  private prompting = false;
  private abortRequested = false;
  private promptAssistant: AssistantMessage | undefined;

  constructor(
    private readonly session: WorkerAgentSession,
    sessionFile: string,
    private readonly reusable: boolean,
    private readonly scope: Scope.Closeable,
  ) {
    this.sessionFile = sessionFile;
  }

  receiveSessionEvent(event: AgentSessionEvent): void {
    if (event.type === "message_start") {
      if (event.message.role === "assistant") {
        this.setMessageDirection("from-model");
      } else if (event.message.role === "user" || event.message.role === "toolResult") {
        this.setMessageDirection("to-model");
      }
      return;
    }

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
    this.emitUsage();
  }

  private emitUsage(): void {
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

  private setMessageDirection(direction: WorkerMessageDirection): void {
    if (this.messageDirection === direction) return;
    this.messageDirection = direction;
    for (const listener of [...this.messageDirectionListeners]) {
      try {
        listener(direction);
      } catch {
        // One subscriber cannot prevent other subscribers from receiving direction.
      }
    }
  }

  async prompt(instructions: string): Promise<WorkerOutcome> {
    if (this.disposed) throw new Error("Worker session has been disposed");
    if (this.prompting) throw new Error("Worker session is already processing a prompt");

    this.prompting = true;
    this.abortRequested = false;
    this.promptAssistant = undefined;
    this.setMessageDirection("to-model");
    const previousAssistant = lastAssistant(this.session.messages);

    try {
      await Effect.runPromise(promptAgentSession(this.session, instructions));
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
    await Effect.runPromise(abortAgentSession(this.session));
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    this.activeToolCalls.clear();
    this.usageListeners.clear();
    this.activityListeners.clear();
    this.messageDirectionListeners.clear();
    Effect.runSync(Scope.close(this.scope, Exit.void));
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

  subscribeMessageDirection(
    listener: (direction: WorkerMessageDirection) => void,
  ): () => void {
    this.messageDirectionListeners.add(listener);
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.messageDirectionListeners.delete(listener);
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

function causeMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error) return cause.message;
  if (typeof cause === "string" && cause !== "") return cause;
  return fallback;
}

const createChildModelRuntime = Effect.fn("WorkerSession.createChildModelRuntime")(function* (
  dependencies: WorkerSessionDependencies,
  input: ModelRuntimeInput,
) {
  return yield* Effect.tryPromise({
    try: () => dependencies.createModelRuntime(input),
    catch: (cause) => new WorkerModelAcquisitionError({
      message: causeMessage(cause, "Worker model runtime creation failed"),
      cause,
    }),
  });
});

const refreshChildModelRuntime = Effect.fn("WorkerSession.refreshChildModelRuntime")(function* (
  modelRuntime: ModelRuntime,
  definition: WorkerDefinition,
) {
  const result = yield* Effect.tryPromise({
    try: () => modelRuntime.refresh({ allowNetwork: false }),
    catch: (cause) => new WorkerModelAcquisitionError({
      message: causeMessage(cause, `Worker "${definition.name}" failed to refresh child model providers`),
      cause,
    }),
  });
  if (result.errors.size > 0) {
    const errors = [...result.errors]
      .map(([providerId, error]) => `${providerId}: ${error.message}`)
      .join("; ");
    const message = `Worker "${definition.name}" failed to refresh child model providers: ${errors}`;
    return yield* new WorkerModelAcquisitionError({ message, cause: new Error(message) });
  }
  if (result.aborted) {
    const message = `Worker "${definition.name}" child model refresh was aborted`;
    return yield* new WorkerModelAcquisitionError({ message, cause: new Error(message) });
  }
});

const prepareChildModelRuntime = Effect.fn("WorkerSession.prepareChildModelRuntime")(function* (
  options: WorkerSessionFactoryOptions,
  dependencies: WorkerSessionDependencies,
) {
  const selected = yield* Effect.try({
    try: () => selectedModelCoordinates(options.definition, options.parentModel),
    catch: (cause) => new WorkerModelAcquisitionError({
      message: causeMessage(cause, "Worker model selection failed"),
      cause,
    }),
  });
  const modelRuntime = yield* createChildModelRuntime(dependencies, {
    authPath: join(options.agentDir, "auth.json"),
    modelsPath: join(options.agentDir, "models.json"),
  });

  yield* Effect.try({
    try: () => {
      for (const providerId of options.modelRegistry.getRegisteredProviderIds()) {
        const providerConfig = options.modelRegistry.getRegisteredProviderConfig(providerId);
        if (providerConfig) modelRuntime.registerProvider(providerId, { ...providerConfig });
      }
    },
    catch: (cause) => new WorkerModelAcquisitionError({
      message: causeMessage(cause, "Worker model provider registration failed"),
      cause,
    }),
  });
  yield* refreshChildModelRuntime(modelRuntime, options.definition);

  const initiallyResolvedModel = modelRuntime.getModel(selected.provider, selected.modelId);
  if (!initiallyResolvedModel) {
    const message = `Worker "${options.definition.name}" configured model "${selected.provider}/${selected.modelId}" was not found`;
    return yield* new WorkerModelAcquisitionError({ message, cause: new Error(message) });
  }

  const resolvedAuth = yield* resolveModelAuthentication(
    options.modelRegistry,
    initiallyResolvedModel,
  );
  const parentUsesOAuth = options.modelRegistry.isUsingOAuth(initiallyResolvedModel);
  if (resolvedAuth.ok) {
    if (resolvedAuth.headers) {
      modelRuntime.registerProvider(selected.provider, {
        headers: { ...resolvedAuth.headers },
      });
    }
    // OAuth resolution returns an access token through the compatibility API, but
    // installing that token as a runtime API key masks the complete OAuth credential
    // that the child already reads from the shared auth.json file.
    const resolvedApiKey = resolvedAuth.apiKey;
    if (resolvedApiKey && !parentUsesOAuth) {
      yield* installRuntimeApiKey(modelRuntime, selected.provider, resolvedApiKey);
    }
    // Pi 0.80.10 exposes resolved provider env here but has no public ModelRuntime
    // runtime-env setter. Provider registrations, resolved headers, and non-OAuth
    // API keys are copied.
  }

  yield* refreshChildModelRuntime(modelRuntime, options.definition);
  const model = modelRuntime.getModel(selected.provider, selected.modelId);
  if (!model) {
    const message = `Worker "${options.definition.name}" configured model "${selected.provider}/${selected.modelId}" was not found`;
    return yield* new WorkerModelAcquisitionError({ message, cause: new Error(message) });
  }
  return { model, modelRuntime };
});

const resolveModelAuthentication = Effect.fn("WorkerSession.resolveModelAuthentication")(function* (
  modelRegistry: ModelRegistry,
  model: Model<Api>,
) {
  return yield* Effect.tryPromise({
    try: () => modelRegistry.getApiKeyAndHeaders(model),
    catch: (cause) => new WorkerModelAcquisitionError({
      message: causeMessage(cause, "Worker model authentication resolution failed"),
      cause,
    }),
  });
});

const installRuntimeApiKey = Effect.fn("WorkerSession.installRuntimeApiKey")(function* (
  modelRuntime: ModelRuntime,
  provider: string,
  apiKey: string,
) {
  yield* Effect.tryPromise({
    try: () => modelRuntime.setRuntimeApiKey(provider, apiKey),
    catch: (cause) => new WorkerModelAcquisitionError({
      message: causeMessage(cause, "Worker model runtime API key installation failed"),
      cause,
    }),
  });
});

const reloadResourceLoader = Effect.fn("WorkerSession.reloadResourceLoader")(function* (
  resourceLoader: ResourceLoader,
) {
  yield* Effect.tryPromise({
    try: () => resourceLoader.reload(),
    catch: (cause) => new WorkerResourceAcquisitionError({
      message: causeMessage(cause, "Worker resource reload failed"),
      cause,
    }),
  });
});

const createChildAgentSession = Effect.fn("WorkerSession.createChildAgentSession")(function* (
  dependencies: WorkerSessionDependencies,
  input: AgentSessionInput,
) {
  return yield* Effect.tryPromise({
    try: () => dependencies.createAgentSession(input),
    catch: (cause) => new WorkerAgentSessionAcquisitionError({
      message: causeMessage(cause, "Worker agent session creation failed"),
      cause,
    }),
  });
});

const createWorkerSession = Effect.fn("WorkerSession.create")(function* (
  options: WorkerSessionFactoryOptions,
  dependencies: WorkerSessionDependencies,
) {
  const scope = yield* Scope.make("sequential");
  const acquire = Effect.gen(function* () {
    const definition = options.definition;
    const { model, modelRuntime } = yield* prepareChildModelRuntime(options, dependencies);
    const selectedSkills = new Set(definition.skills);
    const settingsManager = yield* Effect.try({
      try: () => dependencies.createSettingsManager(
        definition.compaction === undefined
          ? undefined
          : { compaction: { ...definition.compaction } },
        { projectTrusted: options.projectTrusted },
      ),
      catch: (cause) => new WorkerResourceAcquisitionError({
        message: causeMessage(cause, "Worker settings manager creation failed"),
        cause,
      }),
    });
    const resourceLoader = yield* Effect.acquireRelease(
      Effect.try({
        try: () => dependencies.createResourceLoader({
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
        }),
        catch: (cause) => new WorkerResourceAcquisitionError({
          message: causeMessage(cause, "Worker resource loader creation failed"),
          cause,
        }),
      }),
      (loader) => Effect.sync(() => disposeResourceLoader(loader)),
    );

    yield* reloadResourceLoader(resourceLoader);
    const loadedSkillNames = new Set(
      resourceLoader.getSkills().skills.map((skill) => skill.name),
    );
    const missingSkills = [...selectedSkills].filter(
      (skillName) => !loadedSkillNames.has(skillName),
    );
    if (missingSkills.length > 0) {
      const message = `Worker "${definition.name}" selected skills were not loaded: ${missingSkills.join(", ")}`;
      return yield* new WorkerResourceAcquisitionError({ message, cause: new Error(message) });
    }

    const sessionManager = yield* Effect.try({
      try: () => dependencies.createSessionManager({
        cwd: options.cwd,
        parentSessionFile: options.parentSessionFile,
      }),
      catch: (cause) => new WorkerAgentSessionAcquisitionError({
        message: causeMessage(cause, "Worker session manager creation failed"),
        cause,
      }),
    });
    const session = yield* Effect.acquireRelease(
      createChildAgentSession(dependencies, {
        cwd: options.cwd,
        agentDir: options.agentDir,
        model,
        modelRuntime,
        thinkingLevel: definition.thinking,
        tools: [...definition.tools],
        resourceLoader,
        sessionManager,
        settingsManager,
      }).pipe(Effect.map((created) => created.session)),
      (ownedSession) => Effect.sync(() => disposeSession(ownedSession)),
    );
    if (!session.sessionFile) {
      const message = "Worker session was not created with durable storage";
      return yield* new WorkerAgentSessionAcquisitionError({ message, cause: new Error(message) });
    }

    const handle = new DefaultWorkerSessionHandle(
      session,
      session.sessionFile,
      definition.lifecycle === "reusable",
      scope,
    );
    yield* Effect.acquireRelease(
      Effect.try({
        try: () => session.subscribe((event) => handle.receiveSessionEvent(event)),
        catch: (cause) => new WorkerAgentSessionAcquisitionError({
          message: causeMessage(cause, "Worker session subscription failed"),
          cause,
        }),
      }),
      (unsubscribe) => Effect.sync(() => runCleanup(unsubscribe)),
    );
    return handle;
  }).pipe(
    Scope.provide(scope),
    Effect.onExit((exit) => Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void),
  );

  return yield* acquire;
});

export function createWorkerSessionFactory(
  dependencies: WorkerSessionDependencies = defaultDependencies,
): WorkerSessionFactory {
  return {
    create: (options) => Effect.runPromise(createWorkerSession(options, dependencies)),
  };
}
