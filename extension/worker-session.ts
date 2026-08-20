import { realpathSync } from "node:fs";
import { isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { AgentMessage, ThinkingLevel } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Model } from "@earendil-works/pi-ai";
import {
  AgentSessionRuntime,
  createAgentSessionFromServices,
  createAgentSessionServices,
  DefaultPackageManager,
  ModelRuntime,
  type ModelRegistry,
  type PromptOptions,
  type ResourceLoader,
  SessionManager,
  SettingsManager,
  type AgentSessionEvent,
  type AgentSessionServices,
  type CreateAgentSessionResult,
  type DefaultResourceLoader,
} from "@earendil-works/pi-coding-agent";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  FiberSet,
  Layer,
  Schema,
  Scope,
  SynchronizedRef,
} from "effect";
import type {
  WorkerDefinition,
  WorkerMessageDirection,
  WorkerOutcome,
  WorkerUsage,
} from "./domain.js";

const DIRECT_CHILD_BOUNDARY =
  "You are a direct child worker session. Do not spawn, delegate to, or orchestrate descendant Pi worker sessions. Complete the assigned task yourself and return the result directly to the parent orchestrator.";
const ORCHESTRATE_PACKAGE_ROOT = canonicalPath(fileURLToPath(new URL("..", import.meta.url)));

interface WorkerAgentSession {
  readonly sessionFile: string | undefined;
  readonly messages: AgentMessage[];
  prompt(instructions: string, options?: PromptOptions): Promise<void>;
  abortCompaction(): void;
  abort(): Promise<void>;
  dispose(): void;
  bindExtensions(bindings: { mode: "print" }): Promise<void>;
  subscribe(listener: (event: AgentSessionEvent) => void): () => void;
}

interface OwnedWorkerRuntime {
  readonly session: WorkerAgentSession;
  dispose(): Promise<void>;
}

type ResourceLoaderOptions = Omit<
  ConstructorParameters<typeof DefaultResourceLoader>[0],
  "cwd" | "agentDir" | "settingsManager"
>;

export interface ChildSessionOptions {
  cwd: string;
  agentDir: string;
  parentSessionFile: string | undefined;
  projectTrusted: boolean;
  definition: WorkerDefinition;
  /** The model selected by the parent, used only when the worker omits a model. */
  parentModel?: Model<Api>;
  modelRegistry: ModelRegistry;
}

export interface WorkerSessionObservation {
  readonly usage: WorkerUsage;
  readonly activity: string | undefined;
  readonly messageDirection: WorkerMessageDirection | undefined;
}

export interface WorkerSessionHandle {
  readonly sessionFile: string;
  prompt(instructions: string): Effect.Effect<WorkerOutcome, never>;
  abort(): Effect.Effect<void, WorkerSessionAbortError>;
  dispose(): Effect.Effect<void, never>;
  subscribeObservation(listener: (observation: WorkerSessionObservation) => void): () => void;
}

const WorkerModelAcquisitionOperation = Schema.Literals([
  "select-model",
  "create-model-runtime",
  "register-providers",
  "refresh-providers",
  "resolve-model",
  "authenticate-model",
  "configure-runtime-auth",
]);
type WorkerModelAcquisitionOperation = typeof WorkerModelAcquisitionOperation.Type;

const WorkerResourceAcquisitionOperation = Schema.Literals([
  "create-settings",
  "resolve-extensions",
  "create-services",
  "validate-skills",
]);
type WorkerResourceAcquisitionOperation = typeof WorkerResourceAcquisitionOperation.Type;

const WorkerAgentSessionAcquisitionOperation = Schema.Literals([
  "create-session-manager",
  "create-session",
  "create-runtime",
  "bind-extensions",
  "verify-durability",
  "subscribe-events",
]);
type WorkerAgentSessionAcquisitionOperation = typeof WorkerAgentSessionAcquisitionOperation.Type;

export class WorkerModelAcquisitionError extends Schema.TaggedError<WorkerModelAcquisitionError>()(
  "WorkerSession.ModelAcquisitionError",
  { operation: WorkerModelAcquisitionOperation, message: Schema.String, cause: Schema.Defect() },
) {}

export class WorkerResourceAcquisitionError extends Schema.TaggedError<WorkerResourceAcquisitionError>()(
  "WorkerSession.ResourceAcquisitionError",
  { operation: WorkerResourceAcquisitionOperation, message: Schema.String, cause: Schema.Defect() },
) {}

export class WorkerAgentSessionAcquisitionError extends Schema.TaggedError<WorkerAgentSessionAcquisitionError>()(
  "WorkerSession.AgentSessionAcquisitionError",
  { operation: WorkerAgentSessionAcquisitionOperation, message: Schema.String, cause: Schema.Defect() },
) {}

export class WorkerSessionAcquisitionClosedError extends Schema.TaggedError<WorkerSessionAcquisitionClosedError>()(
  "WorkerSession.AcquisitionClosedError",
  { message: Schema.String },
) {}

const WorkerSessionAbortOperation = Schema.Literals([
  "abort-compaction",
  "abort-prompt",
]);
export type WorkerSessionAbortOperation = typeof WorkerSessionAbortOperation.Type;

const WorkerSessionAbortStage = Schema.Literals(["compaction", "prompt"]);
export type WorkerSessionAbortStage = typeof WorkerSessionAbortStage.Type;

export class WorkerSessionAbortError extends Schema.TaggedError<WorkerSessionAbortError>()(
  "WorkerSession.AbortError",
  {
    operation: WorkerSessionAbortOperation,
    stage: WorkerSessionAbortStage,
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export type WorkerSessionAcquisitionError =
  | WorkerModelAcquisitionError
  | WorkerResourceAcquisitionError
  | WorkerAgentSessionAcquisitionError
  | WorkerSessionAcquisitionClosedError;

export interface ChildSessionsService {
  /**
   * Acquires a child session through a process-owned producer. The adopter must
   * synchronously install ownership before returning a value. Returning undefined
   * rejects adoption and leaves ChildSessions responsible for disposal.
   */
  readonly acquire: <Adopted>(
    options: ChildSessionOptions,
    adopt: (session: WorkerSessionHandle) => Adopted | undefined,
  ) => Effect.Effect<Adopted | undefined, WorkerSessionAcquisitionError>;
  /** Closes every handoff without waiting for uncancellable Pi calls. */
  readonly shutdown: () => Effect.Effect<void>;
}

export class ChildSessions extends Context.Service<ChildSessions, ChildSessionsService>()(
  "@zachwill/pi-orchestrate/ChildSessions",
) {}

const WorkerSessionCleanupOperation = Schema.Literals([
  "unsubscribe",
  "runtime",
  "raw-session",
  "resource-loader",
  "scope-close",
]);
export type WorkerSessionCleanupOperation = typeof WorkerSessionCleanupOperation.Type;

export interface WorkerSessionCleanupFailure {
  readonly operation: WorkerSessionCleanupOperation;
  readonly cause: unknown;
}

export type WorkerSessionCleanupReporter = (failure: WorkerSessionCleanupFailure) => void;

interface SettingsManagerInput {
  cwd: string;
  agentDir: string;
  projectTrusted: boolean;
  compaction: WorkerDefinition["compaction"];
}

interface ExtensionPathInput {
  cwd: string;
  agentDir: string;
  settingsManager: SettingsManager;
}

interface ModelRuntimeInput {
  authPath: string;
  modelsPath: string;
}

interface ServicesInput {
  cwd: string;
  agentDir: string;
  settingsManager: SettingsManager;
  modelRuntime: ModelRuntime;
  resourceLoaderOptions: ResourceLoaderOptions;
}

interface AgentSessionInput {
  services: AgentSessionServices;
  sessionManager: SessionManager;
  model: Model<Api>;
  thinkingLevel: ThinkingLevel | undefined;
  tools: string[];
}

interface RuntimeInput {
  session: WorkerAgentSession;
  services: AgentSessionServices;
}

export interface WorkerSessionDependencies {
  createSettingsManager(input: SettingsManagerInput): SettingsManager;
  resolveExtensionPaths(input: ExtensionPathInput): Promise<readonly string[]>;
  createSessionManager(input: { cwd: string; parentSessionFile: string | undefined }): SessionManager;
  createModelRuntime(input: ModelRuntimeInput): Promise<ModelRuntime>;
  createServices(input: ServicesInput): Promise<AgentSessionServices>;
  createAgentSession(input: AgentSessionInput): Promise<{ session: WorkerAgentSession }>;
  createRuntime(input: RuntimeInput): OwnedWorkerRuntime;
  reportCleanupFailure(failure: WorkerSessionCleanupFailure): void;
  /** Deterministic boundary for verifying reclamation admission during root closure. */
  onReclamationOpenObserved(): void;
}

const defaultDependencies: WorkerSessionDependencies = {
  createSettingsManager: ({ cwd, agentDir, projectTrusted, compaction }) => {
    const settingsManager = SettingsManager.create(cwd, agentDir, { projectTrusted });
    if (compaction !== undefined) settingsManager.applyOverrides({ compaction: { ...compaction } });
    return settingsManager;
  },
  resolveExtensionPaths: async ({ cwd, agentDir, settingsManager }) => {
    const resolved = await new DefaultPackageManager({ cwd, agentDir, settingsManager }).resolve();
    return resolved.extensions.filter((entry) => entry.enabled).map((entry) => entry.path);
  },
  createSessionManager: ({ cwd, parentSessionFile }) =>
    SessionManager.create(cwd, undefined, { parentSession: parentSessionFile }),
  createModelRuntime: (input) => ModelRuntime.create(input),
  createServices: (input) => createAgentSessionServices(input),
  createAgentSession: (input) =>
    createAgentSessionFromServices(input) as Promise<CreateAgentSessionResult & { session: WorkerAgentSession }>,
  createRuntime: ({ session, services }) =>
    new AgentSessionRuntime(
      session as CreateAgentSessionResult["session"],
      services,
      async () => {
        throw new Error("Worker sessions do not support runtime replacement");
      },
    ) as unknown as OwnedWorkerRuntime,
  reportCleanupFailure: ({ operation }) => {
    process.emitWarning("Worker session cleanup failed", {
      code: "PI_ORCHESTRATE_WORKER_CLEANUP",
      detail: `Operation: ${operation}`,
    });
  },
  onReclamationOpenObserved: () => {},
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

function canonicalPath(path: string): string {
  try {
    return realpathSync.native(path);
  } catch {
    return resolve(path);
  }
}

function isWithin(path: string, root: string): boolean {
  const child = relative(root, path);
  return child === "" || (!child.startsWith("..") && !isAbsolute(child));
}

export function isOrchestrationExtensionPath(path: string): boolean {
  return isWithin(canonicalPath(path), ORCHESTRATE_PACKAGE_ROOT);
}

function reportCleanupFailure(
  reporter: WorkerSessionCleanupReporter,
  operation: WorkerSessionCleanupOperation,
  cause: unknown,
): Effect.Effect<void> {
  return Effect.sync(() => {
    try {
      reporter({ operation, cause });
    } catch {
      // Reporting must never make best-effort cleanup fail.
    }
  });
}

function bestEffortCleanup(
  reporter: WorkerSessionCleanupReporter,
  operation: WorkerSessionCleanupOperation,
  cleanup: () => void | Promise<void>,
): Effect.Effect<void> {
  return Effect.tryPromise({
    try: async () => cleanup(),
    catch: (cause) => cause,
  }).pipe(
    Effect.catch((cause) => reportCleanupFailure(reporter, operation, cause)),
  );
}

function resourceLoaderFinalizer(
  resourceLoader: ResourceLoader,
  reporter: WorkerSessionCleanupReporter,
): Effect.Effect<void> {
  return bestEffortCleanup(reporter, "resource-loader", () => {
    if (!("dispose" in resourceLoader) || typeof resourceLoader.dispose !== "function") return;
    resourceLoader.dispose();
  });
}

function describeError(error: unknown, fallback: string): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string" && error !== "") return error;
  return fallback;
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
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, contextTokens: 0, turns: 0 };
}

interface ActivePrompt {
  readonly _tag: "Active";
  readonly previousAssistant: AssistantMessage | undefined;
  assistant: AssistantMessage | undefined;
  abortRequested: boolean;
}

type PromptState =
  | { readonly _tag: "Idle" }
  | ActivePrompt
  | { readonly _tag: "Disposed" };

interface PromptCompletion {
  readonly failureMessage: string | undefined;
  readonly message: AssistantMessage | undefined;
  readonly prompt: ActivePrompt;
}

class DefaultWorkerSessionHandle implements WorkerSessionHandle {
  readonly sessionFile: string;
  private readonly usage = emptyUsage();
  private readonly observationListeners = new Set<(observation: WorkerSessionObservation) => void>();
  private readonly activeToolCalls = new Map<string, string>();
  private activity: string | undefined;
  private messageDirection: WorkerMessageDirection | undefined;
  private promptState: PromptState = { _tag: "Idle" };
  private disposeOperation!: Effect.Effect<void, never>;

  private constructor(
    private readonly runtime: OwnedWorkerRuntime,
    private readonly interactive: boolean,
    sessionFile: string,
  ) {
    this.sessionFile = sessionFile;
  }

  static make(
    runtime: OwnedWorkerRuntime,
    interactive: boolean,
    sessionFile: string,
    scope: Scope.Closeable,
    cleanupReporter: WorkerSessionCleanupReporter,
  ): Effect.Effect<DefaultWorkerSessionHandle> {
    return Effect.gen(function* () {
      const handle = new DefaultWorkerSessionHandle(runtime, interactive, sessionFile);
      handle.disposeOperation = yield* Effect.cached(
        Effect.sync(() => handle.beginDispose()).pipe(
          Effect.andThen(disposeWorkerSession(scope, cleanupReporter)),
          Effect.uninterruptible,
        ),
      );
      return handle;
    });
  }

  private beginDispose(): void {
    this.promptState = { _tag: "Disposed" };
    this.activeToolCalls.clear();
    this.observationListeners.clear();
  }

  receiveSessionEvent(event: AgentSessionEvent): void {
    if (event.type === "message_start") {
      if (event.message.role === "assistant") this.setMessageDirection("from-model");
      else if (event.message.role === "user" || event.message.role === "toolResult") this.setMessageDirection("to-model");
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
      this.setActivity([...this.activeToolCalls.values()].at(-1));
      return;
    }
    if (event.type !== "turn_end" || event.message.role !== "assistant") return;
    if (this.promptState._tag === "Active") this.promptState.assistant = event.message;
    this.usage.input += event.message.usage.input ?? 0;
    this.usage.output += event.message.usage.output ?? 0;
    this.usage.cacheRead += event.message.usage.cacheRead ?? 0;
    this.usage.cacheWrite += event.message.usage.cacheWrite ?? 0;
    this.usage.cost += event.message.usage.cost?.total ?? 0;
    this.usage.contextTokens = event.message.usage.totalTokens ?? 0;
    this.usage.turns += 1;
    this.emitObservation();
  }

  private setActivity(activity: string | undefined): void {
    if (this.activity === activity) return;
    this.activity = activity;
    this.emitObservation();
  }

  private setMessageDirection(direction: WorkerMessageDirection): void {
    if (this.messageDirection === direction) return;
    this.messageDirection = direction;
    this.emitObservation();
  }

  private emitObservation(): void {
    const observation: WorkerSessionObservation = Object.freeze({
      usage: Object.freeze({ ...this.usage }),
      activity: this.activity,
      messageDirection: this.messageDirection,
    });
    for (const listener of [...this.observationListeners]) {
      safelyNotify(() => listener(observation));
    }
  }

  prompt(instructions: string): Effect.Effect<WorkerOutcome, never> {
    return Effect.fn("WorkerSession.prompt")(function* (this: DefaultWorkerSessionHandle) {
      const completion = yield* Effect.sync(() => this.startPrompt(instructions));
      const { failureMessage, message, prompt } = yield* Effect.promise(() => completion);
      const text = assistantText(message);
      const assistantPayload = text === undefined ? {} : { assistantText: text };
      if (prompt.abortRequested || message?.stopReason === "aborted") {
        const abortMessage = message?.errorMessage ?? failureMessage;
        return { status: "aborted", ...(abortMessage ? { message: abortMessage } : {}), ...assistantPayload } satisfies WorkerOutcome;
      }
      if (message?.stopReason === "error") {
        return { status: "failed", message: message.errorMessage ?? failureMessage ?? "Worker assistant reported a failure", ...assistantPayload } satisfies WorkerOutcome;
      }
      if (failureMessage) return { status: "failed", message: failureMessage, ...assistantPayload } satisfies WorkerOutcome;
      return { status: this.interactive ? "ready" : "completed", assistantText: text ?? "" } satisfies WorkerOutcome;
    }).call(this);
  }

  private startPrompt(instructions: string): Promise<PromptCompletion> {
    if (this.promptState._tag === "Disposed") throw new Error("Worker session has been disposed");
    if (this.promptState._tag === "Active") throw new Error("Worker session is already processing a prompt");

    const prompt: ActivePrompt = {
      _tag: "Active",
      previousAssistant: lastAssistant(this.runtime.session.messages),
      assistant: undefined,
      abortRequested: false,
    };
    this.promptState = prompt;
    this.setMessageDirection("to-model");

    let physicalPrompt: Promise<void>;
    try {
      physicalPrompt = this.runtime.session.prompt(instructions, {
        expandPromptTemplates: false,
        source: "extension",
      });
    } catch (cause) {
      return Promise.resolve(this.completePrompt(
        prompt,
        describeError(cause, "Worker prompt failed"),
      ));
    }

    return physicalPrompt.then(
      () => this.completePrompt(prompt, undefined),
      (cause) => this.completePrompt(prompt, describeError(cause, "Worker prompt failed")),
    );
  }

  private completePrompt(
    prompt: ActivePrompt,
    failureMessage: string | undefined,
  ): PromptCompletion {
    const latestAssistant = lastAssistant(this.runtime.session.messages);
    const message = prompt.assistant ??
      (latestAssistant !== prompt.previousAssistant ? latestAssistant : undefined);
    if (this.promptState === prompt) this.promptState = { _tag: "Idle" };
    return { failureMessage, message, prompt };
  }

  abort(): Effect.Effect<void, WorkerSessionAbortError> {
    return Effect.fn("WorkerSession.abort")(function* (this: DefaultWorkerSessionHandle) {
      if (this.promptState._tag === "Active") this.promptState.abortRequested = true;

      const compaction = yield* Effect.result(Effect.try({
        try: () => this.runtime.session.abortCompaction(),
        catch: (cause) => new WorkerSessionAbortError({
          operation: "abort-compaction",
          stage: "compaction",
          message: describeError(cause, "Worker compaction abort failed"),
          cause,
        }),
      }));
      const prompt = yield* Effect.result(Effect.tryPromise({
        try: () => this.runtime.session.abort(),
        catch: (cause) => new WorkerSessionAbortError({
          operation: "abort-prompt",
          stage: "prompt",
          message: describeError(cause, "Worker prompt abort failed"),
          cause,
        }),
      }));

      if (compaction._tag === "Failure") return yield* Effect.fail(compaction.failure);
      if (prompt._tag === "Failure") return yield* Effect.fail(prompt.failure);
    }).call(this);
  }

  dispose(): Effect.Effect<void, never> {
    return this.disposeOperation;
  }

  subscribeObservation(listener: (observation: WorkerSessionObservation) => void): () => void {
    // A disposed handle has no current event source. Late subscription is an
    // explicit no-op rather than a listener retained forever.
    if (this.promptState._tag === "Disposed") return () => {};
    return subscribe(this.observationListeners, listener);
  }
}

function safelyNotify(callback: () => void): void {
  try { callback(); } catch { /* One cleanup/listener cannot block another. */ }
}

function subscribe<T>(listeners: Set<(value: T) => void>, listener: (value: T) => void): () => void {
  listeners.add(listener);
  let active = true;
  return () => {
    if (!active) return;
    active = false;
    listeners.delete(listener);
  };
}

function selectedModelCoordinates(
  definition: WorkerDefinition,
  parentModel: Model<Api> | undefined,
): { provider: string; modelId: string } {
  if (definition.model) return definition.model;
  if (parentModel) return { provider: parentModel.provider, modelId: parentModel.id };
  throw new Error(`Worker "${definition.name}" has no configured model and no parent model is available`);
}

function modelAcquisitionError(
  operation: WorkerModelAcquisitionOperation,
  fallback: string,
): (cause: unknown) => WorkerModelAcquisitionError {
  return (cause) => new WorkerModelAcquisitionError({
    operation,
    message: describeError(cause, fallback),
    cause,
  });
}

function resourceAcquisitionError(
  operation: WorkerResourceAcquisitionOperation,
): (cause: unknown) => WorkerResourceAcquisitionError {
  return (cause) => new WorkerResourceAcquisitionError({
    operation,
    message: describeError(cause, "Worker resource acquisition failed"),
    cause,
  });
}

function agentSessionAcquisitionError(
  operation: WorkerAgentSessionAcquisitionOperation,
): (cause: unknown) => WorkerAgentSessionAcquisitionError {
  return (cause) => new WorkerAgentSessionAcquisitionError({
    operation,
    message: describeError(cause, "Worker agent session acquisition failed"),
    cause,
  });
}

const refreshModelRuntime = Effect.fn("WorkerSession.refreshModelRuntime")(function* (
  modelRuntime: ModelRuntime,
  definition: WorkerDefinition,
) {
  const refreshError = modelAcquisitionError(
    "refresh-providers",
    "Worker model refresh failed",
  );
  const result = yield* Effect.tryPromise({
    try: () => modelRuntime.refresh({ allowNetwork: false }),
    catch: refreshError,
  });
  if (result.errors.size > 0) {
    const errors = [...result.errors].map(([id, error]) => `${id}: ${error.message}`).join("; ");
    const cause = new Error(
      `Worker "${definition.name}" failed to refresh child model providers: ${errors}`,
    );
    return yield* Effect.fail(refreshError(cause));
  }
  if (result.aborted) {
    const cause = new Error(`Worker "${definition.name}" child model refresh was aborted`);
    return yield* Effect.fail(refreshError(cause));
  }
});

const prepareChildModelRuntime = Effect.fn("WorkerSession.prepareChildModelRuntime")(function* (
  options: ChildSessionOptions,
  dependencies: WorkerSessionDependencies,
) {
  const selected = yield* Effect.try({
    try: () => selectedModelCoordinates(options.definition, options.parentModel),
    catch: modelAcquisitionError("select-model", "Worker model selection failed"),
  });
  const modelRuntime = yield* Effect.tryPromise({
    try: () => dependencies.createModelRuntime({
      authPath: join(options.agentDir, "auth.json"),
      modelsPath: join(options.agentDir, "models.json"),
    }),
    catch: modelAcquisitionError("create-model-runtime", "Worker model runtime creation failed"),
  });

  yield* Effect.try({
    try: () => {
      for (const providerId of options.modelRegistry.getRegisteredProviderIds()) {
        const config = options.modelRegistry.getRegisteredProviderConfig(providerId);
        if (config) modelRuntime.registerProvider(providerId, { ...config });
      }
    },
    catch: modelAcquisitionError("register-providers", "Worker model provider registration failed"),
  });
  // Worker-session owns this pre-service refresh so dynamic parent providers exist
  // before selected-model authentication is copied.
  yield* refreshModelRuntime(modelRuntime, options.definition);

  const model = yield* Effect.try({
    try: () => modelRuntime.getModel(selected.provider, selected.modelId),
    catch: modelAcquisitionError("resolve-model", "Worker model resolution failed"),
  });
  if (model) {
    const auth = yield* Effect.tryPromise({
      try: () => options.modelRegistry.getApiKeyAndHeaders(model),
      catch: modelAcquisitionError("authenticate-model", "Worker model authentication failed"),
    });
    if (auth.ok) {
      yield* Effect.tryPromise({
        try: async () => {
          if (auth.headers) {
            modelRuntime.registerProvider(selected.provider, { headers: { ...auth.headers } });
          }
          if (auth.apiKey && !options.modelRegistry.isUsingOAuth(model)) {
            await modelRuntime.setRuntimeApiKey(selected.provider, auth.apiKey);
          }
        },
        catch: modelAcquisitionError("configure-runtime-auth", "Worker model runtime key setup failed"),
      });
    }
  }

  return { selected, modelRuntime };
});

function skillLoaderOptions(definition: WorkerDefinition): Pick<ResourceLoaderOptions, "noSkills" | "skillsOverride"> {
  if (definition.skills === undefined) return {};
  const selectedSkills = new Set(definition.skills);
  return {
    noSkills: selectedSkills.size === 0,
    skillsOverride: (base) => ({
      skills: base.skills.filter((skill) => selectedSkills.has(skill.name)),
      diagnostics: base.diagnostics,
    }),
  };
}

function contextLoaderOptions(
  agentDir: string,
  projectTrusted: boolean,
): Pick<ResourceLoaderOptions, "agentsFilesOverride"> {
  if (projectTrusted) return {};
  const globalRoot = canonicalPath(agentDir);
  return {
    agentsFilesOverride: (base) => ({
      agentsFiles: base.agentsFiles.filter((file) => isWithin(canonicalPath(file.path), globalRoot)),
    }),
  };
}

const disposeWorkerSession = Effect.fn("WorkerSession.dispose")(function* (
  scope: Scope.Closeable,
  reporter: WorkerSessionCleanupReporter,
) {
  yield* Scope.close(scope, Exit.void).pipe(
    Effect.catchCause((cause) =>
      reportCleanupFailure(reporter, "scope-close", Cause.squash(cause))
    ),
  );
});

const acquireWorkerServices = Effect.fn("WorkerSession.acquireServices")(function* (
  options: ChildSessionOptions,
  dependencies: WorkerSessionDependencies,
  modelRuntime: ModelRuntime,
) {
  const definition = options.definition;
  const settingsManager = yield* Effect.try({
    try: () => dependencies.createSettingsManager({
      cwd: options.cwd,
      agentDir: options.agentDir,
      projectTrusted: options.projectTrusted,
      compaction: definition.compaction,
    }),
    catch: resourceAcquisitionError("create-settings"),
  });
  const extensionPaths = yield* Effect.tryPromise({
    try: () => dependencies.resolveExtensionPaths({
      cwd: options.cwd,
      agentDir: options.agentDir,
      settingsManager,
    }),
    catch: resourceAcquisitionError("resolve-extensions"),
  }).pipe(Effect.map((paths) => paths.filter((path) => !isOrchestrationExtensionPath(path))));

  return yield* Effect.acquireRelease(
    Effect.tryPromise({
      // Pi's supported helper constructs and reloads its loader internally. If reload rejects,
      // it exposes no loader to dispose; callers cannot make that acquisition failure-atomic.
      try: () => dependencies.createServices({
        cwd: options.cwd,
        agentDir: options.agentDir,
        settingsManager,
        modelRuntime,
        resourceLoaderOptions: {
          noExtensions: true,
          additionalExtensionPaths: extensionPaths,
          noPromptTemplates: true,
          noThemes: true,
          ...skillLoaderOptions(definition),
          ...contextLoaderOptions(options.agentDir, options.projectTrusted),
          appendSystemPrompt: [definition.systemPrompt, DIRECT_CHILD_BOUNDARY],
        },
      }),
      catch: resourceAcquisitionError("create-services"),
    }),
    (services) => resourceLoaderFinalizer(
      services.resourceLoader,
      dependencies.reportCleanupFailure,
    ),
  );
});

interface AgentSessionOwnership {
  readonly session: WorkerAgentSession;
  runtime: OwnedWorkerRuntime | undefined;
}

function rawSessionFinalizer(
  session: WorkerAgentSession,
  reporter: WorkerSessionCleanupReporter,
): Effect.Effect<void> {
  return bestEffortCleanup(reporter, "raw-session", () => session.dispose());
}

function agentSessionFinalizer(
  ownership: AgentSessionOwnership,
  reporter: WorkerSessionCleanupReporter,
): Effect.Effect<void> {
  const runtime = ownership.runtime;
  if (!runtime) return rawSessionFinalizer(ownership.session, reporter);
  return Effect.tryPromise({
    try: () => runtime.dispose(),
    catch: (cause) => cause,
  }).pipe(
    Effect.catch((cause) =>
      reportCleanupFailure(reporter, "runtime", cause).pipe(
        Effect.andThen(rawSessionFinalizer(ownership.session, reporter)),
      )
    ),
  );
}

const acquireAgentSession = Effect.fn("WorkerSession.acquireAgentSession")(function* (
  dependencies: WorkerSessionDependencies,
  input: AgentSessionInput,
) {
  return yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () => dependencies.createAgentSession(input),
      catch: agentSessionAcquisitionError("create-session"),
    }).pipe(Effect.map(({ session }) => {
      const ownership: AgentSessionOwnership = { session, runtime: undefined };
      return ownership;
    })),
    (ownership) => agentSessionFinalizer(
      ownership,
      dependencies.reportCleanupFailure,
    ),
  );
});

const acquireAgentSessionRuntime = Effect.fn("WorkerSession.acquireAgentSessionRuntime")(function* (
  dependencies: WorkerSessionDependencies,
  ownership: AgentSessionOwnership,
  services: AgentSessionServices,
) {
  const runtime = yield* Effect.try({
    try: () => dependencies.createRuntime({ session: ownership.session, services }),
    catch: agentSessionAcquisitionError("create-runtime"),
  });
  ownership.runtime = runtime;
  return runtime;
});

const acquireSessionSubscription = Effect.fn("WorkerSession.acquireSubscription")(function* (
  dependencies: WorkerSessionDependencies,
  runtime: OwnedWorkerRuntime,
  handle: DefaultWorkerSessionHandle,
) {
  return yield* Effect.acquireRelease(
    Effect.try({
      try: () => runtime.session.subscribe((event) => handle.receiveSessionEvent(event)),
      catch: agentSessionAcquisitionError("subscribe-events"),
    }),
    (unsubscribe) => bestEffortCleanup(
      dependencies.reportCleanupFailure,
      "unsubscribe",
      unsubscribe,
    ),
  );
});

const createWorkerSession = Effect.fn("WorkerSession.create")(function* (
  options: ChildSessionOptions,
  dependencies: WorkerSessionDependencies,
) {
  const scope = yield* Scope.make("sequential");
  const acquisition = Effect.gen(function* () {
    const definition = options.definition;
    const { selected, modelRuntime } = yield* prepareChildModelRuntime(options, dependencies);
    const services = yield* acquireWorkerServices(options, dependencies, modelRuntime);

    // createAgentSessionServices owns extension-provider registration and refresh, but
    // Pi 0.80.10 discards that refresh result. Keep this worker-owned probe so provider
    // errors and aborts remain typed acquisition failures before session creation.
    yield* refreshModelRuntime(modelRuntime, definition);
    const model = yield* Effect.try({
      try: () => {
        const resolvedModel = modelRuntime.getModel(selected.provider, selected.modelId);
        if (!resolvedModel) {
          throw new Error(
            `Worker "${definition.name}" configured model "${selected.provider}/${selected.modelId}" was not found`,
          );
        }
        return resolvedModel;
      },
      catch: modelAcquisitionError("resolve-model", "Worker model resolution failed"),
    });
    const sessionManager = yield* Effect.try({
      try: () => dependencies.createSessionManager({
        cwd: options.cwd,
        parentSessionFile: options.parentSessionFile,
      }),
      catch: agentSessionAcquisitionError("create-session-manager"),
    });
    const ownership = yield* acquireAgentSession(dependencies, {
      services,
      sessionManager,
      model,
      thinkingLevel: definition.thinking,
      tools: [...definition.tools],
    });
    const runtime = yield* acquireAgentSessionRuntime(dependencies, ownership, services);
    yield* Effect.tryPromise({
      try: () => runtime.session.bindExtensions({ mode: "print" }),
      catch: agentSessionAcquisitionError("bind-extensions"),
    });

    yield* Effect.try({
      try: () => {
        if (definition.skills === undefined) return;
        const loadedNames = new Set(
          services.resourceLoader.getSkills().skills.map((skill) => skill.name),
        );
        const missing = definition.skills.filter((name) => !loadedNames.has(name));
        if (missing.length > 0) {
          throw new Error(
            `Worker "${definition.name}" selected skills were not loaded: ${missing.join(", ")}`,
          );
        }
      },
      catch: resourceAcquisitionError("validate-skills"),
    });
    const sessionFile = yield* Effect.try({
      try: () => {
        if (!runtime.session.sessionFile) {
          throw new Error("Worker session was not created with durable storage");
        }
        return runtime.session.sessionFile;
      },
      catch: agentSessionAcquisitionError("verify-durability"),
    });

    const handle = yield* DefaultWorkerSessionHandle.make(
      runtime,
      definition.lifecycle === "interactive",
      sessionFile,
      scope,
      dependencies.reportCleanupFailure,
    );
    yield* acquireSessionSubscription(dependencies, runtime, handle);
    return handle;
  }).pipe(Scope.provide(scope));

  return yield* acquisition.pipe(
    Effect.catchCause((cause) =>
      Effect.gen(function* () {
        yield* Scope.close(scope, Exit.failCause(cause)).pipe(
          Effect.catchCause((cleanupCause) =>
            reportCleanupFailure(
              dependencies.reportCleanupFailure,
              "scope-close",
              Cause.squash(cleanupCause),
            )
          ),
        );
        return yield* Effect.failCause(cause);
      })
    ),
  );
});

type AcquisitionHandoffState =
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Offered"; readonly session: WorkerSessionHandle }
  | { readonly _tag: "Adopting"; readonly session: WorkerSessionHandle }
  | { readonly _tag: "Adopted" }
  | { readonly _tag: "Abandoned"; readonly error?: WorkerSessionAcquisitionClosedError }
  | { readonly _tag: "Failed" };

interface AcquisitionHandoff {
  readonly state: SynchronizedRef.SynchronizedRef<AcquisitionHandoffState>;
  readonly result: Deferred.Deferred<WorkerSessionHandle, WorkerSessionAcquisitionError>;
}

type ChildSessionsState =
  | { readonly _tag: "Open"; readonly handoffs: ReadonlySet<AcquisitionHandoff> }
  | { readonly _tag: "Closed"; readonly error: WorkerSessionAcquisitionClosedError };

type AdoptionReservation =
  | { readonly _tag: "Reserved"; readonly session: WorkerSessionHandle }
  | { readonly _tag: "Closed"; readonly error: WorkerSessionAcquisitionClosedError }
  | { readonly _tag: "Abandoned" };

function disposeLateSession(session: WorkerSessionHandle): Effect.Effect<void> {
  return session.dispose().pipe(Effect.catchCause(() => Effect.void));
}

function admitReclamationOrJoinAfterClosure(
  fibers: FiberSet.FiberSet<void, never>,
  reclamation: Effect.Effect<void>,
  onOpenObserved: () => void,
): Effect.Effect<void> {
  return Effect.suspend(() => {
    if (fibers.state._tag === "Closed") return reclamation;
    safelyNotify(onOpenObserved);
    return Effect.gen(function* () {
      yield* FiberSet.run(fibers, reclamation, { startImmediately: true });
      if (fibers.state._tag === "Closed") {
        // FiberSet.run returns an interrupted sentinel when closure wins admission.
        // If admission won, closure interrupts the admitted fiber instead. Real
        // session disposal is cached and uninterruptible, so this fallback safely
        // joins that same disposal in both cases rather than starting cleanup twice.
        yield* reclamation;
      }
    });
  });
}

export function createChildSessionsLayer(
  overrides: Partial<WorkerSessionDependencies> = {},
): Layer.Layer<ChildSessions> {
  return Layer.effect(
    ChildSessions,
    Effect.gen(function* () {
      const dependencies: WorkerSessionDependencies = { ...defaultDependencies, ...overrides };
      const fibers = yield* FiberSet.make<void, never>();
      const serviceState = yield* SynchronizedRef.make<ChildSessionsState>({
        _tag: "Open",
        handoffs: new Set(),
      });

      const withoutHandoff = (
        state: ChildSessionsState,
        handoff: AcquisitionHandoff,
      ): ChildSessionsState => {
        if (state._tag === "Closed" || !state.handoffs.has(handoff)) return state;
        const handoffs = new Set(state.handoffs);
        handoffs.delete(handoff);
        return { _tag: "Open", handoffs };
      };

      const removeHandoff = (handoff: AcquisitionHandoff): Effect.Effect<void> =>
        SynchronizedRef.update(serviceState, (state) => withoutHandoff(state, handoff));

      const runReclamation = (session: WorkerSessionHandle): Effect.Effect<void> =>
        admitReclamationOrJoinAfterClosure(
          fibers,
          disposeLateSession(session).pipe(Effect.uninterruptible),
          dependencies.onReclamationOpenObserved,
        );

      const abandonHandoff = Effect.fn("ChildSessions.abandonHandoff")(function* (
        handoff: AcquisitionHandoff,
      ) {
        const session = yield* SynchronizedRef.modifyEffect(serviceState, (service) => {
          const nextService = withoutHandoff(service, handoff);
          return SynchronizedRef.modify(handoff.state, (state): readonly [
            { readonly session: WorkerSessionHandle | undefined },
            AcquisitionHandoffState,
          ] => {
            if (state._tag === "Pending") {
              return [{ session: undefined }, { _tag: "Abandoned" }];
            }
            if (state._tag === "Offered") {
              return [{ session: state.session }, { _tag: "Abandoned" }];
            }
            return [{ session: undefined }, state];
          }).pipe(Effect.map(({ session }) => [session, nextService] as const));
        });
        if (session) yield* runReclamation(session);
      });

      const shutdown = Effect.fn("ChildSessions.shutdown")(function* () {
        const closed = yield* SynchronizedRef.modifyEffect(serviceState, (state) => {
          if (state._tag === "Closed") return Effect.succeed([undefined, state] as const);
          const error = new WorkerSessionAcquisitionClosedError({
            message: "Child sessions are shutting down",
          });
          return Effect.gen(function* () {
            const sessions: WorkerSessionHandle[] = [];
            for (const handoff of state.handoffs) {
              const session = yield* SynchronizedRef.modifyEffect(handoff.state, (handoffState) => {
                if (handoffState._tag === "Pending") {
                  return Deferred.fail(handoff.result, error).pipe(
                    Effect.as([undefined, { _tag: "Abandoned", error }] as const),
                  );
                }
                if (handoffState._tag === "Offered") {
                  return Effect.succeed([
                    handoffState.session,
                    { _tag: "Abandoned", error },
                  ] as const);
                }
                // Failed already settled its Deferred. Adopting is an ownership
                // reservation whose synchronous winner must be allowed to commit.
                return Effect.succeed([undefined, handoffState] as const);
              });
              if (session) sessions.push(session);
            }
            return [{ sessions }, { _tag: "Closed", error }] as const;
          });
        });
        if (!closed) return;
        for (const session of closed.sessions) yield* runReclamation(session);
      });

      yield* Effect.addFinalizer(() => shutdown());

      const acquire = Effect.fn("ChildSessions.acquire")(function* <Adopted>(
        options: ChildSessionOptions,
        adopt: (session: WorkerSessionHandle) => Adopted | undefined,
      ) {
        const handoff: AcquisitionHandoff = {
          state: yield* SynchronizedRef.make<AcquisitionHandoffState>({ _tag: "Pending" }),
          result: yield* Deferred.make<WorkerSessionHandle, WorkerSessionAcquisitionError>(),
        };
        return yield* Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
          const admissionError = yield* SynchronizedRef.modify(serviceState, (state) => {
            if (state._tag === "Closed") return [state.error, state] as const;
            return [undefined, {
              _tag: "Open",
              handoffs: new Set([...state.handoffs, handoff]),
            }] as const;
          });
          if (admissionError) return yield* Effect.fail(admissionError);

          const producer = createWorkerSession(options, dependencies).pipe(
            Effect.matchCauseEffect({
              onFailure: (cause) => Effect.gen(function* () {
                const failed = yield* SynchronizedRef.modifyEffect(handoff.state, (state) =>
                  state._tag === "Pending"
                    ? Deferred.failCause(handoff.result, cause).pipe(
                        Effect.as([true, { _tag: "Failed" }] as const),
                      )
                    : Effect.succeed([false, state] as const));
                if (failed) yield* removeHandoff(handoff);
              }),
              onSuccess: (session) => Effect.gen(function* () {
                const offered = yield* SynchronizedRef.modifyEffect(handoff.state, (state) =>
                  state._tag === "Pending"
                    ? Deferred.succeed(handoff.result, session).pipe(
                        Effect.as([true, { _tag: "Offered", session }] as const),
                      )
                    : Effect.succeed([false, state] as const));
                if (!offered) {
                  yield* removeHandoff(handoff);
                  yield* runReclamation(session);
                }
              }),
            }),
            Effect.uninterruptible,
          );
          yield* FiberSet.run(fibers, producer, { startImmediately: true });

          const session = yield* restore(Deferred.await(handoff.result)).pipe(
            Effect.onInterrupt(() => abandonHandoff(handoff)),
          );
          const reservation = yield* SynchronizedRef.modifyEffect(serviceState, (service) => {
            if (service._tag === "Closed") {
              return Effect.succeed([
                { _tag: "Closed", error: service.error } satisfies AdoptionReservation,
                service,
              ] as const);
            }
            return SynchronizedRef.modify(
              handoff.state,
              (state): readonly [AdoptionReservation, AcquisitionHandoffState] => {
                if (state._tag === "Abandoned") {
                  return state.error
                    ? [{ _tag: "Closed", error: state.error }, state]
                    : [{ _tag: "Abandoned" }, state];
                }
                if (state._tag !== "Offered" || state.session !== session) {
                  return [{ _tag: "Abandoned" }, state];
                }
                return [
                  { _tag: "Reserved", session },
                  { _tag: "Adopting", session },
                ];
              },
            ).pipe(Effect.map((result) => [result, service] as const));
          });
          if (reservation._tag === "Closed") return yield* Effect.fail(reservation.error);
          if (reservation._tag === "Abandoned") {
            return yield* Effect.die(new Error("Child session acquisition handoff was abandoned"));
          }

          // The adopter is arbitrary synchronous runtime code. The reservation
          // protects it from shutdown, but no SynchronizedRef semaphore is held.
          const adopted = yield* Effect.exit(Effect.sync(() => adopt(reservation.session)));
          const transferred = Exit.isSuccess(adopted) && adopted.value !== undefined;
          yield* SynchronizedRef.update(handoff.state, (state): AcquisitionHandoffState => {
            if (state._tag !== "Adopting" || state.session !== reservation.session) return state;
            return transferred ? { _tag: "Adopted" } : { _tag: "Abandoned" };
          });
          yield* removeHandoff(handoff);
          if (transferred) return adopted.value;

          yield* runReclamation(reservation.session);
          if (Exit.isFailure(adopted)) return yield* Effect.failCause(adopted.cause);
          return undefined;
        }));
      });

      return ChildSessions.of({ acquire, shutdown });
    }),
  );
}
