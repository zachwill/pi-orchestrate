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
  type ResourceLoader,
  SessionManager,
  SettingsManager,
  type AgentSessionEvent,
  type AgentSessionServices,
  type CreateAgentSessionResult,
  type DefaultResourceLoader,
} from "@earendil-works/pi-coding-agent";
import { Schema } from "effect";
import type {
  WorkerDefinition,
  WorkerMessageDirection,
  WorkerOutcome,
  WorkerUsage,
} from "./domain.js";

const DIRECT_CHILD_BOUNDARY =
  "You are a direct child worker session. Do not spawn, delegate to, or orchestrate other workers or child sessions. Complete the assigned task yourself and return the result directly to the parent orchestrator.";
const ORCHESTRATE_PACKAGE_ROOT = canonicalPath(fileURLToPath(new URL("..", import.meta.url)));

interface WorkerAgentSession {
  readonly sessionFile: string | undefined;
  readonly messages: AgentMessage[];
  prompt(instructions: string): Promise<void>;
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
  dispose(): Promise<void>;
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

function disposeResourceLoader(resourceLoader: ResourceLoader): void {
  if (!("dispose" in resourceLoader) || typeof resourceLoader.dispose !== "function") return;
  try {
    resourceLoader.dispose();
  } catch {
    // Cleanup is best-effort.
  }
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

class DefaultWorkerSessionHandle implements WorkerSessionHandle {
  readonly sessionFile: string;
  private readonly usage = emptyUsage();
  private readonly usageListeners = new Set<(usage: WorkerUsage) => void>();
  private readonly activityListeners = new Set<(activity: string | undefined) => void>();
  private readonly messageDirectionListeners = new Set<(direction: WorkerMessageDirection) => void>();
  private readonly activeToolCalls = new Map<string, string>();
  private disposed = false;
  private disposePromise: Promise<void> | undefined;
  private activity: string | undefined;
  private messageDirection: WorkerMessageDirection | undefined;
  private prompting = false;
  private abortRequested = false;
  private promptAssistant: AssistantMessage | undefined;

  constructor(
    private readonly runtime: OwnedWorkerRuntime,
    private readonly reusable: boolean,
    private readonly unsubscribe: () => void,
    private readonly resourceLoader: ResourceLoader,
  ) {
    this.sessionFile = runtime.session.sessionFile!;
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
    this.promptAssistant = event.message;
    this.usage.input += event.message.usage.input ?? 0;
    this.usage.output += event.message.usage.output ?? 0;
    this.usage.cacheRead += event.message.usage.cacheRead ?? 0;
    this.usage.cacheWrite += event.message.usage.cacheWrite ?? 0;
    this.usage.cost += event.message.usage.cost?.total ?? 0;
    this.usage.contextTokens = event.message.usage.totalTokens ?? 0;
    this.usage.turns += 1;
    for (const listener of [...this.usageListeners]) safelyNotify(() => listener({ ...this.usage }));
  }

  private setActivity(activity: string | undefined): void {
    if (this.activity === activity) return;
    this.activity = activity;
    for (const listener of [...this.activityListeners]) safelyNotify(() => listener(activity));
  }

  private setMessageDirection(direction: WorkerMessageDirection): void {
    if (this.messageDirection === direction) return;
    this.messageDirection = direction;
    for (const listener of [...this.messageDirectionListeners]) safelyNotify(() => listener(direction));
  }

  async prompt(instructions: string): Promise<WorkerOutcome> {
    if (this.disposed) throw new Error("Worker session has been disposed");
    if (this.prompting) throw new Error("Worker session is already processing a prompt");
    this.prompting = true;
    this.abortRequested = false;
    this.promptAssistant = undefined;
    this.setMessageDirection("to-model");
    const previousAssistant = lastAssistant(this.runtime.session.messages);
    let failureMessage: string | undefined;
    try {
      await this.runtime.session.prompt(instructions);
    } catch (error) {
      failureMessage = describeError(error, "Worker prompt failed");
    } finally {
      this.prompting = false;
    }
    const latestAssistant = lastAssistant(this.runtime.session.messages);
    const message = this.promptAssistant ?? (latestAssistant !== previousAssistant ? latestAssistant : undefined);
    const text = assistantText(message);
    const assistantPayload = text === undefined ? {} : { assistantText: text };
    if (this.abortRequested || message?.stopReason === "aborted") {
      const abortMessage = message?.errorMessage ?? failureMessage;
      return { status: "aborted", ...(abortMessage ? { message: abortMessage } : {}), ...assistantPayload };
    }
    if (message?.stopReason === "error") {
      return { status: "failed", message: message.errorMessage ?? failureMessage ?? "Worker assistant reported a failure", ...assistantPayload };
    }
    if (failureMessage) return { status: "failed", message: failureMessage, ...assistantPayload };
    return { status: this.reusable ? "ready" : "completed", assistantText: text ?? "" };
  }

  async abort(): Promise<void> {
    if (this.prompting) this.abortRequested = true;
    await this.runtime.session.abort();
  }

  dispose(): Promise<void> {
    if (this.disposePromise) return this.disposePromise;

    this.disposed = true;
    this.activeToolCalls.clear();
    this.usageListeners.clear();
    this.activityListeners.clear();
    this.messageDirectionListeners.clear();
    safelyNotify(this.unsubscribe);

    this.disposePromise = this.runtime.dispose()
      .catch(() => {})
      .finally(() => disposeResourceLoader(this.resourceLoader));
    return this.disposePromise;
  }

  subscribeUsage(listener: (usage: WorkerUsage) => void): () => void {
    return subscribe(this.usageListeners, listener);
  }
  subscribeActivity(listener: (activity: string | undefined) => void): () => void {
    return subscribe(this.activityListeners, listener);
  }
  subscribeMessageDirection(listener: (direction: WorkerMessageDirection) => void): () => void {
    return subscribe(this.messageDirectionListeners, listener);
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

async function prepareChildModelRuntime(
  options: WorkerSessionFactoryOptions,
  dependencies: WorkerSessionDependencies,
): Promise<{ selected: { provider: string; modelId: string }; modelRuntime: ModelRuntime }> {
  let selected: { provider: string; modelId: string };
  try {
    selected = selectedModelCoordinates(options.definition, options.parentModel);
  } catch (cause) {
    throw new WorkerModelAcquisitionError({ message: describeError(cause, "Worker model selection failed"), cause });
  }
  let modelRuntime: ModelRuntime;
  try {
    modelRuntime = await dependencies.createModelRuntime({
      authPath: join(options.agentDir, "auth.json"),
      modelsPath: join(options.agentDir, "models.json"),
    });
    for (const providerId of options.modelRegistry.getRegisteredProviderIds()) {
      const config = options.modelRegistry.getRegisteredProviderConfig(providerId);
      if (config) modelRuntime.registerProvider(providerId, { ...config });
    }
    await refreshModelRuntime(modelRuntime, options.definition);
    const model = modelRuntime.getModel(selected.provider, selected.modelId);
    if (model) {
      const auth = await options.modelRegistry.getApiKeyAndHeaders(model);
      if (auth.ok) {
        if (auth.headers) modelRuntime.registerProvider(selected.provider, { headers: { ...auth.headers } });
        if (auth.apiKey && !options.modelRegistry.isUsingOAuth(model)) {
          await modelRuntime.setRuntimeApiKey(selected.provider, auth.apiKey);
        }
      }
    }
  } catch (cause) {
    if (cause instanceof WorkerModelAcquisitionError) throw cause;
    throw new WorkerModelAcquisitionError({ message: describeError(cause, "Worker model runtime creation failed"), cause });
  }
  return { selected, modelRuntime };
}

async function refreshModelRuntime(modelRuntime: ModelRuntime, definition: WorkerDefinition): Promise<void> {
  const result = await modelRuntime.refresh({ allowNetwork: false });
  if (result.errors.size > 0) {
    const errors = [...result.errors].map(([id, error]) => `${id}: ${error.message}`).join("; ");
    throw new Error(`Worker "${definition.name}" failed to refresh child model providers: ${errors}`);
  }
  if (result.aborted) throw new Error(`Worker "${definition.name}" child model refresh was aborted`);
}

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

async function createWorkerSession(
  options: WorkerSessionFactoryOptions,
  dependencies: WorkerSessionDependencies,
): Promise<WorkerSessionHandle> {
  const definition = options.definition;
  const { selected, modelRuntime } = await prepareChildModelRuntime(options, dependencies);
  let settingsManager: SettingsManager;
  let services: AgentSessionServices | undefined;
  let session: WorkerAgentSession | undefined;
  let runtime: OwnedWorkerRuntime | undefined;
  let unsubscribe: (() => void) | undefined;

  try {
    settingsManager = dependencies.createSettingsManager({
      cwd: options.cwd,
      agentDir: options.agentDir,
      projectTrusted: options.projectTrusted,
      compaction: definition.compaction,
    });
    const extensionPaths = (await dependencies.resolveExtensionPaths({
      cwd: options.cwd,
      agentDir: options.agentDir,
      settingsManager,
    })).filter((path) => !isOrchestrationExtensionPath(path));

    services = await dependencies.createServices({
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
    });
  } catch (cause) {
    if (services) disposeResourceLoader(services.resourceLoader);
    throw new WorkerResourceAcquisitionError({ message: describeError(cause, "Worker resource acquisition failed"), cause });
  }

  try {
    await refreshModelRuntime(modelRuntime, definition);
    const model = modelRuntime.getModel(selected.provider, selected.modelId);
    if (!model) throw new Error(`Worker "${definition.name}" configured model "${selected.provider}/${selected.modelId}" was not found`);
    const sessionManager = dependencies.createSessionManager({
      cwd: options.cwd,
      parentSessionFile: options.parentSessionFile,
    });
    const created = await dependencies.createAgentSession({
      services,
      sessionManager,
      model,
      thinkingLevel: definition.thinking,
      tools: [...definition.tools],
    });
    session = created.session;
    runtime = dependencies.createRuntime({ session, services });
    await runtime.session.bindExtensions({ mode: "print" });

    if (definition.skills !== undefined) {
      const loadedNames = new Set(services.resourceLoader.getSkills().skills.map((skill) => skill.name));
      const missing = definition.skills.filter((name) => !loadedNames.has(name));
      if (missing.length > 0) throw new Error(`Worker "${definition.name}" selected skills were not loaded: ${missing.join(", ")}`);
    }
    if (!runtime.session.sessionFile) throw new Error("Worker session was not created with durable storage");

    let handle!: DefaultWorkerSessionHandle;
    unsubscribe = runtime.session.subscribe((event) => handle.receiveSessionEvent(event));
    handle = new DefaultWorkerSessionHandle(
      runtime,
      definition.lifecycle === "reusable",
      unsubscribe,
      services.resourceLoader,
    );
    return handle;
  } catch (cause) {
    safelyNotify(() => unsubscribe?.());
    if (runtime) {
      try { await runtime.dispose(); } catch { /* best effort */ }
    } else if (session) {
      try { session.dispose(); } catch { /* best effort */ }
    }
    disposeResourceLoader(services.resourceLoader);
    throw new WorkerAgentSessionAcquisitionError({ message: describeError(cause, "Worker agent session acquisition failed"), cause });
  }
}

export function createWorkerSessionFactory(
  overrides: Partial<WorkerSessionDependencies> = {},
): WorkerSessionFactory {
  const dependencies: WorkerSessionDependencies = { ...defaultDependencies, ...overrides };
  return { create: (options) => createWorkerSession(options, dependencies) };
}
