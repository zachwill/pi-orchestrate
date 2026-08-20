import { describe, expect, test } from "bun:test";
import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Cause, Effect, Exit, Layer, ManagedRuntime } from "effect";
import { TestClock } from "effect/testing";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  createSequentialIdFactories,
  createWorkerCatalog,
  type OrchestrateTaskInput,
  type RunId,
  type WorkerDefinition,
  type WorkerId,
  type WorkerMessageDirection,
  type WorkerOutcome,
  type WorkerUsage,
} from "../extension/domain.ts";
import {
  MAX_COMPLETED_RUN_HISTORY,
  MAX_TERMINAL_WORKER_HISTORY,
  SHUTDOWN_CLEANUP_GRACE_MS,
  Orchestration,
  OrchestrationActionRejected,
  orchestrationLayer,
  type CompletedRun,
  type OrchestrationContext,
  type OrchestrationLayerOptions,
  type OrchestrationService,
} from "../extension/runtime.ts";
import { DeliveryCoordinator } from "../extension/delivery.ts";
import {
  createProcessHostRuntimeAdapter,
  destroyProcessHost,
  type ProcessHost,
} from "../extension/host.ts";
import {
  CleanupSupervisor,
  GenerationSupervisor,
  processSupervisorLayer,
  type CleanupSupervisorService,
  type GenerationSupervisorService,
} from "../extension/scheduler.ts";
import {
  ChildSessions,
  WorkerAgentSessionAcquisitionError,
  WorkerSessionAbortError,
  type ChildSessionOptions,
  type ChildSessionsService,
  type WorkerSessionHandle,
} from "../extension/worker-session.ts";
import type { WorkerSettlement } from "../extension/worker-settlement.ts";

class Deferred<T = void> {
  readonly promise: Promise<T>;
  private complete!: (value: T | PromiseLike<T>) => void;

  constructor() {
    this.promise = new Promise<T>((resolve) => {
      this.complete = resolve;
    });
  }

  resolve(value: T extends void ? undefined : T): void {
    this.complete(value as T);
  }
}

class Counter {
  value = 0;
  private readonly waiters: Array<{ count: number; resolve: () => void }> = [];

  increment(): void {
    this.value += 1;
    for (const waiter of [...this.waiters]) {
      if (this.value >= waiter.count) waiter.resolve();
    }
  }

  waitFor(count: number): Promise<void> {
    if (this.value >= count) return Promise.resolve();
    return new Promise((resolve) => this.waiters.push({ count, resolve }));
  }
}

const EMPTY_USAGE: WorkerUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  cost: 0,
  contextTokens: 0,
  turns: 0,
};

interface PromptPlan {
  readonly gate: Deferred;
  readonly outcome: WorkerOutcome;
}

class PromptTracker {
  readonly starts = new Counter();
  active = 0;
  maximumActive = 0;

  start(): void {
    this.active += 1;
    this.maximumActive = Math.max(this.maximumActive, this.active);
    this.starts.increment();
  }

  finish(): void {
    this.active -= 1;
  }
}

class FakeHandle implements WorkerSessionHandle {
  readonly sessionFile: string;
  readonly prompts: string[] = [];
  readonly disposed = new Deferred();
  readonly abortStarts = new Counter();
  readonly promptFinalizerStarts = new Counter();
  abortGate: Deferred | undefined;
  abortFailure: WorkerSessionAbortError | undefined;
  promptFinalizerGate: Deferred | undefined;
  abortCalls = 0;
  disposeCalls = 0;
  disposeFailure: Error | undefined;
  disposeGate: Deferred | undefined;
  subscriptionFailure: Error | undefined;
  private disposeStarted = false;
  private readonly disposeCompleted = new Deferred();
  private readonly usageListeners = new Set<(usage: WorkerUsage) => void>();
  private readonly activityListeners = new Set<
    (activity: string | undefined) => void
  >();
  private readonly activityListenerHistory: Array<
    (activity: string | undefined) => void
  > = [];
  private readonly messageDirectionListeners = new Set<
    (direction: WorkerMessageDirection) => void
  >();

  constructor(
    name: string,
    readonly promptPlans: PromptPlan[],
    private readonly tracker: PromptTracker,
  ) {
    this.sessionFile = `/sessions/${name}.jsonl`;
  }

  prompt(instructions: string): Effect.Effect<WorkerOutcome, never> {
    return Effect.suspend(() => {
      const plan = this.promptPlans[this.prompts.length];
      if (!plan) return Effect.die(new Error("Missing fake prompt plan"));
      this.prompts.push(instructions);
      this.tracker.start();
      const finalizer = this.promptFinalizerGate
        ? Effect.sync(() => this.promptFinalizerStarts.increment()).pipe(
            Effect.andThen(Effect.promise(() => this.promptFinalizerGate!.promise)),
            Effect.uninterruptible,
          )
        : Effect.void;
      return Effect.promise(() => plan.gate.promise.then(() => plan.outcome)).pipe(
        Effect.ensuring(
          Effect.sync(() => this.tracker.finish()).pipe(Effect.andThen(finalizer)),
        ),
      );
    });
  }

  abort(): Effect.Effect<void, WorkerSessionAbortError> {
    return Effect.suspend(() => {
      this.abortCalls += 1;
      this.abortStarts.increment();
      const wait = this.abortGate
        ? Effect.promise(() => this.abortGate!.promise)
        : Effect.void;
      return wait.pipe(
        Effect.andThen(this.abortFailure ? Effect.fail(this.abortFailure) : Effect.void),
      );
    });
  }

  dispose(): Effect.Effect<void, never> {
    return Effect.suspend(() => {
      if (this.disposeStarted) {
        return Effect.promise(() => this.disposeCompleted.promise);
      }
      this.disposeStarted = true;
      this.disposeCalls += 1;
      this.disposed.resolve(undefined);
      const wait = this.disposeGate
        ? Effect.promise(() => this.disposeGate!.promise)
        : Effect.void;
      return wait.pipe(
        Effect.andThen(Effect.sync(() => {
          if (this.disposeFailure) throw this.disposeFailure;
        })),
        Effect.ensuring(Effect.sync(() => this.disposeCompleted.resolve(undefined))),
      );
    });
  }

  subscribeUsage(listener: (usage: WorkerUsage) => void): () => void {
    if (this.subscriptionFailure) throw this.subscriptionFailure;
    this.usageListeners.add(listener);
    return () => this.usageListeners.delete(listener);
  }

  emitUsage(usage: WorkerUsage): void {
    for (const listener of this.usageListeners) listener(usage);
  }

  subscribeActivity(
    listener: (activity: string | undefined) => void,
  ): () => void {
    this.activityListeners.add(listener);
    this.activityListenerHistory.push(listener);
    return () => this.activityListeners.delete(listener);
  }

  emitActivity(activity: string | undefined): void {
    for (const listener of this.activityListeners) listener(activity);
  }

  emitStaleActivity(activity: string | undefined): void {
    for (const listener of this.activityListenerHistory) listener(activity);
  }

  subscribeMessageDirection(
    listener: (direction: WorkerMessageDirection) => void,
  ): () => void {
    this.messageDirectionListeners.add(listener);
    return () => this.messageDirectionListeners.delete(listener);
  }

  emitMessageDirection(direction: WorkerMessageDirection): void {
    for (const listener of this.messageDirectionListeners) listener(direction);
  }
}

interface CreatePlan {
  readonly handle: FakeHandle;
  readonly gate?: Deferred;
}

class FakeChildSessions implements ChildSessionsService {
  readonly creates = new Counter();
  readonly options: ChildSessionOptions[] = [];
  private readonly pending = new Set<() => void>();

  constructor(private readonly plans: CreatePlan[]) {}

  readonly acquire = <Adopted>(
    options: ChildSessionOptions,
    adopt: (session: WorkerSessionHandle) => Adopted | undefined,
  ): Effect.Effect<Adopted | undefined> =>
    Effect.callback<Adopted | undefined>((resume) => {
      const plan = this.plans[this.options.length];
      if (!plan) {
        resume(Effect.die(new Error("Missing fake create plan")));
        return;
      }
      this.options.push(options);
      this.creates.increment();
      let abandoned = false;
      const abandon = () => {
        abandoned = true;
      };
      this.pending.add(abandon);
      void (plan.gate?.promise ?? Promise.resolve()).then(() => {
        this.pending.delete(abandon);
        if (abandoned) {
          void Effect.runPromise(plan.handle.dispose().pipe(Effect.ignore));
          return;
        }
        try {
          const adopted = adopt(plan.handle);
          if (adopted === undefined) {
            void Effect.runPromise(plan.handle.dispose().pipe(Effect.ignore));
          }
          resume(Effect.succeed(adopted));
        } catch (error) {
          void Effect.runPromise(plan.handle.dispose().pipe(Effect.ignore));
          resume(Effect.die(error));
        }
      });
      return Effect.sync(abandon);
    });

  readonly shutdown = (): Effect.Effect<void> => Effect.sync(() => {
    for (const abandon of [...this.pending]) abandon();
  });
}

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

function definition(
  name: string,
  lifecycle: WorkerDefinition["lifecycle"] = "one-shot",
  configuredModel?: WorkerDefinition["model"],
): WorkerDefinition {
  return {
    name,
    source: { kind: "package", filePath: `/workers/${name}.md` },
    description: `${name} worker`,
    systemPrompt: `You are ${name}.`,
    lifecycle,
    tools: ["read"],
    skills: [],
    model: configuredModel,
  };
}

function context(
  ownerSessionId: string,
  workers: readonly WorkerDefinition[],
  overrides: Partial<OrchestrationContext> = {},
): OrchestrationContext {
  return {
    ownerSessionId,
    cwd: "/project",
    agentDir: "/agent",
    parentSessionFile: "/sessions/parent.jsonl",
    projectTrusted: true,
    catalog: createWorkerCatalog(workers),
    parentModel: model("parent", "selected"),
    modelRegistry: { find: () => undefined } as unknown as ModelRegistry,
    ...overrides,
  };
}

function task(
  worker: string,
  title = `Use ${worker}`,
  instructions = `Instructions for ${worker}`,
): OrchestrateTaskInput {
  return { worker, title, instructions };
}

function promptPlan(outcome: WorkerOutcome, resolved = false): PromptPlan {
  const gate = new Deferred();
  if (resolved) gate.resolve(undefined);
  return { gate, outcome };
}

interface TestSupervisors {
  readonly generations: GenerationSupervisorService;
  readonly cleanup: CleanupSupervisorService;
}

interface TestRuntimeOptions extends OrchestrationLayerOptions {
  readonly supervisors?: TestSupervisors;
}

function directRuntime(
  sessions: ChildSessionsService,
  overrides: TestRuntimeOptions = {},
) {
  const supervisorLayer = overrides.supervisors
    ? Layer.merge(
        Layer.succeed(GenerationSupervisor, overrides.supervisors.generations),
        Layer.succeed(CleanupSupervisor, overrides.supervisors.cleanup),
      )
    : processSupervisorLayer;
  const dependencies = Layer.merge(
    supervisorLayer,
    Layer.succeed(ChildSessions, sessions),
  );
  const effectRuntime = ManagedRuntime.make(
    orchestrationLayer({
      idFactories: overrides.idFactories ?? createSequentialIdFactories(),
      clock: overrides.clock ?? (() => 1_700_000_000_000),
    }).pipe(Layer.provide(dependencies)),
  );
  return {
    effectRuntime,
    orchestration: effectRuntime.runSync(Orchestration),
  };
}

function runtime(
  sessions: ChildSessionsService,
  overrides: TestRuntimeOptions = {},
) {
  const { effectRuntime } = directRuntime(sessions, overrides);
  return createProcessHostRuntimeAdapter(effectRuntime);
}

function supervisorHarness() {
  const effectRuntime = ManagedRuntime.make(processSupervisorLayer);
  const supervisors: TestSupervisors = {
    generations: effectRuntime.runSync(GenerationSupervisor),
    cleanup: effectRuntime.runSync(CleanupSupervisor),
  };
  return { effectRuntime, supervisors };
}

async function expectPending(promise: Promise<unknown>): Promise<void> {
  let settled = false;
  void promise.finally(() => {
    settled = true;
  });
  await Promise.resolve();
  await Promise.resolve();
  expect(settled).toBe(false);
}

describe("orchestration admission and concurrency", () => {
  test("composes Orchestration from its required service Layers", async () => {
    const dependencies = Layer.merge(
      processSupervisorLayer,
      Layer.succeed(ChildSessions, new FakeChildSessions([])),
    );
    const effectRuntime = ManagedRuntime.make(
      orchestrationLayer({ idFactories: createSequentialIdFactories() }).pipe(
        Layer.provide(dependencies),
      ),
    );
    const orchestration = effectRuntime.runSync(Orchestration);

    expect(await effectRuntime.runPromise(orchestration.snapshot("owner"))).toEqual({
      runs: [],
      workers: [],
    });
    await effectRuntime.runPromise(orchestration.shutdown());
    await effectRuntime.dispose();
  });

  test("reports expected request rejections through the typed Effect error channel", async () => {
    const known = definition("known");
    const missingModel = definition("missing-model", "one-shot", {
      provider: "provider",
      modelId: "missing",
    });
    const cases = [
      {
        operation: "orchestrate",
        reason: "validation",
        message: "ownerSessionId must not be blank",
        effect: (service: OrchestrationService) =>
          service.orchestrate(context(" ", [known]), task("known"), "async"),
      },
      {
        operation: "orchestrate",
        reason: "unknown-worker",
        message: "Unknown worker: missing",
        effect: (service: OrchestrationService) =>
          service.orchestrate(context("owner", [known]), task("missing"), "async"),
      },
      {
        operation: "orchestrate",
        reason: "model-unavailable",
        message: 'Worker "missing-model" configured model "provider/missing" was not found',
        effect: (service: OrchestrationService) =>
          service.orchestrate(
            context("owner", [missingModel]),
            task("missing-model"),
            "async",
          ),
      },
      {
        operation: "sendInteractive",
        reason: "validation",
        message: "worker_id must not be blank",
        effect: (service: OrchestrationService) =>
          service.sendInteractive(context("owner", [known]), " \t ", "Continue", "async"),
      },
      {
        operation: "sendInteractive",
        reason: "ownership",
        message: "Worker is not owned by this session",
        effect: (service: OrchestrationService) =>
          service.sendInteractive(
            context("owner", [known]),
            "worker-missing",
            "Continue",
            "async",
          ),
      },
      {
        operation: "abort",
        reason: "validation",
        message: "worker_id must not be blank",
        effect: (service: OrchestrationService) =>
          service.abort("owner", { workerIds: ["\n"] }),
      },
      {
        operation: "abort",
        reason: "target",
        message: "Invalid abort target",
        effect: (service: OrchestrationService) =>
          service.abort("owner", undefined as unknown as { all: true }),
      },
      {
        operation: "abort",
        reason: "target",
        message: "Abort target must specify exactly one of workerIds or all: true",
        effect: (service: OrchestrationService) => service.abort("owner", {}),
      },
      {
        operation: "abort",
        reason: "target",
        message: "Abort target must specify exactly one of workerIds or all: true",
        effect: (service: OrchestrationService) =>
          service.abort("owner", { workerIds: ["worker-1"], all: true }),
      },
      {
        operation: "abort",
        reason: "target",
        message: "workerIds must contain at least one worker ID",
        effect: (service: OrchestrationService) =>
          service.abort("owner", { workerIds: [] }),
      },
      {
        operation: "closeInteractive",
        reason: "validation",
        message: "worker_id must not be blank",
        effect: (service: OrchestrationService) =>
          service.closeInteractive("owner", "  "),
      },
      {
        operation: "closeInteractive",
        reason: "ownership",
        message: "Worker is not owned by this session",
        effect: (service: OrchestrationService) =>
          service.closeInteractive("owner", "worker-missing"),
      },
      {
        operation: "snapshot",
        reason: "validation",
        message: "ownerSessionId must not be blank",
        effect: (service: OrchestrationService) => service.snapshot(" "),
      },
    ] as const;

    for (const expected of cases) {
      const sessions = new FakeChildSessions([]);
      const { effectRuntime, orchestration } = directRuntime(sessions);
      const exit = await effectRuntime.runPromiseExit(expected.effect(orchestration));
      expect(Exit.isFailure(exit)).toBe(true);
      if (!Exit.isFailure(exit)) throw new Error("Expected orchestration rejection");
      const rejected = Cause.squash(exit.cause);
      expect(rejected).toBeInstanceOf(OrchestrationActionRejected);
      expect(rejected).toBeInstanceOf(Error);
      expect(rejected).toMatchObject({
        operation: expected.operation,
        reason: expected.reason,
        message: expected.message,
      });
      expect((rejected as Error).message).toBe(expected.message);
      expect(sessions.creates.value).toBe(0);
      expect(await effectRuntime.runPromise(orchestration.snapshot("owner"))).toEqual({
        runs: [],
        workers: [],
      });
      await effectRuntime.runPromise(orchestration.shutdown());
      await effectRuntime.dispose();
    }

    const { effectRuntime, orchestration } = directRuntime(new FakeChildSessions([]));
    await effectRuntime.runPromise(orchestration.shutdown());
    const exit = await effectRuntime.runPromiseExit(
      orchestration.abort("owner", { all: true }),
    );
    if (!Exit.isFailure(exit)) throw new Error("Expected shutdown rejection");
    expect(Cause.squash(exit.cause)).toMatchObject({
      operation: "abort",
      reason: "shutdown",
      message: "Orchestrator runtime is shutting down",
    });
    expect(await effectRuntime.runPromise(orchestration.snapshot("owner"))).toEqual({
      runs: [],
      workers: [],
    });
    await effectRuntime.dispose();
  });

  test("classifies worker-state rejection without mutating the ready worker", async () => {
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "typed-ready",
      [promptPlan({ status: "ready", assistantText: "ready" }, true)],
      tracker,
    );
    const { effectRuntime, orchestration } = directRuntime(
      new FakeChildSessions([{ handle }]),
    );
    const owner = context("owner", [definition("interactive", "interactive")]);
    const completed = await effectRuntime.runPromise(
      orchestration.orchestrate(owner, task("interactive"), "inline"),
    );
    const exit = await effectRuntime.runPromiseExit(
      orchestration.abort("owner", { workerIds: [completed.result.workerId] }),
    );

    if (!Exit.isFailure(exit)) throw new Error("Expected worker-state rejection");
    expect(Cause.squash(exit.cause)).toMatchObject({
      operation: "abort",
      reason: "worker-state",
      message: "Ready interactive workers are not active; use interactive_close",
    });
    expect((await effectRuntime.runPromise(orchestration.snapshot("owner"))).workers[0])
      .toMatchObject({ status: "ready" });
    await effectRuntime.runPromise(
      orchestration.closeInteractive("owner", completed.result.workerId),
    );
    await effectRuntime.runPromise(orchestration.shutdown());
    await effectRuntime.dispose();
  });

  test("rejects mixed valid and blank abort IDs atomically before ownership or cancellation", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("atomic-abort-validation", [prompt], tracker);
    const { effectRuntime, orchestration } = directRuntime(
      new FakeChildSessions([{ handle }]),
    );
    const accepted = await effectRuntime.runPromise(
      orchestration.orchestrate(
        context("owner", [definition("worker")]),
        task("worker"),
        "async",
      ),
    );
    await tracker.starts.waitFor(1);

    const exit = await effectRuntime.runPromiseExit(
      orchestration.abort("owner", {
        workerIds: [accepted.workerId, " \t"],
      }),
    );

    if (!Exit.isFailure(exit)) throw new Error("Expected abort validation rejection");
    expect(Cause.squash(exit.cause)).toMatchObject({
      operation: "abort",
      reason: "validation",
      message: "worker_id must not be blank",
    });
    expect(handle.abortCalls).toBe(0);
    expect((await effectRuntime.runPromise(orchestration.snapshot("owner"))).workers[0])
      .toMatchObject({ id: accepted.workerId, status: "running" });

    await effectRuntime.runPromise(orchestration.abort("owner", { all: true }));
    prompt.gate.resolve(undefined);
    await effectRuntime.runPromise(orchestration.shutdown());
    await effectRuntime.dispose();
  });

  test("keeps model registry and generated-ID invariant failures as defects", async () => {
    const tracker = new PromptTracker();
    const registryDefect = new Error("registry defect");
    let throwFromRegistry = true;
    const configured = definition("configured", "one-shot", {
      provider: "provider",
      modelId: "available",
    });
    const registry = {
      find() {
        if (throwFromRegistry) throw registryDefect;
        return model("provider", "available");
      },
    } as unknown as ModelRegistry;
    const firstHandle = new FakeHandle(
      "registry-defect",
      [promptPlan({ status: "completed", assistantText: "done" }, true)],
      tracker,
    );
    const first = directRuntime(new FakeChildSessions([{ handle: firstHandle }]));
    const owner = context("owner", [configured], { modelRegistry: registry });

    const registryExit = await first.effectRuntime.runPromiseExit(
      first.orchestration.orchestrate(owner, task("configured"), "async"),
    );
    if (!Exit.isFailure(registryExit)) throw new Error("Expected registry defect");
    expect(Cause.squash(registryExit.cause)).toBe(registryDefect);
    expect(Cause.squash(registryExit.cause)).not.toBeInstanceOf(
      OrchestrationActionRejected,
    );
    expect(await first.effectRuntime.runPromise(first.orchestration.snapshot("owner")))
      .toEqual({ runs: [], workers: [] });

    throwFromRegistry = false;
    const accepted = await first.effectRuntime.runPromise(
      first.orchestration.orchestrate(owner, task("configured"), "async"),
    );
    expect(String(accepted.id)).toBe("run-1");
    await first.effectRuntime.runPromise(first.orchestration.shutdown());
    await first.effectRuntime.dispose();

    const duplicateHandle = new FakeHandle(
      "duplicate-id",
      [promptPlan({ status: "completed", assistantText: "late" })],
      tracker,
    );
    const duplicate = directRuntime(
      new FakeChildSessions([{ handle: duplicateHandle }]),
      {
        idFactories: {
          runId: () => "run-fixed" as RunId,
          workerId: () => "worker-fixed" as WorkerId,
        },
      },
    );
    await duplicate.effectRuntime.runPromise(
      duplicate.orchestration.orchestrate(
        context("owner", [definition("worker")]),
        task("worker"),
        "async",
      ),
    );
    const duplicateExit = await duplicate.effectRuntime.runPromiseExit(
      duplicate.orchestration.orchestrate(
        context("owner", [definition("worker")]),
        task("worker"),
        "async",
      ),
    );
    if (!Exit.isFailure(duplicateExit)) throw new Error("Expected duplicate-ID defect");
    const duplicateError = Cause.squash(duplicateExit.cause);
    expect(duplicateError).toBeInstanceOf(Error);
    expect((duplicateError as Error).message).toBe("Duplicate run ID: run-fixed");
    expect(duplicateError).not.toBeInstanceOf(OrchestrationActionRejected);
    expect(
      (await duplicate.effectRuntime.runPromise(duplicate.orchestration.snapshot("owner")))
        .workers,
    ).toHaveLength(1);
    await duplicate.effectRuntime.runPromise(
      duplicate.orchestration.abort("owner", { all: true }),
    );
    duplicateHandle.promptPlans[0]!.gate.resolve(undefined);
    await duplicate.effectRuntime.runPromise(duplicate.orchestration.shutdown());
    await duplicate.effectRuntime.dispose();
  });

  test("admits and notifies synchronously through the single host adapter execution", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "done" });
    const handle = new FakeHandle("synchronous-admission", [prompt], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const notifications: string[] = [];
    orchestrator.subscribeState((owner) => notifications.push(owner));

    const accepted = orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "async",
    );

    expect(notifications).toEqual(["owner"]);
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("starting");
    await accepted;
    await tracker.starts.waitFor(1);
    prompt.gate.resolve(undefined);
    await handle.disposed.promise;
    await orchestrator.shutdown();
  });

  test("rejects invalid input and model preflight atomically before IDs, state, or sessions", async () => {
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "known",
      [promptPlan({ status: "completed", assistantText: "ok" })],
      tracker,
    );
    const factory = new FakeChildSessions([{ handle }]);
    const orchestrator = runtime(factory);
    const known = definition("known");
    const missingModel = definition("missing-model", "one-shot", {
      provider: "provider",
      modelId: "missing",
    });
    const owner = context("owner", [known, missingModel]);

    await expect(
      orchestrator.orchestrate(owner, task("unknown"), "async"),
    ).rejects.toThrow("Unknown worker");
    await expect(
      orchestrator.orchestrate(owner, task("known", " "), "async"),
    ).rejects.toThrow("title must not be blank");
    await expect(
      orchestrator.orchestrate(
        owner,
        task("known", "x".repeat(MAX_WORKER_TITLE_LENGTH + 1)),
        "async",
      ),
    ).rejects.toThrow("title must be at most");
    await expect(
      orchestrator.orchestrate(
        owner,
        task("known", "title", "x".repeat(MAX_WORKER_INSTRUCTIONS_LENGTH + 1)),
        "async",
      ),
    ).rejects.toThrow("instructions must be at most");
    await expect(
      orchestrator.orchestrate(owner, task("missing-model"), "async"),
    ).rejects.toThrow('configured model "provider/missing" was not found');
    await expect(
      orchestrator.orchestrate(
        context("owner", [known], { parentModel: undefined }),
        task("known"),
        "async",
      ),
    ).rejects.toThrow("no configured model and no parent model is available");

    expect(factory.creates.value).toBe(0);
    expect(await orchestrator.snapshot("owner")).toEqual({ runs: [], workers: [] });

    const accepted = await orchestrator.orchestrate(owner, task("known"), "async");
    expect({
      id: String(accepted.id),
      workerId: String(accepted.workerId),
    }).toEqual({ id: "run-1", workerId: "worker-1" });
    await tracker.starts.waitFor(1);
    handle.promptPlans[0]!.gate.resolve(undefined);
    await handle.disposed.promise;
    await orchestrator.shutdown();
  });

});

describe("completion, interactive workers, and run ownership", () => {
  test("keeps one worker ID across interactive ready, interactive send, and interactive close", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("interactive", [first, second], tracker);
    const factory = new FakeChildSessions([{ handle }]);
    const orchestrator = runtime(factory);
    const owner = context("owner", [definition("interactive", "interactive")]);

    const initial = orchestrator.orchestrate(owner, task("interactive"), "inline");
    await tracker.starts.waitFor(1);
    handle.emitMessageDirection("from-model");
    first.gate.resolve(undefined);
    const firstRun = await initial;
    const workerId = firstRun.result.workerId;
    expect(firstRun.result?.status).toBe("ready");
    expect(handle.disposeCalls).toBe(0);

    const followUp = orchestrator.sendInteractive(owner, workerId, "Follow up", "inline");
    await tracker.starts.waitFor(2);
    const running = await orchestrator.snapshot("owner");
    expect(running.workers[0]?.status).toBe("running");
    expect(running.workers[0]?.messageDirection).toBe("to-model");
    expect(running.workers[0]?.outcome).toBeUndefined();
    second.gate.resolve(undefined);

    const secondRun = await followUp;
    expect(secondRun.result?.workerId).toBe(workerId);
    expect(secondRun.result?.outcome).toEqual({ status: "ready", assistantText: "second" });
    expect(factory.creates.value).toBe(1);
    expect(handle.prompts).toEqual(["Instructions for interactive", "Follow up"]);

    await orchestrator.closeInteractive("owner", workerId);
    await expect(orchestrator.closeInteractive("owner", workerId)).rejects.toThrow(
      "interactive_close requires an owned ready interactive worker",
    );
    await expect(
      orchestrator.sendInteractive(owner, workerId, "Again", "async"),
    ).rejects.toThrow(
      "interactive_send requires an owned ready interactive worker",
    );
    expect(handle.disposeCalls).toBe(1);
    expect((await orchestrator.snapshot("owner")).workers[0]).toMatchObject({
      id: workerId,
      status: "closed",
      outcome: { status: "closed" },
    });
    expect((orchestrator as unknown as { send?: unknown }).send).toBeUndefined();
    expect((orchestrator as unknown as { close?: unknown }).close).toBeUndefined();
    await orchestrator.shutdown();
  });

  test("automatically disposes completed one-shot workers and rejects interactive close", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "done" });
    const handle = new FakeHandle("one-shot", [prompt], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("one-shot")]);

    const completion = orchestrator.orchestrate(owner, task("one-shot"), "inline");
    await tracker.starts.waitFor(1);
    prompt.gate.resolve(undefined);
    const completed = await completion;
    await handle.disposed.promise;

    expect(completed.result.status).toBe("completed");
    expect(handle.disposeCalls).toBe(1);
    await expect(
      orchestrator.closeInteractive("owner", completed.result.workerId),
    ).rejects.toThrow(
      "interactive_close requires an owned ready interactive worker",
    );
    await expect(
      orchestrator.sendInteractive(
        owner,
        completed.result.workerId,
        "not eligible",
        "async",
      ),
    ).rejects.toThrow(
      "interactive_send requires an owned ready interactive worker",
    );
    expect(handle.disposeCalls).toBe(1);
    await orchestrator.shutdown();
  });

  test("bounds completed run history across repeated interactive responses", async () => {
    const tracker = new PromptTracker();
    const responseCount = MAX_COMPLETED_RUN_HISTORY + 1;
    const handle = new FakeHandle(
      "interactive-history",
      Array.from({ length: responseCount }, (_, index) =>
        promptPlan({ status: "ready", assistantText: `response-${index}` }, true)),
      tracker,
    );
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("interactive", "interactive")]);

    const initial = await orchestrator.orchestrate(owner, task("interactive"), "inline");
    const workerId = initial.result.workerId;
    for (let index = 1; index < responseCount; index += 1) {
      await orchestrator.sendInteractive(owner, workerId, `Follow up ${index}`, "inline");
    }

    const snapshot = await orchestrator.snapshot("owner");
    expect(snapshot.runs).toHaveLength(MAX_COMPLETED_RUN_HISTORY);
    expect(snapshot.workers).toHaveLength(1);
    expect(snapshot.workers[0]).toMatchObject({ id: workerId, status: "ready" });
    expect(handle.prompts).toHaveLength(responseCount);
    await orchestrator.shutdown();
  });

  test("retains ready workers and bounds terminal worker and completed run history globally", async () => {
    const tracker = new PromptTracker();
    const interactiveHandle = new FakeHandle(
      "interactive",
      [promptPlan({ status: "ready", assistantText: "ready" }, true)],
      tracker,
    );
    const terminalPlans = Array.from({ length: 101 }, (_, index) => ({
      handle: new FakeHandle(
        `one-${index}`,
        [promptPlan({ status: "completed", assistantText: `${index}` }, true)],
        tracker,
      ),
    }));
    const orchestrator = runtime(new FakeChildSessions([{ handle: interactiveHandle }, ...terminalPlans]));
    const interactive = definition("interactive", "interactive");
    const oneShot = definition("one-shot");
    const owner = context("owner", [interactive, oneShot]);

    const interactiveRun = await orchestrator.orchestrate(owner, task("interactive"), "inline");
    const interactiveId = interactiveRun.result.workerId;
    for (let index = 0; index < 101; index += 1) {
      await orchestrator.orchestrate(owner, task("one-shot", `Work ${index}`), "inline");
    }

    const snapshot = await orchestrator.snapshot("owner");
    expect(snapshot.runs).toHaveLength(MAX_COMPLETED_RUN_HISTORY);
    expect(String(snapshot.runs[0]?.id)).toBe("run-3");
    expect(snapshot.workers).toHaveLength(MAX_TERMINAL_WORKER_HISTORY + 1);
    expect(snapshot.workers.some((worker) => worker.id === interactiveId && worker.status === "ready")).toBe(true);
    expect(snapshot.workers.some((worker) => worker.id === "worker-2")).toBe(false);

    await orchestrator.closeInteractive("owner", interactiveId);
    const closed = await orchestrator.snapshot("owner");
    expect(closed.workers).toHaveLength(MAX_TERMINAL_WORKER_HISTORY);
    expect(closed.workers.some((worker) => worker.id === interactiveId && worker.status === "closed")).toBe(true);
    await orchestrator.shutdown();
  });
});

describe("per-worker settlement observability", () => {
  test("permits pruned worker and run IDs to be reused with new settlement identities", async () => {
    const tracker = new PromptTracker();
    const count = MAX_TERMINAL_WORKER_HISTORY + 2;
    const plans = Array.from({ length: count }, (_, index) => ({
      handle: new FakeHandle(
        `reuse-${index}`,
        [promptPlan({ status: "completed", assistantText: `${index}` }, true)],
        tracker,
      ),
    }));
    let nextRun = 0;
    let nextWorker = 0;
    const orchestrator = runtime(new FakeChildSessions(plans), {
      idFactories: {
        runId: () => `run-${nextRun++ % (MAX_COMPLETED_RUN_HISTORY + 1)}` as RunId,
        workerId: () => `worker-${nextWorker++ % (MAX_TERMINAL_WORKER_HISTORY + 1)}` as WorkerId,
      },
    });
    const events: WorkerSettlement[] = [];
    orchestrator.subscribeSettlement((event) => events.push(event));
    const owner = context("owner", [definition("worker")]);

    for (let index = 0; index < count; index += 1) {
      await orchestrator.orchestrate(owner, task("worker", `Reuse ${index}`), "inline");
    }

    expect(events[0]?.workerId).toBe(events.at(-1)?.workerId);
    expect(events[0]?.runId).toBe(events.at(-1)?.runId);
    expect(events[0]?.eventId).not.toBe(events.at(-1)?.eventId);
    expect(events.at(-1)?.sequence).toBe(count);
    await orchestrator.shutdown();
  });

  test("observes inline interactive generations locally with distinct event IDs and final usage", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("local-interactive", [first, second], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("interactive", "interactive")]);
    const local: WorkerSettlement[] = [];
    const global: WorkerSettlement[] = [];
    orchestrator.subscribeSettlement((event) => global.push(event));

    const initial = orchestrator.orchestrate(owner, task("interactive"), "inline", undefined, (event) => local.push(event));
    await tracker.starts.waitFor(1);
    first.gate.resolve(undefined);
    const workerId = (await initial).result.workerId;

    const followUp = orchestrator.sendInteractive(owner, workerId, "follow up", "inline", undefined, (event) => local.push(event));
    await tracker.starts.waitFor(2);
    const finalUsage: WorkerUsage = { ...EMPTY_USAGE, output: 9, turns: 2 };
    handle.emitUsage(finalUsage);
    second.gate.resolve(undefined);
    await followUp;

    expect(local.map((event) => event.generation)).toEqual([1, 2]);
    expect(global.map((event) => event.mode)).toEqual(["inline", "inline"]);
    expect(local[0]?.workerId).toBe(local[1]?.workerId);
    expect(local[0]?.eventId).not.toBe(local[1]?.eventId);
    expect(local[1]?.usage).toEqual(finalUsage);
    await orchestrator.closeInteractive("owner", workerId);
    expect(local).toHaveLength(2);
    await orchestrator.shutdown();
  });

  test("emits failed startup and aborted active workers exactly once", async () => {
    const failedFactory: ChildSessionsService = {
      acquire: () => Effect.fail(new WorkerAgentSessionAcquisitionError({
        operation: "create-session",
        message: "startup failed",
        cause: new Error("startup failed"),
      })),
      shutdown: () => Effect.void,
    };
    const failedRuntime = runtime(failedFactory);
    const failed: WorkerSettlement[] = [];
    failedRuntime.subscribeSettlement((event) => failed.push(event));
    await failedRuntime.orchestrate(
      context("failed-owner", [definition("worker")]),
      task("worker"),
      "async",
    );
    while (failed.length === 0) await Promise.resolve();
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      status: "failed",
      failureStage: "startup",
    });
    expect(failed[0]?.outcome).toEqual({ status: "failed", message: "startup failed" });
    await failedRuntime.shutdown();

    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("aborted-settlement", [prompt], tracker);
    const abortedRuntime = runtime(new FakeChildSessions([{ handle }]));
    const aborted: WorkerSettlement[] = [];
    abortedRuntime.subscribeSettlement((event) => aborted.push(event));
    const accepted = await abortedRuntime.orchestrate(
      context("abort-owner", [definition("worker")]),
      task("worker"),
      "async",
    );
    await tracker.starts.waitFor(1);
    await abortedRuntime.abort("abort-owner", { workerIds: [accepted.workerId] });
    prompt.gate.resolve(undefined);
    await Promise.resolve();
    expect(aborted).toHaveLength(1);
    expect(aborted[0]).toMatchObject({
      status: "aborted",
      failureStage: "cancellation",
    });
    await abortedRuntime.shutdown();
  });
});

describe("runtime state observability", () => {
  test("emits owner-scoped state and snapshots usage and activity despite listener errors", async () => {
    const tracker = new PromptTracker();
    const first = new FakeHandle(
      "first",
      [promptPlan({ status: "completed", assistantText: "first" })],
      tracker,
    );
    const second = new FakeHandle(
      "second",
      [promptPlan({ status: "completed", assistantText: "second" })],
      tracker,
    );
    const orchestrator = runtime(new FakeChildSessions([{ handle: first }, { handle: second }]));
    const notifications: string[] = [];
    orchestrator.subscribeState(() => {
      throw new Error("listener failed");
    });
    const unsubscribe = orchestrator.subscribeState((ownerSessionId) => {
      notifications.push(ownerSessionId);
    });

    await orchestrator.orchestrate(
      context("owner-a", [definition("first")]),
      task("first"),
      "async",
    );
    await orchestrator.orchestrate(
      context("owner-b", [definition("second")]),
      task("second"),
      "async",
    );
    await tracker.starts.waitFor(2);
    notifications.length = 0;

    const usage: WorkerUsage = {
      input: 12,
      output: 4,
      cacheRead: 3,
      cacheWrite: 1,
      cost: 0.25,
      contextTokens: 20,
      turns: 1,
    };
    first.emitUsage(usage);
    first.emitActivity("read");
    first.emitMessageDirection("from-model");

    expect(notifications).toEqual(["owner-a", "owner-a", "owner-a"]);
    const ownerA = await orchestrator.snapshot("owner-a");
    expect(ownerA.workers[0]?.usage).toEqual(usage);
    expect(ownerA.workers[0]?.activity).toBe("read");
    expect(ownerA.workers[0]?.messageDirection).toBe("from-model");
    try {
      (ownerA.workers[0]!.usage as { input: number }).input = 999;
    } catch {
      // Frozen snapshots may reject mutation; fresh reads must remain authoritative either way.
    }
    expect((await orchestrator.snapshot("owner-a")).workers[0]?.usage.input).toBe(12);
    const ownerBWorker = (await orchestrator.snapshot("owner-b")).workers[0];
    expect(ownerBWorker?.usage).toEqual(EMPTY_USAGE);
    expect(ownerBWorker?.activity).toBeUndefined();
    expect(ownerBWorker?.messageDirection).toBe("to-model");

    unsubscribe();
    unsubscribe();
    first.emitActivity("bash");
    expect(notifications).toEqual(["owner-a", "owner-a", "owner-a"]);

    first.promptPlans[0]!.gate.resolve(undefined);
    second.promptPlans[0]!.gate.resolve(undefined);
    await Promise.all([first.disposed.promise, second.disposed.promise]);
    await orchestrator.shutdown();
  });
});

describe("ownership, cancellation, and shutdown", () => {
  test("keeps snapshots and worker operations isolated by owner", async () => {
    const tracker = new PromptTracker();
    const plan = promptPlan({ status: "ready", assistantText: "ready" });
    const handle = new FakeHandle("interactive", [plan], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const worker = definition("interactive", "interactive");
    const ownerA = context("owner-a", [worker]);
    const ownerB = context("owner-b", [worker]);
    const accepted = await orchestrator.orchestrate(ownerA, task("interactive"), "async");

    expect(await orchestrator.snapshot("owner-b")).toEqual({ runs: [], workers: [] });
    await expect(
      orchestrator.sendInteractive(ownerB, accepted.workerId, "intrude", "async"),
    ).rejects.toThrow("not owned");
    await expect(orchestrator.closeInteractive("owner-b", accepted.workerId)).rejects.toThrow("not owned");
    await expect(
      orchestrator.abort("owner-b", { workerIds: [accepted.workerId] }),
    ).rejects.toThrow("not owned");

    await orchestrator.abort("owner-a", { all: true });
    plan.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("rejected runtime adoption rolls ownership back for one handoff disposal", async () => {
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "adoption-rejected",
      [promptPlan({ status: "completed", assistantText: "unused" })],
      tracker,
    );
    handle.subscriptionFailure = new Error("subscription failed during adoption");
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));

    const completed = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "inline",
    );
    await handle.disposed.promise;

    expect(completed.result).toMatchObject({
      status: "failed",
      outcome: { status: "failed", message: "subscription failed during adoption" },
    });
    expect(handle.disposeCalls).toBe(1);
    expect(handle.prompts).toHaveLength(0);
    await orchestrator.shutdown();
    expect(handle.disposeCalls).toBe(1);
  });

  test("aborts pending bootstrap and disposes a session created late", async () => {
    const tracker = new PromptTracker();
    const createGate = new Deferred();
    const handle = new FakeHandle(
      "late",
      [promptPlan({ status: "completed", assistantText: "late" })],
      tracker,
    );
    const factory = new FakeChildSessions([{ handle, gate: createGate }]);
    const orchestrator = runtime(factory);
    const settlement = new Deferred<WorkerSettlement>();
    orchestrator.subscribeSettlement((event) => settlement.resolve(event));

    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("late")]),
      task("late"),
      "async",
    );
    await factory.creates.waitFor(1);
    await orchestrator.abort("owner", { workerIds: [accepted.workerId] });
    expect((await settlement.promise).status).toBe("aborted");

    createGate.resolve(undefined);
    await handle.disposed.promise;
    expect(handle.disposeCalls).toBe(1);
    expect(handle.prompts).toHaveLength(0);
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("aborted");
    await orchestrator.shutdown();
  });

  test("awaits prompt abort and ignores a late prompt result", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "too late" });
    const handle = new FakeHandle("running", [prompt], tracker);
    handle.abortGate = new Deferred();
    handle.promptFinalizerGate = new Deferred();
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const settlement = new Deferred<WorkerSettlement>();
    orchestrator.subscribeSettlement((event) => settlement.resolve(event));

    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("running")]),
      task("running"),
      "async",
    );
    await tracker.starts.waitFor(1);
    handle.emitActivity("read");
    expect((await orchestrator.snapshot("owner")).workers[0]?.activity).toBe("read");
    const aborting = orchestrator.abort("owner", { workerIds: [accepted.workerId] });
    await expectPending(aborting);
    expect(handle.abortCalls).toBe(1);

    handle.abortGate.resolve(undefined);
    await handle.promptFinalizerStarts.waitFor(1);
    await expectPending(aborting);
    handle.promptFinalizerGate.resolve(undefined);
    await aborting;
    expect((await settlement.promise).status).toBe("aborted");
    handle.emitStaleActivity("bash");
    prompt.gate.resolve(undefined);
    await Promise.resolve();
    const abortedWorker = (await orchestrator.snapshot("owner")).workers[0];
    expect(abortedWorker?.activity).toBeUndefined();
    expect(abortedWorker?.outcome).toEqual({ status: "aborted" });
    await orchestrator.shutdown();
  });

  test("does not retain subscriptions registered after shutdown begins", async () => {
    const { effectRuntime, orchestration } = directRuntime(new FakeChildSessions([]));
    const shutdown = orchestration.shutdown();
    const listeners = orchestration as unknown as {
      settlementListeners: Set<unknown>;
      stateListeners: Set<unknown>;
    };

    const subscriptions = [
      {
        subscribe: () => orchestration.subscribeSettlement(() => {}),
        retained: () => listeners.settlementListeners.size,
      },
      {
        subscribe: () => orchestration.subscribeState(() => {}),
        retained: () => listeners.stateListeners.size,
      },
    ];
    for (const subscription of subscriptions) {
      const unsubscribe = subscription.subscribe();
      expect(unsubscribe).toBeFunction();
      expect(subscription.retained()).toBe(0);
      expect(() => unsubscribe()).not.toThrow();
    }
    expect(() => orchestration.subscribeSettlement(undefined as never)).toThrow(
      "Settlement listener must be a function",
    );
    expect(() => orchestration.subscribeState(undefined as never)).toThrow(
      "State listener must be a function",
    );

    await effectRuntime.runPromise(shutdown);
    await effectRuntime.dispose();
  });

  test("shutdown force-closes and disposes a ready interactive session", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "ready", assistantText: "ready" });
    const handle = new FakeHandle("interactive", [prompt], tracker);
    handle.abortGate = new Deferred();
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("interactive", "interactive")]);

    const initial = orchestrator.orchestrate(owner, task("interactive"), "inline");
    await tracker.starts.waitFor(1);
    prompt.gate.resolve(undefined);
    await initial;

    const shutdown = orchestrator.shutdown();
    const rejectedAdmission = orchestrator.orchestrate(owner, task("interactive"), "async");
    await shutdown;

    expect(handle.abortCalls).toBe(0);
    expect(handle.disposeCalls).toBe(1);
    expect((await orchestrator.snapshot("owner")).workers[0]).toMatchObject({
      status: "closed",
      outcome: { status: "closed" },
    });
    await expect(rejectedAdmission).rejects.toThrow("shutting down");
  });
});

describe("process-owned supervision and reentrant interactive operations", () => {
  test("replacement interrupts the Effect generation and reports defects", async () => {
    const { effectRuntime, supervisors } = supervisorHarness();
    const firstStarted = new Deferred();
    const firstFinalized = new Deferred();
    const defect = new Deferred<unknown>();

    supervisors.generations.start(
      "worker",
      Effect.sync(() => firstStarted.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Effect.sync(() => firstFinalized.resolve(undefined))),
      ),
      (error) => defect.resolve(error),
    );
    await firstStarted.promise;

    supervisors.generations.start(
      "worker",
      Effect.die(new Error("scheduler boom")),
      (error) => defect.resolve(error),
    );
    await firstFinalized.promise;

    const reported = await defect.promise;
    expect(reported).toBeInstanceOf(Error);
    if (!(reported instanceof Error)) throw new Error("Expected scheduler defect");
    expect(reported.message).toContain("scheduler boom");
    await effectRuntime.dispose();
  });

  test("removal waits for generation finalization and ignores interruption-only causes", async () => {
    const { effectRuntime, supervisors } = supervisorHarness();
    const started = new Deferred();
    const finalizerStarted = new Deferred();
    const finalizerGate = new Deferred();
    const defects: unknown[] = [];

    supervisors.generations.start(
      "worker",
      Effect.sync(() => started.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(
          Effect.sync(() => finalizerStarted.resolve(undefined)).pipe(
            Effect.andThen(Effect.promise(() => finalizerGate.promise)),
          ),
        ),
      ),
      (error) => defects.push(error),
    );
    await started.promise;

    const removing = effectRuntime.runPromise(supervisors.generations.remove("worker"));
    await finalizerStarted.promise;
    await expectPending(removing);
    finalizerGate.resolve(undefined);
    await removing;
    expect(defects).toEqual([]);
    await effectRuntime.dispose();
  });

  test("state listeners can synchronously send the next interactive generation", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("state-reentrant", [first, second], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("interactive", "interactive")]);
    const accepted = await orchestrator.orchestrate(owner, task("interactive"), "async");
    await tracker.starts.waitFor(1);
    let followUp: Promise<CompletedRun> | undefined;
    let sent = false;

    orchestrator.subscribeState(() => {
      if (sent) return;
      sent = true;
      followUp = orchestrator.sendInteractive(
        owner,
        accepted.workerId,
        "State follow-up",
        "inline",
      );
    });

    first.gate.resolve(undefined);
    await tracker.starts.waitFor(2);
    expect(handle.prompts).toEqual(["Instructions for interactive", "State follow-up"]);
    second.gate.resolve(undefined);
    expect((await followUp!).result?.status).toBe("ready");
    await orchestrator.closeInteractive("owner", accepted.workerId);
    await orchestrator.shutdown();
  });
});

describe("inline AbortSignal ownership", () => {
  test("already-aborted signals preserve null, object, and default reason identity", async () => {
    const orchestrator = runtime(new FakeChildSessions([]));
    const owner = context("owner", [definition("worker")]);
    const objectReason = { kind: "parent-turn-ended" };
    const controllers = [new AbortController(), new AbortController(), new AbortController()];
    controllers[0]!.abort(null);
    controllers[1]!.abort(objectReason);
    controllers[2]!.abort();

    for (const controller of controllers) {
      const rejectedReason = await orchestrator
        .orchestrate(owner, task("worker"), "inline", controller.signal)
        .then(
          () => "unexpected success",
          (reason: unknown) => reason,
        );
      expect(rejectedReason).toBe(controller.signal.reason);
      expect(await orchestrator.snapshot("owner")).toEqual({ runs: [], workers: [] });
    }
    expect(controllers[0]!.signal.reason).toBeNull();
    expect(controllers[1]!.signal.reason).toBe(objectReason);
    expect(controllers[2]!.signal.reason).toBeInstanceOf(DOMException);
    await orchestrator.shutdown();
  });

  test("admitted inline signals preserve null, object, and default reason identity", async () => {
    const tracker = new PromptTracker();
    const plans = Array.from({ length: 3 }, (_, index) => ({
      handle: new FakeHandle(
        `inline-reason-${index}`,
        [promptPlan({ status: "completed", assistantText: "late" })],
        tracker,
      ),
    }));
    const orchestrator = runtime(new FakeChildSessions(plans));
    const owner = context("owner", [definition("worker")]);
    const objectReason = { kind: "inline-cancelled" };
    const aborts = [
      (controller: AbortController) => controller.abort(null),
      (controller: AbortController) => controller.abort(objectReason),
      (controller: AbortController) => controller.abort(),
    ];

    for (let index = 0; index < aborts.length; index += 1) {
      const controller = new AbortController();
      const inline = orchestrator.orchestrate(
        owner,
        task("worker"),
        "inline",
        controller.signal,
      );
      await tracker.starts.waitFor(index + 1);
      aborts[index]!(controller);

      const rejectedReason = await inline.then(
        () => "unexpected success",
        (reason: unknown) => reason,
      );
      expect(rejectedReason).toBe(controller.signal.reason);
      const snapshot = await orchestrator.snapshot("owner");
      expect(snapshot.runs[index]).toMatchObject({ state: "complete" });
      expect(snapshot.workers[index]).toMatchObject({
        status: "aborted",
        outcome: { status: "aborted" },
      });
    }
    expect((await orchestrator.snapshot("owner")).runs).toHaveLength(3);
    for (const plan of plans) plan.handle.promptPlans[0]!.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("settlement wins a synchronous state-listener race with abort", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "done" });
    const handle = new FakeHandle("inline-settlement-race", [prompt], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("worker")]);
    const controller = new AbortController();
    const reason = { kind: "abort-after-settlement" };

    const inline = orchestrator.orchestrate(
      owner,
      task("worker"),
      "inline",
      controller.signal,
    );
    await tracker.starts.waitFor(1);
    const unsubscribe = orchestrator.subscribeState(() => controller.abort(reason));
    prompt.gate.resolve(undefined);

    const completed = await inline;
    unsubscribe();
    expect(controller.signal.reason).toBe(reason);
    expect(completed.result).toMatchObject({
      status: "completed",
      outcome: { status: "completed", assistantText: "done" },
    });
    expect(handle.abortCalls).toBe(0);
    await orchestrator.shutdown();
  });

  test("abort after inline admission cancels the exact run and rejects after settlement", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("inline-abort", [prompt], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("worker")]);
    const controller = new AbortController();
    const reason = new Error("inline cancelled");
    const settlements: WorkerSettlement[] = [];
    orchestrator.subscribeSettlement((settlement) => settlements.push(settlement));

    const inline = orchestrator.orchestrate(
      owner,
      task("worker"),
      "inline",
      controller.signal,
    );
    await tracker.starts.waitFor(1);
    controller.abort(reason);

    await expect(inline).rejects.toBe(reason);
    expect(await orchestrator.snapshot("owner")).toMatchObject({
      runs: [{ state: "complete" }],
      workers: [{ status: "aborted", outcome: { status: "aborted" } }],
    });
    prompt.gate.resolve(undefined);
    await Promise.resolve();
    expect(settlements).toHaveLength(1);
    expect(settlements[0]).toMatchObject({
      status: "aborted",
      failureStage: "cancellation",
    });
    await orchestrator.shutdown();
  });

  test("abort after inline interactive send cancels that generation", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "ready" });
    const second = promptPlan({ status: "ready", assistantText: "late" });
    const handle = new FakeHandle("interactive-abort", [first, second], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("interactive", "interactive")]);

    const initial = orchestrator.orchestrate(owner, task("interactive"), "inline");
    await tracker.starts.waitFor(1);
    first.gate.resolve(undefined);
    const workerId = (await initial).result.workerId;
    const controller = new AbortController();
    const followUp = orchestrator.sendInteractive(
      owner,
      workerId,
      "cancel this generation",
      "inline",
      controller.signal,
    );
    await tracker.starts.waitFor(2);
    controller.abort(new Error("interactive send cancelled"));

    await expect(followUp).rejects.toThrow("interactive send cancelled");
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("aborted");
    second.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("async dispatch does not retain the caller's signal after acceptance", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "done" });
    const handle = new FakeHandle("async-signal", [prompt], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("worker")]);
    const controller = new AbortController();
    const settlement = new Deferred<WorkerSettlement>();
    orchestrator.subscribeSettlement((event) => settlement.resolve(event));

    await orchestrator.orchestrate(owner, task("worker"), "async", controller.signal);
    await tracker.starts.waitFor(1);
    controller.abort();
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("running");

    prompt.gate.resolve(undefined);
    expect((await settlement.promise).status).toBe("completed");
    await orchestrator.shutdown();
  });
});

describe("active-only aborts and bounded lifecycle barriers", () => {
  test("ready workers reject explicit abort with use interactive_close and all aborts active only", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "ready", assistantText: "ready" });
    const handle = new FakeHandle("ready", [prompt], tracker);
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const owner = context("owner", [definition("interactive", "interactive")]);

    const initial = orchestrator.orchestrate(owner, task("interactive"), "inline");
    await tracker.starts.waitFor(1);
    prompt.gate.resolve(undefined);
    const workerId = (await initial).result.workerId;

    await expect(orchestrator.abort("owner", { workerIds: [workerId] })).rejects.toThrow(
      "use interactive_close",
    );
    await orchestrator.abort("owner", { all: true });
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("ready");
    expect(handle.abortCalls).toBe(0);

    await orchestrator.closeInteractive("owner", workerId);
    await orchestrator.shutdown();
  });

  test("expected session abort failures still settle cancellation as aborted", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("abort-failure", [prompt], tracker);
    const cause = new Error("prompt abort failed");
    handle.abortFailure = new WorkerSessionAbortError({
      operation: "abort-prompt",
      stage: "prompt",
      message: cause.message,
      cause,
    });
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "async",
    );
    await tracker.starts.waitFor(1);

    await orchestrator.abort("owner", { workerIds: [accepted.workerId] });

    expect((await orchestrator.snapshot("owner")).workers[0]).toMatchObject({
      status: "aborted",
      outcome: { status: "aborted" },
    });
    prompt.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("concurrent aborts join one stored cancellation promise", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("joined-abort", [prompt], tracker);
    handle.abortGate = new Deferred();
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "async",
    );
    await tracker.starts.waitFor(1);

    const firstAbort = orchestrator.abort("owner", { workerIds: [accepted.workerId] });
    await handle.abortStarts.waitFor(1);
    const secondAbort = orchestrator.abort("owner", { workerIds: [accepted.workerId] });
    await expectPending(firstAbort);
    await expectPending(secondAbort);
    expect(handle.abortCalls).toBe(1);

    handle.abortGate.resolve(undefined);
    await Promise.all([firstAbort, secondAbort]);
    expect(handle.abortCalls).toBe(1);
    prompt.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("host destruction bounds an uninterruptible generation finalizer that never releases", async () => {
    const testSupervisors = processSupervisorLayer.pipe(
      Layer.provideMerge(TestClock.layer()),
    );
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "uninterruptible-finalizer",
      [promptPlan({ status: "completed", assistantText: "late" })],
      tracker,
    );
    handle.promptFinalizerGate = new Deferred();
    const dependencies = Layer.merge(
      testSupervisors,
      Layer.succeed(ChildSessions, new FakeChildSessions([{ handle }])),
    );
    const effectRuntime = ManagedRuntime.make(
      orchestrationLayer({ idFactories: createSequentialIdFactories() }).pipe(
        Layer.provideMerge(dependencies),
      ),
    );
    const orchestrator = createProcessHostRuntimeAdapter(effectRuntime);

    await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "async",
    );
    await tracker.starts.waitFor(1);
    const host = Object.assign(
      {
        runtime: orchestrator,
        delivery: new DeliveryCoordinator(),
      } satisfies ProcessHost,
      { effectRuntime },
    );
    const destruction = destroyProcessHost(host, {
      // Cancellation uses TestClock; this injected process-boundary deadline
      // deterministically abandons the subsequently blocked root finalizer.
      awaitRootDisposal: async () => {},
    });
    await handle.promptFinalizerStarts.waitFor(1);
    // Root disposal may interrupt the TestClock driver immediately after the
    // adjustment wakes shutdown; destruction itself is the authoritative barrier.
    await effectRuntime.runPromise(
      TestClock.adjust(SHUTDOWN_CLEANUP_GRACE_MS),
    ).catch(() => {});
    await destruction;

    expect((await orchestrator.snapshot("owner")).workers[0]).toMatchObject({
      status: "aborted",
      outcome: { status: "aborted" },
    });
    expect(handle.disposeCalls).toBe(1);
    // The real uninterruptible finalizer intentionally never releases.
  });

  test("shutdown bounds pending bootstrap cleanup and still disposes a late session", async () => {
    const testSupervisors = processSupervisorLayer.pipe(
      Layer.provideMerge(TestClock.layer()),
    );
    const tracker = new PromptTracker();
    const createGate = new Deferred();
    const handle = new FakeHandle("late-shutdown", [], tracker);
    const dependencies = Layer.merge(
      testSupervisors,
      Layer.succeed(
        ChildSessions,
        new FakeChildSessions([{ handle, gate: createGate }]),
      ),
    );
    const effectRuntime = ManagedRuntime.make(
      orchestrationLayer({ idFactories: createSequentialIdFactories() }).pipe(
        Layer.provideMerge(dependencies),
      ),
    );
    const orchestrator = createProcessHostRuntimeAdapter(effectRuntime);

    await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "async",
    );
    const shutdown = orchestrator.shutdown();
    await Promise.resolve();
    await effectRuntime.runPromise(TestClock.adjust(SHUTDOWN_CLEANUP_GRACE_MS));
    await shutdown;

    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("aborted");
    expect(handle.disposeCalls).toBe(0);
    await effectRuntime.dispose();
    createGate.resolve(undefined);
    await handle.disposed.promise;
    expect(handle.disposeCalls).toBe(1);
  });
});

describe("defect and cleanup supervision", () => {
  test("supervises session disposal as an Effect operation", async () => {
    const { effectRuntime, supervisors } = supervisorHarness();
    const disposalStarted = new Deferred();
    const supervised: TestSupervisors = {
      cleanup: {
        supervise(cleanup) {
          disposalStarted.resolve(undefined);
          supervisors.cleanup.supervise(cleanup);
        },
        awaitEmpty: supervisors.cleanup.awaitEmpty,
      },
      generations: supervisors.generations,
    };
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "effect-disposal",
      [promptPlan({ status: "completed", assistantText: "done" }, true)],
      tracker,
    );
    handle.disposeGate = new Deferred();
    const orchestrator = runtime(new FakeChildSessions([{ handle }]), {
      supervisors: supervised,
    });

    await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "inline",
    );
    await disposalStarted.promise;
    await handle.disposed.promise;
    expect(handle.disposeCalls).toBe(1);

    const shutdown = orchestrator.shutdown();
    await expectPending(shutdown);
    handle.disposeGate.resolve(undefined);
    await shutdown;
    await effectRuntime.dispose();
  });

  test("awaits multiple concurrent session disposals", async () => {
    const { effectRuntime, supervisors } = supervisorHarness();
    const tracker = new PromptTracker();
    const first = new FakeHandle(
      "concurrent-disposal-first",
      [promptPlan({ status: "completed", assistantText: "first" }, true)],
      tracker,
    );
    const second = new FakeHandle(
      "concurrent-disposal-second",
      [promptPlan({ status: "completed", assistantText: "second" }, true)],
      tracker,
    );
    first.disposeGate = new Deferred();
    second.disposeGate = new Deferred();
    const orchestrator = runtime(
      new FakeChildSessions([{ handle: first }, { handle: second }]),
      { supervisors },
    );
    const owner = context("owner", [definition("worker")]);

    await Promise.all([
      orchestrator.orchestrate(owner, task("worker", "First"), "inline"),
      orchestrator.orchestrate(owner, task("worker", "Second"), "inline"),
    ]);
    await Promise.all([first.disposed.promise, second.disposed.promise]);

    const shutdown = orchestrator.shutdown();
    await expectPending(shutdown);
    first.disposeGate.resolve(undefined);
    await expectPending(shutdown);
    second.disposeGate.resolve(undefined);
    await shutdown;

    expect(first.disposeCalls).toBe(1);
    expect(second.disposeCalls).toBe(1);
    await effectRuntime.dispose();
  });

  test("repeated disposal requests share the handle's idempotent Effect", async () => {
    const { effectRuntime, supervisors } = supervisorHarness();
    const duplicateCleanup: CleanupSupervisorService = {
      supervise(cleanup) {
        supervisors.cleanup.supervise(cleanup);
        supervisors.cleanup.supervise(cleanup);
      },
      awaitEmpty: supervisors.cleanup.awaitEmpty,
    };
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "repeated-disposal",
      [promptPlan({ status: "completed", assistantText: "done" }, true)],
      tracker,
    );
    handle.disposeGate = new Deferred();
    const orchestrator = runtime(new FakeChildSessions([{ handle }]), {
      supervisors: { generations: supervisors.generations, cleanup: duplicateCleanup },
    });

    await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "inline",
    );
    await handle.disposed.promise;
    expect(handle.disposeCalls).toBe(1);

    const shutdown = orchestrator.shutdown();
    await expectPending(shutdown);
    handle.disposeGate.resolve(undefined);
    await shutdown;
    expect(handle.disposeCalls).toBe(1);
    await effectRuntime.dispose();
  });

  test("shutdown bounds a live supervised disposal with the Effect Clock", async () => {
    const supervisorLayer = processSupervisorLayer.pipe(
      Layer.provideMerge(TestClock.layer()),
    );
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "bounded-disposal",
      [promptPlan({ status: "completed", assistantText: "done" }, true)],
      tracker,
    );
    handle.disposeGate = new Deferred();
    const dependencies = Layer.merge(
      supervisorLayer,
      Layer.succeed(ChildSessions, new FakeChildSessions([{ handle }])),
    );
    const effectRuntime = ManagedRuntime.make(
      orchestrationLayer({ idFactories: createSequentialIdFactories() }).pipe(
        Layer.provideMerge(dependencies),
      ),
    );
    const orchestrator = createProcessHostRuntimeAdapter(effectRuntime);
    const cleanup = effectRuntime.runSync(CleanupSupervisor);

    await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "inline",
    );
    await handle.disposed.promise;

    const shutdown = orchestrator.shutdown();
    await effectRuntime.runPromise(Effect.yieldNow);
    await effectRuntime.runPromise(TestClock.adjust(SHUTDOWN_CLEANUP_GRACE_MS));
    await shutdown;
    expect(handle.disposeCalls).toBe(1);

    handle.disposeGate.resolve(undefined);
    await effectRuntime.runPromise(cleanup.awaitEmpty());
    await effectRuntime.dispose();
  });

  test("an injected generation defect fails the current worker and completes its run", async () => {
    const { effectRuntime, supervisors } = supervisorHarness();
    const defectiveSupervisors: TestSupervisors = {
      cleanup: supervisors.cleanup,
      generations: {
        start(_key, _workflow, onDefect) {
          onDefect(new Error("workflow defect"));
        },
        remove: () => Effect.void,
      },
    };
    const orchestrator = runtime(new FakeChildSessions([]), {
      supervisors: defectiveSupervisors,
    });

    const completed = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "inline",
    );

    expect(completed.result).toMatchObject({
      status: "failed",
      outcome: { status: "failed", message: "workflow defect" },
    });
    await orchestrator.shutdown();
    await effectRuntime.dispose();
  });

  test("session disposal exceptions cannot strand completion or shutdown", async () => {
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "dispose-error",
      [promptPlan({ status: "completed", assistantText: "done" })],
      tracker,
    );
    handle.disposeFailure = new Error("dispose failed");
    const orchestrator = runtime(new FakeChildSessions([{ handle }]));
    const inline = orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      task("worker"),
      "inline",
    );
    await tracker.starts.waitFor(1);
    handle.promptPlans[0]!.gate.resolve(undefined);

    expect((await inline).result?.status).toBe("completed");
    expect(handle.disposeCalls).toBe(1);
    await orchestrator.shutdown();
  });
});
