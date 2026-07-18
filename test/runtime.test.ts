import { describe, expect, test } from "bun:test";
import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Effect } from "effect";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  createSequentialIdFactories,
  createWorkerCatalog,
  type OrchestrateTaskInput,
  type WaveId,
  type WorkerDefinition,
  type WorkerId,
  type WorkerMessageDirection,
  type WorkerOutcome,
  type WorkerUsage,
} from "../extension/domain.ts";
import {
  MAX_COMPLETED_WAVE_HISTORY,
  MAX_TERMINAL_WORKER_HISTORY,
  SHUTDOWN_CLEANUP_GRACE_MS,
  createOrchestratorRuntime,
  type BestEffortDeadline,
  type CompletedWave,
  type OrchestrationContext,
  type OrchestratorRuntimeOptions,
} from "../extension/runtime.ts";
import {
  createWorkflowScheduler,
  type WorkflowScheduler,
} from "../extension/scheduler.ts";
import type {
  WorkerSessionFactory,
  WorkerSessionFactoryOptions,
  WorkerSessionHandle,
} from "../extension/worker-session.ts";

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
  abortGate: Deferred | undefined;
  abortCalls = 0;
  disposeCalls = 0;
  disposeFailure: Error | undefined;
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

  async prompt(instructions: string): Promise<WorkerOutcome> {
    const plan = this.promptPlans[this.prompts.length];
    if (!plan) throw new Error("Missing fake prompt plan");
    this.prompts.push(instructions);
    this.tracker.start();
    try {
      await plan.gate.promise;
      return plan.outcome;
    } finally {
      this.tracker.finish();
    }
  }

  async abort(): Promise<void> {
    this.abortCalls += 1;
    this.abortStarts.increment();
    await this.abortGate?.promise;
  }

  async dispose(): Promise<void> {
    this.disposeCalls += 1;
    this.disposed.resolve(undefined);
    if (this.disposeFailure) throw this.disposeFailure;
  }

  subscribeUsage(listener: (usage: WorkerUsage) => void): () => void {
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

class FakeFactory implements WorkerSessionFactory {
  readonly creates = new Counter();
  readonly options: WorkerSessionFactoryOptions[] = [];

  constructor(private readonly plans: CreatePlan[]) {}

  async create(options: WorkerSessionFactoryOptions): Promise<WorkerSessionHandle> {
    const plan = this.plans[this.options.length];
    if (!plan) throw new Error("Missing fake create plan");
    this.options.push(options);
    this.creates.increment();
    await plan.gate?.promise;
    return plan.handle;
  }
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

function runtime(
  factory: WorkerSessionFactory,
  overrides: Partial<OrchestratorRuntimeOptions> = {},
) {
  return createOrchestratorRuntime({
    workerSessionFactory: factory,
    idFactories: createSequentialIdFactories(),
    clock: () => 1_700_000_000_000,
    ...overrides,
  });
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
  test("rejects invalid input and model preflight atomically before IDs, state, or sessions", async () => {
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "known",
      [promptPlan({ status: "completed", assistantText: "ok" })],
      tracker,
    );
    const factory = new FakeFactory([{ handle }]);
    const orchestrator = runtime(factory);
    const known = definition("known");
    const missingModel = definition("missing-model", "one-shot", {
      provider: "provider",
      modelId: "missing",
    });
    const owner = context("owner", [known, missingModel]);

    await expect(
      orchestrator.orchestrate(owner, [task("known"), task("unknown")], "async"),
    ).rejects.toThrow("Unknown worker");
    await expect(
      orchestrator.orchestrate(owner, [task("known", " ")], "async"),
    ).rejects.toThrow("title must not be blank");
    await expect(
      orchestrator.orchestrate(
        owner,
        [task("known", "x".repeat(MAX_WORKER_TITLE_LENGTH + 1))],
        "async",
      ),
    ).rejects.toThrow("title must be at most");
    await expect(
      orchestrator.orchestrate(
        owner,
        [task("known", "title", "x".repeat(MAX_WORKER_INSTRUCTIONS_LENGTH + 1))],
        "async",
      ),
    ).rejects.toThrow("instructions must be at most");
    await expect(
      orchestrator.orchestrate(owner, [task("known"), task("missing-model")], "async"),
    ).rejects.toThrow('configured model "provider/missing" was not found');
    await expect(
      orchestrator.orchestrate(
        context("owner", [known], { parentModel: undefined }),
        [task("known")],
        "async",
      ),
    ).rejects.toThrow("no configured model and no parent model is available");

    expect(factory.creates.value).toBe(0);
    expect(await orchestrator.snapshot("owner")).toEqual({ waves: [], workers: [] });

    const accepted = await orchestrator.orchestrate(owner, [task("known")], "async");
    expect({
      id: String(accepted.id),
      workerIds: accepted.workerIds.map(String),
    }).toEqual({ id: "wave-1", workerIds: ["worker-1"] });
    await tracker.starts.waitFor(1);
    handle.promptPlans[0]!.gate.resolve(undefined);
    await handle.disposed.promise;
    await orchestrator.shutdown();
  });

  test("preflights every configured model before creating any session", async () => {
    const tracker = new PromptTracker();
    const factory = new FakeFactory([
      { handle: new FakeHandle("unused", [], tracker) },
    ]);
    const configured = definition("configured", "one-shot", {
      provider: "provider",
      modelId: "available",
    });
    const unavailable = definition("unavailable", "one-shot", {
      provider: "provider",
      modelId: "unavailable",
    });
    const findCalls: string[] = [];
    const registry = {
      find(provider: string, id: string) {
        findCalls.push(`${provider}/${id}`);
        return id === "available" ? model(provider, id) : undefined;
      },
    } as unknown as ModelRegistry;
    const orchestrator = runtime(factory);

    await expect(
      orchestrator.orchestrate(
        context("owner", [configured, unavailable], { modelRegistry: registry }),
        [task("configured"), task("unavailable")],
        "async",
      ),
    ).rejects.toThrow("provider/unavailable");

    expect(findCalls).toEqual(["provider/available", "provider/unavailable"]);
    expect(factory.creates.value).toBe(0);
    expect(await orchestrator.snapshot("owner")).toEqual({ waves: [], workers: [] });
    await orchestrator.shutdown();
  });

  test("starts all twelve prompts before any prompt completes", async () => {
    const tracker = new PromptTracker();
    const workers = Array.from({ length: 12 }, (_, index) => definition(`worker-${index}`));
    const plans = workers.map((worker) => ({
      handle: new FakeHandle(
        worker.name,
        [promptPlan({ status: "completed", assistantText: worker.name })],
        tracker,
      ),
    }));
    const factory = new FakeFactory(plans);
    const orchestrator = runtime(factory);
    const completion = new Deferred<CompletedWave>();
    orchestrator.subscribeCompletion((wave) => completion.resolve(wave));

    await orchestrator.orchestrate(
      context("owner", workers),
      workers.map((worker) => task(worker.name)),
      "async",
    );
    await factory.creates.waitFor(12);
    await tracker.starts.waitFor(12);

    expect(factory.creates.value).toBe(12);
    expect(tracker.maximumActive).toBe(12);
    expect(plans.every((plan) => plan.handle.promptPlans[0]!.gate)).toBe(true);

    for (const plan of plans) plan.handle.promptPlans[0]!.gate.resolve(undefined);

    await completion.promise;
    expect(tracker.maximumActive).toBe(12);
    await orchestrator.shutdown();
  });
});

describe("completion, reusable workers, and wave ownership", () => {
  test("returns ordered results and marks async waves complete without claiming delivery", async () => {
    const tracker = new PromptTracker();
    const workers = [definition("alpha"), definition("beta"), definition("gamma")];
    const plans = workers.map((worker) => ({
      handle: new FakeHandle(
        worker.name,
        [promptPlan({ status: "completed", assistantText: `result-${worker.name}` })],
        tracker,
      ),
    }));
    const orchestrator = runtime(new FakeFactory(plans));
    const completion = new Deferred<CompletedWave>();
    orchestrator.subscribeCompletion((wave) => completion.resolve(wave));

    const accepted = await orchestrator.orchestrate(
      context("owner", workers),
      workers.map((worker) => task(worker.name)),
      "async",
    );
    await tracker.starts.waitFor(3);
    plans[2]!.handle.promptPlans[0]!.gate.resolve(undefined);
    plans[1]!.handle.promptPlans[0]!.gate.resolve(undefined);
    plans[0]!.handle.promptPlans[0]!.gate.resolve(undefined);

    const completed = await completion.promise;
    expect(completed.results.map((item) => item.worker)).toEqual(["alpha", "beta", "gamma"]);
    expect(completed.results.map((item) => item.workerId)).toEqual([...accepted.workerIds]);
    expect(completed.results.map((item) => item.outcome.status)).toEqual([
      "completed",
      "completed",
      "completed",
    ]);
    expect((await orchestrator.snapshot("owner")).waves[0]?.state).toBe("complete");
    expect((await orchestrator.snapshot("owner")).waves[0]).not.toHaveProperty("delivered");
    await orchestrator.shutdown();
  });

  test("keeps one worker ID across reusable ready, send, and close", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("reusable", [first, second], tracker);
    const factory = new FakeFactory([{ handle }]);
    const orchestrator = runtime(factory);
    const owner = context("owner", [definition("reusable", "reusable")]);

    const initial = orchestrator.orchestrate(owner, [task("reusable")], "inline");
    await tracker.starts.waitFor(1);
    handle.emitMessageDirection("from-model");
    first.gate.resolve(undefined);
    const firstWave = await initial;
    const workerId = firstWave.results[0]!.workerId;
    expect(firstWave.results[0]?.status).toBe("ready");
    expect(handle.disposeCalls).toBe(0);

    const followUp = orchestrator.send(owner, workerId, "Follow up", "inline");
    await tracker.starts.waitFor(2);
    const running = await orchestrator.snapshot("owner");
    expect(running.workers[0]?.status).toBe("running");
    expect(running.workers[0]?.messageDirection).toBe("to-model");
    expect(running.workers[0]?.outcome).toBeUndefined();
    second.gate.resolve(undefined);

    const secondWave = await followUp;
    expect(secondWave.results[0]?.workerId).toBe(workerId);
    expect(secondWave.results[0]?.outcome).toEqual({ status: "ready", assistantText: "second" });
    expect(factory.creates.value).toBe(1);
    expect(handle.prompts).toEqual(["Instructions for reusable", "Follow up"]);

    await orchestrator.close("owner", workerId);
    await expect(orchestrator.close("owner", workerId)).rejects.toThrow("ready reusable");
    await expect(orchestrator.send(owner, workerId, "Again", "async")).rejects.toThrow(
      "worker_send",
    );
    expect(handle.disposeCalls).toBe(1);
    expect((await orchestrator.snapshot("owner")).workers[0]).toMatchObject({
      id: workerId,
      status: "closed",
      outcome: { status: "closed" },
    });
    await orchestrator.shutdown();
  });

  test("bounds completed wave history across repeated reusable responses", async () => {
    const tracker = new PromptTracker();
    const responseCount = MAX_COMPLETED_WAVE_HISTORY + 1;
    const handle = new FakeHandle(
      "reusable-history",
      Array.from({ length: responseCount }, (_, index) =>
        promptPlan({ status: "ready", assistantText: `response-${index}` }, true)),
      tracker,
    );
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);

    const initial = await orchestrator.orchestrate(owner, [task("reusable")], "inline");
    const workerId = initial.results[0]!.workerId;
    for (let index = 1; index < responseCount; index += 1) {
      await orchestrator.send(owner, workerId, `Follow up ${index}`, "inline");
    }

    const snapshot = await orchestrator.snapshot("owner");
    expect(snapshot.waves).toHaveLength(MAX_COMPLETED_WAVE_HISTORY);
    expect(snapshot.workers).toHaveLength(1);
    expect(snapshot.workers[0]).toMatchObject({ id: workerId, status: "ready" });
    expect(handle.prompts).toHaveLength(responseCount);
    await orchestrator.shutdown();
  });

  test("retains ready workers and bounds terminal worker and completed wave history globally", async () => {
    const tracker = new PromptTracker();
    const reusableHandle = new FakeHandle(
      "reusable",
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
    const orchestrator = runtime(new FakeFactory([{ handle: reusableHandle }, ...terminalPlans]));
    const reusable = definition("reusable", "reusable");
    const oneShot = definition("one-shot");
    const owner = context("owner", [reusable, oneShot]);

    const reusableWave = await orchestrator.orchestrate(owner, [task("reusable")], "inline");
    const reusableId = reusableWave.results[0]!.workerId;
    for (let index = 0; index < 101; index += 1) {
      await orchestrator.orchestrate(owner, [task("one-shot", `Work ${index}`)], "inline");
    }

    const snapshot = await orchestrator.snapshot("owner");
    expect(snapshot.waves).toHaveLength(MAX_COMPLETED_WAVE_HISTORY);
    expect(String(snapshot.waves[0]?.id)).toBe("wave-3");
    expect(snapshot.workers).toHaveLength(MAX_TERMINAL_WORKER_HISTORY + 1);
    expect(snapshot.workers.some((worker) => worker.id === reusableId && worker.status === "ready")).toBe(true);
    expect(snapshot.workers.some((worker) => worker.id === "worker-2")).toBe(false);

    await orchestrator.close("owner", reusableId);
    const closed = await orchestrator.snapshot("owner");
    expect(closed.workers).toHaveLength(MAX_TERMINAL_WORKER_HISTORY);
    expect(closed.workers.some((worker) => worker.id === reusableId && worker.status === "closed")).toBe(true);
    await orchestrator.shutdown();
  });
});

describe("per-worker settlement observability", () => {
  test("emits settlement and compatibility completion before pruning under terminal-history pressure", async () => {
    const tracker = new PromptTracker();
    const plans = Array.from({ length: MAX_TERMINAL_WORKER_HISTORY + 12 }, (_, index) => ({
      handle: new FakeHandle(
        `pressure-${index}`,
        [promptPlan({ status: "completed", assistantText: `result-${index}` }, true)],
        tracker,
      ),
    }));
    const orchestrator = runtime(new FakeFactory(plans));
    const owner = context("owner", [definition("worker")]);

    for (let index = 0; index < MAX_TERMINAL_WORKER_HISTORY; index += 1) {
      await orchestrator.orchestrate(owner, [task("worker", `Prefill ${index}`)], "inline");
    }

    const settlements: import("../extension/runtime.ts").WorkerSettlement[] = [];
    const completions: CompletedWave[] = [];
    orchestrator.subscribeSettlement((event) => settlements.push(event));
    orchestrator.subscribeCompletion((wave) => completions.push(wave));
    const tasks = Array.from({ length: 12 }, (_, index) => task("worker", `Pressure ${index}`));
    await orchestrator.orchestrate(owner, tasks, "async");
    while (settlements.length < 12) await Promise.resolve();

    expect(settlements).toHaveLength(12);
    expect(settlements.at(-1)?.waveComplete).toBe(true);
    expect(completions).toHaveLength(1);
    expect(completions[0]?.results).toHaveLength(12);
    expect((await orchestrator.snapshot("owner")).workers).toHaveLength(
      MAX_TERMINAL_WORKER_HISTORY,
    );
    await orchestrator.shutdown();
  });

  test("permits pruned worker and wave IDs to be reused with new settlement identities", async () => {
    const tracker = new PromptTracker();
    const count = MAX_TERMINAL_WORKER_HISTORY + 2;
    const plans = Array.from({ length: count }, (_, index) => ({
      handle: new FakeHandle(
        `reuse-${index}`,
        [promptPlan({ status: "completed", assistantText: `${index}` }, true)],
        tracker,
      ),
    }));
    let nextWave = 0;
    let nextWorker = 0;
    const orchestrator = runtime(new FakeFactory(plans), {
      idFactories: {
        waveId: () => `wave-${nextWave++ % (MAX_COMPLETED_WAVE_HISTORY + 1)}` as WaveId,
        workerId: () => `worker-${nextWorker++ % (MAX_TERMINAL_WORKER_HISTORY + 1)}` as WorkerId,
      },
    });
    const events: import("../extension/runtime.ts").WorkerSettlement[] = [];
    orchestrator.subscribeSettlement((event) => events.push(event));
    const owner = context("owner", [definition("worker")]);

    for (let index = 0; index < count; index += 1) {
      await orchestrator.orchestrate(owner, [task("worker", `Reuse ${index}`)], "inline");
    }

    expect(events[0]?.workerId).toBe(events.at(-1)?.workerId);
    expect(events[0]?.waveId).toBe(events.at(-1)?.waveId);
    expect(events[0]?.eventId).not.toBe(events.at(-1)?.eventId);
    expect(events.at(-1)?.sequence).toBe(count);
    await orchestrator.shutdown();
  });

  test("emits each settlement immediately in completion order and marks only the last wave result final", async () => {
    const tracker = new PromptTracker();
    const alpha = new FakeHandle(
      "alpha-settlement",
      [promptPlan({ status: "completed", assistantText: "alpha result" })],
      tracker,
    );
    const beta = new FakeHandle(
      "beta-settlement",
      [promptPlan({ status: "completed", assistantText: "beta result" })],
      tracker,
    );
    const orchestrator = runtime(new FakeFactory([{ handle: alpha }, { handle: beta }]));
    const settlements: import("../extension/runtime.ts").WorkerSettlement[] = [];
    orchestrator.subscribeSettlement((event) => settlements.push(event));

    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("alpha"), definition("beta")]),
      [task("alpha"), task("beta")],
      "async",
    );
    await tracker.starts.waitFor(2);
    alpha.promptPlans[0]!.gate.resolve(undefined);
    await alpha.disposed.promise;

    expect(settlements).toHaveLength(1);
    expect(settlements[0]).toMatchObject({
      ownerSessionId: "owner",
      waveId: accepted.id,
      workerId: accepted.workerIds[0],
      generation: 1,
      mode: "async",
      status: "completed",
      remainingActive: 1,
      waveSize: 2,
      waveComplete: false,
      startedAt: 1_700_000_000_000,
      settledAt: 1_700_000_000_000,
      sessionFile: "/sessions/alpha-settlement.jsonl",
    });
    expect(Object.isFrozen(settlements[0])).toBe(true);
    expect(Object.isFrozen(settlements[0]?.outcome)).toBe(true);
    expect(Object.isFrozen(settlements[0]?.usage)).toBe(true);

    beta.promptPlans[0]!.gate.resolve(undefined);
    await beta.disposed.promise;
    expect(settlements.map((event) => event.worker)).toEqual(["alpha", "beta"]);
    expect(settlements.map((event) => event.sequence)).toEqual([1, 2]);
    expect(settlements.map((event) => event.waveComplete)).toEqual([false, true]);
    expect(new Set(settlements.map((event) => event.eventId)).size).toBe(2);
    await orchestrator.shutdown();
  });

  test("observes inline reusable generations locally with distinct event IDs and final usage", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("local-reusable", [first, second], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);
    const local: import("../extension/runtime.ts").WorkerSettlement[] = [];
    const global: import("../extension/runtime.ts").WorkerSettlement[] = [];
    orchestrator.subscribeSettlement((event) => global.push(event));

    const initial = orchestrator.orchestrate(owner, [task("reusable")], "inline", undefined, (event) => local.push(event));
    await tracker.starts.waitFor(1);
    first.gate.resolve(undefined);
    const workerId = (await initial).results[0]!.workerId;

    const followUp = orchestrator.send(owner, workerId, "follow up", "inline", undefined, (event) => local.push(event));
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
    await orchestrator.close("owner", workerId);
    expect(local).toHaveLength(2);
    await orchestrator.shutdown();
  });

  test("emits failed startup and aborted active workers exactly once", async () => {
    const failedFactory: WorkerSessionFactory = {
      create() {
        return Promise.reject(new Error("startup failed"));
      },
    };
    const failedRuntime = runtime(failedFactory);
    const failed: import("../extension/runtime.ts").WorkerSettlement[] = [];
    failedRuntime.subscribeSettlement((event) => failed.push(event));
    await failedRuntime.orchestrate(
      context("failed-owner", [definition("worker")]),
      [task("worker")],
      "async",
    );
    while (failed.length === 0) await Promise.resolve();
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      status: "failed",
      failureStage: "startup",
      waveComplete: true,
    });
    expect(failed[0]?.outcome).toEqual({ status: "failed", message: "startup failed" });
    await failedRuntime.shutdown();

    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("aborted-settlement", [prompt], tracker);
    const abortedRuntime = runtime(new FakeFactory([{ handle }]));
    const aborted: import("../extension/runtime.ts").WorkerSettlement[] = [];
    abortedRuntime.subscribeSettlement((event) => aborted.push(event));
    const accepted = await abortedRuntime.orchestrate(
      context("abort-owner", [definition("worker")]),
      [task("worker")],
      "async",
    );
    await tracker.starts.waitFor(1);
    await abortedRuntime.abort("abort-owner", { workerIds: accepted.workerIds });
    prompt.gate.resolve(undefined);
    await Promise.resolve();
    expect(aborted).toHaveLength(1);
    expect(aborted[0]).toMatchObject({
      status: "aborted",
      failureStage: "cancellation",
      waveComplete: true,
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
    const orchestrator = runtime(new FakeFactory([{ handle: first }, { handle: second }]));
    const notifications: string[] = [];
    orchestrator.subscribeState(() => {
      throw new Error("listener failed");
    });
    const unsubscribe = orchestrator.subscribeState((ownerSessionId) => {
      notifications.push(ownerSessionId);
    });

    await orchestrator.orchestrate(
      context("owner-a", [definition("first")]),
      [task("first")],
      "async",
    );
    await orchestrator.orchestrate(
      context("owner-b", [definition("second")]),
      [task("second")],
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
    expect(Object.isFrozen(ownerA)).toBe(true);
    expect(Object.isFrozen(ownerA.workers)).toBe(true);
    expect(Object.isFrozen(ownerA.workers[0])).toBe(true);
    expect(Object.isFrozen(ownerA.workers[0]?.usage)).toBe(true);
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
    const handle = new FakeHandle("reusable", [plan], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const worker = definition("reusable", "reusable");
    const ownerA = context("owner-a", [worker]);
    const ownerB = context("owner-b", [worker]);
    const accepted = await orchestrator.orchestrate(ownerA, [task("reusable")], "async");

    expect(await orchestrator.snapshot("owner-b")).toEqual({ waves: [], workers: [] });
    await expect(
      orchestrator.send(ownerB, accepted.workerIds[0]!, "intrude", "async"),
    ).rejects.toThrow("not owned");
    await expect(orchestrator.close("owner-b", accepted.workerIds[0]!)).rejects.toThrow("not owned");
    await expect(
      orchestrator.abort("owner-b", { workerIds: accepted.workerIds }),
    ).rejects.toThrow("not owned");

    await orchestrator.abort("owner-a", { all: true });
    plan.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("aborts pending bootstrap and disposes a session created late", async () => {
    const tracker = new PromptTracker();
    const createGate = new Deferred();
    const handle = new FakeHandle(
      "late",
      [promptPlan({ status: "completed", assistantText: "late" })],
      tracker,
    );
    const factory = new FakeFactory([{ handle, gate: createGate }]);
    const orchestrator = runtime(factory);
    const completion = new Deferred<CompletedWave>();
    orchestrator.subscribeCompletion((wave) => completion.resolve(wave));

    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("late")]),
      [task("late")],
      "async",
    );
    await factory.creates.waitFor(1);
    await orchestrator.abort("owner", { waveId: accepted.id });
    expect((await completion.promise).results[0]?.status).toBe("aborted");

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
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const completion = new Deferred<CompletedWave>();
    orchestrator.subscribeCompletion((wave) => completion.resolve(wave));

    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("running")]),
      [task("running")],
      "async",
    );
    await tracker.starts.waitFor(1);
    handle.emitActivity("read");
    expect((await orchestrator.snapshot("owner")).workers[0]?.activity).toBe("read");
    const aborting = orchestrator.abort("owner", { workerIds: accepted.workerIds });
    await expectPending(aborting);
    expect(handle.abortCalls).toBe(1);

    handle.abortGate.resolve(undefined);
    await aborting;
    expect((await completion.promise).results[0]?.status).toBe("aborted");
    handle.emitStaleActivity("bash");
    prompt.gate.resolve(undefined);
    await Promise.resolve();
    const abortedWorker = (await orchestrator.snapshot("owner")).workers[0];
    expect(abortedWorker?.activity).toBeUndefined();
    expect(abortedWorker?.outcome).toEqual({ status: "aborted" });
    await orchestrator.shutdown();
  });

  test("shutdown force-closes and disposes a ready reusable session", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "ready", assistantText: "ready" });
    const handle = new FakeHandle("reusable", [prompt], tracker);
    handle.abortGate = new Deferred();
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);

    const initial = orchestrator.orchestrate(owner, [task("reusable")], "inline");
    await tracker.starts.waitFor(1);
    prompt.gate.resolve(undefined);
    await initial;

    await orchestrator.shutdown();

    expect(handle.abortCalls).toBe(0);
    expect(handle.disposeCalls).toBe(1);
    expect((await orchestrator.snapshot("owner")).workers[0]).toMatchObject({
      status: "closed",
      outcome: { status: "closed" },
    });
    await expect(orchestrator.orchestrate(owner, [task("reusable")], "async")).rejects.toThrow(
      "shutting down",
    );
  });
});

describe("keyed scheduling and reentrant reusable sends", () => {
  test("replacement interrupts the Effect workflow and runs its finalizer", async () => {
    const scheduler = createWorkflowScheduler<string>();
    const firstStarted = new Deferred();
    const firstFinalized = new Deferred();
    const secondStarted = new Deferred();
    const defect = new Deferred<unknown>();

    scheduler.start(
      "worker",
      Effect.sync(() => firstStarted.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Effect.sync(() => firstFinalized.resolve(undefined))),
      ),
      (error) => defect.resolve(error),
    );
    await firstStarted.promise;

    scheduler.start(
      "worker",
      Effect.sync(() => secondStarted.resolve(undefined)).pipe(
        Effect.andThen(Effect.die(new Error("scheduler boom"))),
      ),
      (error) => defect.resolve(error),
    );
    await secondStarted.promise;
    await firstFinalized.promise;

    const reported = await defect.promise;
    expect(reported).toBeInstanceOf(Error);
    if (!(reported instanceof Error)) throw new Error("Expected scheduler defect");
    expect(reported.message).toContain("scheduler boom");
    await scheduler.close();
  });

  test("remove waits for interrupted workflow finalization", async () => {
    const scheduler = createWorkflowScheduler<string>();
    const started = new Deferred();
    const finalizerStarted = new Deferred();
    const finalizerGate = new Deferred();
    const finalized = new Deferred();

    scheduler.start(
      "worker",
      Effect.sync(() => started.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(
          Effect.sync(() => finalizerStarted.resolve(undefined)).pipe(
            Effect.andThen(Effect.promise(() => finalizerGate.promise)),
            Effect.andThen(Effect.sync(() => finalized.resolve(undefined))),
          ),
        ),
      ),
      () => undefined,
    );
    await started.promise;

    const removing = scheduler.remove("worker");
    await finalizerStarted.promise;
    await expectPending(removing);
    finalizerGate.resolve(undefined);
    await removing;
    await finalized.promise;
    await scheduler.close();
  });

  test("interruption-only causes never call the defect handler", async () => {
    const scheduler = createWorkflowScheduler<string>();
    const started = new Deferred();
    const finalized = new Deferred();
    const defects: unknown[] = [];

    scheduler.start(
      "worker",
      Effect.sync(() => started.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Effect.sync(() => finalized.resolve(undefined))),
      ),
      (error) => defects.push(error),
    );
    await started.promise;

    await scheduler.remove("worker");
    await finalized.promise;
    expect(defects).toEqual([]);
    await scheduler.close();
  });

  test("concurrent close calls join one disposal and run finalizers once", async () => {
    const scheduler = createWorkflowScheduler<string>();
    const started = new Deferred();
    const finalizerStarted = new Deferred();
    const finalizerGate = new Deferred();
    let finalizerRuns = 0;

    scheduler.start(
      "worker",
      Effect.sync(() => started.resolve(undefined)).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(
          Effect.sync(() => {
            finalizerRuns += 1;
            finalizerStarted.resolve(undefined);
          }).pipe(Effect.andThen(Effect.promise(() => finalizerGate.promise))),
        ),
      ),
      () => undefined,
    );
    await started.promise;

    const firstClose = scheduler.close();
    const secondClose = scheduler.close();
    expect(secondClose).toBe(firstClose);
    await finalizerStarted.promise;
    await Promise.all([expectPending(firstClose), expectPending(secondClose)]);
    expect(finalizerRuns).toBe(1);

    finalizerGate.resolve(undefined);
    await Promise.all([firstClose, secondClose]);
    expect(finalizerRuns).toBe(1);
  });

  test("completion listeners can synchronously send the next reusable generation", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("completion-reentrant", [first, second], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);
    let followUp: Promise<CompletedWave> | undefined;

    orchestrator.subscribeCompletion((wave) => {
      if (wave.id !== "wave-1") return;
      followUp = orchestrator.send(
        owner,
        wave.results[0]!.workerId,
        "Completion follow-up",
        "inline",
      );
    });

    await orchestrator.orchestrate(owner, [task("reusable")], "async");
    await tracker.starts.waitFor(1);
    first.gate.resolve(undefined);
    await tracker.starts.waitFor(2);

    expect(handle.prompts).toEqual(["Instructions for reusable", "Completion follow-up"]);
    second.gate.resolve(undefined);
    const completed = await followUp!;
    expect(completed.results[0]?.outcome).toEqual({
      status: "ready",
      assistantText: "second",
    });
    await orchestrator.close("owner", completed.results[0]!.workerId);
    await orchestrator.shutdown();
  });

  test("state listeners can synchronously send the next reusable generation", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "first" });
    const second = promptPlan({ status: "ready", assistantText: "second" });
    const handle = new FakeHandle("state-reentrant", [first, second], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);
    const accepted = await orchestrator.orchestrate(owner, [task("reusable")], "async");
    await tracker.starts.waitFor(1);
    let followUp: Promise<CompletedWave> | undefined;
    let sent = false;

    orchestrator.subscribeState(() => {
      if (sent) return;
      sent = true;
      followUp = orchestrator.send(
        owner,
        accepted.workerIds[0]!,
        "State follow-up",
        "inline",
      );
    });

    first.gate.resolve(undefined);
    await tracker.starts.waitFor(2);
    expect(handle.prompts).toEqual(["Instructions for reusable", "State follow-up"]);
    second.gate.resolve(undefined);
    expect((await followUp!).results[0]?.status).toBe("ready");
    await orchestrator.close("owner", accepted.workerIds[0]!);
    await orchestrator.shutdown();
  });
});

describe("inline AbortSignal ownership", () => {
  test("already-aborted signals preserve null, object, and default reason identity", async () => {
    const orchestrator = runtime(new FakeFactory([]));
    const owner = context("owner", [definition("worker")]);
    const objectReason = { kind: "parent-turn-ended" };
    const controllers = [new AbortController(), new AbortController(), new AbortController()];
    controllers[0]!.abort(null);
    controllers[1]!.abort(objectReason);
    controllers[2]!.abort();

    for (const controller of controllers) {
      const rejectedReason = await orchestrator
        .orchestrate(owner, [task("worker")], "inline", controller.signal)
        .then(
          () => "unexpected success",
          (reason: unknown) => reason,
        );
      expect(rejectedReason).toBe(controller.signal.reason);
      expect(await orchestrator.snapshot("owner")).toEqual({ waves: [], workers: [] });
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
    const orchestrator = runtime(new FakeFactory(plans));
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
        [task("worker")],
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
      expect(snapshot.waves[index]).toMatchObject({ state: "complete" });
      expect(snapshot.workers[index]).toMatchObject({
        status: "aborted",
        outcome: { status: "aborted" },
      });
    }
    expect((await orchestrator.snapshot("owner")).waves).toHaveLength(3);
    for (const plan of plans) plan.handle.promptPlans[0]!.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("abort after inline admission cancels the exact wave and rejects after settlement", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("inline-abort", [prompt], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("worker")]);
    const controller = new AbortController();
    const reason = new Error("inline cancelled");

    const inline = orchestrator.orchestrate(
      owner,
      [task("worker")],
      "inline",
      controller.signal,
    );
    await tracker.starts.waitFor(1);
    controller.abort(reason);

    await expect(inline).rejects.toBe(reason);
    expect(await orchestrator.snapshot("owner")).toMatchObject({
      waves: [{ state: "complete" }],
      workers: [{ status: "aborted", outcome: { status: "aborted" } }],
    });
    prompt.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("abort after inline send cancels that reusable generation", async () => {
    const tracker = new PromptTracker();
    const first = promptPlan({ status: "ready", assistantText: "ready" });
    const second = promptPlan({ status: "ready", assistantText: "late" });
    const handle = new FakeHandle("send-abort", [first, second], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);

    const initial = orchestrator.orchestrate(owner, [task("reusable")], "inline");
    await tracker.starts.waitFor(1);
    first.gate.resolve(undefined);
    const workerId = (await initial).results[0]!.workerId;
    const controller = new AbortController();
    const followUp = orchestrator.send(
      owner,
      workerId,
      "cancel this generation",
      "inline",
      controller.signal,
    );
    await tracker.starts.waitFor(2);
    controller.abort(new Error("send cancelled"));

    await expect(followUp).rejects.toThrow("send cancelled");
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("aborted");
    second.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("async dispatch does not retain the caller's signal after acceptance", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "done" });
    const handle = new FakeHandle("async-signal", [prompt], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("worker")]);
    const controller = new AbortController();
    const completion = new Deferred<CompletedWave>();
    orchestrator.subscribeCompletion((wave) => completion.resolve(wave));

    await orchestrator.orchestrate(owner, [task("worker")], "async", controller.signal);
    await tracker.starts.waitFor(1);
    controller.abort();
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("running");

    prompt.gate.resolve(undefined);
    expect((await completion.promise).results[0]?.status).toBe("completed");
    await orchestrator.shutdown();
  });
});

describe("active-only aborts and bounded lifecycle barriers", () => {
  test("ready workers reject explicit abort with use worker_close and all aborts active only", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "ready", assistantText: "ready" });
    const handle = new FakeHandle("ready", [prompt], tracker);
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const owner = context("owner", [definition("reusable", "reusable")]);

    const initial = orchestrator.orchestrate(owner, [task("reusable")], "inline");
    await tracker.starts.waitFor(1);
    prompt.gate.resolve(undefined);
    const workerId = (await initial).results[0]!.workerId;

    await expect(orchestrator.abort("owner", { workerIds: [workerId] })).rejects.toThrow(
      "use worker_close",
    );
    await orchestrator.abort("owner", { all: true });
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("ready");
    expect(handle.abortCalls).toBe(0);

    await orchestrator.close("owner", workerId);
    await orchestrator.shutdown();
  });

  test("concurrent aborts join one stored cancellation promise", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("joined-abort", [prompt], tracker);
    handle.abortGate = new Deferred();
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      [task("worker")],
      "async",
    );
    await tracker.starts.waitFor(1);

    const firstAbort = orchestrator.abort("owner", { workerIds: accepted.workerIds });
    await handle.abortStarts.waitFor(1);
    const secondAbort = orchestrator.abort("owner", { workerIds: accepted.workerIds });
    await expectPending(firstAbort);
    await expectPending(secondAbort);
    expect(handle.abortCalls).toBe(1);

    handle.abortGate.resolve(undefined);
    await Promise.all([firstAbort, secondAbort]);
    expect(handle.abortCalls).toBe(1);
    prompt.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("cancellation uses the injected bounded timeout policy", async () => {
    const tracker = new PromptTracker();
    const prompt = promptPlan({ status: "completed", assistantText: "late" });
    const handle = new FakeHandle("timed-abort", [prompt], tracker);
    handle.abortGate = new Deferred();
    const deadlineGate = new Deferred<"settled" | "timed-out">();
    const calls: number[] = [];
    const deadline: BestEffortDeadline = {
      wait(_promise, timeoutMs) {
        calls.push(timeoutMs);
        return calls.length === 1
          ? deadlineGate.promise
          : Promise.resolve("timed-out");
      },
    };
    const orchestrator = runtime(new FakeFactory([{ handle }]), {
      bestEffortDeadline: deadline,
    });
    const accepted = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      [task("worker")],
      "async",
    );
    await tracker.starts.waitFor(1);

    const aborting = orchestrator.abort("owner", { workerIds: accepted.workerIds });
    await handle.abortStarts.waitFor(1);
    await expectPending(aborting);
    deadlineGate.resolve("timed-out");
    await aborting;

    expect(calls[0]).toBe(SHUTDOWN_CLEANUP_GRACE_MS);
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("aborted");
    handle.abortGate.resolve(undefined);
    prompt.gate.resolve(undefined);
    await orchestrator.shutdown();
  });

  test("shutdown bounds pending bootstrap cleanup and still disposes a late session", async () => {
    const tracker = new PromptTracker();
    const createGate = new Deferred();
    const handle = new FakeHandle("late-shutdown", [], tracker);
    const deadlineCalls: number[] = [];
    const deadline: BestEffortDeadline = {
      wait(_promise, timeoutMs) {
        deadlineCalls.push(timeoutMs);
        return Promise.resolve("timed-out");
      },
    };
    const orchestrator = runtime(
      new FakeFactory([{ handle, gate: createGate }]),
      { bestEffortDeadline: deadline },
    );

    await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      [task("worker")],
      "async",
    );
    await orchestrator.shutdown();

    expect(deadlineCalls).toEqual([SHUTDOWN_CLEANUP_GRACE_MS]);
    expect((await orchestrator.snapshot("owner")).workers[0]?.status).toBe("aborted");
    expect(handle.disposeCalls).toBe(0);
    createGate.resolve(undefined);
    await handle.disposed.promise;
    expect(handle.disposeCalls).toBe(1);
  });
});

describe("defect and cleanup supervision", () => {
  test("an injected scheduler defect fails the current worker and completes its wave", async () => {
    let closeCalls = 0;
    const scheduler: WorkflowScheduler<WorkerId> = {
      start(_key, _workflow, onDefect) {
        onDefect(new Error("workflow defect"));
      },
      remove() {
        return Promise.resolve();
      },
      close() {
        closeCalls += 1;
        return Promise.resolve();
      },
    };
    const orchestrator = runtime(new FakeFactory([]), { scheduler });

    const completed = await orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      [task("worker")],
      "inline",
    );

    expect(completed.results[0]).toMatchObject({
      status: "failed",
      outcome: { status: "failed", message: "workflow defect" },
    });
    await orchestrator.shutdown();
    expect(closeCalls).toBe(1);
  });

  test("session disposal exceptions cannot strand completion or shutdown", async () => {
    const tracker = new PromptTracker();
    const handle = new FakeHandle(
      "dispose-error",
      [promptPlan({ status: "completed", assistantText: "done" })],
      tracker,
    );
    handle.disposeFailure = new Error("dispose failed");
    const orchestrator = runtime(new FakeFactory([{ handle }]));
    const inline = orchestrator.orchestrate(
      context("owner", [definition("worker")]),
      [task("worker")],
      "inline",
    );
    await tracker.starts.waitFor(1);
    handle.promptPlans[0]!.gate.resolve(undefined);

    expect((await inline).results[0]?.status).toBe("completed");
    expect(handle.disposeCalls).toBe(1);
    await orchestrator.shutdown();
  });
});
