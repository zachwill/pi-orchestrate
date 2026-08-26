import { describe, expect, test } from "bun:test";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Effect, Layer, ManagedRuntime } from "effect";
import {
  Delivery,
  DeliveryCoordinator,
  deliveryLayer,
} from "../../extension/parent/delivery.ts";
import { createWorkerCatalog } from "../../extension/catalog/definition.ts";
import {
  createSequentialRunIdFactory,
  createSequentialWorkerIdFactory,
  type OrchestrateTaskInput,
  type RunMode,
} from "../../extension/orchestration/model.ts";
import {
  attachProcessHost,
  createOrchestrationClient,
  createProcessHost,
  destroyProcessHost,
  getProcessHost,
  makeProcessHostLayer,
  ManagedOrchestrationClient,
  type ProcessHost,
} from "../../extension/parent/process-host.ts";
import {
  Orchestration,
  SHUTDOWN_CLEANUP_GRACE_MS,
} from "../../extension/orchestration/service.ts";
import { OrchestrationActionRejected } from "../../extension/orchestration/admission.ts";
import type { WorkerSettlement } from "../../extension/orchestration/settlement.ts";
import type { OrchestrationContext } from "../../extension/orchestration/admission.ts";
import type {
  AcceptedRun,
  CompletedRun,
  OrchestrationService,
  OwnerSnapshot,
  SettlementListener,
} from "../../extension/orchestration/service.ts";

class TestOrchestration implements OrchestrationService {
  effect: Effect.Effect<
    AcceptedRun | CompletedRun,
    OrchestrationActionRejected
  > = Effect.never;

  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    onSettlement?: SettlementListener,
  ): Effect.Effect<CompletedRun, OrchestrationActionRejected>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected>;
  orchestrate(
    _context: OrchestrationContext,
    _task: OrchestrateTaskInput,
    _mode: RunMode,
    _onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected> {
    return this.effect;
  }

  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "async",
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "inline",
    onSettlement?: SettlementListener,
  ): Effect.Effect<CompletedRun, OrchestrationActionRejected>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected>;
  sendInteractive(
    _context: OrchestrationContext,
    _workerId: string,
    _instructions: string,
    _mode: RunMode,
    _onSettlement?: SettlementListener,
  ): Effect.Effect<AcceptedRun | CompletedRun, OrchestrationActionRejected> {
    return this.effect;
  }

  readonly abort = (): Effect.Effect<void> => Effect.void;
  readonly closeInteractive = (): Effect.Effect<void> => Effect.void;
  readonly snapshot = () => Effect.succeed({ runs: [], workers: [] });
  unsubscribeSettlement = () => {};
  readonly subscribeSettlement = () => this.unsubscribeSettlement;
  readonly subscribeState = (
    _ownerSessionId: string,
    listener: (snapshot: OwnerSnapshot) => void,
  ) => {
    listener({ runs: [], workers: [] });
    return () => {};
  };
  readonly shutdown = (): Effect.Effect<void> => Effect.void;
}

const context: OrchestrationContext = {
  ownerSessionId: "owner",
  cwd: "/project",
  agentDir: "/agent",
  parentSessionFile: undefined,
  projectTrusted: true,
  catalog: createWorkerCatalog([]),
  modelRegistry: { find: () => undefined } as unknown as ModelRegistry,
};
const task: OrchestrateTaskInput = {
  worker: "worker",
  title: "Test worker",
  instructions: "Test the adapter",
};

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function adapterHarness() {
  const orchestration = new TestOrchestration();
  const effectRuntime = ManagedRuntime.make(
    Layer.succeed(Orchestration, orchestration),
  );
  const adapter = new ManagedOrchestrationClient(effectRuntime, orchestration);
  return { adapter, effectRuntime, orchestration };
}

function destructionHost(
  shutdown: () => Promise<void>,
  dispose: () => Promise<void> = async () => {},
): ProcessHost {
  return Object.assign(
    {
      orchestration: { shutdown } as unknown as ProcessHost["orchestration"],
      delivery: new DeliveryCoordinator(),
    } satisfies ProcessHost,
    { effectRuntime: { dispose } },
  );
}

describe("ProcessHost AbortSignal adapter", () => {
  test("squashes typed failures to the same tagged Error object and message", async () => {
    const { adapter, effectRuntime, orchestration } = adapterHarness();
    const rejected = new OrchestrationActionRejected({
      operation: "orchestrate",
      reason: "unknown-worker",
      message: "Unknown worker: missing",
    });
    orchestration.effect = Effect.fail(rejected);

    const observed = await adapter.orchestrate(context, task, "inline").then(
      () => "unexpected success",
      (error: unknown) => error,
    );

    expect(observed).toBe(rejected);
    expect(observed).toBeInstanceOf(Error);
    expect((observed as Error).message).toBe("Unknown worker: missing");
    await effectRuntime.dispose();
  });

  test("preserves a synchronously settled defect when abort happens before rejection observation", async () => {
    const { adapter, effectRuntime, orchestration } = adapterHarness();
    const controller = new AbortController();
    const defect = new Error("validation defect");
    const abortReason = new Error("late abort");
    orchestration.effect = Effect.sync(() => {
      queueMicrotask(() => controller.abort(abortReason));
      throw defect;
    });

    const result = adapter.orchestrate(context, task, "inline", controller.signal);

    await expect(result).rejects.toBe(defect);
    expect(controller.signal.reason).toBe(abortReason);
    await effectRuntime.dispose();
  });

  test("removes the catch-up listener when orchestration aborts synchronously", async () => {
    const { adapter, effectRuntime, orchestration } = adapterHarness();
    const controller = new AbortController();
    const signal = controller.signal;
    const reason = new Error("synchronous abort");
    const originalAdd = signal.addEventListener.bind(signal);
    const originalRemove = signal.removeEventListener.bind(signal);
    let abortListenerAdds = 0;
    let abortListenerRemoves = 0;

    Object.defineProperties(signal, {
      addEventListener: {
        value: (
          type: string,
          listener: EventListenerOrEventListenerObject,
          options?: boolean | AddEventListenerOptions,
        ) => {
          if (type === "abort") abortListenerAdds += 1;
          originalAdd(type, listener, options);
        },
      },
      removeEventListener: {
        value: (
          type: string,
          listener: EventListenerOrEventListenerObject,
          options?: boolean | EventListenerOptions,
        ) => {
          if (type === "abort") abortListenerRemoves += 1;
          originalRemove(type, listener, options);
        },
      },
    });
    orchestration.effect = Effect.sync(() => {
      controller.abort(reason);
    }).pipe(Effect.andThen(Effect.never));

    await expect(
      adapter.orchestrate(context, task, "inline", signal),
    ).rejects.toBe(reason);
    expect(signal.reason).toBe(reason);
    expect(abortListenerAdds).toBe(1);
    expect(abortListenerRemoves).toBe(abortListenerAdds);
    await effectRuntime.dispose();
  });

  test("restores the exact reason only after live interruption has settled", async () => {
    const { adapter, effectRuntime, orchestration } = adapterHarness();
    const controller = new AbortController();
    const reason = { kind: "parent-turn-ended" };
    const started = deferred();
    let interruptionSettled = false;
    orchestration.effect = Effect.callback<CompletedRun>(() => {
      started.resolve();
      return Effect.sync(() => {
        interruptionSettled = true;
      });
    });

    const result = adapter.orchestrate(context, task, "inline", controller.signal);
    await started.promise;
    controller.abort(reason);

    await expect(result).rejects.toBe(reason);
    expect(interruptionSettled).toBe(true);
    await effectRuntime.dispose();
  });

  test("rejects already-aborted ingress with the exact reason", async () => {
    const { adapter, effectRuntime } = adapterHarness();
    const controller = new AbortController();
    const reason = { kind: "already-ended" };
    controller.abort(reason);

    await expect(
      adapter.orchestrate(context, task, "inline", controller.signal),
    ).rejects.toBe(reason);
    await effectRuntime.dispose();
  });

  test("committed completion wins over a later abort", async () => {
    const { adapter, effectRuntime, orchestration } = adapterHarness();
    const controller = new AbortController();
    const workerId = createSequentialWorkerIdFactory()();
    const accepted: AcceptedRun = { id: "run-accepted" as AcceptedRun["id"], workerId };
    orchestration.effect = Effect.succeed(accepted);

    const result = await adapter.orchestrate(
      context,
      task,
      "async",
      controller.signal,
    );
    controller.abort(new Error("after acceptance"));

    expect(result).toBe(accepted);
    await effectRuntime.dispose();
  });
});

describe("ProcessHost root lifetime", () => {
  test("shares one orchestration acquisition across the process application layer", async () => {
    const effectRuntime = ManagedRuntime.make(makeProcessHostLayer());

    const first = effectRuntime.runSync(Orchestration);
    const second = effectRuntime.runSync(Orchestration);
    const delivery = effectRuntime.runSync(Delivery);

    expect(second).toBe(first);
    expect(delivery).toBeDefined();
    await effectRuntime.dispose();
  });

  test("keeps retained snapshots readable after disposal while runtime-backed operations fail", async () => {
    const effectRuntime = ManagedRuntime.make(makeProcessHostLayer());
    const adapter = createOrchestrationClient(effectRuntime);

    expect(await adapter.snapshot("owner")).toEqual({ runs: [], workers: [] });
    await effectRuntime.dispose();

    expect(await adapter.snapshot("owner")).toEqual({ runs: [], workers: [] });
    await expect(adapter.abort("owner", { all: true })).rejects.toBeDefined();
  });

  test("clears delivery state even when settlement unsubscription throws", async () => {
    const orchestration = new TestOrchestration();
    const unsubscribeFailure = new Error("unsubscribe failed");
    orchestration.unsubscribeSettlement = () => {
      throw unsubscribeFailure;
    };
    const root = deliveryLayer.pipe(
      Layer.provideMerge(Layer.succeed(Orchestration, orchestration)),
    );
    const effectRuntime = ManagedRuntime.make(root);
    const delivery = effectRuntime.runSync(Delivery);
    const settlement: WorkerSettlement = {
      eventId: "event",
      sequence: 1,
      ownerSessionId: "owner",
      runId: createSequentialRunIdFactory()(),
      workerId: createSequentialWorkerIdFactory()(),
      generation: 1,
      mode: "async",
      worker: "worker",
      title: "Test worker",
      lifecycle: "one-shot",
      status: "completed",
      outcome: {
        status: "completed",
        assistantText: "Test complete.",
      },
      usage: {
        input: 1,
        output: 1,
        cacheRead: 0,
        cacheWrite: 0,
        cost: 0,
        contextTokens: 2,
        turns: 1,
      },
      startedAt: 1,
      settledAt: 2,
      sessionFile: "/sessions/worker.jsonl",
    };
    delivery.accept(settlement);
    expect(delivery.pendingCount("owner")).toBe(1);

    await expect(effectRuntime.dispose()).rejects.toBeDefined();
    expect(delivery.pendingCount("owner")).toBe(0);
  });
});

describe("ProcessHost lifecycle", () => {
  test("reuses an active host without lifecycle normalization and deletes it after finalization", async () => {
    const first = createProcessHost();
    const second = createProcessHost();

    expect(second).toBe(first);
    expect(
      (first as ProcessHost & { lifecycle?: "destroying" | "destroyed" })
        .lifecycle,
    ).toBeUndefined();

    await destroyProcessHost(first);

    expect(getProcessHost()).toBeUndefined();
    expect(() => attachProcessHost(first)).toThrow(
      "Cannot attach to a destroyed process host",
    );

    const replacement = createProcessHost();
    expect(replacement).not.toBe(first);
    await destroyProcessHost(replacement);
    expect(getProcessHost()).toBeUndefined();
  });

  test("rejects a malformed globally retained destroyed host instead of recovering it", () => {
    const processHostKey = Symbol.for("@zachwill/pi-orchestrate/process-host/v3");
    const globalHosts = globalThis as unknown as Record<symbol, unknown>;
    const malformed = Object.assign(destructionHost(async () => {}), {
      lifecycle: "destroyed" as const,
    });
    globalHosts[processHostKey] = malformed;

    try {
      expect(() => createProcessHost()).toThrow(
        "Cannot create a process host after the current host was destroyed",
      );
      expect(globalHosts[processHostKey]).toBe(malformed);
    } finally {
      delete globalHosts[processHostKey];
    }
  });
});

describe("ProcessHost destruction", () => {
  test("publishes the shared destruction Promise before synchronous shutdown reentry", async () => {
    const shutdownFailure = new Error("shutdown failed after reentry");
    let reentrantDestruction: Promise<void> | undefined;
    let shutdownCalls = 0;
    let disposalCalls = 0;
    let host!: ProcessHost;
    host = destructionHost(
      () => {
        shutdownCalls += 1;
        reentrantDestruction = destroyProcessHost(host);
        return Promise.reject(shutdownFailure);
      },
      async () => {
        disposalCalls += 1;
      },
    );

    const destruction = destroyProcessHost(host);

    expect(reentrantDestruction).toBe(destruction);
    await expect(destruction).rejects.toBe(shutdownFailure);
    expect(shutdownCalls).toBe(1);
    expect(disposalCalls).toBe(1);
  });

  test("bounds orchestration shutdown before root disposal and observes late rejection", async () => {
    let rejectShutdown!: (error: Error) => void;
    const shutdown = new Promise<void>((_resolve, reject) => {
      rejectShutdown = reject;
    });
    let disposalCalls = 0;
    const host = destructionHost(
      () => shutdown,
      async () => {
        disposalCalls += 1;
      },
    );
    const deadline = deferred();
    const deadlineStarted = deferred();
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown) => unhandled.push(error);
    process.on("unhandledRejection", onUnhandled);
    try {
      const destruction = destroyProcessHost(host, {
        awaitShutdown: async (pending, graceMs) => {
          expect(pending).toBe(shutdown);
          expect(graceMs).toBe(SHUTDOWN_CLEANUP_GRACE_MS);
          deadlineStarted.resolve();
          await deadline.promise;
        },
      });
      await deadlineStarted.promise;
      expect(disposalCalls).toBe(0);

      deadline.resolve();
      await destruction;
      expect(disposalCalls).toBe(1);

      rejectShutdown(new Error("late shutdown failure"));
      await Promise.resolve();
      await Promise.resolve();
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  test("bounds root disposal and shares repeated destruction after logical teardown", async () => {
    const disposalGate = deferred();
    let disposalCalls = 0;
    const host = destructionHost(
      async () => {},
      () => {
        disposalCalls += 1;
        return disposalGate.promise;
      },
    );
    let observedGrace: number | undefined;
    const destruction = destroyProcessHost(host, {
      awaitRootDisposal: async (_disposal, graceMs) => {
        observedGrace = graceMs;
      },
    });

    await destruction;
    const repeated = destroyProcessHost(host);

    expect(repeated).toBe(destruction);
    expect(observedGrace).toBe(SHUTDOWN_CLEANUP_GRACE_MS);
    expect(disposalCalls).toBe(1);

    disposalGate.resolve();
    await repeated;
    expect(disposalCalls).toBe(1);
  });

  test("awaits an in-grace root rejection and observes a rejection after timeout", async () => {
    const immediateFailure = new Error("root disposal failed");
    const immediateHost = destructionHost(
      async () => {},
      () => Promise.reject(immediateFailure),
    );
    await expect(
      destroyProcessHost(immediateHost, {
        awaitRootDisposal: async (disposal) => disposal,
      }),
    ).rejects.toBe(immediateFailure);

    let rejectLate!: (error: Error) => void;
    const lateDisposal = new Promise<void>((_resolve, reject) => {
      rejectLate = reject;
    });
    const lateHost = destructionHost(async () => {}, () => lateDisposal);
    const lateFailure = new Error("late root disposal failed");
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown) => unhandled.push(error);
    process.on("unhandledRejection", onUnhandled);
    try {
      await destroyProcessHost(lateHost, {
        awaitRootDisposal: async () => {},
      });
      rejectLate(lateFailure);
      await Promise.resolve();
      await Promise.resolve();
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });
});
