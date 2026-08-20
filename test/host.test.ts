import { describe, expect, test } from "bun:test";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Deferred, Effect, Layer, ManagedRuntime } from "effect";
import { DeliveryCoordinator } from "../extension/delivery.js";
import { createWorkerCatalog, createSequentialWorkerIdFactory, type OrchestrateTaskInput, type RunMode } from "../extension/domain.js";
import {
  destroyProcessHost,
  ProcessHostRuntimeAdapter,
  type ProcessHost,
} from "../extension/host.js";
import {
  Orchestration,
  OrchestrationActionRejected,
  SHUTDOWN_CLEANUP_GRACE_MS,
} from "../extension/runtime.js";
import { CleanupSupervisor, processSupervisorLayer } from "../extension/scheduler.js";
import type {
  AcceptedRun,
  CompletedRun,
  OrchestrationContext,
  OrchestrationService,
  SettlementListener,
} from "../extension/runtime.js";

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
  readonly subscribeSettlement = () => () => {};
  readonly subscribeState = () => () => {};
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
  const adapter = new ProcessHostRuntimeAdapter(effectRuntime, orchestration);
  return { adapter, effectRuntime, orchestration };
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

describe("ProcessHost destruction", () => {
  test("publishes the shared destruction Promise before synchronous shutdown reentry", async () => {
    const shutdownFailure = new Error("shutdown failed after reentry");
    let reentrantDestruction: Promise<void> | undefined;
    let shutdownCalls = 0;
    let disposalCalls = 0;
    let host!: ProcessHost;
    host = Object.assign(
      {
        runtime: {
          shutdown: () => {
            shutdownCalls += 1;
            reentrantDestruction = destroyProcessHost(host);
            return Promise.reject(shutdownFailure);
          },
        } as unknown as ProcessHost["runtime"],
        delivery: new DeliveryCoordinator(),
      } satisfies ProcessHost,
      {
        effectRuntime: {
          dispose: async () => {
            disposalCalls += 1;
          },
        },
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
    const host = Object.assign(
      {
        runtime: { shutdown: () => shutdown } as unknown as ProcessHost["runtime"],
        delivery: new DeliveryCoordinator(),
      } satisfies ProcessHost,
      {
        effectRuntime: {
          dispose: async () => {
            disposalCalls += 1;
          },
        },
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

  test("bounds root disposal while a real supervised uninterruptible cleanup remains gated", async () => {
    const orchestration = new TestOrchestration();
    const effectRuntime = ManagedRuntime.make(
      Layer.merge(
        Layer.succeed(Orchestration, orchestration),
        processSupervisorLayer,
      ),
    );
    const runtime = new ProcessHostRuntimeAdapter(effectRuntime, orchestration);
    const delivery = new DeliveryCoordinator();
    const host = Object.assign(
      { runtime, delivery } satisfies ProcessHost,
      { effectRuntime },
    );
    const cleanup = effectRuntime.runSync(CleanupSupervisor);
    const started = Deferred.makeUnsafe<void>();
    const release = Deferred.makeUnsafe<void>();
    let finalized = false;

    cleanup.supervise(
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.uninterruptible,
        Effect.ensuring(Effect.sync(() => {
          finalized = true;
        })),
      ),
    );
    await Effect.runPromise(Deferred.await(started));

    const deadline = deferred();
    let rootDisposal: Promise<void> | undefined;
    let observedGrace: number | undefined;
    const destruction = destroyProcessHost(host, {
      awaitRootDisposal: async (disposal, graceMs) => {
        rootDisposal = disposal;
        observedGrace = graceMs;
        await Promise.race([disposal, deadline.promise]);
      },
    });
    await Promise.resolve();
    deadline.resolve();
    await destruction;

    expect(observedGrace).toBe(SHUTDOWN_CLEANUP_GRACE_MS);
    expect(finalized).toBe(false);
    expect(rootDisposal).toBeDefined();

    Deferred.doneUnsafe(release, Effect.void);
    await rootDisposal;
    expect(finalized).toBe(true);
  });

  test("awaits an in-grace root rejection and observes a rejection after timeout", async () => {
    const immediateFailure = new Error("root disposal failed");
    const immediateHost = Object.assign(
      {
        runtime: { shutdown: async () => {} } as unknown as ProcessHost["runtime"],
        delivery: new DeliveryCoordinator(),
      } satisfies ProcessHost,
      { effectRuntime: { dispose: () => Promise.reject(immediateFailure) } },
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
    const lateHost = Object.assign(
      {
        runtime: { shutdown: async () => {} } as unknown as ProcessHost["runtime"],
        delivery: new DeliveryCoordinator(),
      } satisfies ProcessHost,
      { effectRuntime: { dispose: () => lateDisposal } },
    );
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
