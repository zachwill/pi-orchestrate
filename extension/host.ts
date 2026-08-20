import { Cause, Effect, Exit, Layer, ManagedRuntime } from "effect";
import { DeliveryCoordinator } from "./delivery.js";
import {
  Orchestration,
  orchestrationLayer,
  type AbortTarget,
  type AcceptedRun,
  type CompletedRun,
  type OrchestrationContext,
  type OrchestrationService,
  SHUTDOWN_CLEANUP_GRACE_MS,
  type RuntimeSnapshot,
  type SettlementListener,
  type UnsubscribeSettlement,
  type StateListener,
} from "./runtime.js";
import { processSupervisorLayer } from "./scheduler.js";
import { createChildSessionsLayer } from "./worker-session.js";
import type { OrchestrateTaskInput, RunMode } from "./domain.js";

const PROCESS_HOST_KEY = Symbol.for("@zachwill/pi-orchestrate/process-host/v3");

export interface OrchestratorRuntime {
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  abort(ownerSessionId: string, target: AbortTarget): Promise<void>;
  closeInteractive(ownerSessionId: string, workerId: string): Promise<void>;
  snapshot(ownerSessionId: string): Promise<RuntimeSnapshot>;
  subscribeSettlement(listener: SettlementListener): UnsubscribeSettlement;
  subscribeState(listener: StateListener): () => void;
  shutdown(): Promise<void>;
}

export interface ProcessHost {
  readonly runtime: OrchestratorRuntime;
  readonly delivery: DeliveryCoordinator;
}

export interface ProcessHostAttachment {
  readonly host: ProcessHost;
}

export interface ProcessHostDestructionOptions {
  /** Process-boundary deadline capability; production uses the orchestration cleanup grace. */
  readonly awaitShutdown?: (
    shutdown: Promise<void>,
    graceMs: number,
  ) => Promise<void>;
  /** Process-boundary deadline capability; production uses the orchestration cleanup grace. */
  readonly awaitRootDisposal?: (
    disposal: Promise<void>,
    graceMs: number,
  ) => Promise<void>;
}

interface AttachmentAwareProcessHost extends ProcessHost {
  attachments?: Set<ProcessHostAttachment>;
}

type ProcessHostLifecycle = "active" | "destroying" | "destroyed";

interface OwnedProcessHost extends AttachmentAwareProcessHost {
  readonly effectRuntime?: ManagedRuntime.ManagedRuntime<Orchestration, never>;
  readonly unsubscribeSettlement?: () => void;
  lifecycle?: ProcessHostLifecycle;
  destroyPromise?: Promise<void>;
}

type ProcessGlobal = typeof globalThis & {
  [PROCESS_HOST_KEY]?: OwnedProcessHost;
};

function processGlobal(): ProcessGlobal {
  return globalThis as ProcessGlobal;
}

export function getProcessHost(): ProcessHost | undefined {
  return processGlobal()[PROCESS_HOST_KEY];
}

/** Pi-facing adapter. Every Promise operation executes one complete Orchestration Effect. */
export class ProcessHostRuntimeAdapter<R = never> implements OrchestratorRuntime {
  constructor(
    private readonly effectRuntime: ManagedRuntime.ManagedRuntime<Orchestration | R, never>,
    private readonly orchestration: OrchestrationService,
  ) {}

  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  orchestrate(
    context: OrchestrationContext,
    task: OrchestrateTaskInput,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    return this.run(
      this.orchestration.orchestrate(context, task, mode, onSettlement),
      signal,
    );
  }

  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "async",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<CompletedRun>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun>;
  sendInteractive(
    context: OrchestrationContext,
    workerId: string,
    instructions: string,
    mode: RunMode,
    signal?: AbortSignal,
    onSettlement?: SettlementListener,
  ): Promise<AcceptedRun | CompletedRun> {
    return this.run(
      this.orchestration.sendInteractive(
        context,
        workerId,
        instructions,
        mode,
        onSettlement,
      ),
      signal,
    );
  }

  abort(ownerSessionId: string, target: AbortTarget): Promise<void> {
    return this.run(this.orchestration.abort(ownerSessionId, target));
  }

  closeInteractive(ownerSessionId: string, workerId: string): Promise<void> {
    return this.run(this.orchestration.closeInteractive(ownerSessionId, workerId));
  }

  snapshot(ownerSessionId: string): Promise<RuntimeSnapshot> {
    // Snapshot is dependency-free and remains readable from a retained host reference
    // after the process root has been disposed.
    return Effect.runPromise(this.orchestration.snapshot(ownerSessionId));
  }

  subscribeSettlement(listener: SettlementListener): UnsubscribeSettlement {
    return this.orchestration.subscribeSettlement(listener);
  }

  subscribeState(listener: StateListener): () => void {
    return this.orchestration.subscribeState(listener);
  }

  shutdown(): Promise<void> {
    return this.run(this.orchestration.shutdown());
  }

  private async run<A, E>(
    effect: Effect.Effect<A, E>,
    signal?: AbortSignal,
  ): Promise<A> {
    if (signal?.aborted) throw abortSignalReason(signal);
    if (!signal) return this.effectRuntime.runPromise(effect);

    const signalInterruption = {};
    const exit = await this.effectRuntime.runPromiseExit(
      Effect.raceFirst(effect, abortSignalEffect(signal, signalInterruption)),
    );
    if (Exit.isSuccess(exit)) return exit.value;

    const error = Cause.squash(exit.cause);
    if (error === signalInterruption) throw abortSignalReason(signal);
    throw error;
  }
}

export function createProcessHostRuntimeAdapter<R>(
  effectRuntime: ManagedRuntime.ManagedRuntime<Orchestration | R, never>,
): OrchestratorRuntime {
  // Orchestration acquisition is synchronous; subscriptions must remain reentrant.
  const orchestration = effectRuntime.runSync(Orchestration);
  return new ProcessHostRuntimeAdapter(effectRuntime, orchestration);
}

export function createProcessHost(): ProcessHost {
  const global = processGlobal();
  const existing = global[PROCESS_HOST_KEY];
  if (existing?.lifecycle === "destroying") {
    throw new Error("Cannot create a process host while the current host is being destroyed");
  }
  if (existing?.lifecycle === "destroyed") {
    delete global[PROCESS_HOST_KEY];
  } else if (existing) {
    existing.lifecycle = "active";
    return existing;
  }

  const infrastructureLayer = Layer.merge(
    processSupervisorLayer,
    createChildSessionsLayer(),
  );
  const rootLayer = orchestrationLayer().pipe(Layer.provide(infrastructureLayer));
  const effectRuntime = ManagedRuntime.make(rootLayer);
  const runtime = createProcessHostRuntimeAdapter(effectRuntime);
  const delivery = new DeliveryCoordinator();
  const host: OwnedProcessHost = {
    runtime,
    delivery,
    effectRuntime,
    attachments: new Set(),
    lifecycle: "active",
    unsubscribeSettlement: runtime.subscribeSettlement((settlement) => {
      delivery.accept(settlement);
    }),
  };
  global[PROCESS_HOST_KEY] = host;
  return host;
}

export function attachProcessHost(host: ProcessHost): ProcessHostAttachment {
  const ownedHost = host as OwnedProcessHost;
  if (ownedHost.lifecycle === "destroying" || ownedHost.lifecycle === "destroyed") {
    throw new Error(`Cannot attach to a ${ownedHost.lifecycle} process host`);
  }

  const attachment: ProcessHostAttachment = { host };
  ownedHost.attachments ??= new Set();
  ownedHost.attachments.add(attachment);
  return attachment;
}

export function detachProcessHost(
  host: ProcessHost,
  attachment: ProcessHostAttachment,
): boolean {
  if (attachment.host !== host) return false;

  const attachments = (host as AttachmentAwareProcessHost).attachments;
  if (!attachments?.delete(attachment)) return false;
  return attachments.size === 0;
}

export function destroyProcessHost(
  host: ProcessHost,
  options: ProcessHostDestructionOptions = {},
): Promise<void> {
  const ownedHost = host as OwnedProcessHost;
  if (ownedHost.destroyPromise) return ownedHost.destroyPromise;
  if ((ownedHost.attachments?.size ?? 0) > 0) return Promise.resolve();
  if (ownedHost.lifecycle === "destroyed") return Promise.resolve();

  ownedHost.lifecycle = "destroying";
  let resolveDestruction!: () => void;
  let rejectDestruction!: (error: unknown) => void;
  const destroyPromise = new Promise<void>((resolve, reject) => {
    resolveDestruction = resolve;
    rejectDestruction = reject;
  });
  // Publish the exact shared result before shutdown can synchronously reenter destruction.
  ownedHost.destroyPromise = destroyPromise;

  let shutdown: Promise<void>;
  try {
    // shutdown() closes Orchestration admission before returning its bounded teardown Promise.
    shutdown = ownedHost.runtime.shutdown();
  } catch (error) {
    shutdown = Promise.reject(error);
  }
  const awaitShutdown =
    options.awaitShutdown ?? awaitPromiseWithinCleanupGrace;
  const awaitRootDisposal =
    options.awaitRootDisposal ?? awaitPromiseWithinCleanupGrace;
  const teardown = (async () => {
    try {
      // A timed-out shutdown keeps physical best-effort finalizers alive. Observe
      // either late outcome while proceeding to the separately bounded root disposal.
      void shutdown.catch(() => {});
      await awaitShutdown(shutdown, SHUTDOWN_CLEANUP_GRACE_MS);
    } finally {
      try {
        const disposal = ownedHost.effectRuntime?.dispose();
        if (disposal) {
          // ManagedRuntime disposal keeps ownership of uninterruptible finalizers after timeout.
          // Observe its eventual rejection while host destruction proceeds best-effort.
          void disposal.catch(() => {});
          await awaitRootDisposal(disposal, SHUTDOWN_CLEANUP_GRACE_MS);
        }
      } finally {
        // After either deadline the host is logically destroyed and detached even
        // though abandoned physical finalizers may still settle. A replacement may
        // overlap only that cleanup; identity guards keep stale completion harmless.
        ownedHost.delivery.clear();
        ownedHost.unsubscribeSettlement?.();
        ownedHost.lifecycle = "destroyed";
        const global = processGlobal();
        if (global[PROCESS_HOST_KEY] === ownedHost) {
          delete global[PROCESS_HOST_KEY];
        }
      }
    }
  })();
  void teardown.then(resolveDestruction, rejectDestruction);
  return destroyPromise;
}

export async function quitProcessHost(): Promise<void> {
  const host = getProcessHost();
  if (!host) return;
  await destroyProcessHost(host);
}

function awaitPromiseWithinCleanupGrace(
  promise: Promise<void>,
  graceMs: number,
): Promise<void> {
  return Effect.runPromise(
    Effect.promise(() => promise).pipe(
      Effect.timeoutOption(graceMs),
      Effect.asVoid,
    ),
  );
}

function abortSignalEffect(
  signal: AbortSignal,
  interruption: object,
): Effect.Effect<never, object> {
  return Effect.callback((resume) => {
    const onAbort = () => resume(Effect.fail(interruption));
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) {
      signal.removeEventListener("abort", onAbort);
      onAbort();
    }
    return Effect.sync(() => signal.removeEventListener("abort", onAbort));
  });
}

function abortSignalReason(signal: AbortSignal): unknown {
  if ("reason" in signal) return signal.reason;
  return new DOMException("This operation was aborted", "AbortError");
}
