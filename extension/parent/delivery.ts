import { Context, Effect, Layer } from "effect";
import { Orchestration } from "../orchestration/service.ts";
import type { WorkerSettlement } from "../orchestration/settlement.ts";

export const MAX_DELIVERY_MARKDOWN_BYTES = 50 * 1024;
export const MAX_WORKER_DELIVERY_MARKDOWN_BYTES = 16 * 1024;
export const DELIVERY_TRUNCATION_MARKER =
  "\n\n[Worker result truncated for parent context. Full output remains in structured details.]";
export const DELIVERY_PARENT_INSTRUCTIONS =
  "Parent: Synthesize all results, resolve conflicts, review changes and evidence, run integration checks, and continue the user's task. Do not merely forward worker reports.";

export type ParentBindingGeneration = string | number | symbol;
export type ScheduleIdleRecheck = (recheck: () => void) => () => void;

export interface WorkerDeliveryMessage {
  readonly customType: "pi-orchestrate-worker-result";
  readonly content: string;
  readonly display: true;
  readonly details: WorkerSettlement;
}

export interface WorkerDeliveryOptions {
  readonly triggerTurn: boolean;
}

export interface ParentBinding {
  readonly ownerSessionId: string;
  readonly generation: ParentBindingGeneration;
  isIdle(): boolean;
  sendMessage(message: WorkerDeliveryMessage, options: WorkerDeliveryOptions): void;
}

interface BoundParent {
  readonly binding: ParentBinding;
  agentRunning: boolean;
}

interface ScheduledIdleRecheck {
  readonly generation: ParentBindingGeneration;
  cancel(): void;
}

interface SynthesisGroupState {
  expected: number;
  readonly acceptedEventIds: string[];
  finalEventId?: string;
}

export interface DeliveryService {
  bind(binding: ParentBinding): void;
  unbind(ownerSessionId: string, generation: ParentBindingGeneration): void;
  markAgentStarted(ownerSessionId: string, generation: ParentBindingGeneration): void;
  markAgentSettled(ownerSessionId: string, generation: ParentBindingGeneration): void;
  accept(settlement: WorkerSettlement): boolean;
  skipSynthesisGroupMember(
    ownerSessionId: string,
    synthesisGroupId: string,
    synthesisGroupSize: number,
  ): void;
  pendingCount(ownerSessionId: string): number;
  clear(): void;
}

export class Delivery extends Context.Service<Delivery, DeliveryService>()(
  "@zachwill/pi-orchestrate/Delivery",
) {}

export class DeliveryCoordinator implements DeliveryService {
  private readonly boundParents = new Map<string, BoundParent>();
  private readonly pendingSettlements: WorkerSettlement[] = [];
  private readonly flushingOwners = new Set<string>();
  private readonly idleRechecks = new Map<string, ScheduledIdleRecheck>();
  private readonly synthesisGroups = new Map<string, SynthesisGroupState>();
  // Orchestration settlement sequences are process-scoped and monotonic across
  // owners, so one watermark is valid.
  private highestAcceptedSequence = 0;

  constructor(
    private readonly scheduleIdleRecheck: ScheduleIdleRecheck = scheduleDeliveryIdleRecheck,
  ) {}

  bind(binding: ParentBinding): void {
    this.cancelIdleRecheck(binding.ownerSessionId);
    this.boundParents.set(binding.ownerSessionId, {
      binding,
      // Non-idle also covers manual compaction. Agent lifecycle events, rather
      // than the broader idle flag, own this state.
      agentRunning: false,
    });
    this.flush(binding.ownerSessionId, binding.generation);
  }

  unbind(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    this.cancelIdleRecheck(ownerSessionId, generation);
    this.boundParents.delete(ownerSessionId);
  }

  markAgentStarted(ownerSessionId: string, generation: ParentBindingGeneration): void {
    const parent = this.boundParents.get(ownerSessionId);
    if (parent?.binding.generation !== generation) return;
    parent.agentRunning = true;
    this.cancelIdleRecheck(ownerSessionId, generation);
  }

  markAgentSettled(ownerSessionId: string, generation: ParentBindingGeneration): void {
    const parent = this.boundParents.get(ownerSessionId);
    if (parent?.binding.generation !== generation) return;
    parent.agentRunning = false;
    this.flush(ownerSessionId, generation);
  }

  accept(settlement: WorkerSettlement): boolean {
    if (settlement.mode === "inline" || settlement.sequence <= this.highestAcceptedSequence) {
      return false;
    }

    this.highestAcceptedSequence = settlement.sequence;
    this.pendingSettlements.push(settlement);
    this.acceptSynthesisGroupSettlement(settlement);

    const parent = this.boundParents.get(settlement.ownerSessionId);
    if (parent) this.flush(settlement.ownerSessionId, parent.binding.generation);
    return true;
  }

  skipSynthesisGroupMember(
    ownerSessionId: string,
    synthesisGroupId: string,
    synthesisGroupSize: number,
  ): void {
    const key = synthesisGroupKey(ownerSessionId, synthesisGroupId);
    const state = this.synthesisGroups.get(key) ?? {
      expected: synthesisGroupSize,
      acceptedEventIds: [],
    };
    state.expected = Math.max(0, state.expected - 1);
    this.synthesisGroups.set(key, state);
    this.refreshSynthesisGroupBoundary(key, state);
  }

  pendingCount(ownerSessionId: string): number {
    return this.pendingSettlements.filter(
      (settlement) => settlement.ownerSessionId === ownerSessionId,
    ).length;
  }

  clear(): void {
    for (const recheck of this.idleRechecks.values()) recheck.cancel();
    this.idleRechecks.clear();
    this.boundParents.clear();
    this.pendingSettlements.length = 0;
    this.flushingOwners.clear();
    this.synthesisGroups.clear();
    this.highestAcceptedSequence = 0;
  }

  private matchesBinding(
    ownerSessionId: string,
    generation: ParentBindingGeneration,
  ): boolean {
    return this.boundParents.get(ownerSessionId)?.binding.generation === generation;
  }

  private canDeliver(ownerSessionId: string, generation: ParentBindingGeneration): boolean {
    const parent = this.boundParents.get(ownerSessionId);
    if (
      parent === undefined ||
      parent.binding.generation !== generation ||
      parent.agentRunning
    ) {
      return false;
    }
    if (!parent.binding.isIdle()) {
      this.ensureIdleRecheck(ownerSessionId, generation);
      return false;
    }
    this.cancelIdleRecheck(ownerSessionId, generation);
    return true;
  }

  private flush(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (this.flushingOwners.has(ownerSessionId)) return;
    if (this.pendingCount(ownerSessionId) === 0) {
      this.cancelIdleRecheck(ownerSessionId, generation);
      return;
    }
    if (!this.canDeliver(ownerSessionId, generation)) return;

    this.flushingOwners.add(ownerSessionId);
    try {
      // Deliver a stable owner-ordered prefix, stopping at the latest complete synthesis boundary.
      const queued = this.pendingSettlements.filter(
        (settlement) => settlement.ownerSessionId === ownerSessionId,
      );
      let latestFinalIndex = -1;
      for (const [index, settlement] of queued.entries()) {
        if (this.isFinalBoundary(settlement)) latestFinalIndex = index;
      }
      const flushThrough = latestFinalIndex >= 0 ? latestFinalIndex : queued.length - 1;
      let flushBytesRemaining = MAX_DELIVERY_MARKDOWN_BYTES;

      for (const [index, settlement] of queued.entries()) {
        if (index > flushThrough) break;
        // Synchronous delivery callbacks can change owner, generation, or idle
        // state before the next send.
        if (!this.canDeliver(ownerSessionId, generation)) return;
        if (!this.pendingSettlements.includes(settlement)) continue;

        const messagesRemaining = flushThrough - index + 1;
        const fairFlushBytes = Math.floor(flushBytesRemaining / messagesRemaining);
        const byteLimit = Math.max(0, Math.min(
          MAX_WORKER_DELIVERY_MARKDOWN_BYTES,
          fairFlushBytes,
          flushBytesRemaining,
        ));
        // Intermediate results add context; only the completed boundary
        // transfers work to a parent turn.
        const triggerTurn = latestFinalIndex >= 0 && index === flushThrough;
        const message = this.renderWorkerMessage(settlement, byteLimit);
        const parent = this.boundParents.get(ownerSessionId);
        if (!parent || parent.binding.generation !== generation) return;

        try {
          parent.binding.sendMessage(message, { triggerTurn });
        } catch {
          // Keep this settlement and the remaining prefix queued for a later retry.
          return;
        }

        const deliveredBytes = Buffer.byteLength(message.content, "utf8");
        flushBytesRemaining = Math.max(0, flushBytesRemaining - deliveredBytes);
        const pendingIndex = this.pendingSettlements.indexOf(settlement);
        if (pendingIndex >= 0) this.pendingSettlements.splice(pendingIndex, 1);

        if (triggerTurn) {
          this.finishSynthesisGroup(settlement);
          parent.agentRunning = true;
          return;
        }
      }
    } finally {
      this.flushingOwners.delete(ownerSessionId);
    }
  }

  private ensureIdleRecheck(
    ownerSessionId: string,
    generation: ParentBindingGeneration,
  ): void {
    const current = this.idleRechecks.get(ownerSessionId);
    if (current?.generation === generation) return;
    current?.cancel();

    const scheduled: ScheduledIdleRecheck = {
      generation,
      cancel: () => {},
    };
    this.idleRechecks.set(ownerSessionId, scheduled);
    scheduled.cancel = this.scheduleIdleRecheck(() => {
      if (this.idleRechecks.get(ownerSessionId) !== scheduled) return;
      this.idleRechecks.delete(ownerSessionId);
      this.flush(ownerSessionId, generation);
    });
  }

  private cancelIdleRecheck(
    ownerSessionId: string,
    generation?: ParentBindingGeneration,
  ): void {
    const scheduled = this.idleRechecks.get(ownerSessionId);
    if (!scheduled || (generation !== undefined && scheduled.generation !== generation)) return;
    this.idleRechecks.delete(ownerSessionId);
    scheduled.cancel();
  }

  private renderWorkerMessage(
    settlement: WorkerSettlement,
    byteLimit: number,
  ): WorkerDeliveryMessage {
    const heading = `## Worker result — ${settlement.title} · ${settlement.worker}`;
    const disposition = renderDisposition(settlement);
    const metadata = [
      `Worker \`${settlement.workerId}\``,
      `run \`${settlement.runId}\``,
      `status \`${settlement.status}\``,
      ...(disposition ? [disposition] : []),
    ].join(" · ");
    const body = renderOutcome(settlement.outcome);
    const content = body.length > 0
      ? `${heading}\n\n${metadata}\n\n${body}`
      : `${heading}\n\n${metadata}`;
    const appendix = this.isFinalBoundary(settlement)
      ? `\n\n---\n\n${DELIVERY_PARENT_INSTRUCTIONS}`
      : "";

    return Object.freeze({
      customType: "pi-orchestrate-worker-result",
      content: capMarkdown(content, appendix, byteLimit),
      display: true,
      details: settlement,
    });
  }

  private acceptSynthesisGroupSettlement(settlement: WorkerSettlement): void {
    if (!settlement.synthesisGroupId || !settlement.synthesisGroupSize) return;
    const key = synthesisGroupKey(settlement.ownerSessionId, settlement.synthesisGroupId);
    const state = this.synthesisGroups.get(key) ?? {
      expected: settlement.synthesisGroupSize,
      acceptedEventIds: [],
    };
    state.acceptedEventIds.push(settlement.eventId);
    this.synthesisGroups.set(key, state);
    this.refreshSynthesisGroupBoundary(key, state);
  }

  private refreshSynthesisGroupBoundary(key: string, state: SynthesisGroupState): void {
    if (state.expected === 0) {
      this.synthesisGroups.delete(key);
      return;
    }
    if (state.acceptedEventIds.length !== state.expected) return;
    const finalEventId = state.acceptedEventIds.at(-1);
    if (finalEventId) state.finalEventId = finalEventId;
  }

  private isFinalBoundary(settlement: WorkerSettlement): boolean {
    if (!settlement.synthesisGroupId) return true;
    const state = this.synthesisGroups.get(
      synthesisGroupKey(settlement.ownerSessionId, settlement.synthesisGroupId),
    );
    return state?.finalEventId === settlement.eventId;
  }

  private finishSynthesisGroup(settlement: WorkerSettlement): void {
    if (!settlement.synthesisGroupId) return;
    this.synthesisGroups.delete(
      synthesisGroupKey(settlement.ownerSessionId, settlement.synthesisGroupId),
    );
  }
}

/** Process-scoped direct delivery state and its Orchestration settlement subscription. */
export const deliveryLayer: Layer.Layer<Delivery, never, Orchestration> = Layer.effect(
  Delivery,
  Effect.gen(function* () {
    const orchestration = yield* Orchestration;
    const coordinator = new DeliveryCoordinator();

    // Registered first so subscription release runs before delivery state is cleared.
    yield* Effect.addFinalizer(() => Effect.sync(() => coordinator.clear()));
    yield* Effect.acquireRelease(
      Effect.sync(() => orchestration.subscribeSettlement((settlement) => {
        coordinator.accept(settlement);
      })),
      (unsubscribe) => Effect.sync(unsubscribe),
    );

    return Delivery.of(coordinator);
  }),
);

function scheduleDeliveryIdleRecheck(recheck: () => void): () => void {
  const timeout = setTimeout(recheck, 100);
  timeout.unref();
  return () => clearTimeout(timeout);
}

function synthesisGroupKey(ownerSessionId: string, synthesisGroupId: string): string {
  return `${ownerSessionId}\u0000${synthesisGroupId}`;
}

function capMarkdown(content: string, appendix: string, byteLimit: number): string {
  const complete = `${content}${appendix}`;
  if (Buffer.byteLength(complete, "utf8") <= byteLimit) return complete;

  const suffix = `${DELIVERY_TRUNCATION_MARKER}${appendix}`;
  const suffixBytes = Buffer.byteLength(suffix, "utf8");
  if (suffixBytes >= byteLimit) return truncateUtf8(suffix, byteLimit);
  const prefix = truncateUtf8(content, byteLimit - suffixBytes);
  return `${prefix}${suffix}`;
}

function truncateUtf8(content: string, byteLimit: number): string {
  if (byteLimit <= 0) return "";
  const bytes = Buffer.from(content, "utf8");
  if (bytes.byteLength <= byteLimit) return content;
  let end = byteLimit;
  while (end > 0) {
    const byte = bytes[end];
    if (byte === undefined || (byte & 0xc0) !== 0x80) break;
    end -= 1;
  }
  return bytes.subarray(0, end).toString("utf8");
}

function renderDisposition(settlement: WorkerSettlement): string | undefined {
  if (settlement.status === "ready" && settlement.lifecycle === "interactive") {
    return "interactive session retained; use `interactive_send` or `interactive_close`";
  }
  return undefined;
}

function renderOutcome(outcome: WorkerSettlement["outcome"]): string {
  switch (outcome.status) {
    case "completed":
    case "ready":
      return outcome.assistantText;
    case "failed":
      return outcome.assistantText
        ? `Failed: ${outcome.message}\n\n${outcome.assistantText}`
        : `Failed: ${outcome.message}`;
    case "aborted": {
      const reason = outcome.message ? `Aborted: ${outcome.message}` : "Aborted";
      return outcome.assistantText ? `${reason}\n\n${outcome.assistantText}` : reason;
    }
  }
}
