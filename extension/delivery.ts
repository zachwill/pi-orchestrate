import type { WorkerSettlement } from "./runtime.js";

export const MAX_DELIVERY_MARKDOWN_BYTES = 50 * 1024;
export const MAX_WORKER_DELIVERY_MARKDOWN_BYTES = 16 * 1024;
export const DELIVERY_TRUNCATION_MARKER =
  "\n\n[Worker result truncated for parent context. Full output remains in structured details.]";
export const DELIVERY_PARENT_INSTRUCTIONS =
  "Parent: Synthesize all results, resolve conflicts, review changes and evidence, run integration checks, and continue the user's task. Do not merely forward worker reports.";

export type ParentBindingGeneration = string | number | symbol;

/** Complete, immutable worker output for presentation and history consumers. */
export type WorkerDeliveryDetails = WorkerSettlement;

export interface WorkerDeliveryMessage {
  readonly customType: "pi-orchestrate-worker-result";
  readonly content: string;
  readonly display: true;
  readonly details: WorkerDeliveryDetails;
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

export class DeliveryCoordinator {
  private readonly boundParents = new Map<string, BoundParent>();
  private readonly pendingSettlements: WorkerSettlement[] = [];
  private readonly flushingOwners = new Set<string>();
  private highestAcceptedSequence = 0;

  bind(binding: ParentBinding): void {
    this.boundParents.set(binding.ownerSessionId, {
      binding,
      agentRunning: !binding.isIdle(),
    });
    this.flush(binding.ownerSessionId, binding.generation);
  }

  unbind(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    this.boundParents.delete(ownerSessionId);
  }

  markAgentStarted(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    const parent = this.boundParents.get(ownerSessionId);
    if (parent) parent.agentRunning = true;
  }

  markAgentSettled(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    const parent = this.boundParents.get(ownerSessionId);
    if (!parent) return;
    parent.agentRunning = false;
    this.flush(ownerSessionId, generation);
  }

  accept(settlement: WorkerSettlement): boolean {
    if (settlement.mode === "inline" || settlement.sequence <= this.highestAcceptedSequence) {
      return false;
    }

    this.highestAcceptedSequence = settlement.sequence;
    this.pendingSettlements.push(settlement);

    const parent = this.boundParents.get(settlement.ownerSessionId);
    if (parent) this.flush(settlement.ownerSessionId, parent.binding.generation);
    return true;
  }

  pendingCount(ownerSessionId: string): number {
    return this.pendingSettlements.filter(
      (settlement) => settlement.ownerSessionId === ownerSessionId,
    ).length;
  }

  clear(): void {
    this.boundParents.clear();
    this.pendingSettlements.length = 0;
    this.flushingOwners.clear();
    this.highestAcceptedSequence = 0;
  }

  close(): void {
    this.clear();
  }

  private matchesBinding(
    ownerSessionId: string,
    generation: ParentBindingGeneration,
  ): boolean {
    return this.boundParents.get(ownerSessionId)?.binding.generation === generation;
  }

  private canDeliver(ownerSessionId: string, generation: ParentBindingGeneration): boolean {
    const parent = this.boundParents.get(ownerSessionId);
    return (
      parent !== undefined &&
      parent.binding.generation === generation &&
      !parent.agentRunning &&
      parent.binding.isIdle()
    );
  }

  private flush(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (this.flushingOwners.has(ownerSessionId) || !this.canDeliver(ownerSessionId, generation)) {
      return;
    }

    this.flushingOwners.add(ownerSessionId);
    try {
      const queued = this.pendingSettlements.filter(
        (settlement) => settlement.ownerSessionId === ownerSessionId,
      );
      let latestFinalIndex = -1;
      for (let index = 0; index < queued.length; index += 1) {
        if (queued[index]?.waveComplete) latestFinalIndex = index;
      }
      const flushThrough = latestFinalIndex >= 0 ? latestFinalIndex : queued.length - 1;
      let flushBytesRemaining = MAX_DELIVERY_MARKDOWN_BYTES;

      for (let index = 0; index <= flushThrough; index += 1) {
        if (!this.canDeliver(ownerSessionId, generation)) return;
        const settlement = queued[index];
        if (!settlement || !this.pendingSettlements.includes(settlement)) continue;

        const messagesRemaining = flushThrough - index + 1;
        const fairFlushBytes = Math.floor(flushBytesRemaining / messagesRemaining);
        const fairWaveBytes = Math.floor(
          MAX_DELIVERY_MARKDOWN_BYTES / Math.max(1, settlement.waveSize),
        );
        const byteLimit = Math.max(0, Math.min(
          MAX_WORKER_DELIVERY_MARKDOWN_BYTES,
          fairFlushBytes,
          fairWaveBytes,
          flushBytesRemaining,
        ));
        const triggerTurn = latestFinalIndex >= 0 && index === flushThrough;
        const message = this.renderWorkerMessage(settlement, byteLimit);
        const parent = this.boundParents.get(ownerSessionId);
        if (!parent || parent.binding.generation !== generation) return;

        try {
          parent.binding.sendMessage(message, { triggerTurn });
        } catch {
          return;
        }

        const deliveredBytes = Buffer.byteLength(message.content, "utf8");
        flushBytesRemaining = Math.max(0, flushBytesRemaining - deliveredBytes);
        const pendingIndex = this.pendingSettlements.indexOf(settlement);
        if (pendingIndex >= 0) this.pendingSettlements.splice(pendingIndex, 1);

        if (triggerTurn) {
          parent.agentRunning = true;
          return;
        }
      }
    } finally {
      this.flushingOwners.delete(ownerSessionId);
    }
  }

  private renderWorkerMessage(
    settlement: WorkerSettlement,
    byteLimit: number,
  ): WorkerDeliveryMessage {
    const heading = `## Worker result — ${settlement.worker}`;
    const metadata = [
      `Worker \`${settlement.workerId}\``,
      `wave \`${settlement.waveId}\``,
      `status \`${settlement.status}\``,
    ].join(" · ");
    const body = renderOutcome(settlement.outcome);
    const content = body.length > 0
      ? `${heading}\n\n### ${settlement.title}\n${metadata}\n\n${body}`
      : `${heading}\n\n### ${settlement.title}\n${metadata}`;
    const appendix = settlement.waveComplete
      ? `\n\n---\n\n${DELIVERY_PARENT_INSTRUCTIONS}`
      : "";

    return Object.freeze({
      customType: "pi-orchestrate-worker-result",
      content: capMarkdown(content, appendix, byteLimit),
      display: true,
      details: settlement,
    });
  }
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
    case "closed":
      return "Closed";
  }
}
