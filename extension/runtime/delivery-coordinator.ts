import { sendWorkerNote, sendWorkerResult, type SendMessageFn, type WorkerPayload } from "../worker-messages.js";

export interface ActiveRuntimeBinding { sessionId: string; isIdle: () => boolean; sendMessage: SendMessageFn }
interface PendingMessage { ownerSessionId: string; payload: WorkerPayload; queuedAt: number }

export class DeliveryCoordinator {
  private binding: ActiveRuntimeBinding | undefined;
  private pendingMessages: PendingMessage[] = [];
  private flushScheduled = false;

  activateSession(binding: ActiveRuntimeBinding, countRunningForOwner: (ownerSessionId: string, excludeId: string) => number): void {
    this.binding = binding;
    if (this.pendingMessages.some((entry) => entry.ownerSessionId === binding.sessionId)) {
      this.flushScheduled = true;
      setTimeout(() => { this.flushScheduled = false; this.flushPending(countRunningForOwner); }, 0);
    }
  }
  deactivateSession(sessionId: string): void { if (this.binding?.sessionId === sessionId) this.binding = undefined; }
  deliver(ownerSessionId: string, payload: WorkerPayload, countRunningForOwner: (ownerSessionId: string, excludeId: string) => number): void {
    if (!this.binding || ownerSessionId !== this.binding.sessionId || this.flushScheduled) { this.pendingMessages.push({ ownerSessionId, payload, queuedAt: Date.now() }); return; }
    this.send(ownerSessionId, payload, countRunningForOwner);
  }
  private cleanStaleMessages(): void { const cutoff = Date.now() - 86_400_000; this.pendingMessages = this.pendingMessages.filter((entry) => entry.queuedAt >= cutoff); }
  private flushPending(countRunningForOwner: (ownerSessionId: string, excludeId: string) => number): void {
    if (!this.binding) return;
    this.cleanStaleMessages();
    const target = this.binding.sessionId;
    const deliver = this.pendingMessages.filter((entry) => entry.ownerSessionId === target);
    this.pendingMessages = this.pendingMessages.filter((entry) => entry.ownerSessionId !== target);
    for (const entry of deliver) this.send(entry.ownerSessionId, entry.payload, countRunningForOwner);
  }
  private send(ownerSessionId: string, payload: WorkerPayload, countRunningForOwner: (ownerSessionId: string, excludeId: string) => number): void {
    if (!this.binding || this.binding.sessionId !== ownerSessionId) { this.pendingMessages.push({ ownerSessionId, payload, queuedAt: Date.now() }); return; }
    const remaining = countRunningForOwner(ownerSessionId, payload.id);
    const isIdle = this.binding.isIdle();
    const triggerTurn = !(isIdle && remaining > 0);
    sendWorkerResult(payload, this.binding.sendMessage, { isIdle, triggerTurn });
    if (remaining > 0) sendWorkerNote(`⏳ ${remaining} worker(s) still running`, this.binding.sendMessage, { isIdle, triggerTurn: isIdle });
  }
}
