import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

export type WorkerStatus = "running" | "waiting" | "done" | "error" | "aborted";
export type SendMessageFn = ExtensionAPI["sendMessage"];
export const STATUS_ICON: Record<WorkerStatus, string> = { running: "⏳", waiting: "⏳", done: "✅", error: "❌", aborted: "⏹️" };
export const STATUS_LABEL: Record<WorkerStatus, string> = { running: "running", waiting: "waiting for response", done: "done", error: "failed", aborted: "aborted" };
export interface WorkerPayload { id: string; workerName: string; sessionFile?: string; status: WorkerStatus; result?: string; error?: string }
export interface WorkerResultMessageDetails { workerId: string; workerName: string; sessionFile?: string; status: WorkerStatus; body?: string }

function body(payload: WorkerPayload): string | undefined { return payload.status === "error" || payload.status === "aborted" ? (payload.error ?? payload.result) : (payload.result ?? payload.error); }
export function resultTitle(details: { workerId: string; workerName: string; status: WorkerStatus }): string { return `Worker '${details.workerName}' (${details.workerId}) ${STATUS_LABEL[details.status]}`; }
export function sendWorkerResult(payload: WorkerPayload, sendMessage: SendMessageFn, opts: { isIdle: boolean; triggerTurn: boolean }): void {
  const messageBody = body(payload);
  const title = resultTitle({ workerId: payload.id, workerName: payload.workerName, status: payload.status });
  sendMessage({ customType: "pi-workers-result", content: messageBody ? `**${STATUS_ICON[payload.status]} ${title}**\n\n${messageBody}` : `**${STATUS_ICON[payload.status]} ${title}**`, display: true, details: { workerId: payload.id, workerName: payload.workerName, sessionFile: payload.sessionFile, status: payload.status, body: messageBody } satisfies WorkerResultMessageDetails }, opts.isIdle ? { triggerTurn: opts.triggerTurn } : { deliverAs: "steer", triggerTurn: opts.triggerTurn });
}
export function sendWorkerNote(content: string, sendMessage: SendMessageFn, opts: { isIdle: boolean; triggerTurn: boolean }): void {
  sendMessage({ customType: "pi-workers-note", content, display: true }, opts.isIdle ? { triggerTurn: opts.triggerTurn } : { deliverAs: "steer", triggerTurn: opts.triggerTurn });
}
