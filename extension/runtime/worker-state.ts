import { randomBytes } from "node:crypto";
import type { AgentSession } from "@mariozechner/pi-coding-agent";
import type { WorkerConfig } from "../worker-discovery.js";
import type { WorkerStatus } from "../worker-messages.js";

export interface WorkerUsageStats { input: number; output: number; cacheRead: number; cacheWrite: number; cost: number; contextTokens: number; turns: number }
export interface WorkerState { id: string; workerConfig: WorkerConfig; task: string; status: WorkerStatus; ownerSessionId: string; session: AgentSession | null; turns: number; contextTokens: number; model: string | undefined; usage: WorkerUsageStats; error?: string; result?: string; promptAbortController?: AbortController; unsubscribe?: () => void }
export interface ActiveWorkerSummary { id: string; workerName: string; status: WorkerStatus; turns: number; contextTokens: number; model: string | undefined; usage: WorkerUsageStats }
export function createEmptyUsageStats(): WorkerUsageStats { return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, contextTokens: 0, turns: 0 }; }
export function generateId(name: string, existingIds: Set<string>): string { for (let i = 0; i < 10; i++) { const id = `${name}-${randomBytes(4).toString("hex")}`; if (!existingIds.has(id)) return id; } return `${name}-${randomBytes(8).toString("hex")}`; }
export function isAbortableStatus(status: WorkerStatus): boolean { return status === "running" || status === "waiting"; }
export function isAborted(state: WorkerState): boolean { return state.status === "aborted"; }
export function buildActiveWorkerSummary(state: WorkerState): ActiveWorkerSummary { return { id: state.id, workerName: state.workerConfig.name, status: state.status, turns: state.turns, contextTokens: state.contextTokens, model: state.model, usage: { ...state.usage } }; }
