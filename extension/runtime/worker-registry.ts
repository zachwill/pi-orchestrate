import type { WorkerConfig } from "../worker-discovery.js";
import type { ActiveWorkerSummary, WorkerState } from "./worker-state.js";
import { buildActiveWorkerSummary, createEmptyUsageStats, generateId, isAbortableStatus } from "./worker-state.js";

export class WorkerRegistry {
  private activeWorkers = new Map<string, WorkerState>();
  create(workerConfig: WorkerConfig, task: string, ownerSessionId: string): WorkerState {
    const id = generateId(workerConfig.name, new Set(this.activeWorkers.keys()));
    const state: WorkerState = { id, workerConfig, task, status: "running", ownerSessionId, session: null, turns: 0, contextTokens: 0, model: undefined, usage: createEmptyUsageStats() };
    this.activeWorkers.set(id, state);
    return state;
  }
  get(id: string): WorkerState | undefined { return this.activeWorkers.get(id); }
  hasState(state: WorkerState): boolean { return this.activeWorkers.get(state.id) === state; }
  delete(id: string): void { this.activeWorkers.delete(id); }
  countRunningForOwner(ownerSessionId: string, excludeId: string): number { return Array.from(this.activeWorkers.values()).filter((state) => state.id !== excludeId && state.ownerSessionId === ownerSessionId && state.status === "running").length; }
  getActiveSummariesForOwner(ownerSessionId: string): ActiveWorkerSummary[] { return Array.from(this.activeWorkers.values()).filter((state) => state.ownerSessionId === ownerSessionId && isAbortableStatus(state.status)).map(buildActiveWorkerSummary); }
  getOwnedAbortableIds(ownerSessionId: string): string[] { return Array.from(this.activeWorkers.values()).filter((state) => state.ownerSessionId === ownerSessionId && isAbortableStatus(state.status)).map((state) => state.id); }
  getAllAbortable(): WorkerState[] { return Array.from(this.activeWorkers.values()).filter((state) => isAbortableStatus(state.status)); }
}
