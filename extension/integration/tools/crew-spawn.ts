import { getAgentDir } from "@mariozechner/pi-coding-agent";
import { Type } from "typebox";
import { discoverWorkers } from "../../worker-discovery.js";
import { renderCrewCall, renderCrewResult, toolError, toolSuccess } from "../tool-presentation.js";
import type { CrewToolDeps } from "./tool-deps.js";

export function scoutScopeAdvisory(task: string): string | undefined {
  const text = task.toLowerCase();
  const broadIndicators = ["synthesis", "synthesize", "review", "audit", "planning", "plan", "strategy", "architecture", "cross-file", "across files", "broad"];
  if (!broadIndicators.some((indicator) => text.includes(indicator))) return undefined;
  return "⚠ Scout advisory: scouts are for narrow discovery; use investigator or planner for broad synthesis, review, or planning.";
}

export function registerCrewSpawnTool({ pi, runtime, extensionDir, notifyDiscoveryWarnings }: CrewToolDeps): void {
  pi.registerTool({
    name: "crew_spawn",
    label: "Spawn Worker",
    description: "Spawn a non-blocking worker in an isolated child session. Accepts canonical `worker` and legacy `subagent` arguments.",
    parameters: Type.Object({ worker: Type.Optional(Type.String({ description: "Worker name from crew_list" })), subagent: Type.Optional(Type.String({ description: "Legacy alias for worker" })), task: Type.String({ description: "Task to delegate to the worker" }) }),
    promptSnippet: "Spawn a non-blocking worker. Use crew_list first to see available workers.",
    promptGuidelines: ["crew_spawn: Spawn a discovered worker for one self-contained task.", "crew_spawn: Role boundaries: scout = narrow discovery only; investigator = broad/cross-file synthesis or review; planner = deterministic implementation specs.", "crew_spawn: Use the `worker` argument; `subagent` is accepted only for legacy compatibility.", "crew_spawn: Include constraints, relevant files, acceptance criteria, and expected output.", "crew_spawn: Results arrive as steering messages; do not poll crew_list."],
    prepareArguments(args) {
      const input = (args && typeof args === "object") ? args as { worker?: string; subagent?: string; task?: string } : {};
      return { ...input, worker: input.worker ?? input.subagent, task: input.task ?? "" };
    },
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const workerName = params.worker ?? params.subagent;
      if (!workerName) return toolError("Provide `worker` (or legacy `subagent`) with a worker name from crew_list.");
      const { workers, warnings, defaults } = discoverWorkers(ctx.cwd);
      notifyDiscoveryWarnings(ctx, warnings);
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const activeWorkers = runtime.getActiveSummariesForOwner(ownerSessionId);
      const maxActive = typeof defaults.maxActiveWorkers === "number" ? defaults.maxActiveWorkers : undefined;
      if (maxActive !== undefined && activeWorkers.length >= maxActive) return toolError(`maxActiveWorkers=${maxActive} reached for this session.`);
      const maxConcurrent = typeof defaults.maxConcurrentSpawns === "number" ? defaults.maxConcurrentSpawns : undefined;
      const runningCount = activeWorkers.filter((worker) => worker.status === "running").length;
      if (maxConcurrent !== undefined && runningCount >= maxConcurrent) return toolError(`maxConcurrentSpawns=${maxConcurrent} reached for this session.`);
      const worker = workers.find((candidate) => candidate.name === workerName);
      if (!worker) return toolError(`Unknown worker: "${workerName}". Available: ${workers.map((w) => w.name).join(", ") || "none"}`);
      if (defaults.confirmProjectWorkers && worker.sourceLabel.startsWith("project") && ctx.hasUI) {
        const ok = await ctx.ui.confirm("Spawn project worker?", `Spawn worker ${worker.name} from ${worker.filePath}?`);
        if (!ok) return toolError(`Spawn cancelled for worker "${worker.name}".`);
      }
      const id = runtime.spawn(worker, params.task, ctx.cwd, ownerSessionId, { model: ctx.model, modelRegistry: ctx.modelRegistry, agentDir: getAgentDir(), parentSessionFile: ctx.sessionManager.getSessionFile(), onWarning: (msg) => ctx.ui.notify(msg, "warning") }, extensionDir);
      const advisory = worker.name === "scout" ? scoutScopeAdvisory(params.task) : undefined;
      const message = `Worker '${worker.name}' spawned as ${id}. Result will be delivered as a steering message when done.${advisory ? `\n\n${advisory}` : ""}`;
      return toolSuccess(message, { id, workerName: worker.name, task: params.task });
    },
    renderCall(args, theme) { return renderCrewCall(theme, "crew_spawn", args.worker ?? args.subagent ?? "...", args.task); },
    renderResult(result, _options, theme) { return renderCrewResult(result, theme); },
  });
}
