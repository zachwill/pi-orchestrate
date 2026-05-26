import { Text } from "@mariozechner/pi-tui";
import { Type } from "typebox";
import { discoverWorkers } from "../../worker-discovery.js";
import { STATUS_ICON, sendWorkerNote } from "../../worker-messages.js";
import type { CrewToolDeps } from "./tool-deps.js";

export function registerCrewListTool({ pi, runtime, notifyDiscoveryWarnings }: CrewToolDeps): void {
  pi.registerTool({
    name: "crew_list",
    label: "List Workers",
    description: "List available worker definitions and active workers. Use for discovery or one-time status snapshots only; worker results are delivered automatically as steering messages.",
    parameters: Type.Object({}),
    promptSnippet: "List worker definitions and active workers.",
    promptGuidelines: ["crew_list: Use before crew_spawn to discover worker names and descriptions.", "crew_list: Role boundaries: scout = narrow discovery only; investigator = broad/cross-file synthesis or review; planner = deterministic implementation specs.", "crew_list: Do not poll for completion; worker results arrive as steering messages."],
    async execute(_id, _params, _signal, _onUpdate, ctx) {
      const { workers, warnings } = discoverWorkers(ctx.cwd);
      notifyDiscoveryWarnings(ctx, warnings);
      const active = runtime.getActiveSummariesForOwner(ctx.sessionManager.getSessionId());
      const lines = ["## Available Workers"];
      if (workers.length === 0) lines.push("No valid worker definitions found. Add Markdown to `<cwd>/.pi/pi-workers/agents/` or `~/.pi/agent/pi-workers/agents/`.");
      for (const worker of workers) { lines.push("", `name: ${worker.name}`, `description: ${worker.description}`, `interactive: ${worker.interactive ? "true" : "false"}`, `source: ${worker.sourceLabel}`); }
      if (warnings.length) { lines.push("", "## Discovery Warnings"); for (const warning of warnings) lines.push(`- ${warning.message} (${warning.filePath})`); }
      lines.push("", "## Active Workers");
      if (active.length === 0) lines.push("No workers currently active.");
      for (const worker of active) lines.push("", `id: ${worker.id}`, `name: ${worker.workerName}`, `status: ${STATUS_ICON[worker.status]} ${worker.status}`);
      if (active.length > 0) Promise.resolve().then(() => sendWorkerNote("⚠ Active workers detected. Do not poll crew_list for completion — results arrive as steering messages.", pi.sendMessage.bind(pi), { isIdle: ctx.isIdle(), triggerTurn: true }));
      return { content: [{ type: "text", text: lines.join("\n") }], details: {} };
    },
    renderCall(_args, theme) { return new Text(theme.fg("toolTitle", theme.bold("crew_list")), 0, 0); },
    renderResult(result) { const first = result.content[0]; return new Text(first?.type === "text" ? first.text : "(no output)", 0, 0); },
  });
}
