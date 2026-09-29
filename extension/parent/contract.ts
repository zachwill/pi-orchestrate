import type { WorkerCatalog } from "../catalog/definition.ts";

const CONTRACT_START = "<!-- pi-orchestrate:contract:start -->";
const CONTRACT_END = "<!-- pi-orchestrate:contract:end -->";

function sortedWorkers(catalog: WorkerCatalog) {
  return [...catalog.workers].sort((left, right) => {
    if (left.name < right.name) return -1;
    if (left.name > right.name) return 1;
    return 0;
  });
}

function escapeContractMarkers(value: string): string {
  return value
    .replaceAll(CONTRACT_START, "&lt;!-- pi-orchestrate:contract:start --&gt;")
    .replaceAll(CONTRACT_END, "&lt;!-- pi-orchestrate:contract:end --&gt;");
}

function formatCatalog(catalog: WorkerCatalog): string {
  const workers = sortedWorkers(catalog);
  if (workers.length === 0) return "- No trusted workers are available for this session.";

  return workers
    .map(
      (worker) =>
        `- \`${escapeContractMarkers(worker.name)}\` [${worker.source.kind}] (${worker.lifecycle}): ${escapeContractMarkers(worker.description)}`,
    )
    .join("\n");
}

interface ContractMarker {
  readonly start: number;
  readonly end: number;
  readonly kind: "start" | "end";
}

function contractMarkers(prompt: string): ContractMarker[] {
  const markers: ContractMarker[] = [];
  for (const [value, kind] of [
    [CONTRACT_START, "start"],
    [CONTRACT_END, "end"],
  ] as const) {
    let offset = 0;
    while (offset < prompt.length) {
      const start = prompt.indexOf(value, offset);
      if (start < 0) break;
      markers.push({ start, end: start + value.length, kind });
      offset = start + value.length;
    }
  }
  return markers.sort((left, right) => left.start - right.start);
}

function removeContractMarkers(prompt: string): {
  readonly prompt: string;
  readonly insertionOffset?: number;
} {
  const markers = contractMarkers(prompt);
  if (markers.length === 0) return { prompt };

  const removed: Array<{ start: number; end: number }> = [];
  const stack: ContractMarker[] = [];
  for (const marker of markers) {
    if (marker.kind === "start") {
      stack.push(marker);
      continue;
    }
    const start = stack.pop();
    if (start && stack.length === 0) removed.push({ start: start.start, end: marker.end });
  }

  for (const marker of markers) {
    if (!removed.some((range) => marker.start >= range.start && marker.end <= range.end)) {
      removed.push({ start: marker.start, end: marker.end });
    }
  }
  removed.sort((left, right) => left.start - right.start);

  const insertionPoint = markers[0]!.start;
  let insertionOffset = 0;
  let cursor = 0;
  let cleaned = "";
  for (const range of removed) {
    if (range.start < cursor) continue;
    const retained = prompt.slice(cursor, range.start);
    cleaned += retained;
    if (range.start <= insertionPoint) insertionOffset = cleaned.length;
    cursor = range.end;
  }
  cleaned += prompt.slice(cursor);
  return { prompt: cleaned, insertionOffset };
}

function buildContract(catalog: WorkerCatalog): string {
  return `${CONTRACT_START}
## Pi Orchestrate Contract

You own the user’s outcome across parent and worker work. Exercise judgment: use workers when they help, continue useful work yourself, and deliver one coherent answer.

### Outcome and scope

- Own the full request across your work and the workers’ assignments. Keep the requested scope and deliverable in view when you divide work and verify the result.
- Investigate what the request needs. Bring material ambiguities and consequential out-of-scope changes back to the user.
- Workers edit your working tree directly. Their changes are available for you to inspect and test, so coordinate your own edits with their assigned scopes. Give one owner each shared file or integration point, and preserve changes you do not own.

### Delegation

- Delegate when it improves quality or saves time. Follow the user’s requested worker choices and counts; otherwise choose workers and assignment sizes to fit the task.
- Workers receive your brief, not your conversation. Give each one a clear outcome, relevant context, owned paths, constraints, and checks that will establish success. Have them report findings outside their assignment for you to consider.
- Use one \`multi_tool_use.parallel\` call to dispatch independent \`functions.orchestrate\` assignments together. Follow-ups to ready interactive workers can join the same batch through \`functions.interactive_send\`.
- Each \`orchestrate\` call creates a fresh worker session. One-shot workers finish automatically. Interactive workers become \`ready\` after returning a result; continue with \`interactive_send\` or release them with \`interactive_close\`. These tools take the worker ID returned at dispatch, and follow-ups retain the worker’s context.

### Dispatch and dependencies

- \`orchestrate\` and \`interactive_send\` return acceptance while work continues in the background. The worker’s result arrives separately. Calls to these tools in the same response form one result group, which resumes you once the group has finished. Other tool calls can run alongside dispatches without joining the result group.
- After dispatch, continue useful independent work. When you need worker results and have no useful independent work left, send a brief pending response without tool calls. This lets automatic result delivery resume you.
- Results that settle while the parent is busy are queued and delivered when the parent run ends.
- Compaction does not stop workers. Pick up existing assignments from the live worker context, which shows current activity and ready interactive sessions. Use \`worker_status\` to recover missing IDs or diagnose unexpected state. Completion arrives automatically, so polling is unnecessary.

### Completion

- Combine worker reports with your own work, resolve material conflicts, and assess the result against the full request.
- Build on credible worker checks. Add verification where the consequences or gaps in the evidence warrant it.
- Give the final answer once every worker result needed for the outcome has arrived and been considered. State what is complete and what remains unresolved.
- Use \`worker_abort\` to stop active work. Workers otherwise continue independently, including while the parent is idle or its conversation is compacted.

### Trusted worker catalog

${formatCatalog(catalog)}
${CONTRACT_END}`;
}

export function applyOrchestratorContract(
  systemPrompt: string,
  catalog: WorkerCatalog,
): string {
  const section = buildContract(catalog);
  const cleaned = removeContractMarkers(systemPrompt);
  if (cleaned.insertionOffset !== undefined) {
    return `${cleaned.prompt.slice(0, cleaned.insertionOffset)}${section}${cleaned.prompt.slice(cleaned.insertionOffset)}`;
  }

  const separator =
    systemPrompt.length === 0 || systemPrompt.endsWith("\n\n")
      ? ""
      : systemPrompt.endsWith("\n")
        ? "\n"
        : "\n\n";
  return `${systemPrompt}${separator}${section}`;
}
