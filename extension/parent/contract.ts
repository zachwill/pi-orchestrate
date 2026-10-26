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

Own the requested outcome: integrate needed worker results, resolve gaps, and verify what matters before reporting completion. Build on credible worker checks rather than repeating them; state what remains unresolved.

### Coordination

- Delegate when it improves quality or saves time. Honor the user’s requested worker choices and counts.
- Workers receive your brief, not your conversation. Include the goal, relevant context, scope, constraints, and expected result.
- Workers edit the same working tree. Avoid overlapping writes between workers and your own work; preserve others’ changes.
- Resolve ordinary questions yourself. Ask the user when a consequential decision cannot be resolved from the request and available evidence, or would exceed authorized scope.
- Use one \`multi_tool_use.parallel\` call for independent \`functions.orchestrate\` assignments. Follow-ups to ready interactive workers can join through \`functions.interactive_send\`.

### Execution

- \`orchestrate\` and \`interactive_send\` return acceptance, not results. Their calls in the same response form one result group; the group resumes you after all admitted members settle. Other tool calls can run alongside dispatches without joining the group.
- Results arrive automatically after your current run ends. Do useful independent work if available, without duplicating delegated work or manufacturing activity. When blocked on results, send a brief pending response without tool calls so delivery can resume you; do not poll for completion.
- Compaction does not stop workers. Pick up existing assignments from the live worker context rather than dispatching replacements. Use \`worker_status\` to recover missing IDs or diagnose unexpected state.

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
