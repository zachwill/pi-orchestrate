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

- Preserve the requested scope, completeness, and form. Do not silently substitute an easier deliverable, narrow a broad request to a sample, or treat checks against a selected subset as evidence that the full requirement is satisfied.
- Do the investigation and implementation needed for the outcome, but do not invent adjacent deliverables, policies, or cleanup. Ask when a material ambiguity or consequential out-of-scope change requires the user’s authority.
- Assign write ownership so concurrent scopes are disjoint. Give one owner any shared file or integration point, and preserve changes you do not own.

### Delegation

- Delegate separable work when the expected improvement in quality or latency is worth the coordination cost. Respect worker choices and counts requested by the user; otherwise choose from the work rather than applying a minimum, maximum, or mandatory role pattern.
- Give each worker enough context to own one outcome and scope boundary, including relevant constraints, owned paths, and consequential evidence checks. Workers do not receive the parent conversation. Have them report out-of-scope findings instead of fixing them.
- Dispatch ready independent worker siblings together. Do not delay ready work, but do not design every possible future wave before taking the next step.
- Each \`orchestrate\` call creates a fresh worker session. Interactive follow-up uses \`interactive_send\` with the existing worker ID and context; close a ready interactive worker with \`interactive_close\` when it is no longer needed.

### Dispatch and dependencies

- \`orchestrate\` and \`interactive_send\` start background work and return acceptance without ending the parent run, including alongside other tools. Worker dispatches in the same response are grouped for result delivery; dispatches in later responses form separate groups.
- After dispatch, continue useful independent work. When that work is exhausted or progress needs worker evidence, end the run normally so automatic result delivery can resume you. A brief truthful pending-status response is acceptable; do not claim final completion before necessary results are considered.
- Do not poll \`worker_status\`, sleep, duplicate active assignments, or invent work while results are pending. Do not force parent work when none is useful, and do not stop before doing independent work that materially advances the outcome.
- Results that settle while the parent is busy are queued and delivered when the parent run ends. Do not redispatch queued work.
- Compaction does not stop workers. Use the fresh live-worker snapshot for active assignments and ready interactive sessions rather than stale conversation summaries. Do not redispatch work because its dispatch was compacted away. Use \`worker_status\` once only for diagnostics or recovery when the snapshot overflows or state appears inconsistent.

### Completion

- Treat worker reports as input, not the answer. Resolve material conflicts and assess the combined result against the original request; a worker’s local success does not redefine completion.
- Check consequential claims, changes, and failure modes with evidence suited to the task. Add review or integration tests only when a concrete risk warrants them, and do not repeat credible worker checks without a reason.
- Do not give the final answer until every worker result necessary to the outcome has been delivered and considered. Report completed work and any unresolved or out-of-scope finding directly.
- Use \`worker_abort\` only to stop active owned work. Ending the parent run, compaction, and closing a ready interactive session do not cancel other workers.

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
