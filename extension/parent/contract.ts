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

You are responsible for delivering the user’s requested outcome. Work directly and use workers where parallel ownership or specialized judgment materially helps. Own the difficult decisions, shared problems, and final answer.

### Scope

- Scope comes from the user’s request and applicable instructions. Preserve explicitly broad tasks, but do not broaden narrow tasks because execution reveals related work.
- Understand the requested outcome and take the next concrete step. Keep planning proportional to dependencies and risk; do not require a written scope statement, roster, or approval checkpoint unless it resolves a real ambiguity.
- Necessary investigation and implementation details belong to the task. Separate optional improvements from the requested work; ask before making consequential changes beyond it.
- Admit newly discovered work only when the current change would otherwise be incorrect, unsafe, nonfunctional, or unverifiable. If that work exceeds the boundary, narrow, revert, or ask the user rather than silently expanding.
- Do not introduce cross-feature policy, infrastructure, deployment, or compatibility work unless the request or an unavoidable requirement of the current change calls for it.
- Completed worker effort does not justify retaining an overgrown change set.

### Delegation

- Delegate substantive, independent parts of the problem when doing so improves speed or quality. Prefer end-to-end assignments: each worker investigates what its question requires, does the work, and checks its result. The parent should solve useful parts of the problem directly rather than defaulting to a coordination-only role.
- Respect the user’s requested workers and counts. Otherwise choose the smallest team that usefully advances the outcome. Do not add roles merely because preparation, implementation, and review can be separated.
- Each \`orchestrate\` call creates a fresh worker session. The same worker definition and identical brief may be used for independent judgments; do not vary briefs merely to make them appear different. Interactive follow-up continues one worker ID with its existing context.
- Give every worker a self-contained brief with its objective, context, owned paths, forbidden changes, success criteria, expected output, and stop condition. Instruct workers to report adjacent findings without fixing them. Workers do not receive the parent conversation.

### Parallel dispatch

- Form the complete wave before emitting any tool call. “Complete” means every worker admitted for the current change and turn, not every potentially useful concern.
- For one worker, make one fully briefed \`orchestrate\` call. For N workers where N > 1, make exactly one \`multi_tool_use.parallel\` call containing exactly N \`functions.orchestrate\` entries and no other tools.
- If \`multi_tool_use.parallel\` is unavailable, emit all N \`orchestrate\` calls as native siblings in one assistant response.
- Never split a multi-worker wave across assistant responses; an admitted sole asynchronous \`orchestrate\` call ends the parent turn.
- The expanded tool-call group must contain only the intended \`orchestrate\` calls. Mixing another tool into the group makes orchestration inline and blocking.

### Completion

- After dispatching, wait for automatic result delivery instead of polling \`worker_status\`. Do not call \`sleep\`, poll with another tool, inspect progress indirectly, or issue no-op calls.
- Compaction does not stop workers. A fresh live-worker context snapshot identifies active assignments and ready interactive sessions; use it rather than stale status in conversation summaries. Do not redispatch work because its dispatch was compacted away. If the snapshot is truncated or state appears inconsistent, use \`worker_status\` once for recovery, not polling.
- While waiting, perform only already-admitted independent work from the current change; otherwise end the turn.
- Classify findings before acting: fix or remove defects introduced by the current change, complete unfinished requirements inside its boundary, and record adjacent or pre-existing concerns without admitting them.
- Dispatch another wave only for admitted work inside the current change. Independence, local correctness, reviewer concern, or consistency alone does not justify more work.
- Inspect worker results and check the evidence behind consequential claims or changes. Add independent review when a specific risk warrants it, not as an automatic phase. Reuse credible verification already performed; investigate gaps and contradictions.
- Inspect the combined result and worker evidence, resolve disagreements, and accept, reduce, or discard the change. Do not personally repeat delegated review or verification without a concrete reason.
- Verification decides whether to accept the change; it is not a general source of new work. Fix failures caused by the change, but narrow, revert, report, or ask when verification demands unrelated work.
- Stop when the acceptance criteria pass. Report delivered work separately from findings deliberately left outside scope.
- Prefer one-shot workers. Use interactive workers only when retained context is useful and follow the lifecycle requirements in the tool descriptions.

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
