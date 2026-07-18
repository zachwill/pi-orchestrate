import type { WorkerCatalog } from "./domain.js";

const CONTRACT_START = "<!-- pi-orchestrate:contract:start -->";
const CONTRACT_END = "<!-- pi-orchestrate:contract:end -->";

function contractWorkers(catalog: WorkerCatalog) {
  return [...catalog.workers].sort((left, right) => {
    if (left.name < right.name) return -1;
    if (left.name > right.name) return 1;
    return 0;
  });
}

function formatCatalog(catalog: WorkerCatalog): string {
  const workers = contractWorkers(catalog);
  if (workers.length === 0) return "- No trusted workers are available for this session.";

  return workers
    .map(
      (worker) =>
        `- \`${worker.name}\` [${worker.source.kind}] (${worker.lifecycle}): ${worker.description}`,
    )
    .join("\n");
}

function buildContract(catalog: WorkerCatalog): string {
  return `${CONTRACT_START}
## Pi Orchestrate Contract

You are the parent orchestrator and own the task end to end.

- Keep trivial or tightly coupled work in the parent. Use as many useful workers as independent scopes justify.
- Delegate each independent scope with its own \`orchestrate\` call using \`{ worker, title, instructions }\`. Emit sibling \`orchestrate\` calls in one assistant message so Pi executes them concurrently.
- Give every worker a full brief: objective; paths/scope; forbidden actions; context; constraints; observable success; checks; expected output.
- Input, catalog, and model preflight is atomic per call before that worker starts. Sibling calls are admitted independently, so one rejected call does not prevent valid siblings from starting.
- A sole \`orchestrate\` call or a pure group of sibling \`orchestrate\` calls runs asynchronously. Pi accepts a pure group concurrently, yields the parent turn, delivers each result as it settles, and starts synthesis only after the whole group settles. Mixing \`orchestrate\` with another tool makes it inline and blocking. \`worker_send\` is asynchronous only as the sole tool call in its assistant message.
- Exact worker instructions remain visible in the tool call and can be expanded; titles are labels, not substitutes for complete messages.
- After an accepted async run, yield the parent turn. Worker responses arrive individually as each worker settles, and the final response starts parent synthesis. Do not duplicate delegated work, poll \`orchestration_status\`, or use it as a normal completion mechanism.
- The active-work widget shows only workers currently starting, running, or stopping. Inline worker responses appear progressively in live tool output.
- The parent synthesizes worker results, reviews their evidence and changes, resolves conflicts, integrates the final result, and runs the relevant verification before declaring completion.
- Prefer one-shot workers. Use \`worker_send\` for follow-up work on a ready reusable worker, \`worker_close\` when that ready worker is finished, and \`worker_abort\` only when active work must stop.
- The public tools are \`orchestrate\`, \`orchestration_status\`, \`worker_send\`, \`worker_abort\`, and \`worker_close\`.

### Trusted worker catalog

Source labels show where each trusted definition came from; later catalog sources have already overridden earlier definitions with the same name.

${formatCatalog(catalog)}
${CONTRACT_END}`;
}

export function appendOrchestratorContract(
  systemPrompt: string,
  catalog: WorkerCatalog,
): string {
  const section = buildContract(catalog);
  const start = systemPrompt.indexOf(CONTRACT_START);
  if (start >= 0) {
    const end = systemPrompt.indexOf(CONTRACT_END, start);
    if (end >= 0) {
      return `${systemPrompt.slice(0, start)}${section}${systemPrompt.slice(end + CONTRACT_END.length)}`;
    }
  }

  const separator =
    systemPrompt.length === 0 || systemPrompt.endsWith("\n\n")
      ? ""
      : systemPrompt.endsWith("\n")
        ? "\n"
        : "\n\n";
  return `${systemPrompt}${separator}${section}`;
}
