import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type {} from "@earendil-works/pi-coding-agent";
import type { WorkerRecord } from "../orchestration/model.ts";
import type { OwnerSnapshot } from "../orchestration/service.ts";

export const LIVE_WORKER_CONTEXT_TYPE = "pi-orchestrate-live-worker-context";
export const MAX_LIVE_WORKER_CONTEXT_BYTES = 12 * 1024;
export const MAX_LIVE_WORKER_ITEM_BYTES = 1024;
export const MAX_LIVE_WORKER_ASSIGNMENT_BYTES = 480;

const ACTIVE_STATUSES = new Set<WorkerRecord["status"]>([
  "starting",
  "running",
  "stopping",
]);
const ITEM_TRUNCATION_MARKER =
  "\n  [entry truncated; use worker_status once for diagnostics/recovery]";
const ASSIGNMENT_TRUNCATION_MARKER = " … [assignment excerpt truncated]";

type CustomMessage = Extract<AgentMessage, { readonly role: "custom" }>;

export interface LiveWorkerContextOptions {
  readonly ownerSessionId: string;
  readonly pendingResultCount: number;
  readonly timestamp?: number;
}

/** Project only current actionable process state; terminal history and outcomes stay out. */
export function projectLiveWorkerContext(
  snapshot: OwnerSnapshot,
  options: LiveWorkerContextOptions,
): CustomMessage | undefined {
  const workers = snapshot.workers.filter((worker) =>
    worker.ownerSessionId === options.ownerSessionId && isActionable(worker)
  );
  const pendingResultCount = normalizeCount(options.pendingResultCount);
  if (workers.length === 0 && pendingResultCount === 0) return undefined;

  const activeCount = workers.filter((worker) => ACTIVE_STATUSES.has(worker.status)).length;
  const readyCount = workers.length - activeCount;
  const items = workers.map(renderWorkerItem);
  const content = renderBoundedContext(
    items,
    activeCount,
    readyCount,
    pendingResultCount,
  );

  return {
    role: "custom",
    customType: LIVE_WORKER_CONTEXT_TYPE,
    content,
    display: false,
    timestamp: options.timestamp ?? Date.now(),
  };
}

/** Replace this extension's transient projection while preserving every unrelated message. */
export function replaceLiveWorkerContext(
  messages: readonly AgentMessage[],
  context: CustomMessage | undefined,
): AgentMessage[] {
  const retained = messages.filter((message) =>
    message.role !== "custom" || message.customType !== LIVE_WORKER_CONTEXT_TYPE
  );
  return context ? [...retained, context] : retained;
}

function isActionable(worker: WorkerRecord): boolean {
  return ACTIVE_STATUSES.has(worker.status) ||
    (worker.status === "ready" && worker.lifecycle === "interactive");
}

function renderWorkerItem(worker: WorkerRecord): string {
  const metadata = [
    `worker_id=${quote(worker.id)}`,
    `run_id=${quote(worker.runId)}`,
    `definition=${quote(normalizeText(worker.worker))}`,
    `title=${quote(normalizeText(worker.title))}`,
    `lifecycle=${worker.lifecycle}`,
    `status=${worker.status}`,
  ].join(" | ");
  const assignment = capUtf8(
    normalizeText(worker.instructions),
    MAX_LIVE_WORKER_ASSIGNMENT_BYTES,
    ASSIGNMENT_TRUNCATION_MARKER,
  );
  const item = `- ${metadata}\n  assignment=${quote(assignment)}`;
  return capUtf8(item, MAX_LIVE_WORKER_ITEM_BYTES, ITEM_TRUNCATION_MARKER);
}

function renderBoundedContext(
  items: readonly string[],
  activeCount: number,
  readyCount: number,
  pendingResultCount: number,
): string {
  for (let shown = items.length; shown >= 0; shown -= 1) {
    const content = renderContext(
      items.slice(0, shown),
      items.length,
      activeCount,
      readyCount,
      pendingResultCount,
    );
    if (utf8Bytes(content) <= MAX_LIVE_WORKER_CONTEXT_BYTES) return content;
  }

  // Fixed guidance is intentionally far below the global limit. Keep a defensive
  // cap so future copy changes cannot violate the public bound.
  return capUtf8(
    renderContext([], items.length, activeCount, readyCount, pendingResultCount),
    MAX_LIVE_WORKER_CONTEXT_BYTES,
    "\n[Live worker context truncated; use worker_status once for recovery.]",
  );
}

function renderContext(
  shownItems: readonly string[],
  totalWorkers: number,
  activeCount: number,
  readyCount: number,
  pendingResultCount: number,
): string {
  const sections = [
    "## Pi Orchestrate live worker context",
    "Authoritative transient process snapshot for this parent provider call; it is not conversation history.",
    `Relevant owned workers: ${totalWorkers} (${activeCount} active, ${readyCount} ready interactive).`,
  ];

  if (shownItems.length > 0) {
    sections.push(shownItems.join("\n"));
  }
  if (shownItems.length < totalWorkers) {
    const omitted = totalWorkers - shownItems.length;
    sections.push(
      `Snapshot overflow: showing ${shownItems.length} of ${totalWorkers}; ${omitted} worker${omitted === 1 ? "" : "s"} omitted by the ${MAX_LIVE_WORKER_CONTEXT_BYTES}-byte limit. Use worker_status once for diagnostics/recovery to inspect omitted worker IDs and current state; do not poll.`,
    );
  }
  if (pendingResultCount > 0) {
    sections.push(
      `Pending delivery: ${pendingResultCount} settled worker result${pendingResultCount === 1 ? "" : "s"} await automatic delivery. Do not redispatch that work.`,
    );
  }

  const guidance: string[] = [];
  if (activeCount > 0) {
    guidance.push(
      "Do not duplicate active assignments. Wait for automatic result delivery; do not poll worker_status for completion.",
    );
  }
  if (readyCount > 0) {
    guidance.push(
      "Ready interactive sessions are retained: use interactive_send with the worker_id for follow-up, or interactive_close when finished.",
    );
  }
  guidance.push(
    "Use worker_status only for diagnostics or recovery when this snapshot reports overflow or state appears inconsistent.",
  );
  sections.push(`Guidance:\n- ${guidance.join("\n- ")}`);

  return sections.join("\n\n");
}

function normalizeText(value: string): string {
  return value.replace(/\s+/gu, " ").trim();
}

function quote(value: string): string {
  return JSON.stringify(value);
}

function normalizeCount(value: number): number {
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
}

function utf8Bytes(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

function capUtf8(value: string, byteLimit: number, marker: string): string {
  if (utf8Bytes(value) <= byteLimit) return value;
  const markerBytes = utf8Bytes(marker);
  if (markerBytes >= byteLimit) return truncateUtf8(marker, byteLimit);
  return `${truncateUtf8(value, byteLimit - markerBytes)}${marker}`;
}

function truncateUtf8(value: string, byteLimit: number): string {
  if (byteLimit <= 0) return "";
  const bytes = Buffer.from(value, "utf8");
  if (bytes.byteLength <= byteLimit) return value;

  let end = byteLimit;
  while (end > 0 && (bytes[end] ?? 0) >> 6 === 0b10) end -= 1;
  return bytes.subarray(0, end).toString("utf8");
}
