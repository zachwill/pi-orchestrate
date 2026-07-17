import {
  getMarkdownTheme,
  keyHint,
  type ExtensionAPI,
  type ExtensionContext,
  type Theme,
} from "@earendil-works/pi-coding-agent";
import {
  Container,
  Markdown,
  Spacer,
  Text,
  truncateToWidth,
  visibleWidth,
  type Component,
} from "@earendil-works/pi-tui";
import type { WaveDeliveryDetails } from "./delivery.js";
import {
  isWorkerCompleteForWave,
  type WaveRecord,
  type WorkerOutcome,
  type WorkerRecord,
  type WorkerStatus,
  type WorkerUsage,
} from "./domain.js";
import type { OrchestratorRuntime, RuntimeSnapshot } from "./runtime.js";

export const ORCHESTRATION_PRESENTATION_KEY = "pi-orchestrate";
export const MAX_RESULT_PREVIEW_LINES = 5;
export const MAX_WIDGET_WORKERS = 8;

export type PresentationRuntime = Pick<
  OrchestratorRuntime,
  "snapshot" | "subscribeState"
>;

interface SafeResult {
  readonly workerId: string;
  readonly worker: string;
  readonly title: string;
  readonly status: string;
  readonly outcome: WorkerOutcome;
  readonly usage: Partial<WorkerUsage>;
  readonly sessionFile?: string;
}

interface SafeDetails {
  readonly id: string;
  readonly results: readonly SafeResult[];
}

interface StatusBinding {
  readonly ownerSessionId: string;
  readonly ctx: ExtensionContext;
}

const ACTIVE_STATUSES: ReadonlySet<WorkerStatus> = new Set([
  "starting",
  "running",
  "stopping",
]);

const KNOWN_RESULT_STATUSES = new Set([
  "completed",
  "failed",
  "aborted",
  "ready",
  "closed",
]);

export function registerOrchestrationPresentation(pi: ExtensionAPI): void {
  pi.registerMessageRenderer<WaveDeliveryDetails>(
    "pi-orchestrate-wave",
    (message, { expanded }, theme) => {
      const details = readDeliveryDetails(message.details);
      const content = messageText(message.content);

      if (expanded) return expandedResult(content, details, theme);
      return new BoundedLines(collapsedResultLines(content, details, theme));
    },
  );
}

export function formatResultStatusSummary(details: unknown): string {
  const parsed = readDeliveryDetails(details);
  if (!parsed) return "Result details unavailable";

  const resultWord = parsed.results.length === 1 ? "result" : "results";
  if (parsed.results.length === 0) return `0 ${resultWord}`;

  const counts = new Map<string, number>();
  for (const result of parsed.results) {
    const status = KNOWN_RESULT_STATUSES.has(result.status) ? result.status : "unknown";
    counts.set(status, (counts.get(status) ?? 0) + 1);
  }

  const statusOrder = ["completed", "ready", "failed", "aborted", "closed", "unknown"];
  const statuses = statusOrder.flatMap((status) => {
    const count = counts.get(status);
    return count === undefined ? [] : [`${count} ${status}`];
  });
  return `${parsed.results.length} ${resultWord} · ${statuses.join(" · ")}`;
}

export function formatResultPreviews(
  details: unknown,
  fallbackContent = "",
  limit = MAX_RESULT_PREVIEW_LINES,
): string[] {
  if (limit <= 0) return [];
  const parsed = readDeliveryDetails(details);
  if (!parsed || parsed.results.length === 0) {
    return firstNonEmptyLines(fallbackContent, limit);
  }

  const visibleResults = parsed.results.slice(0, limit);
  const previews = visibleResults.map((result) => {
    const label = `${statusIcon(result.status)} ${result.worker} — ${result.title} · ${result.status}`;
    const outcome = outcomePreview(result);
    return outcome ? `${label}: ${outcome}` : label;
  });

  if (parsed.results.length > limit && previews.length > 0) {
    previews[previews.length - 1] = `… ${parsed.results.length - limit + 1} more results`;
  }
  return previews;
}

export function formatWorkerUsage(usage: Partial<WorkerUsage> | undefined): string | undefined {
  if (!usage) return undefined;

  const parts = contextTurnUsageParts(usage);
  if (isPositiveNumber(usage.input)) parts.push(`↑${formatCompactNumber(usage.input)}`);
  if (isPositiveNumber(usage.output)) parts.push(`↓${formatCompactNumber(usage.output)}`);
  if (isPositiveNumber(usage.cacheRead)) parts.push(`R${formatCompactNumber(usage.cacheRead)}`);
  if (isPositiveNumber(usage.cacheWrite)) parts.push(`W${formatCompactNumber(usage.cacheWrite)}`);
  if (isPositiveNumber(usage.cost)) parts.push(`$${usage.cost.toFixed(4)}`);
  return parts.length > 0 ? parts.join(" · ") : undefined;
}

export function formatWorkerStatusLine(worker: WorkerRecord): string {
  const metadata = [
    worker.worker,
    workerStateLabel(worker),
    formatContextCost(worker.usage),
  ].filter((part): part is string => Boolean(part));

  return `${worker.title} · ${metadata.join(" · ")} · ${worker.id}`;
}

export function formatFooterStatus(snapshot: RuntimeSnapshot): string | undefined {
  const active = snapshot.workers.filter((worker) => ACTIVE_STATUSES.has(worker.status)).length;
  const ready = snapshot.workers.filter((worker) => worker.status === "ready").length;
  if (active === 0 && ready === 0) return undefined;

  return `Orchestrate: ${active} active · ${ready} ready`;
}

export class StatusController {
  private binding: StatusBinding | undefined;
  private bindingGeneration = 0;
  private refreshGeneration = 0;
  private disposed = false;
  private unsubscribeState: (() => void) | undefined;

  constructor(private readonly runtime: PresentationRuntime) {}

  bind(ownerSessionId: string, ctx: ExtensionContext): void {
    if (this.disposed) return;
    this.clearBinding();
    this.bindingGeneration += 1;
    this.binding = { ownerSessionId, ctx };
    this.unsubscribeState = this.runtime.subscribeState((changedOwnerSessionId) => {
      if (changedOwnerSessionId !== this.binding?.ownerSessionId) return;
      void this.refresh();
    });
    void this.refresh();
  }

  unbind(ownerSessionId?: string): void {
    if (ownerSessionId !== undefined && ownerSessionId !== this.binding?.ownerSessionId) return;
    this.clearBinding();
    this.bindingGeneration += 1;
    this.refreshGeneration += 1;
  }

  async refresh(): Promise<void> {
    const binding = this.binding;
    if (!binding || this.disposed) return;

    const bindingGeneration = this.bindingGeneration;
    const refreshGeneration = ++this.refreshGeneration;
    let snapshot: RuntimeSnapshot;
    try {
      snapshot = await this.runtime.snapshot(binding.ownerSessionId);
    } catch {
      return;
    }

    if (
      this.disposed ||
      this.binding !== binding ||
      this.bindingGeneration !== bindingGeneration ||
      this.refreshGeneration !== refreshGeneration
    ) {
      return;
    }

    this.present(binding.ctx, snapshot);
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.clearBinding();
    this.bindingGeneration += 1;
    this.refreshGeneration += 1;
  }

  private present(ctx: ExtensionContext, snapshot: RuntimeSnapshot): void {
    const footer = formatFooterStatus(snapshot);
    ctx.ui.setStatus(ORCHESTRATION_PRESENTATION_KEY, footer);

    if (ctx.mode !== "tui") return;
    if (widgetWorkerCount(snapshot) === 0) {
      ctx.ui.setWidget(ORCHESTRATION_PRESENTATION_KEY, undefined);
      return;
    }

    ctx.ui.setWidget(
      ORCHESTRATION_PRESENTATION_KEY,
      (_tui, theme) => new WorkerStatusComponent(snapshot, theme),
      { placement: "aboveEditor" },
    );
  }

  private clearBinding(): void {
    this.unsubscribeState?.();
    this.unsubscribeState = undefined;

    const current = this.binding;
    if (!current) return;

    current.ctx.ui.setStatus(ORCHESTRATION_PRESENTATION_KEY, undefined);
    if (current.ctx.mode === "tui") {
      current.ctx.ui.setWidget(ORCHESTRATION_PRESENTATION_KEY, undefined);
    }
    this.binding = undefined;
  }
}

export function createStatusController(runtime: PresentationRuntime): StatusController {
  return new StatusController(runtime);
}

class BoundedLines implements Component {
  constructor(private readonly lines: readonly string[]) {}

  render(width: number): string[] {
    const boundedWidth = Math.max(1, width);
    return this.lines.map((line) => truncateToWidth(line, boundedWidth, "…"));
  }

  invalidate(): void {}
}

interface WaveGroup {
  readonly wave: WaveRecord;
  readonly workers: readonly WorkerRecord[];
  readonly settled: number;
}

class WorkerStatusComponent implements Component {
  constructor(
    private readonly snapshot: RuntimeSnapshot,
    private readonly theme: Theme,
  ) {}

  render(width: number): string[] {
    const boundedWidth = Math.max(1, width);
    const groups = activeWaveGroups(this.snapshot);
    const groupedWorkerIds = new Set(groups.flatMap((group) => group.workers.map((worker) => worker.id)));
    const ready = readyWorkers(this.snapshot).filter((worker) => !groupedWorkerIds.has(worker.id));
    const totalWorkers = groups.reduce((total, group) => total + group.workers.length, 0) + ready.length;
    const lines: string[] = [];
    let remaining = MAX_WIDGET_WORKERS;
    let shown = 0;

    for (const group of groups) {
      if (remaining === 0) break;
      const visibleWorkers = group.workers.slice(0, remaining);
      if (visibleWorkers.length === 0) continue;
      lines.push(this.waveHeader(group));
      for (const worker of visibleWorkers) lines.push(this.workerLine(worker, boundedWidth));
      shown += visibleWorkers.length;
      remaining -= visibleWorkers.length;
    }

    if (remaining > 0) {
      const visibleReady = ready.slice(0, remaining);
      if (visibleReady.length > 0) {
        lines.push(this.theme.fg("toolTitle", this.theme.bold(`Ready · ${ready.length}`)));
        for (const worker of visibleReady) lines.push(this.workerLine(worker, boundedWidth));
        shown += visibleReady.length;
      }
    }

    if (totalWorkers > shown) {
      lines.push(this.theme.fg("dim", `… ${totalWorkers - shown} more workers`));
    }
    return lines.map((line) => truncateToWidth(line, boundedWidth, "…"));
  }

  invalidate(): void {}

  private waveHeader(group: WaveGroup): string {
    return this.theme.fg(
      "toolTitle",
      this.theme.bold(
        `Wave ${group.wave.id} · ${group.settled}/${group.wave.workerIds.length} settled`,
      ),
    );
  }

  private workerLine(worker: WorkerRecord, width: number): string {
    const state = workerStateLabel(worker);
    const title = `${statusIcon(worker.status)} ${this.theme.fg("text", this.theme.bold(worker.title))}`;
    const workerType = this.theme.fg("muted", worker.worker);
    const activity = this.theme.fg("text", state);
    const context = isPositiveNumber(worker.usage.contextTokens)
      ? this.theme.fg("dim", `${formatCompactNumber(worker.usage.contextTokens)} ctx`)
      : undefined;
    const cost = isPositiveNumber(worker.usage.cost)
      ? this.theme.fg("dim", `$${worker.usage.cost.toFixed(4)}`)
      : undefined;
    const id = this.theme.fg("muted", String(worker.id));
    const variants = [
      [title, workerType, activity, context, cost, id],
      [title, workerType, activity, context, id],
      [title, workerType, activity, id],
      [title, activity, id],
    ].map((parts) => parts.filter((part): part is string => Boolean(part)).join(" · "));
    const fitting = variants.find((line) => visibleWidth(line) <= width);
    if (fitting) return fitting;

    const suffix = ` · ${activity} · ${id}`;
    const titleWidth = Math.max(1, width - visibleWidth(suffix));
    if (titleWidth > 1) return `${truncateToWidth(title, titleWidth, "…")}${suffix}`;
    return truncateToWidth(`${statusIcon(worker.status)} ${id}`, width, "…");
  }
}

function collapsedResultLines(
  content: string,
  details: SafeDetails | undefined,
  theme: Theme,
): string[] {
  const waveId = details?.id ?? "unknown wave";
  const heading = `${theme.fg("toolTitle", theme.bold("Worker results"))} ${theme.fg("dim", `· ${waveId}`)}`;
  const summary = theme.fg("muted", formatResultStatusSummary(details));
  const previews = formatResultPreviews(details, content).map((line) => theme.fg("customMessageText", line));
  const hint = theme.fg("dim", keyHint("app.tools.expand", "to expand results"));
  return [heading, summary, ...previews, hint];
}

function expandedResult(
  content: string,
  details: SafeDetails | undefined,
  theme: Theme,
): Component {
  const container = new Container();
  const waveId = details?.id ?? "unknown wave";
  container.addChild(
    new Text(
      `${theme.fg("toolTitle", theme.bold("Worker results"))} ${theme.fg("dim", `· ${waveId}`)}`,
      0,
      0,
    ),
  );
  container.addChild(new Text(theme.fg("muted", formatResultStatusSummary(details)), 0, 0));
  container.addChild(new Spacer(1));

  if (!details) {
    container.addChild(new Markdown(content, 0, 0, getMarkdownTheme()));
    container.addChild(new Spacer(1));
    container.addChild(new Text(theme.fg("dim", "Structured worker metadata unavailable"), 0, 0));
    return container;
  }

  container.addChild(new Markdown(reconstructOutcomeMarkdown(details), 0, 0, getMarkdownTheme()));
  container.addChild(new Spacer(1));
  container.addChild(new Text(theme.fg("toolTitle", theme.bold("Worker details")), 0, 0));

  for (const result of details.results) {
    const usage = formatWorkerUsage(result.usage) ?? "unavailable";
    const session = result.sessionFile ?? "unavailable";
    container.addChild(new Text(theme.fg("text", `${result.worker} — ${result.title}`), 0, 0));
    container.addChild(
      new Text(
        `${theme.fg("muted", "ID")} ${result.workerId} · ${theme.fg("muted", "status")} ${result.status}`,
        0,
        0,
      ),
    );
    container.addChild(new Text(theme.fg("dim", `usage ${usage}`), 0, 0));
    container.addChild(new Text(theme.fg("dim", `session ${session}`), 0, 0));
  }
  return container;
}

function readDeliveryDetails(value: unknown): SafeDetails | undefined {
  if (!isRecord(value) || typeof value.id !== "string" || !Array.isArray(value.results)) {
    return undefined;
  }

  const results: SafeResult[] = [];
  for (const candidate of value.results) {
    if (
      !isRecord(candidate) ||
      typeof candidate.workerId !== "string" ||
      typeof candidate.worker !== "string" ||
      typeof candidate.title !== "string" ||
      typeof candidate.status !== "string" ||
      !KNOWN_RESULT_STATUSES.has(candidate.status) ||
      !isRecord(candidate.usage)
    ) {
      return undefined;
    }

    const outcome = readOutcome(candidate.outcome, candidate.status);
    if (!outcome) return undefined;
    results.push({
      workerId: candidate.workerId,
      worker: candidate.worker,
      title: candidate.title,
      status: candidate.status,
      outcome,
      usage: candidate.usage as Partial<WorkerUsage>,
      ...(typeof candidate.sessionFile === "string" ? { sessionFile: candidate.sessionFile } : {}),
    });
  }
  return { id: value.id, results };
}

function messageText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";

  return content.flatMap((part) => {
    if (isRecord(part) && part.type === "text" && typeof part.text === "string") {
      return [part.text];
    }
    return [];
  }).join("\n");
}

function outcomePreview(result: SafeResult): string | undefined {
  const outcome = result.outcome;
  const assistantText = "assistantText" in outcome
    ? firstNonEmptyLines(outcome.assistantText ?? "", 1)[0]
    : undefined;
  const message = "message" in outcome ? outcome.message?.trim() : undefined;
  if (result.status === "failed") return message;
  if (result.status === "aborted") return message;
  return assistantText;
}

function reconstructOutcomeMarkdown(details: SafeDetails): string {
  return details.results.map((result) => {
    const heading = `### ${result.worker} — ${result.title}`;
    const outcome = renderOutcomeMarkdown(result.outcome);
    return outcome ? `${heading}\n\n${outcome}` : heading;
  }).join("\n\n");
}

function renderOutcomeMarkdown(outcome: WorkerOutcome): string {
  switch (outcome.status) {
    case "completed":
    case "ready":
      return outcome.assistantText;
    case "failed":
      return outcome.assistantText
        ? `Failed: ${outcome.message}\n\n${outcome.assistantText}`
        : `Failed: ${outcome.message}`;
    case "aborted": {
      const reason = outcome.message ? `Aborted: ${outcome.message}` : "Aborted";
      return outcome.assistantText ? `${reason}\n\n${outcome.assistantText}` : reason;
    }
    case "closed":
      return "Closed";
  }
}

function readOutcome(value: unknown, status: string): WorkerOutcome | undefined {
  if (!isRecord(value) || value.status !== status) return undefined;
  if (status === "completed" || status === "ready") {
    return typeof value.assistantText === "string"
      ? { status, assistantText: value.assistantText }
      : undefined;
  }
  if (status === "failed") {
    if (typeof value.message !== "string") return undefined;
    return {
      status: "failed",
      message: value.message,
      ...(typeof value.assistantText === "string" ? { assistantText: value.assistantText } : {}),
    };
  }
  if (status === "aborted") {
    return {
      status: "aborted",
      ...(typeof value.message === "string" ? { message: value.message } : {}),
      ...(typeof value.assistantText === "string" ? { assistantText: value.assistantText } : {}),
    };
  }
  if (status === "closed") return { status: "closed" };
  return undefined;
}

function firstNonEmptyLines(text: string, limit: number): string[] {
  const lines: string[] = [];
  let start = 0;
  for (let index = 0; index <= text.length && lines.length < limit; index += 1) {
    if (index !== text.length && text[index] !== "\n") continue;
    const line = text.slice(start, index).replace(/\r$/, "").trim();
    if (line) lines.push(line);
    start = index + 1;
  }
  return lines;
}

function activeWaveGroups(snapshot: RuntimeSnapshot): WaveGroup[] {
  const workersById = new Map(snapshot.workers.map((worker) => [worker.id, worker]));
  return snapshot.waves.flatMap((wave) => {
    if (wave.state !== "running") return [];
    const workers = wave.workerIds.flatMap((workerId) => {
      const worker = workersById.get(workerId);
      return worker?.waveId === wave.id ? [worker] : [];
    });
    const settled = workers.filter((worker) => isWorkerCompleteForWave(worker.status)).length;
    return [{ wave, workers, settled }];
  });
}

function readyWorkers(snapshot: RuntimeSnapshot): WorkerRecord[] {
  return snapshot.workers.filter((worker) => worker.status === "ready");
}

function widgetWorkerCount(snapshot: RuntimeSnapshot): number {
  const groups = activeWaveGroups(snapshot);
  const groupedWorkerIds = new Set(groups.flatMap((group) => group.workers.map((worker) => worker.id)));
  return groups.reduce((total, group) => total + group.workers.length, 0) +
    readyWorkers(snapshot).filter((worker) => !groupedWorkerIds.has(worker.id)).length;
}

const TOOL_ACTIVITY: Readonly<Record<string, string>> = {
  read: "reading",
  grep: "searching",
  find: "finding files",
  ls: "listing",
  bash: "running command",
  edit: "editing",
  write: "writing",
};

function workerStateLabel(worker: Pick<WorkerRecord, "status" | "activity">): string {
  if (worker.status === "starting" || worker.status === "stopping") return worker.status;
  if (worker.status !== "running") return worker.status;
  if (!worker.activity?.trim()) return "thinking";
  return TOOL_ACTIVITY[worker.activity] ?? worker.activity;
}

function formatContextCost(usage: Partial<WorkerUsage> | undefined): string | undefined {
  if (!usage) return undefined;
  const parts: string[] = [];
  if (isPositiveNumber(usage.contextTokens)) parts.push(`${formatCompactNumber(usage.contextTokens)} ctx`);
  if (isPositiveNumber(usage.cost)) parts.push(`$${usage.cost.toFixed(4)}`);
  return parts.length > 0 ? parts.join(" · ") : undefined;
}

function contextTurnUsageParts(usage: Partial<WorkerUsage>): string[] {
  const parts: string[] = [];
  if (isPositiveNumber(usage.contextTokens)) parts.push(`${formatCompactNumber(usage.contextTokens)} ctx`);
  if (isPositiveNumber(usage.turns)) {
    parts.push(`${usage.turns} ${usage.turns === 1 ? "turn" : "turns"}`);
  }
  return parts;
}

function formatCompactNumber(value: number): string {
  if (value >= 1_000_000) return `${trimDecimal(value / 1_000_000)}m`;
  if (value >= 1_000) return `${trimDecimal(value / 1_000)}k`;
  return String(Math.round(value));
}

function trimDecimal(value: number): string {
  return value.toFixed(1).replace(/\.0$/, "");
}

function statusIcon(status: string): string {
  switch (status) {
    case "starting":
      return "◌";
    case "running":
      return "●";
    case "stopping":
      return "◍";
    case "ready":
      return "○";
    case "completed":
      return "✓";
    case "failed":
      return "✗";
    case "aborted":
      return "■";
    case "closed":
      return "×";
    default:
      return "·";
  }
}

function isPositiveNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
