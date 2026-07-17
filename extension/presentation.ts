import {
  getMarkdownTheme,
  keyHint,
  type ExtensionAPI,
  type ExtensionContext,
  type Theme,
} from "@earendil-works/pi-coding-agent";
import {
  Box,
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

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] as const;
const SPINNER_INTERVAL_MS = 80;

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

interface RenderRequester {
  requestRender(): void;
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
      return expanded
        ? expandedResult(content, details, theme)
        : collapsedResult(content, details, theme);
    },
  );
}

export function formatResultStatusSummary(details: unknown): string {
  const parsed = readDeliveryDetails(details);
  if (!parsed) return "Result details unavailable";

  const resultWord = parsed.results.length === 1 ? "reply" : "replies";
  if (parsed.results.length === 0) return `0 ${resultWord}`;

  const counts = new Map<string, number>();
  for (const result of parsed.results) {
    const status = KNOWN_RESULT_STATUSES.has(result.status) ? result.status : "unknown";
    counts.set(status, (counts.get(status) ?? 0) + 1);
  }

  const statusOrder = ["completed", "ready", "failed", "aborted", "closed", "unknown"];
  const statuses = statusOrder.flatMap((status) => {
    const count = counts.get(status);
    return count === undefined ? [] : [`${count} ${resultStatusLabel(status)}`];
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
    const label = `${statusIcon(result.status)} ${result.worker} ← ${result.title}`;
    const outcome = outcomePreview(result);
    return outcome ? `${label} — ${outcome}` : `${label} · ${resultStatusLabel(result.status)}`;
  });

  if (parsed.results.length > limit && previews.length > 0) {
    previews[previews.length - 1] = `… ${parsed.results.length - limit + 1} more replies`;
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
    workerStateLabel(worker),
    formatContextCost(worker.usage),
  ].filter((part): part is string => Boolean(part));

  return `${worker.worker} ${workerDirection(worker.status)} ${worker.title} · ${metadata.join(" · ")}`;
}

export function formatFooterStatus(snapshot: RuntimeSnapshot): string | undefined {
  const active = snapshot.workers.filter((worker) => ACTIVE_STATUSES.has(worker.status)).length;
  const ready = snapshot.workers.filter((worker) => worker.status === "ready").length;
  if (active === 0 && ready === 0) return undefined;

  const parts = [];
  if (active > 0) parts.push(`${active} working`);
  if (ready > 0) parts.push(`${ready} ready`);
  return `Workers: ${parts.join(" · ")}`;
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
    ctx.ui.setStatus(ORCHESTRATION_PRESENTATION_KEY, formatFooterStatus(snapshot));

    if (ctx.mode !== "tui") return;
    if (widgetWorkerCount(snapshot) === 0) {
      ctx.ui.setWidget(ORCHESTRATION_PRESENTATION_KEY, undefined);
      return;
    }

    ctx.ui.setWidget(
      ORCHESTRATION_PRESENTATION_KEY,
      (tui, theme) => new WorkerStatusComponent(snapshot, theme, tui),
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

interface WaveGroup {
  readonly wave: WaveRecord;
  readonly workers: readonly WorkerRecord[];
  readonly settled: number;
}

export class WorkerStatusComponent implements Component {
  private frameIndex = 0;
  private readonly timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly snapshot: RuntimeSnapshot,
    private readonly theme: Theme,
    private readonly tui?: RenderRequester,
  ) {
    if (!snapshot.workers.some((worker) => ACTIVE_STATUSES.has(worker.status))) return;
    this.timer = setInterval(() => {
      this.frameIndex = (this.frameIndex + 1) % SPINNER_FRAMES.length;
      this.tui?.requestRender();
    }, SPINNER_INTERVAL_MS);
    (this.timer as unknown as { unref?: () => void }).unref?.();
  }

  render(width: number): string[] {
    const boundedWidth = Math.max(1, width);
    const groups = activeWaveGroups(this.snapshot);
    const groupedWorkerIds = new Set(groups.flatMap((group) => group.workers.map((worker) => worker.id)));
    const ready = readyWorkers(this.snapshot).filter((worker) => !groupedWorkerIds.has(worker.id));
    const totalWorkers = groups.reduce((total, group) => total + group.workers.length, 0) + ready.length;
    const lines: string[] = [];
    let remaining = MAX_WIDGET_WORKERS;
    let shown = 0;

    groups.forEach((group, index) => {
      if (remaining === 0) return;
      const visibleWorkers = group.workers.slice(0, remaining);
      if (visibleWorkers.length === 0) return;
      lines.push(this.waveHeader(group, groups.length > 1 ? index + 1 : undefined));
      for (const worker of visibleWorkers) lines.push(this.workerLine(worker, boundedWidth));
      shown += visibleWorkers.length;
      remaining -= visibleWorkers.length;
    });

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

  dispose(): void {
    if (this.timer !== undefined) clearInterval(this.timer);
  }

  private waveHeader(group: WaveGroup, waveNumber?: number): string {
    const label = waveNumber === undefined ? "Workers" : `Workers · wave ${waveNumber}`;
    return this.theme.fg(
      "toolTitle",
      this.theme.bold(`${label} · ${group.settled}/${group.wave.workerIds.length} replied`),
    );
  }

  private workerLine(worker: WorkerRecord, width: number): string {
    const icon = this.workerIcon(worker.status);
    const workerType = this.theme.fg("muted", worker.worker);
    const direction = this.theme.fg(ACTIVE_STATUSES.has(worker.status) ? "accent" : "success", workerDirection(worker.status));
    const title = this.theme.fg("text", this.theme.bold(worker.title));
    const activity = this.theme.fg("text", workerStateLabel(worker));
    const usage = formatContextCost(worker.usage);
    const dimUsage = usage ? this.theme.fg("dim", usage) : undefined;
    const identity = `${icon} ${workerType} ${direction} ${title}`;
    const variants = [
      [identity, activity, dimUsage],
      [identity, activity],
      [identity],
    ].map((parts) => parts.filter((part): part is string => Boolean(part)).join(" · "));
    const fitting = variants.find((line) => visibleWidth(line) <= width);
    if (fitting) return fitting;

    const suffix = ` · ${activity}`;
    const identityWidth = Math.max(1, width - visibleWidth(suffix));
    return `${truncateToWidth(identity, identityWidth, "…")}${suffix}`;
  }

  private workerIcon(status: WorkerStatus): string {
    if (ACTIVE_STATUSES.has(status)) {
      return this.theme.fg("warning", SPINNER_FRAMES[this.frameIndex] ?? SPINNER_FRAMES[0]);
    }
    const color = status === "failed" || status === "aborted" ? "error" : "success";
    return this.theme.fg(color, statusIcon(status));
  }
}

function collapsedResult(
  content: string,
  details: SafeDetails | undefined,
  theme: Theme,
): Component {
  const box = new Box(1, 1, (text) => theme.bg("customMessageBg", text));
  const count = details?.results.length;
  const heading = count === undefined
    ? "Worker replies"
    : `${count} ${count === 1 ? "worker replied" : "workers replied"}`;
  box.addChild(new Text(theme.fg("success", theme.bold(`✓ ${heading}`)), 0, 0));
  box.addChild(new Text(theme.fg("muted", formatResultStatusSummary(details)), 0, 0));

  const previews = formatResultPreviews(details, content);
  if (previews.length > 0) box.addChild(new Spacer(1));
  for (const line of previews) box.addChild(new Text(theme.fg("customMessageText", line), 0, 0));
  box.addChild(new Spacer(1));
  box.addChild(new Text(theme.fg("dim", keyHint("app.tools.expand", "to expand replies")), 0, 0));
  return box;
}

function expandedResult(
  content: string,
  details: SafeDetails | undefined,
  theme: Theme,
): Component {
  const box = new Box(1, 1, (text) => theme.bg("customMessageBg", text));
  const count = details?.results.length;
  const heading = count === undefined
    ? "Worker replies"
    : `${count} ${count === 1 ? "worker replied" : "workers replied"}`;
  box.addChild(new Text(theme.fg("success", theme.bold(`✓ ${heading}`)), 0, 0));
  box.addChild(new Text(theme.fg("muted", formatResultStatusSummary(details)), 0, 0));
  box.addChild(new Spacer(1));

  if (!details) {
    box.addChild(new Markdown(content, 0, 0, getMarkdownTheme()));
    box.addChild(new Spacer(1));
    box.addChild(new Text(theme.fg("dim", "Structured worker metadata unavailable"), 0, 0));
    return box;
  }

  box.addChild(new Markdown(reconstructOutcomeMarkdown(details), 0, 0, getMarkdownTheme()));
  box.addChild(new Spacer(1));
  box.addChild(new Text(theme.fg("toolTitle", theme.bold("Worker details")), 0, 0));

  for (const result of details.results) {
    const usage = formatWorkerUsage(result.usage) ?? "unavailable";
    const session = result.sessionFile ?? "unavailable";
    box.addChild(new Text(theme.fg("text", `${result.worker} — ${result.title}`), 0, 0));
    box.addChild(
      new Text(
        `${theme.fg("muted", "ID")} ${result.workerId} · ${theme.fg("muted", "status")} ${result.status}`,
        0,
        0,
      ),
    );
    box.addChild(new Text(theme.fg("dim", `usage ${usage}`), 0, 0));
    box.addChild(new Text(theme.fg("dim", `session ${session}`), 0, 0));
  }
  return box;
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
  if (result.status === "failed" || result.status === "aborted") return message;
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
  if (worker.status === "completed") return "replied";
  if (worker.status !== "running") return worker.status;
  if (!worker.activity?.trim()) return "thinking";
  return TOOL_ACTIVITY[worker.activity] ?? worker.activity;
}

function workerDirection(status: WorkerStatus): "→" | "←" {
  return ACTIVE_STATUSES.has(status) ? "→" : "←";
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

function resultStatusLabel(status: string): string {
  switch (status) {
    case "completed":
      return "completed";
    case "ready":
      return "ready";
    case "failed":
      return "failed";
    case "aborted":
      return "aborted";
    case "closed":
      return "closed";
    default:
      return "unknown";
  }
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
