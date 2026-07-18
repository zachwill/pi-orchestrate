import {
  getMarkdownTheme,
  keyHint,
  type ExtensionAPI,
  type ExtensionContext,
  type Theme,
} from "@earendil-works/pi-coding-agent";
import {
  Box,
  Markdown,
  Spacer,
  Text,
  truncateToWidth,
  visibleWidth,
  type Component,
} from "@earendil-works/pi-tui";
import type { WorkerDeliveryDetails } from "./delivery.js";
import type { WorkerOutcome, WorkerRecord, WorkerStatus, WorkerUsage } from "./domain.js";
import type { OrchestratorRuntime, RuntimeSnapshot } from "./runtime.js";

export const ORCHESTRATION_PRESENTATION_KEY = "pi-orchestrate";
export const MAX_RESULT_PREVIEW_LINES = 6;
export const MAX_WIDGET_WORKERS = 8;

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] as const;
const SPINNER_INTERVAL_MS = 140;
const ACTIVE_STATUSES: ReadonlySet<WorkerStatus> = new Set(["starting", "running", "stopping"]);

export type PresentationRuntime = Pick<OrchestratorRuntime, "snapshot" | "subscribeState">;

interface SafeSettlement {
  eventId?: string;
  sequence?: number;
  ownerSessionId: string;
  waveId: string;
  workerId: string;
  generation: number;
  mode: "async" | "inline";
  worker: string;
  title: string;
  lifecycle: "one-shot" | "reusable";
  status: "completed" | "ready" | "failed" | "aborted";
  outcome: Exclude<WorkerOutcome, { status: "closed" }>;
  usage: Partial<WorkerUsage>;
  startedAt?: number;
  settledAt?: number;
  remainingActive?: number;
  waveComplete?: boolean;
  sessionFile?: string;
  failureStage?: "startup" | "prompt" | "workflow" | "cancellation";
}

interface StatusBinding {
  readonly ownerSessionId: string;
  readonly ctx: ExtensionContext;
}

interface RenderRequester { requestRender(): void }

export function registerOrchestrationPresentation(pi: ExtensionAPI): void {
  pi.registerMessageRenderer<WorkerDeliveryDetails>(
    "pi-orchestrate-worker-result",
    (message, { expanded }, theme) =>
      new WorkerResultComponent(messageText(message.content), message.details, expanded, theme),
  );
}

export function formatResultStatusSummary(details: unknown): string {
  const result = readSettlement(details);
  return result ? statusHeading(result) : "Worker result details unavailable";
}

export function formatResultPreviews(
  details: unknown,
  fallbackContent = "",
  limit = MAX_RESULT_PREVIEW_LINES,
): string[] {
  if (limit <= 0) return [];
  const result = readSettlement(details);
  const body = result ? outcomeText(result.outcome) : fallbackContent;
  return firstNonEmptyLines(body, limit);
}

export function formatWorkerUsage(usage: Partial<WorkerUsage> | undefined): string | undefined {
  if (!usage) return undefined;
  const parts = [
    `${numberOrZero(usage.turns)}t`,
    `${formatCompactNumber(numberOrZero(usage.contextTokens))} ctx`,
    `↑${formatCompactNumber(numberOrZero(usage.input))}`,
    `↓${formatCompactNumber(numberOrZero(usage.output))}`,
    `R${formatCompactNumber(numberOrZero(usage.cacheRead))}`,
    `W${formatCompactNumber(numberOrZero(usage.cacheWrite))}`,
    `$${numberOrZero(usage.cost).toFixed(4)}`,
  ];
  return parts.join(" · ");
}

export function formatWorkerStatusLine(worker: WorkerRecord): string {
  return `${worker.worker} → ${worker.title} · ${workerStateLabel(worker)} · ${compactLiveUsage(worker.usage)}`;
}

export function formatFooterStatus(snapshot: RuntimeSnapshot): string | undefined {
  const ready = snapshot.workers.filter((worker) => worker.status === "ready").length;
  return ready > 0 ? `${ready} available for follow-up` : undefined;
}

export class StatusController {
  private binding: StatusBinding | undefined;
  private bindingGeneration = 0;
  private refreshGeneration = 0;
  private disposed = false;
  private unsubscribeState: (() => void) | undefined;
  private widget: WorkerStatusComponent | undefined;
  private widgetInstalled = false;
  private pendingSnapshot: RuntimeSnapshot | undefined;

  constructor(private readonly runtime: PresentationRuntime) {}

  bind(ownerSessionId: string, ctx: ExtensionContext): void {
    if (this.disposed) return;
    this.clearBinding();
    this.bindingGeneration += 1;
    this.binding = { ownerSessionId, ctx };
    this.unsubscribeState = this.runtime.subscribeState((changedOwner) => {
      if (changedOwner === this.binding?.ownerSessionId) void this.refresh();
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
    try { snapshot = await this.runtime.snapshot(binding.ownerSessionId); } catch { return; }
    if (this.disposed || this.binding !== binding || this.bindingGeneration !== bindingGeneration || this.refreshGeneration !== refreshGeneration) return;
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
    const active = activeWorkers(snapshot);
    this.pendingSnapshot = snapshot;
    if (active.length === 0) {
      if (this.widgetInstalled) ctx.ui.setWidget(ORCHESTRATION_PRESENTATION_KEY, undefined);
      this.widget = undefined;
      this.widgetInstalled = false;
      return;
    }
    if (this.widget) {
      this.widget.update(snapshot);
      return;
    }
    if (this.widgetInstalled) return;
    ctx.ui.setWidget(ORCHESTRATION_PRESENTATION_KEY, (tui, theme) => {
      this.widget = new WorkerStatusComponent(this.pendingSnapshot ?? snapshot, theme, tui);
      return this.widget;
    }, { placement: "aboveEditor" });
    this.widgetInstalled = true;
  }

  private clearBinding(): void {
    this.unsubscribeState?.();
    this.unsubscribeState = undefined;
    this.widget = undefined;
    this.pendingSnapshot = undefined;
    const current = this.binding;
    if (!current) return;
    current.ctx.ui.setStatus(ORCHESTRATION_PRESENTATION_KEY, undefined);
    if (current.ctx.mode === "tui" && this.widgetInstalled) current.ctx.ui.setWidget(ORCHESTRATION_PRESENTATION_KEY, undefined);
    this.widgetInstalled = false;
    this.binding = undefined;
  }
}

export function createStatusController(runtime: PresentationRuntime): StatusController {
  return new StatusController(runtime);
}

export class WorkerStatusComponent implements Component {
  private frameIndex = 0;
  private snapshot: RuntimeSnapshot;
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(snapshot: RuntimeSnapshot, private readonly theme: Theme, private readonly tui?: RenderRequester) {
    this.snapshot = snapshot;
    this.startTimer();
  }

  update(snapshot: RuntimeSnapshot): void {
    this.snapshot = snapshot;
    if (activeWorkers(snapshot).length > 0) this.startTimer();
    else this.stopTimer();
    this.tui?.requestRender();
  }

  render(width: number): string[] {
    const boundedWidth = Math.max(1, width);
    const active = activeWorkers(this.snapshot);
    if (active.length === 0) return [];
    const oldest = Math.min(...active.map((worker) => worker.startedAt));
    const elapsed = formatElapsed(Math.max(0, Date.now() - oldest));
    const lines = [this.theme.fg("toolTitle", this.theme.bold(`Workers · ${active.length} active · ${elapsed}`))];
    for (const worker of active.slice(0, MAX_WIDGET_WORKERS)) lines.push(this.workerLine(worker, boundedWidth));
    if (active.length > MAX_WIDGET_WORKERS) lines.push(this.theme.fg("dim", `… ${active.length - MAX_WIDGET_WORKERS} more active`));
    return lines.map((line) => truncateToWidth(line, boundedWidth, "…"));
  }

  invalidate(): void {}
  dispose(): void { this.stopTimer(); }

  private startTimer(): void {
    if (this.timer || activeWorkers(this.snapshot).length === 0) return;
    this.timer = setInterval(() => {
      this.frameIndex = (this.frameIndex + 1) % SPINNER_FRAMES.length;
      this.tui?.requestRender();
    }, SPINNER_INTERVAL_MS);
    const timer = this.timer;
    if (typeof timer === "object" && timer !== null && "unref" in timer && typeof timer.unref === "function") timer.unref();
  }

  private stopTimer(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = undefined;
  }

  private workerLine(worker: WorkerRecord, width: number): string {
    const glyph = this.theme.fg("warning", SPINNER_FRAMES[this.frameIndex] ?? SPINNER_FRAMES[0]);
    const turns = `${numberOrZero(worker.usage?.turns)}t`;
    const context = `${formatCompactNumber(numberOrZero(worker.usage?.contextTokens))} ctx`;
    const suffixFields = width >= 28 ? [turns, context] : width >= 12 ? [turns] : [];
    const activity = workerStateLabel(worker);
    const requiredWidth = (fields: readonly string[]) => visibleWidth(`⠋  · ${fields.join(" · ")}`) + 10;
    if (width >= 42 && requiredWidth([activity, ...suffixFields]) <= width) suffixFields.unshift(activity);
    const showWorker = width >= 72 && requiredWidth([worker.worker, ...suffixFields]) <= width;
    const prefix = showWorker ? `${glyph} ${this.theme.fg("muted", worker.worker)} → ` : `${glyph} `;
    const suffix = suffixFields.length ? ` · ${suffixFields.join(" · ")}` : "";
    const titleWidth = Math.max(1, width - visibleWidth(prefix) - visibleWidth(suffix));
    const title = truncateToWidth(this.theme.fg("text", this.theme.bold(worker.title)), titleWidth, "…");
    return `${prefix}${title}${suffix}`;
  }
}

class WidthBoundComponent implements Component {
  constructor(private readonly child: Component, private readonly maxLines?: number) {}
  render(width: number): string[] {
    const bounded = Math.max(1, Math.floor(width));
    const lines = this.child.render(bounded);
    const selected = this.maxLines === undefined ? lines : lines.slice(0, this.maxLines);
    return selected.map((line) => truncateToWidth(line, bounded, "…"));
  }
  invalidate(): void { this.child.invalidate(); }
  dispose(): void { (this.child as Component & { dispose?: () => void }).dispose?.(); }
}

export class WorkerResultComponent implements Component {
  private child: Component;

  constructor(
    private readonly content: string,
    private readonly rawDetails: unknown,
    private readonly expanded: boolean,
    private readonly theme: Theme,
  ) {
    this.child = this.build();
  }

  render(width: number): string[] { return new WidthBoundComponent(this.child).render(width); }
  invalidate(): void {
    (this.child as Component & { dispose?: () => void }).dispose?.();
    this.child = this.build();
  }
  dispose(): void { (this.child as Component & { dispose?: () => void }).dispose?.(); }

  private build(): Component {
    const details = readSettlement(this.rawDetails);
    const box = new Box(1, 1, (text) => this.theme.bg("customMessageBg", text));
    if (!details) {
      box.addChild(new Text(this.theme.fg("warning", this.theme.bold("Worker result details unavailable")), 0, 0));
      box.addChild(new WidthBoundComponent(new Markdown(this.content, 0, 0, getMarkdownTheme()), this.expanded ? undefined : MAX_RESULT_PREVIEW_LINES));
      if (!this.expanded) box.addChild(new Text(this.theme.fg("dim", keyHint("app.tools.expand", "to expand")), 0, 0));
      return box;
    }

    const color = details.status === "failed" ? "error" : details.status === "aborted" ? "warning" : "success";
    const elapsed = elapsedBetween(details.startedAt, details.settledAt);
    const status = details.status === "aborted"
      ? " · aborted"
      : details.status === "failed" && details.failureStage === "startup"
        ? " · could not start"
        : "";
    const header = `${statusIcon(details)} ${details.worker} · ${details.title}${status}${elapsed ? ` · ${elapsed}` : ""}`;
    box.addChild(new Text(this.theme.fg(color, this.theme.bold(header)), 0, 0));
    box.addChild(new Spacer(1));
    box.addChild(new WidthBoundComponent(new Markdown(outcomeText(details.outcome), 0, 0, getMarkdownTheme()), this.expanded ? undefined : MAX_RESULT_PREVIEW_LINES));
    if (!this.expanded) {
      box.addChild(new Spacer(1));
      box.addChild(new Text(this.theme.fg("dim", keyHint("app.tools.expand", "to expand")), 0, 0));
      return box;
    }
    box.addChild(new Spacer(1));
    box.addChild(new Text(this.theme.fg("toolTitle", this.theme.bold("Worker details")), 0, 0));
    for (const line of settlementMetadata(details)) box.addChild(new Text(this.theme.fg("dim", line), 0, 0));
    return box;
  }
}

function readSettlement(value: unknown): SafeSettlement | undefined {
  const candidate = isRecord(value) && isRecord(value.settlement) ? value.settlement : value;
  if (!isRecord(candidate)) return undefined;
  const mode = enumField(candidate.mode, ["async", "inline"]);
  const lifecycle = enumField(candidate.lifecycle, ["one-shot", "reusable"]);
  const status = enumField(candidate.status, ["completed", "ready", "failed", "aborted"]);
  const outcome = readOutcome(candidate.outcome);
  const usage = readUsage(candidate.usage);
  const generation = nonnegativeInteger(candidate.generation);
  if (!mode || !lifecycle || !status || !outcome || outcome.status !== status || !usage || generation === undefined) return undefined;
  const ownerSessionId = requiredString(candidate.ownerSessionId);
  const waveId = requiredString(candidate.waveId);
  const workerId = requiredString(candidate.workerId);
  const worker = requiredString(candidate.worker);
  const title = requiredString(candidate.title);
  if (ownerSessionId === undefined || waveId === undefined || workerId === undefined || worker === undefined || title === undefined) return undefined;
  const eventId = optionalString(candidate.eventId);
  const sessionFile = optionalString(candidate.sessionFile);
  const sequence = optionalInteger(candidate.sequence);
  const startedAt = optionalInteger(candidate.startedAt);
  const settledAt = optionalInteger(candidate.settledAt);
  const remainingActive = optionalInteger(candidate.remainingActive);
  const waveComplete = optionalBoolean(candidate.waveComplete);
  const failureStage = optionalEnum(candidate.failureStage, ["startup", "prompt", "workflow", "cancellation"]);
  if (eventId === INVALID || sessionFile === INVALID || sequence === INVALID || startedAt === INVALID || settledAt === INVALID ||
      remainingActive === INVALID || waveComplete === INVALID || failureStage === INVALID) return undefined;
  if (startedAt === undefined || settledAt === undefined || settledAt < startedAt) return undefined;
  if (failureStage !== undefined && status !== "failed" && status !== "aborted") return undefined;
  return {
    ownerSessionId, waveId, workerId, generation, mode, worker, title, lifecycle, status, outcome, usage,
    ...(eventId === undefined ? {} : { eventId }),
    ...(sequence === undefined ? {} : { sequence }),
    ...(startedAt === undefined ? {} : { startedAt }),
    ...(settledAt === undefined ? {} : { settledAt }),
    ...(remainingActive === undefined ? {} : { remainingActive }),
    ...(waveComplete === undefined ? {} : { waveComplete }),
    ...(sessionFile === undefined ? {} : { sessionFile }),
    ...(failureStage === undefined ? {} : { failureStage }),
  };
}

function readOutcome(value: unknown): WorkerOutcome | undefined {
  if (!isRecord(value) || typeof value.status !== "string") return undefined;
  if ((value.status === "completed" || value.status === "ready") && typeof value.assistantText === "string") return { status: value.status, assistantText: value.assistantText };
  const message = optionalString(value.message);
  const assistantText = optionalString(value.assistantText);
  if (message === INVALID || assistantText === INVALID) return undefined;
  if (value.status === "failed" && message !== undefined) return { status: "failed", message, ...(assistantText === undefined ? {} : { assistantText }) };
  if (value.status === "aborted") return { status: "aborted", ...(message === undefined ? {} : { message }), ...(assistantText === undefined ? {} : { assistantText }) };
  return undefined;
}

const INVALID = Symbol("invalid");
type Invalid = typeof INVALID;
function requiredString(value: unknown): string | undefined { return typeof value === "string" ? value : undefined; }
function optionalString(value: unknown): string | undefined | Invalid { return value === undefined ? undefined : typeof value === "string" ? value : INVALID; }
function nonnegativeInteger(value: unknown): number | undefined { return typeof value === "number" && Number.isFinite(value) && Number.isInteger(value) && value >= 0 ? value : undefined; }
function optionalInteger(value: unknown): number | undefined | Invalid { return value === undefined ? undefined : nonnegativeInteger(value) ?? INVALID; }
function optionalBoolean(value: unknown): boolean | undefined | Invalid { return value === undefined ? undefined : typeof value === "boolean" ? value : INVALID; }
function enumField<const T extends string>(value: unknown, values: readonly T[]): T | undefined { return typeof value === "string" && values.includes(value as T) ? value as T : undefined; }
function optionalEnum<const T extends string>(value: unknown, values: readonly T[]): T | undefined | Invalid { return value === undefined ? undefined : enumField(value, values) ?? INVALID; }
function readUsage(value: unknown): WorkerUsage | undefined {
  if (!isRecord(value)) return undefined;
  const input = nonnegativeNumber(value.input);
  const output = nonnegativeNumber(value.output);
  const cacheRead = nonnegativeNumber(value.cacheRead);
  const cacheWrite = nonnegativeNumber(value.cacheWrite);
  const cost = nonnegativeNumber(value.cost);
  const contextTokens = nonnegativeNumber(value.contextTokens);
  const turns = nonnegativeInteger(value.turns);
  if (input === undefined || output === undefined || cacheRead === undefined || cacheWrite === undefined || cost === undefined || contextTokens === undefined || turns === undefined) return undefined;
  return { input, output, cacheRead, cacheWrite, cost, contextTokens, turns };
}
function nonnegativeNumber(value: unknown): number | undefined { return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined; }

function statusHeading(result: SafeSettlement): string {
  if (result.status === "failed") return result.failureStage === "startup" ? "could not start" : "failed";
  if (result.status === "aborted") return "aborted";
  if (result.status === "ready") return "response complete";
  return "completed";
}

function statusIcon(result: SafeSettlement): string {
  if (result.status === "failed") return "✗";
  if (result.status === "aborted") return "■";
  return "✓";
}

function outcomeText(outcome: WorkerOutcome): string {
  if (outcome.status === "completed" || outcome.status === "ready") return outcome.assistantText;
  if (outcome.status === "failed") return outcome.assistantText ? `${outcome.message}\n\n${outcome.assistantText}` : outcome.message;
  if (outcome.status === "aborted") return [outcome.message || "Worker was aborted.", outcome.assistantText].filter(Boolean).join("\n\n");
  return "Worker session closed.";
}

function settlementMetadata(result: SafeSettlement): string[] {
  return [
    `worker ID ${result.workerId} · wave ID ${result.waveId}`,
    `status ${result.status} · generation ${result.generation}`,
    `turns ${numberOrZero(result.usage.turns)} · current context ${formatCompactNumber(numberOrZero(result.usage.contextTokens))}`,
    `input ${numberOrZero(result.usage.input)} · output ${numberOrZero(result.usage.output)} · cache read ${numberOrZero(result.usage.cacheRead)} · cache write ${numberOrZero(result.usage.cacheWrite)} · cost $${numberOrZero(result.usage.cost).toFixed(4)}`,
    `session ${result.sessionFile ?? "unavailable"}`,
  ];
}

function messageText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.flatMap((part) => isRecord(part) && part.type === "text" && typeof part.text === "string" ? [part.text] : []).join("\n");
}

function activeWorkers(snapshot: RuntimeSnapshot): WorkerRecord[] {
  return snapshot.workers.filter((worker) => ACTIVE_STATUSES.has(worker.status));
}

const TOOL_ACTIVITY: Readonly<Record<string, string>> = { read: "reading", grep: "searching", find: "finding files", ls: "listing", bash: "running command", edit: "editing", write: "writing" };
function workerStateLabel(worker: Pick<WorkerRecord, "status" | "activity">): string {
  if (worker.status === "starting" || worker.status === "stopping") return worker.status;
  if (worker.status !== "running") return worker.status;
  if (!worker.activity?.trim()) return "working";
  return TOOL_ACTIVITY[worker.activity] ?? worker.activity;
}

function compactLiveUsage(usage: Partial<WorkerUsage> | undefined): string {
  return `${numberOrZero(usage?.turns)}t · ${formatCompactNumber(numberOrZero(usage?.contextTokens))} ctx`;
}
function elapsedBetween(start?: number, end?: number): string | undefined {
  return start !== undefined && end !== undefined && end >= start ? formatElapsed(end - start) : undefined;
}
function formatElapsed(milliseconds: number): string {
  const seconds = Math.floor(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  return seconds % 60 === 0 ? `${minutes}m` : `${minutes}m ${seconds % 60}s`;
}
function firstNonEmptyLines(text: string, limit: number): string[] {
  return text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean).slice(0, limit);
}
function numberOrZero(value: unknown): number { return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : 0; }
function formatCompactNumber(value: number): string {
  if (value >= 1_000_000) return `${trimDecimal(value / 1_000_000)}m`;
  if (value >= 1_000) return `${trimDecimal(value / 1_000)}k`;
  return String(Math.round(value));
}
function trimDecimal(value: number): string { return value.toFixed(1).replace(/\.0$/, ""); }
function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }
