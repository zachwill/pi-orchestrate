import type { ExtensionContext } from "@mariozechner/pi-coding-agent";
import { Text } from "@mariozechner/pi-tui";
import type { ActiveWorkerSummary, WorkerRuntime } from "./runtime/worker-runtime.js";
import { STATUS_ICON } from "./worker-messages.js";

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const SPINNER_INTERVAL_MS = 80;

interface WidgetState {
  ctx: ExtensionContext;
  ownerSessionId: string;
  text: Text;
  // TUI is provided by the setWidget factory; pi does not export a named type for it.
  tui: { requestRender(): void };
  timer: ReturnType<typeof setInterval>;
  frameIndex: number;
}

let widget: WidgetState | undefined;

function formatTokens(count: number): string {
  if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
  if (count >= 1_000) return `${(count / 1_000).toFixed(1)}k`;
  return count.toString();
}

function formatUsage(worker: ActiveWorkerSummary): string {
  const usage = worker.usage;
  const contextTokens = usage.contextTokens || worker.contextTokens;
  const parts = [`turn ${worker.turns}`];

  if (usage.input) parts.push(`↑${formatTokens(usage.input)}`);
  if (usage.output) parts.push(`↓${formatTokens(usage.output)}`);
  if (usage.cacheRead) parts.push(`R${formatTokens(usage.cacheRead)}`);
  if (usage.cacheWrite) parts.push(`W${formatTokens(usage.cacheWrite)}`);
  if (usage.cost) parts.push(`$${usage.cost.toFixed(4)}`);
  parts.push(`${formatTokens(contextTokens)} ctx`);

  return parts.join(" · ");
}

function buildLine(worker: ActiveWorkerSummary, frame: string): string {
  const model = worker.model ?? "…";
  const icon = worker.status === "waiting" ? "⏳" : worker.status === "running" ? frame : STATUS_ICON[worker.status];
  return `${icon} ${worker.id} (${model}) · ${formatUsage(worker)}`;
}

function hasRunningWorker(workers: ActiveWorkerSummary[]): boolean {
  return workers.some((worker) => worker.status === "running");
}

function disposeWidget(state: WidgetState): void {
  clearInterval(state.timer);
  if (widget === state) widget = undefined;
}

export function stopWidgetAnimation(): void {
  const current = widget;
  if (!current) return;

  disposeWidget(current);
  current.ctx.ui.setWidget("pi-workers", undefined);
  current.ctx.ui.setStatus("pi-workers", undefined);
}

function syncWidgetText(state: WidgetState, workers: ActiveWorkerSummary[]): void {
  const frame = SPINNER_FRAMES[state.frameIndex % SPINNER_FRAMES.length] ?? SPINNER_FRAMES[0]!;
  state.text.setText(["pi-workers", ...workers.map((worker) => buildLine(worker, frame))].join("\n"));
  state.tui.requestRender();
}

export function updateWidget(ctx: ExtensionContext, runtime: WorkerRuntime): void {
  if (!ctx.hasUI) {
    stopWidgetAnimation();
    return;
  }

  const ownerSessionId = ctx.sessionManager.getSessionId();
  const active = runtime.getActiveSummariesForOwner(ownerSessionId);
  if (!active.length) {
    stopWidgetAnimation();
    ctx.ui.setStatus("pi-workers", undefined);
    ctx.ui.setWidget("pi-workers", undefined);
    return;
  }

  ctx.ui.setStatus("pi-workers", `${active.length} worker${active.length === 1 ? "" : "s"}`);

  if (widget && (widget.ctx !== ctx || widget.ownerSessionId !== ownerSessionId)) {
    stopWidgetAnimation();
  }

  if (widget) {
    syncWidgetText(widget, active);
    return;
  }

  ctx.ui.setWidget("pi-workers", (tui, _theme) => {
    const text = new Text("", 1, 0);
    const state: WidgetState = {
      ctx,
      ownerSessionId,
      text,
      tui,
      frameIndex: 0,
      timer: setInterval(() => {
        const workers = runtime.getActiveSummariesForOwner(ownerSessionId);
        if (!workers.length) {
          stopWidgetAnimation();
          return;
        }
        if (!hasRunningWorker(workers)) return;
        state.frameIndex++;
        syncWidgetText(state, workers);
      }, SPINNER_INTERVAL_MS),
    };

    widget = state;
    syncWidgetText(state, active);

    return Object.assign(text, {
      dispose() {
        disposeWidget(state);
      },
    });
  });
}
