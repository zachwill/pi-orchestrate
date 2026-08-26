import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { Theme } from "@earendil-works/pi-coding-agent";
import {
  getMarkdownTheme,
  keyHint,
} from "@earendil-works/pi-coding-agent";
import {
  Container,
  Markdown,
  Spacer,
  Text,
  truncateToWidth,
  wrapTextWithAnsi,
  type Component,
} from "@earendil-works/pi-tui";
import { Result, Schema } from "effect";
import {
  disposeComponent,
  formatElapsed,
  resultAppearance,
  WidthBoundComponent,
} from "./tui.ts";
import {
  decodeInlineWorkerToolDetails,
  type InlineWorkerSettlementDetails,
  WorkerSettlement,
} from "../orchestration/settlement.ts";

const MAX_INSTRUCTION_PREVIEW_LINES = 2;

const AcceptedRunRenderDetails = Schema.Struct({
  mode: Schema.Literal("async"),
  run_id: WorkerSettlement.fields.runId,
  worker_id: WorkerSettlement.fields.workerId,
});
const UnavailableWorkerRenderDetails = Schema.Union([
  Schema.Struct({ result: Schema.Unknown }),
  Schema.Struct({ worker_id: Schema.Unknown }),
]);
const decodeAcceptedRunRenderDetails = Schema.decodeUnknownResult(
  AcceptedRunRenderDetails,
);
const decodeUnavailableWorkerRenderDetails = Schema.decodeUnknownResult(
  UnavailableWorkerRenderDetails,
);

interface RenderOptions {
  readonly isPartial: boolean;
  readonly expanded: boolean;
}

interface RenderContext {
  readonly isError: boolean;
  readonly lastComponent?: unknown;
}

export const orchestrateToolRenderer = {
  renderCall(args: unknown, theme: Theme, { expanded }: RenderOptions) {
    return renderDispatchCall(theme, args, expanded);
  },
  renderResult(
    result: AgentToolResult<unknown>,
    { isPartial, expanded }: RenderOptions,
    theme: Theme,
    context: RenderContext,
  ) {
    return renderOrchestrationResult(
      result,
      isPartial,
      expanded,
      theme,
      context.isError,
      context.lastComponent,
    );
  },
};

export const workerStatusToolRenderer = {
  renderCall(_args: unknown, theme: Theme) {
    return new Text(theme.fg("toolTitle", theme.bold("worker_status")), 0, 0);
  },
  renderResult(
    result: AgentToolResult<unknown>,
    { isPartial }: RenderOptions,
    theme: Theme,
    context: RenderContext,
  ) {
    return renderDiagnosticsResult(result, isPartial, context.isError, theme);
  },
};

export const interactiveSendToolRenderer = {
  renderCall(args: unknown, theme: Theme, { expanded }: RenderOptions) {
    const fields = objectFields(args);
    return renderInteractiveMessageCall(
      theme,
      "interactive_send",
      fields.worker_id,
      fields.instructions,
      expanded,
    );
  },
  renderResult: orchestrateToolRenderer.renderResult,
};

export const workerAbortToolRenderer = {
  renderCall(args: unknown, theme: Theme) {
    const fields = objectFields(args);
    const workerIds = Array.isArray(fields.worker_ids)
      ? fields.worker_ids
      : undefined;
    const target = workerIds
      ? `${workerIds.length} worker${workerIds.length === 1 ? "" : "s"}`
      : fields.all === true ? "all workers" : "";
    return renderCompactCall(theme, "worker_abort", target);
  },
  renderResult(
    result: AgentToolResult<unknown>,
    { isPartial }: RenderOptions,
    theme: Theme,
    context: RenderContext,
  ) {
    return renderSimpleResult(
      result,
      context.isError,
      isPartial ? "Requesting worker stop…" : "Worker stop requested",
      theme,
      "warning",
    );
  },
};

export const interactiveCloseToolRenderer = {
  renderCall(args: unknown, theme: Theme) {
    return renderCompactCall(
      theme,
      "interactive_close",
      objectFields(args).worker_id,
    );
  },
  renderResult(
    result: AgentToolResult<unknown>,
    { isPartial }: RenderOptions,
    theme: Theme,
    context: RenderContext,
  ) {
    return renderSimpleResult(
      result,
      context.isError,
      isPartial ? "Closing worker…" : "✓ Worker closed",
      theme,
    );
  },
};

function renderDispatchCall(
  theme: Theme,
  task: unknown,
  expanded: boolean,
): Component {
  const fields = isRecord(task) ? task : {};
  const container = new Container();
  container.addChild(new Text(
    theme.fg("toolTitle", theme.bold("orchestrate ")) + theme.fg("muted", safeTerminalText(fields.worker)),
    0, 0,
  ));
  container.addChild(new Text(
    `${theme.fg("accent", "→")} ${theme.fg("text", theme.bold(safeTerminalText(fields.title)))}`,
    0, 0,
  ));
  if (expanded) {
    container.addChild(new Text(safeTerminalText(fields.instructions), 2, 0));
    return new WidthBoundComponent(container);
  }
  container.addChild(new InstructionPreview(fields.instructions, theme));
  container.addChild(new Text(theme.fg("dim", keyHint("app.tools.expand", "to inspect full instructions")), 0, 0));
  return new WidthBoundComponent(container);
}

class InstructionPreview implements Component {
  constructor(
    private readonly instructions: unknown,
    private readonly theme: Theme,
  ) {}
  render(width: number): string[] {
    const bounded = Math.max(1, width);
    const contentWidth = Math.max(1, bounded - 2);
    const characterLimit = Math.max(256, Math.min(4096, contentWidth * 3));
    const preview = compactInstructionPreview(this.instructions, characterLimit);
    if (!preview.text) return [];

    const wrapped = wrapTextWithAnsi(preview.text, contentWidth);
    const previewLines = wrapped.slice(0, MAX_INSTRUCTION_PREVIEW_LINES);
    if (preview.truncated || wrapped.length > MAX_INSTRUCTION_PREVIEW_LINES) {
      const lastIndex = previewLines.length - 1;
      previewLines[lastIndex] = truncateToWidth(`${previewLines[lastIndex] ?? ""}…`, contentWidth, "…");
    }
    return previewLines.map((line) =>
      truncateToWidth(this.theme.fg("dim", `  ${line}`), bounded, "…")
    );
  }
  invalidate(): void {}
}

function renderInteractiveMessageCall(
  theme: Theme,
  tool: string,
  workerId: unknown,
  instructions: unknown,
  expanded: boolean,
): Component {
  const container = new Container();
  container.addChild(new Text(theme.fg("toolTitle", theme.bold(`${tool} `)) + theme.fg("muted", safeTerminalText(workerId)), 0, 0));
  if (expanded) container.addChild(new Text(safeTerminalText(instructions), 2, 0));
  else {
    container.addChild(new Text(`${theme.fg("accent", "→")} ${truncateInstruction(instructions, 240)}`, 0, 0));
    container.addChild(new Text(theme.fg("dim", keyHint("app.tools.expand", "to inspect full message")), 0, 0));
  }
  return new WidthBoundComponent(container);
}

function safeTerminalText(value: unknown): string {
  const text = typeof value === "string" ? value : value == null ? "" : String(value);
  return text.replace(/\r\n?/g, "\n").replace(/\t/g, "    ").replace(/[\x00-\x08\x0B-\x1F\x7F]/g, (character) => {
    const code = character.charCodeAt(0);
    return code === 0x7f ? "␡" : String.fromCodePoint(0x2400 + code);
  });
}

function compactInstructionPreview(instructions: unknown, characterLimit: number): { text: string; truncated: boolean } {
  const text = typeof instructions === "string" ? instructions : instructions == null ? "" : String(instructions);
  const source = text.slice(0, characterLimit);
  return {
    text: safeTerminalText(source).replace(/\s+/g, " ").trim(),
    truncated: source.length < text.length,
  };
}

function firstInstructionLine(instructions: unknown): string | undefined {
  const text = typeof instructions === "string" ? instructions : instructions == null ? "" : String(instructions);
  return text.split(/\r\n?|\n/).find((line) => line.trim().length > 0);
}

function truncateInstruction(instructions: unknown, limit: number): string {
  const first = firstInstructionLine(instructions) ?? "";
  return first.length > limit ? `${first.slice(0, limit - 1)}…` : first;
}

function renderCompactCall(theme: Theme, tool: string, target: unknown): Text {
  return new Text(
    theme.fg("toolTitle", theme.bold(`${tool} `)) + theme.fg("muted", safeTerminalText(target)),
    0,
    0,
  );
}

function renderOrchestrationResult(
  result: AgentToolResult<unknown>,
  isPartial: boolean,
  expanded: boolean,
  theme: Theme,
  isError: boolean,
  lastComponent: unknown,
): Component {
  if (isError) {
    return new WidthBoundComponent(renderSimpleResult(
      result,
      true,
      firstResultLine(result) || "Worker operation failed",
      theme,
      "warning",
    ));
  }
  const details = result.details;
  if (Result.isSuccess(decodeAcceptedRunRenderDetails(details))) {
    return new WidthBoundComponent(new Text(theme.fg("success", "Sent to worker") + theme.fg("dim", " · response arrives when complete"), 0, 0));
  }
  const inlineResult = readInlineResult(details);
  if (inlineResult) {
    const component = lastComponent instanceof InlineResultComponent
      ? lastComponent
      : new InlineResultComponent(theme);
    component.update(inlineResult, isPartial, expanded);
    return component;
  }
  if (Result.isSuccess(decodeUnavailableWorkerRenderDetails(details))) {
    return new WidthBoundComponent(new Text(theme.fg("warning", "Worker result details unavailable"), 0, 0));
  }
  if (isPartial) return new WidthBoundComponent(new Text(theme.fg("warning", "Sending work…"), 0, 0));
  return new WidthBoundComponent(renderSimpleResult(
    result,
    false,
    firstResultLine(result) || "Work sent",
    theme,
    "warning",
  ));
}

interface RenderedInlineSettlement {
  worker: string;
  title: string;
  status: InlineWorkerSettlementDetails["status"];
  response: string;
  elapsed?: string;
}

class InlineResultComponent implements Component {
  private result: RenderedInlineSettlement | undefined;
  private partial = false;
  private expanded = false;
  private child: Component = new Container();
  constructor(private readonly theme: Theme) {}
  update(result: RenderedInlineSettlement, partial: boolean, expanded: boolean): void {
    this.result = result;
    this.partial = partial;
    this.expanded = expanded;
    this.rebuild();
  }
  render(width: number): string[] { return new WidthBoundComponent(this.child).render(width); }
  invalidate(): void { this.rebuild(); }
  dispose(): void { disposeComponent(this.child); }
  private rebuild(): void {
    disposeComponent(this.child);
    const container = new Container();
    const result = this.result;
    if (!result) {
      this.child = container;
      return;
    }
    const appearance = resultAppearance(result.status, "ready for follow-up");
    const suffix = [appearance.qualifier, result.elapsed].filter(Boolean).join(" · ");
    const title = this.theme.bold(result.title);
    const workerName = this.theme.fg("muted", this.theme.italic(result.worker));
    const header = [
      this.theme.fg(appearance.color, `${appearance.icon} ${title}`),
      workerName,
      ...(suffix ? [this.theme.fg(appearance.color, suffix)] : []),
    ].join(" · ");
    container.addChild(new WidthBoundComponent(new Text(header, 0, 0), 1));
    if (result.response) {
      const markdown = new Markdown(result.response, this.expanded ? 2 : 0, 0, getMarkdownTheme());
      container.addChild(new WidthBoundComponent(markdown, this.expanded ? undefined : 2));
    }
    container.addChild(new Spacer(1));
    if (this.partial) container.addChild(new Text(this.theme.fg("warning", "Receiving worker response…"), 0, 0));
    else if (!this.expanded) container.addChild(new Text(this.theme.fg("dim", keyHint("app.tools.expand", "to inspect full response")), 0, 0));
    this.child = container;
  }
}

function readInlineResult(details: unknown): RenderedInlineSettlement | undefined {
  const decoded = decodeInlineWorkerToolDetails(details);
  if (Result.isFailure(decoded)) return undefined;
  const settlement = decoded.success.result;
  const outcome = settlement.outcome;
  const message = outcome.status === "failed" || outcome.status === "aborted"
    ? outcome.message
    : undefined;
  const assistantText = outcome.assistantText;
  const response = [message, assistantText]
    .filter((item): item is string => typeof item === "string" && item.length > 0)
    .join("\n\n");
  return {
    worker: settlement.worker,
    title: settlement.title,
    status: settlement.status,
    response,
    elapsed: formatElapsed(settlement.settledAt - settlement.startedAt),
  };
}

function renderDiagnosticsResult(
  result: AgentToolResult<unknown>,
  isPartial: boolean,
  isError: boolean,
  theme: Theme,
): Text {
  if (isError) {
    return renderSimpleResult(
      result,
      true,
      firstResultLine(result) || "Worker diagnostics failed",
      theme,
    );
  }
  if (isPartial) return new Text(theme.fg("muted", "Reading worker diagnostics…"), 0, 0);
  const details = result.details;
  if (isRecord(details) && isRecord(details.state) && Array.isArray(details.state.workers)) {
    const workers = details.state.workers.filter(isRecord);
    const active = workers.filter((worker) => ["starting", "running", "stopping"].includes(String(worker.status))).length;
    const ready = workers.filter((worker) => worker.status === "ready").length;
    const diagnostics = isRecord(details.catalog) && Array.isArray(details.catalog.diagnostics) ? details.catalog.diagnostics.length : 0;
    const facts = [active ? `${active} active` : "No active workers", ready ? `${ready} available for follow-up` : undefined, diagnostics ? `${diagnostics} catalog diagnostic${diagnostics === 1 ? "" : "s"}` : undefined].filter(Boolean);
    return new Text(theme.fg("muted", facts.join(" · ")), 0, 0);
  }
  return new Text(theme.fg("muted", firstResultLine(result) || "Diagnostics unavailable"), 0, 0);
}

function renderSimpleResult(
  result: AgentToolResult<unknown>,
  isError: boolean,
  message: string,
  theme: Theme,
  normalColor: "success" | "warning" = "success",
): Text {
  const text = isError ? firstResultLine(result) || message : message;
  return new Text(theme.fg(isError ? "error" : normalColor, text), 0, 0);
}

function firstResultLine(result: AgentToolResult<unknown>): string | undefined {
  const first = result.content[0];
  if (first?.type !== "text") return undefined;
  return first.text.split("\n").find((line) => line.trim())?.trim();
}

function objectFields(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
