import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type {
  ExtensionAPI,
  ExtensionContext,
  Theme,
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
import {
  formatSize,
  getAgentDir,
  getMarkdownTheme,
  keyHint,
  truncateHead,
} from "@earendil-works/pi-coding-agent";
import { Result, Schema } from "effect";
import { Type } from "typebox";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  type CatalogDiagnostic,
  type RunRecord,
  type WorkerCatalog,
  type WorkerDefinition,
  type WorkerOutcome,
  type WorkerRecord,
  type WorkerUsage,
} from "./domain.js";
import type {
  AbortTarget,
  AcceptedRun,
  CompletedRun,
  OrchestrationContext,
  RunResult,
  RuntimeSnapshot,
  SettlementListener,
} from "./runtime.js";
import type { OrchestratorRuntime } from "./host.js";
import {
  disposeComponent,
  formatElapsed,
  resultAppearance,
  WidthBoundComponent,
} from "./tui.js";
import {
  decodeInlineWorkerToolDetails,
  encodeInlineWorkerToolDetails,
  type InlineWorkerSettlementDetails,
  WorkerSettlementDetails,
  type WorkerSettlement,
} from "./worker-settlement.js";

const STRICT_OBJECT = { additionalProperties: false } as const;
const MAX_INSTRUCTION_PREVIEW_LINES = 2;
const shortTextSchema = Type.String({
  pattern: "\\S",
  maxLength: MAX_WORKER_TITLE_LENGTH,
});
const instructionsSchema = Type.String({
  pattern: "\\S",
  maxLength: MAX_WORKER_INSTRUCTIONS_LENGTH,
});
const workerIdSchema = Type.String({ pattern: "^worker-\\S+$" });

const AcceptedRunRenderDetails = Schema.Struct({
  mode: Schema.Literal("async"),
  run_id: WorkerSettlementDetails.fields.runId,
  worker_id: WorkerSettlementDetails.fields.workerId,
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

const taskSchema = Type.Object(
  {
    worker: shortTextSchema,
    title: shortTextSchema,
    instructions: instructionsSchema,
  },
  STRICT_OBJECT,
);

const statusSchema = Type.Object({}, STRICT_OBJECT);

const interactiveSendSchema = Type.Object(
  {
    worker_id: workerIdSchema,
    instructions: instructionsSchema,
  },
  STRICT_OBJECT,
);

const workerAbortSchema = Type.Union([
  Type.Object(
    {
      worker_ids: Type.Array(workerIdSchema, { minItems: 1 }),
    },
    STRICT_OBJECT,
  ),
  Type.Object(
    {
      all: Type.Literal(true),
    },
    STRICT_OBJECT,
  ),
]);

const interactiveCloseSchema = Type.Object(
  {
    worker_id: workerIdSchema,
  },
  STRICT_OBJECT,
);

export interface DispatchDecision {
  readonly mode: "async" | "inline";
  readonly synthesisGroup?: {
    readonly id: string;
    readonly size: number;
  };
}

export interface OrchestrationToolDependencies {
  readonly runtime: OrchestratorRuntime;
  getCatalog(ctx: ExtensionContext): WorkerCatalog;
  getDispatchDecision(toolCallId: string): DispatchDecision;
}

export function registerOrchestrationTools(
  pi: ExtensionAPI,
  deps: OrchestrationToolDependencies,
): void {
  pi.registerTool({
    name: "orchestrate",
    label: "Orchestrate",
    description:
      "Dispatch fully briefed worker scopes. Pi executes native sibling tools concurrently; Pi Orchestrate treats a successfully admitted sole orchestrate call or pure sibling group as async. Mixing orchestrate with another tool makes it inline and blocking.",
    promptSnippet: "Dispatch fully briefed parallel worker scopes",
    promptGuidelines: [
      "Spin up as many workers as needed to cover every useful parallel scope and distinct validation perspective. Treat user-named workers or counts as a floor unless explicitly capped, and reuse the same worker role across multiple calls when useful.",
      "For an intended async wave of N workers, the next assistant response must contain exactly N separate, fully briefed orchestrate calls; one call is valid only when N=1. To run it asynchronously, include no other tool calls; harmless response text does not affect runtime classification.",
      "When a parallel tool dispatcher is available, use it once with exactly N orchestrate entries and no other tools; for example, put N functions.orchestrate entries in multi_tool_use.parallel. Otherwise emit N native sibling orchestrate calls in one assistant response.",
      "Form all N calls before emitting or finalizing the response. Never emit one call and wait for its result before forming the rest of the wave: a successfully admitted sole async orchestrate call returns terminate=true and ends the turn.",
    ],
    executionMode: "parallel",
    parameters: taskSchema,
    renderCall(args, theme, { expanded }) {
      return renderDispatchCall(theme, args, expanded);
    },
    renderResult(result, { isPartial, expanded }, theme, context) {
      return renderOrchestrationResult(result, isPartial, expanded, theme, context.lastComponent);
    },
    async execute(toolCallId, params, signal, onUpdate, ctx) {
      const decision = deps.getDispatchDecision(toolCallId);
      const mode = decision.mode;
      const runtimeContext = buildRuntimeContext(ctx, deps, decision.synthesisGroup);
      if (mode === "async") {
        const acceptedRun = await deps.runtime.orchestrate(
          runtimeContext,
          params,
          "async",
          signal,
        );
        const readable = acceptedRunDetails(acceptedRun);
        return {
          ...readableToolResult(`Accepted async run ${readable.run_id}.`, readable),
          terminate: true,
        };
      }

      const completedRun = await deps.runtime.orchestrate(
        runtimeContext,
        params,
        "inline",
        signal,
        createInlineSettlementListener(onUpdate),
      );
      const readable = completedRunDetails(completedRun);
      return readableToolResult(
        `Completed inline run ${readable.run_id}.`,
        readable,
      );
    },
  });

  pi.registerTool({
    name: "worker_status",
    label: "Worker Status",
    description:
      "Diagnostics and recovery only: inspect trusted catalog entries, catalog diagnostics, and this session's runtime state. Never poll for completion.",
    promptSnippet: "Inspect owned worker state for diagnostics or recovery",
    promptGuidelines: [
      "Use worker_status only for diagnostics or recovery; never poll it for completion.",
    ],
    parameters: statusSchema,
    renderCall(_args, theme) {
      return new Text(theme.fg("toolTitle", theme.bold("worker_status")), 0, 0);
    },
    renderResult(result, { isPartial }, theme) {
      return renderDiagnosticsResult(result, isPartial, theme);
    },
    async execute(_toolCallId, _params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const catalog = deps.getCatalog(ctx);
      const snapshot = await deps.runtime.snapshot(ownerSessionId);
      const readable = statusDetails(catalog, snapshot);
      return readableToolResult(
        "Worker diagnostics and recovery snapshot.",
        readable,
      );
    },
  });

  pi.registerTool({
    name: "interactive_send",
    label: "Interactive Send",
    description:
      "Send follow-up instructions only to an owned lifecycle interactive worker whose status is ready. Never use for one-shot or completed workers; one-shot sessions terminate automatically. A sole tool call runs asynchronously; sibling tool calls make it inline and blocking.",
    promptSnippet: "Use only for an owned lifecycle interactive worker with status ready; never one-shot/completed because one-shot sessions terminate automatically",
    promptGuidelines: [
      "Use interactive_send only for an owned lifecycle interactive worker whose status is ready; never use it for one-shot or completed workers because one-shot sessions terminate automatically.",
    ],
    parameters: interactiveSendSchema,
    renderCall(args, theme, { expanded }) {
      return renderInteractiveMessageCall(theme, "interactive_send", args.worker_id, args.instructions, expanded);
    },
    renderResult(result, { isPartial, expanded }, theme, context) {
      return renderOrchestrationResult(result, isPartial, expanded, theme, context.lastComponent);
    },
    async execute(toolCallId, params, signal, onUpdate, ctx) {
      const workerId = params.worker_id;
      const mode = deps.getDispatchDecision(toolCallId).mode;
      const runtimeContext = buildRuntimeContext(ctx, deps);
      if (mode === "async") {
        const acceptedRun = await deps.runtime.sendInteractive(
          runtimeContext,
          workerId,
          params.instructions,
          "async",
          signal,
        );
        const readable = acceptedRunDetails(acceptedRun);
        return {
          ...readableToolResult(`Accepted async run ${readable.run_id}.`, readable),
          terminate: true,
        };
      }

      const completedRun = await deps.runtime.sendInteractive(
        runtimeContext,
        workerId,
        params.instructions,
        "inline",
        signal,
        createInlineSettlementListener(onUpdate),
      );
      const readable = completedRunDetails(completedRun);
      return readableToolResult(
        `Completed inline run ${readable.run_id}.`,
        readable,
      );
    },
  });

  pi.registerTool({
    name: "worker_abort",
    label: "Worker Abort",
    description:
      "Abort owned active work by worker IDs or all active owned workers. Use interactive_close for owned lifecycle interactive workers whose status is ready.",
    promptSnippet: "Abort active owned workers by worker IDs or all",
    promptGuidelines: [
      "Use worker_abort only for active work; use interactive_close only for an owned lifecycle interactive worker whose status is ready, never for one-shot or completed workers because one-shot sessions terminate automatically.",
    ],
    parameters: workerAbortSchema,
    renderCall(args, theme) {
      const target = "worker_ids" in args
        ? `${args.worker_ids.length} worker${args.worker_ids.length === 1 ? "" : "s"}`
        : "all workers";
      return renderCompactCall(theme, "worker_abort", target);
    },
    renderResult(result, { isPartial }, theme) {
      return renderSimpleResult(result, isPartial ? "Requesting worker stop…" : "Worker stop requested", theme, "warning");
    },
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const target = normalizeAbortTarget(params);
      await deps.runtime.abort(ownerSessionId, target);
      const readable = {
        target: "worker_ids" in params
          ? { worker_ids: params.worker_ids }
          : { all: params.all },
      };
      return readableToolResult("Abort request completed.", readable);
    },
  });

  pi.registerTool({
    name: "interactive_close",
    label: "Interactive Close",
    description: "Close only an owned lifecycle interactive worker whose status is ready. Never use for one-shot or completed workers; one-shot sessions terminate automatically.",
    promptSnippet: "Use only for an owned lifecycle interactive worker with status ready; never one-shot/completed because one-shot sessions terminate automatically",
    promptGuidelines: [
      "Use interactive_close only for an owned lifecycle interactive worker whose status is ready; never use it for one-shot or completed workers because one-shot sessions terminate automatically.",
    ],
    parameters: interactiveCloseSchema,
    renderCall(args, theme) {
      return renderCompactCall(theme, "interactive_close", args.worker_id);
    },
    renderResult(result, { isPartial }, theme) {
      return renderSimpleResult(result, isPartial ? "Closing worker…" : "✓ Worker closed", theme);
    },
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const workerId = params.worker_id;
      await deps.runtime.closeInteractive(ownerSessionId, workerId);
      const readable = { worker_id: workerId };
      return readableToolResult(`Closed worker ${workerId}.`, readable);
    },
  });
}

function buildRuntimeContext(
  ctx: ExtensionContext,
  deps: OrchestrationToolDependencies,
  synthesisGroup?: DispatchDecision["synthesisGroup"],
): OrchestrationContext {
  return {
    ownerSessionId: ctx.sessionManager.getSessionId(),
    cwd: ctx.cwd,
    agentDir: getAgentDir(),
    parentSessionFile: ctx.sessionManager.getSessionFile(),
    projectTrusted: ctx.isProjectTrusted(),
    catalog: deps.getCatalog(ctx),
    parentModel: ctx.model,
    modelRegistry: ctx.modelRegistry,
    ...(synthesisGroup ? { synthesisGroup } : {}),
  };
}

function createInlineSettlementListener(
  onUpdate: ((result: AgentToolResult<unknown>) => void) | undefined,
): SettlementListener {
  return (settlement) => {
    onUpdate?.({
      content: [{ type: "text", text: "Worker response received." }],
      details: encodeInlineWorkerToolDetails({
        mode: "inline",
        result: inlineResultValue(settlement),
      }),
    });
  };
}

function normalizeAbortTarget(params: {
  worker_ids?: string[];
  all?: boolean;
}): AbortTarget {
  return {
    ...(params.worker_ids !== undefined ? { workerIds: params.worker_ids } : {}),
    ...(params.all !== undefined ? { all: params.all } : {}),
  };
}

function acceptedRunDetails(run: AcceptedRun) {
  return {
    mode: "async" as const,
    run_id: run.id,
    worker_id: run.workerId,
  };
}

function completedRunDetails(run: CompletedRun) {
  return encodeInlineWorkerToolDetails({
    mode: "inline",
    runId: run.id,
    ownerSessionId: run.ownerSessionId,
    result: inlineResultValue(run.result),
  });
}

function inlineResultValue(
  result: RunResult | WorkerSettlement,
): InlineWorkerSettlementDetails {
  return {
    workerId: result.workerId,
    worker: result.worker,
    title: result.title,
    status: result.status,
    outcome: result.outcome,
    usage: result.usage,
    startedAt: result.startedAt,
    settledAt: result.settledAt,
    ...(result.sessionFile === undefined
      ? {}
      : { sessionFile: result.sessionFile }),
  };
}

function statusDetails(catalog: WorkerCatalog, snapshot: RuntimeSnapshot) {
  return {
    catalog: {
      workers: catalog.workers.map(catalogWorkerDetails),
      diagnostics: catalog.diagnostics.map(diagnosticDetails),
    },
    state: {
      runs: snapshot.runs.map(runDetails),
      workers: snapshot.workers.map(workerDetails),
    },
  };
}

function catalogWorkerDetails(worker: WorkerDefinition) {
  return {
    name: worker.name,
    description: worker.description,
    lifecycle: worker.lifecycle,
    source: {
      kind: worker.source.kind,
      file_path: worker.source.filePath,
    },
    tools: [...worker.tools],
    skills: worker.skills === undefined ? undefined : [...worker.skills],
    model: worker.model
      ? { provider: worker.model.provider, model_id: worker.model.modelId }
      : undefined,
    thinking: worker.thinking,
    compaction: worker.compaction
      ? {
          enabled: worker.compaction.enabled,
          reserve_tokens: worker.compaction.reserveTokens,
          keep_recent_tokens: worker.compaction.keepRecentTokens,
        }
      : undefined,
  };
}

function diagnosticDetails(diagnostic: CatalogDiagnostic) {
  return {
    severity: diagnostic.severity,
    source: diagnostic.source,
    message: diagnostic.message,
    file_path: diagnostic.filePath,
  };
}

function runDetails(run: RunRecord) {
  return {
    run_id: run.id,
    owner_session_id: run.ownerSessionId,
    worker_id: run.workerId,
    mode: run.mode,
    state: run.state,
    created_at: run.createdAt,
  };
}

function workerDetails(worker: WorkerRecord) {
  return {
    worker_id: worker.id,
    worker: worker.worker,
    owner_session_id: worker.ownerSessionId,
    run_id: worker.runId,
    title: worker.title,
    lifecycle: worker.lifecycle,
    status: worker.status,
    activity: worker.activity,
    usage: usageDetails(worker.usage),
    outcome: worker.outcome ? outcomeDetails(worker.outcome) : undefined,
    session_file: worker.sessionFile,
  };
}

function usageDetails(usage: WorkerUsage) {
  return {
    input: usage.input,
    output: usage.output,
    cache_read: usage.cacheRead,
    cache_write: usage.cacheWrite,
    cost: usage.cost,
    context_tokens: usage.contextTokens,
    turns: usage.turns,
  };
}

function outcomeDetails(outcome: WorkerOutcome) {
  switch (outcome.status) {
    case "completed":
    case "ready":
      return {
        status: outcome.status,
        assistant_text: outcome.assistantText,
      };
    case "failed":
    case "aborted":
      return {
        status: outcome.status,
        message: outcome.message,
        assistant_text: outcome.assistantText,
      };
    case "closed":
      return { status: outcome.status };
  }
}

function readableToolResult<T>(title: string, details: T) {
  return {
    content: [{ type: "text" as const, text: readableDetails(title, details) }],
    details,
  };
}

function readableDetails(title: string, details: unknown): string {
  const content = `${title}\n\n${JSON.stringify(details, null, 2)}`;
  const truncation = truncateHead(content);
  if (!truncation.truncated) return content;

  return `${truncation.content}\n\n[Output truncated: ${formatSize(truncation.outputBytes)} of ${formatSize(truncation.totalBytes)}. Full structured details remain available.]`;
}

interface RenderableTask {
  readonly worker?: unknown;
  readonly title?: unknown;
  readonly instructions?: unknown;
}

function renderDispatchCall(
  theme: Theme,
  task: RenderableTask,
  expanded: boolean,
): Component {
  const container = new Container();
  container.addChild(new Text(
    theme.fg("toolTitle", theme.bold("orchestrate ")) + theme.fg("muted", safeTerminalText(task.worker)),
    0, 0,
  ));
  container.addChild(new Text(
    `${theme.fg("accent", "→")} ${theme.fg("text", theme.bold(safeTerminalText(task.title)))}`,
    0, 0,
  ));
  if (expanded) {
    container.addChild(new Text(safeTerminalText(task.instructions), 2, 0));
    return new WidthBoundComponent(container);
  }
  container.addChild(new InstructionPreview(task.instructions, theme));
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
  lastComponent: unknown,
): Component {
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
  return new WidthBoundComponent(renderSimpleResult(result, firstResultLine(result) || "Work sent", theme, "warning"));
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

function renderDiagnosticsResult(result: AgentToolResult<unknown>, isPartial: boolean, theme: Theme): Text {
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
  message: string,
  theme: Theme,
  normalColor: "success" | "warning" = "success",
): Text {
  const failed = "isError" in result && result.isError === true;
  return new Text(theme.fg(failed ? "error" : normalColor, failed ? firstResultLine(result) || message : message), 0, 0);
}

function firstResultLine(result: AgentToolResult<unknown>): string | undefined {
  const first = result.content[0];
  if (first?.type !== "text") return undefined;
  return first.text.split("\n").find((line) => line.trim())?.trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
