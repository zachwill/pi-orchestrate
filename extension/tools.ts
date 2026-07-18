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
import { Type } from "typebox";
import {
  type CatalogDiagnostic,
  type OrchestrateTaskInput,
  type WaveId,
  type WaveRecord,
  type WorkerCatalog,
  type WorkerDefinition,
  type WorkerId,
  type WorkerOutcome,
  type WorkerRecord,
  type WorkerUsage,
} from "./domain.js";
import type {
  AbortTarget,
  AcceptedWave,
  CompletedResult,
  CompletedWave,
  OrchestrationContext,
  OrchestratorRuntime,
  RuntimeSnapshot,
} from "./runtime.js";

const STRICT_OBJECT = { additionalProperties: false } as const;
const MAX_INSTRUCTION_PREVIEW_LINES = 2;

const taskSchema = Type.Object(
  {
    worker: Type.String(),
    title: Type.String(),
    instructions: Type.String(),
  },
  STRICT_OBJECT,
);

const orchestrateSchema = taskSchema;

const statusSchema = Type.Object({}, STRICT_OBJECT);

const workerSendSchema = Type.Object(
  {
    worker_id: Type.String({ minLength: 1 }),
    instructions: Type.String(),
  },
  STRICT_OBJECT,
);

const workerAbortSchema = Type.Union([
  Type.Object(
    {
      worker_ids: Type.Array(Type.String({ minLength: 1 }), { minItems: 1 }),
    },
    STRICT_OBJECT,
  ),
  Type.Object(
    {
      wave_id: Type.String({ minLength: 1 }),
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

const workerCloseSchema = Type.Object(
  {
    worker_id: Type.String({ minLength: 1 }),
  },
  STRICT_OBJECT,
);

export interface DispatchDecision {
  readonly mode: "async" | "inline";
  readonly group?: {
    readonly id: string;
    readonly size: number;
  };
}

export interface OrchestrationToolDependencies {
  readonly runtime: OrchestratorRuntime;
  getCatalog(ctx: ExtensionContext): WorkerCatalog | Promise<WorkerCatalog>;
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
      "Dispatch one fully briefed task. One or more sibling orchestrate calls run concurrently and asynchronously. Mixing orchestrate with another tool makes it inline and blocking.",
    promptSnippet: "Dispatch one fully briefed worker task",
    promptGuidelines: [
      "Use sibling orchestrate calls for independent tasks, with one complete brief per call.",
    ],
    executionMode: "parallel",
    parameters: orchestrateSchema,
    renderCall(args, theme, { expanded }) {
      return renderDispatchCall(theme, args, expanded);
    },
    renderResult(result, { isPartial, expanded }, theme, context) {
      return renderOrchestrationResult(result, isPartial, expanded, theme, context.lastComponent);
    },
    async execute(toolCallId, params, signal, onUpdate, ctx) {
      const decision = deps.getDispatchDecision(toolCallId);
      const mode = decision.mode;
      const runtimeContext = await buildRuntimeContext(ctx, deps, decision.group);
      const settlements: unknown[] = [];
      const onSettlement = mode === "inline" ? (settlement: unknown) => {
        settlements.push(settlement);
        onUpdate?.({
          content: [{ type: "text", text: `${settlements.length} worker response(s) received.` }],
          details: { mode: "inline", settlements: [...settlements] },
        });
      } : undefined;
      const wave = await orchestrateWithMode(
        deps.runtime,
        runtimeContext,
        [params],
        mode,
        signal,
        onSettlement,
      );

      if (mode === "async") {
        const acceptedWave = wave as AcceptedWave;
        const readable = acceptedWaveDetails(acceptedWave);
        return {
          content: [
            {
              type: "text",
              text: readableDetails(`Accepted async wave ${readable.wave_id}.`, readable),
            },
          ],
          details: acceptedWave,
          terminate: true,
        };
      }

      const completedWave = wave as CompletedWave;
      const readable = completedWaveDetails(completedWave);
      return {
        content: [
          {
            type: "text",
            text: readableDetails(
              `Completed inline wave ${readable.wave_id} with ${readable.results.length} result(s).`,
              readable,
            ),
          },
        ],
        details: completedWave,
      };
    },
  });

  pi.registerTool({
    name: "orchestration_status",
    label: "Orchestration Status",
    description:
      "Diagnostics and recovery only: inspect trusted catalog entries, catalog diagnostics, and this session's runtime state. Never poll for completion.",
    promptSnippet: "Inspect owned orchestration state for diagnostics or recovery",
    promptGuidelines: [
      "Use orchestration_status only for diagnostics or recovery; never poll it for completion.",
    ],
    parameters: statusSchema,
    renderCall(_args, theme) {
      return new Text(theme.fg("toolTitle", theme.bold("orchestration_status")), 0, 0);
    },
    renderResult(result, { isPartial }, theme) {
      return renderDiagnosticsResult(result, isPartial, theme);
    },
    async execute(_toolCallId, _params, _signal, _onUpdate, ctx) {
      const ownerSessionId = requireNonblank(
        "owner session ID",
        ctx.sessionManager.getSessionId(),
      );
      const [catalog, snapshot] = await Promise.all([
        deps.getCatalog(ctx),
        deps.runtime.snapshot(ownerSessionId),
      ]);
      const readable = statusDetails(catalog, snapshot);
      return {
        content: [
          {
            type: "text",
            text: readableDetails("Orchestration diagnostics and recovery snapshot.", readable),
          },
        ],
        details: readable,
      };
    },
  });

  pi.registerTool({
    name: "worker_send",
    label: "Worker Send",
    description:
      "Send follow-up instructions to an owned ready reusable worker. A sole tool call runs asynchronously; sibling tool calls make it inline and blocking.",
    promptSnippet: "Send follow-up work to an owned ready reusable worker",
    promptGuidelines: [
      "Use worker_send only for follow-up work on an owned ready reusable worker.",
    ],
    parameters: workerSendSchema,
    renderCall(args, theme, { expanded }) {
      return renderWorkerMessageCall(theme, "worker_send", args.worker_id, args.instructions, expanded);
    },
    renderResult(result, { isPartial, expanded }, theme, context) {
      return renderOrchestrationResult(result, isPartial, expanded, theme, context.lastComponent);
    },
    async execute(toolCallId, params, signal, onUpdate, ctx) {
      const workerId = asWorkerId(params.worker_id);
      const mode = deps.getDispatchDecision(toolCallId).mode;
      const runtimeContext = await buildRuntimeContext(ctx, deps);
      const settlements: unknown[] = [];
      const onSettlement = mode === "inline" ? (settlement: unknown) => {
        settlements.push(settlement);
        onUpdate?.({
          content: [{ type: "text", text: `${settlements.length} worker response(s) received.` }],
          details: { mode: "inline", settlements: [...settlements] },
        });
      } : undefined;
      const wave = await sendWithMode(
        deps.runtime,
        runtimeContext,
        workerId,
        params.instructions,
        mode,
        signal,
        onSettlement,
      );

      if (mode === "async") {
        const acceptedWave = wave as AcceptedWave;
        const readable = acceptedWaveDetails(acceptedWave);
        return {
          content: [
            {
              type: "text",
              text: readableDetails(`Accepted async wave ${readable.wave_id}.`, readable),
            },
          ],
          details: acceptedWave,
          terminate: true,
        };
      }

      const completedWave = wave as CompletedWave;
      const readable = completedWaveDetails(completedWave);
      return {
        content: [
          {
            type: "text",
            text: readableDetails(
              `Completed inline wave ${readable.wave_id} with ${readable.results.length} result(s).`,
              readable,
            ),
          },
        ],
        details: completedWave,
      };
    },
  });

  pi.registerTool({
    name: "worker_abort",
    label: "Worker Abort",
    description:
      "Abort owned active work by worker IDs, wave ID, or all active owned workers. Use worker_close for ready reusable workers.",
    promptSnippet: "Abort active owned workers by worker IDs, wave ID, or all",
    promptGuidelines: [
      "Use worker_abort only for active work; use worker_close for a ready reusable worker.",
    ],
    parameters: workerAbortSchema,
    renderCall(args, theme) {
      const target = "wave_id" in args
        ? args.wave_id
        : "worker_ids" in args
          ? `${args.worker_ids.length} worker${args.worker_ids.length === 1 ? "" : "s"}`
          : "all workers";
      return renderCompactCall(theme, "worker_abort", target);
    },
    renderResult(result, { isPartial }, theme) {
      return renderSimpleResult(result, isPartial ? "Requesting worker stop…" : "Worker stop requested", theme, "warning");
    },
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const ownerSessionId = requireNonblank(
        "owner session ID",
        ctx.sessionManager.getSessionId(),
      );
      const target = abortTarget(params);
      await deps.runtime.abort(ownerSessionId, target.runtime);
      const readable = { target: target.external };
      return {
        content: [
          {
            type: "text",
            text: readableDetails("Abort request completed.", readable),
          },
        ],
        details: { target: target.runtime },
      };
    },
  });

  pi.registerTool({
    name: "worker_close",
    label: "Worker Close",
    description: "Close an owned ready reusable worker that no longer needs follow-up work.",
    promptSnippet: "Close an owned ready reusable worker",
    promptGuidelines: [
      "Use worker_close when an owned ready reusable worker is finished.",
    ],
    parameters: workerCloseSchema,
    renderCall(args, theme) {
      return renderCompactCall(theme, "worker_close", args.worker_id);
    },
    renderResult(result, { isPartial }, theme) {
      return renderSimpleResult(result, isPartial ? "Closing worker…" : "✓ Worker closed", theme);
    },
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const ownerSessionId = requireNonblank(
        "owner session ID",
        ctx.sessionManager.getSessionId(),
      );
      const workerId = asWorkerId(params.worker_id);
      await deps.runtime.close(ownerSessionId, workerId);
      const readable = { worker_id: workerId };
      return {
        content: [
          {
            type: "text",
            text: readableDetails(`Closed worker ${workerId}.`, readable),
          },
        ],
        details: { workerId },
      };
    },
  });
}

async function buildRuntimeContext(
  ctx: ExtensionContext,
  deps: OrchestrationToolDependencies,
  dispatchGroup?: DispatchDecision["group"],
): Promise<OrchestrationContext> {
  return {
    ownerSessionId: requireNonblank(
      "owner session ID",
      ctx.sessionManager.getSessionId(),
    ),
    cwd: ctx.cwd,
    agentDir: getAgentDir(),
    parentSessionFile: ctx.sessionManager.getSessionFile(),
    projectTrusted: ctx.isProjectTrusted(),
    catalog: await deps.getCatalog(ctx),
    parentModel: ctx.model,
    modelRegistry: ctx.modelRegistry,
    ...(dispatchGroup ? { dispatchGroup } : {}),
  };
}

function orchestrateWithMode(
  runtime: OrchestratorRuntime,
  context: OrchestrationContext,
  tasks: readonly OrchestrateTaskInput[],
  mode: "async" | "inline",
  signal: AbortSignal | undefined,
  onSettlement?: (settlement: unknown) => void,
): Promise<AcceptedWave | CompletedWave> {
  if (mode === "async") return runtime.orchestrate(context, tasks, "async");
  return runtime.orchestrate(context, tasks, "inline", signal, onSettlement);
}

function sendWithMode(
  runtime: OrchestratorRuntime,
  context: OrchestrationContext,
  workerId: WorkerId,
  instructions: string,
  mode: "async" | "inline",
  signal: AbortSignal | undefined,
  onSettlement?: (settlement: unknown) => void,
): Promise<AcceptedWave | CompletedWave> {
  if (mode === "async") return runtime.send(context, workerId, instructions, "async");
  return runtime.send(context, workerId, instructions, "inline", signal, onSettlement);
}

function requireNonblank(name: string, value: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${name} must not be blank`);
  }
  return value;
}

function asWorkerId(value: string): WorkerId {
  return requireNonblank("worker_id", value) as WorkerId;
}

function asWaveId(value: string): WaveId {
  return requireNonblank("wave_id", value) as WaveId;
}

function abortTarget(params: {
  worker_ids?: string[];
  wave_id?: string;
  all?: true;
}): {
  runtime: AbortTarget;
  external:
    | { worker_ids: readonly WorkerId[] }
    | { wave_id: WaveId }
    | { all: true };
} {
  const selectedTargetCount = [
    params.worker_ids !== undefined,
    params.wave_id !== undefined,
    params.all !== undefined,
  ].filter(Boolean).length;
  if (selectedTargetCount !== 1 || (params.all !== undefined && params.all !== true)) {
    throw new Error("Abort target must specify exactly one target");
  }

  if (params.worker_ids !== undefined) {
    if (!Array.isArray(params.worker_ids) || params.worker_ids.length === 0) {
      throw new Error("worker_ids must contain at least one worker ID");
    }
    const workerIds = params.worker_ids.map(asWorkerId);
    return {
      runtime: { workerIds },
      external: { worker_ids: workerIds },
    };
  }
  if (params.wave_id !== undefined) {
    const waveId = asWaveId(params.wave_id);
    return {
      runtime: { waveId },
      external: { wave_id: waveId },
    };
  }
  return {
    runtime: { all: true },
    external: { all: true },
  };
}

function acceptedWaveDetails(wave: AcceptedWave) {
  return {
    mode: "async" as const,
    wave_id: wave.id,
    worker_ids: [...wave.workerIds],
  };
}

function completedWaveDetails(wave: CompletedWave) {
  return {
    mode: wave.mode,
    wave_id: wave.id,
    owner_session_id: wave.ownerSessionId,
    results: wave.results.map(completedResultDetails),
  };
}

function completedResultDetails(result: CompletedResult) {
  return {
    worker_id: result.workerId,
    worker: result.worker,
    title: result.title,
    status: result.status,
    outcome: outcomeDetails(result.outcome),
    usage: usageDetails(result.usage),
    session_file: result.sessionFile,
  };
}

function statusDetails(catalog: WorkerCatalog, snapshot: RuntimeSnapshot) {
  return {
    catalog: {
      workers: catalog.workers.map(catalogWorkerDetails),
      diagnostics: catalog.diagnostics.map(diagnosticDetails),
    },
    snapshot: {
      waves: snapshot.waves.map(waveDetails),
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

function waveDetails(wave: WaveRecord) {
  return {
    wave_id: wave.id,
    owner_session_id: wave.ownerSessionId,
    worker_ids: [...wave.workerIds],
    mode: wave.mode,
    state: wave.state,
    created_at: wave.createdAt,
  };
}

function workerDetails(worker: WorkerRecord) {
  return {
    worker_id: worker.id,
    worker: worker.worker,
    owner_session_id: worker.ownerSessionId,
    wave_id: worker.waveId,
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

function renderWorkerMessageCall(
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

class WidthBoundComponent implements Component {
  constructor(private readonly child: Component, private readonly maxLines?: number) {}
  render(width: number): string[] {
    const bounded = Math.max(1, Math.floor(width));
    const lines = this.child.render(bounded);
    return (this.maxLines === undefined ? lines : lines.slice(0, this.maxLines))
      .map((line) => truncateToWidth(line, bounded, "…"));
  }
  invalidate(): void { this.child.invalidate(); }
  dispose(): void { (this.child as Component & { dispose?: () => void }).dispose?.(); }
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
  if (isRecord(details) && typeof details.id === "string" && Array.isArray(details.workerIds) && details.workerIds.every((id) => typeof id === "string")) {
    const count = details.workerIds.length;
    return new WidthBoundComponent(new Text(theme.fg("success", `Sent to ${count} worker${count === 1 ? "" : "s"}`) + theme.fg("dim", " · responses arrive as they complete"), 0, 0));
  }
  const settlements = inlineSettlements(details);
  if (settlements.length > 0) {
    const component = lastComponent instanceof InlineResultComponent
      ? lastComponent
      : new InlineResultComponent(theme);
    component.update(settlements, isPartial, expanded);
    return component;
  }
  if (isRecord(details) && (Array.isArray(details.settlements) || Array.isArray(details.results) || "workerIds" in details)) {
    return new WidthBoundComponent(new Text(theme.fg("warning", "Worker result details unavailable"), 0, 0));
  }
  if (isPartial) return new WidthBoundComponent(new Text(theme.fg("warning", "Sending work…"), 0, 0));
  return new WidthBoundComponent(renderSimpleResult(result, firstResultLine(result) || "Work sent", theme, "warning"));
}

interface InlineSettlement {
  worker: string;
  title: string;
  status: "completed" | "ready" | "failed" | "aborted";
  response: string;
}

class InlineResultComponent implements Component {
  private settlements: readonly InlineSettlement[] = [];
  private partial = false;
  private expanded = false;
  private child: Component = new Container();
  constructor(private readonly theme: Theme) {}
  update(settlements: readonly InlineSettlement[], partial: boolean, expanded: boolean): void {
    this.settlements = settlements;
    this.partial = partial;
    this.expanded = expanded;
    this.rebuild();
  }
  render(width: number): string[] { return new WidthBoundComponent(this.child).render(width); }
  invalidate(): void { this.rebuild(); }
  dispose(): void { (this.child as Component & { dispose?: () => void }).dispose?.(); }
  private rebuild(): void {
    (this.child as Component & { dispose?: () => void }).dispose?.();
    const container = new Container();
    for (const settlement of this.settlements) {
      const failed = settlement.status === "failed";
      const aborted = settlement.status === "aborted";
      const color = failed ? "error" : aborted ? "warning" : "success";
      const icon = failed ? "✗" : aborted ? "■" : "✓";
      container.addChild(new WidthBoundComponent(new Text(this.theme.fg(color, this.theme.bold(`${icon} ${settlement.worker} · ${settlement.title} · ${settlement.status}`)), 0, 0), 1));
      if (settlement.response) {
        const markdown = new Markdown(settlement.response, this.expanded ? 2 : 0, 0, getMarkdownTheme());
        container.addChild(new WidthBoundComponent(markdown, this.expanded ? undefined : 2));
      }
      container.addChild(new Spacer(1));
    }
    if (this.partial) container.addChild(new Text(this.theme.fg("warning", "Waiting for remaining workers…"), 0, 0));
    else if (!this.expanded) container.addChild(new Text(this.theme.fg("dim", keyHint("app.tools.expand", "to inspect full responses")), 0, 0));
    this.child = container;
  }
}

function inlineSettlements(details: unknown): InlineSettlement[] {
  if (!isRecord(details)) return [];
  const values = Array.isArray(details.settlements) ? details.settlements : Array.isArray(details.results) ? details.results : [];
  const parsed: InlineSettlement[] = [];
  for (const value of values) {
    const settlement = readInlineSettlement(value);
    if (settlement) parsed.push(settlement);
  }
  return parsed;
}

function readInlineSettlement(value: unknown): InlineSettlement | undefined {
  if (!isRecord(value) || typeof value.worker !== "string" || typeof value.title !== "string" || !isRecord(value.outcome)) return undefined;
  const outcome = value.outcome;
  const statuses = ["completed", "ready", "failed", "aborted"] as const;
  const status = statuses.find((item) => item === value.status);
  const outcomeStatus = statuses.find((item) => item === outcome.status);
  if (!status || outcomeStatus !== status) return undefined;
  const message = outcome.message;
  const camelAssistant = outcome.assistantText;
  const snakeAssistant = outcome.assistant_text;
  if (message !== undefined && typeof message !== "string") return undefined;
  if (camelAssistant !== undefined && typeof camelAssistant !== "string") return undefined;
  if (snakeAssistant !== undefined && typeof snakeAssistant !== "string") return undefined;
  const assistantText = typeof camelAssistant === "string" ? camelAssistant : snakeAssistant;
  if ((status === "completed" || status === "ready") && typeof assistantText !== "string") return undefined;
  if (status === "failed" && typeof message !== "string") return undefined;
  return {
    worker: value.worker,
    title: value.title,
    status,
    response: [message, assistantText].filter((item): item is string => typeof item === "string" && item.length > 0).join("\n\n"),
  };
}

function renderDiagnosticsResult(result: AgentToolResult<unknown>, isPartial: boolean, theme: Theme): Text {
  if (isPartial) return new Text(theme.fg("muted", "Reading orchestration diagnostics…"), 0, 0);
  const details = result.details;
  if (isRecord(details) && isRecord(details.snapshot) && Array.isArray(details.snapshot.workers)) {
    const workers = details.snapshot.workers.filter(isRecord);
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
