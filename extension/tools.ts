import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type {
  ExtensionAPI,
  ExtensionContext,
  Theme,
} from "@earendil-works/pi-coding-agent";
import { Container, Text } from "@earendil-works/pi-tui";
import {
  formatSize,
  getAgentDir,
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
const MAX_TASKS_PER_WAVE = 12;

const taskSchema = Type.Object(
  {
    worker: Type.String(),
    title: Type.String(),
    instructions: Type.String(),
  },
  STRICT_OBJECT,
);

const orchestrateSchema = Type.Object(
  {
    tasks: Type.Array(taskSchema, {
      minItems: 1,
      maxItems: MAX_TASKS_PER_WAVE,
    }),
  },
  STRICT_OBJECT,
);

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

export interface OrchestrationToolDependencies {
  readonly runtime: OrchestratorRuntime;
  getCatalog(ctx: ExtensionContext): WorkerCatalog | Promise<WorkerCatalog>;
  getDispatchMode(toolCallId: string): "async" | "inline";
}

export function registerOrchestrationTools(
  pi: ExtensionAPI,
  deps: OrchestrationToolDependencies,
): void {
  pi.registerTool({
    name: "orchestrate",
    label: "Orchestrate",
    description:
      "Dispatch 1 to 12 independent, fully briefed tasks as one concurrent wave. A sole tool call runs asynchronously; sibling tool calls make it inline and blocking.",
    promptSnippet: "Dispatch one concurrent wave of fully briefed worker tasks",
    promptGuidelines: [
      "Use orchestrate for one independent worker wave, with a complete brief for every task.",
    ],
    parameters: orchestrateSchema,
    renderCall(args, theme) {
      return renderDispatchCall(theme, args.tasks);
    },
    renderResult(result, { isPartial }, theme) {
      return renderOrchestrationResult(result, isPartial, theme);
    },
    async execute(toolCallId, params, signal, _onUpdate, ctx) {
      const mode = deps.getDispatchMode(toolCallId);
      const runtimeContext = await buildRuntimeContext(ctx, deps);
      const wave = await orchestrateWithMode(
        deps.runtime,
        runtimeContext,
        params.tasks,
        mode,
        signal,
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
      return renderSimpleResult(result, isPartial ? "Reading orchestration state…" : "Orchestration state ready", theme);
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
    renderCall(args, theme) {
      return renderWorkerMessageCall(theme, "worker_send", args.worker_id, args.instructions);
    },
    renderResult(result, { isPartial }, theme) {
      return renderOrchestrationResult(result, isPartial, theme);
    },
    async execute(toolCallId, params, signal, _onUpdate, ctx) {
      const workerId = asWorkerId(params.worker_id);
      const mode = deps.getDispatchMode(toolCallId);
      const runtimeContext = await buildRuntimeContext(ctx, deps);
      const wave = await sendWithMode(
        deps.runtime,
        runtimeContext,
        workerId,
        params.instructions,
        mode,
        signal,
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
      return renderSimpleResult(result, isPartial ? "Stopping workers…" : "✓ Workers stopped", theme);
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
  };
}

function orchestrateWithMode(
  runtime: OrchestratorRuntime,
  context: OrchestrationContext,
  tasks: readonly OrchestrateTaskInput[],
  mode: "async" | "inline",
  signal: AbortSignal | undefined,
): Promise<AcceptedWave | CompletedWave> {
  if (mode === "async") return runtime.orchestrate(context, tasks, "async");

  const orchestrateInline = runtime.orchestrate as unknown as (
    context: OrchestrationContext,
    tasks: readonly OrchestrateTaskInput[],
    mode: "inline",
    signal?: AbortSignal,
  ) => Promise<CompletedWave>;
  return orchestrateInline.call(runtime, context, tasks, "inline", signal);
}

function sendWithMode(
  runtime: OrchestratorRuntime,
  context: OrchestrationContext,
  workerId: WorkerId,
  instructions: string,
  mode: "async" | "inline",
  signal: AbortSignal | undefined,
): Promise<AcceptedWave | CompletedWave> {
  if (mode === "async") return runtime.send(context, workerId, instructions, "async");

  const sendInline = runtime.send as unknown as (
    context: OrchestrationContext,
    workerId: WorkerId,
    instructions: string,
    mode: "inline",
    signal?: AbortSignal,
  ) => Promise<CompletedWave>;
  return sendInline.call(runtime, context, workerId, instructions, "inline", signal);
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
    skills: [...worker.skills],
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

function renderDispatchCall(
  theme: Theme,
  tasks: readonly { worker: string; title: string }[],
): Container {
  const container = new Container();
  const count = tasks.length;
  container.addChild(
    new Text(
      theme.fg("toolTitle", theme.bold("orchestrate ")) +
        theme.fg("muted", `${count} worker${count === 1 ? "" : "s"}`),
      0,
      0,
    ),
  );

  const visibleTasks = tasks.slice(0, 6);
  for (const task of visibleTasks) {
    container.addChild(
      new Text(
        `${theme.fg("accent", "→")} ${theme.fg("muted", task.worker)} · ${theme.fg("dim", task.title)}`,
        0,
        0,
      ),
    );
  }
  if (tasks.length > visibleTasks.length) {
    container.addChild(new Text(theme.fg("dim", `… ${tasks.length - visibleTasks.length} more`), 0, 0));
  }
  return container;
}

function renderWorkerMessageCall(
  theme: Theme,
  tool: string,
  workerId: string,
  instructions: string,
): Container {
  const container = new Container();
  container.addChild(
    new Text(
      theme.fg("toolTitle", theme.bold(`${tool} `)) + theme.fg("muted", workerId),
      0,
      0,
    ),
  );
  const firstLine = instructions.split("\n").find((line) => line.trim())?.trim();
  if (firstLine) {
    container.addChild(new Text(`${theme.fg("accent", "→")} ${theme.fg("dim", firstLine)}`, 0, 0));
  }
  return container;
}

function renderCompactCall(theme: Theme, tool: string, target: string): Text {
  return new Text(
    theme.fg("toolTitle", theme.bold(`${tool} `)) + theme.fg("muted", target),
    0,
    0,
  );
}

function renderOrchestrationResult(
  result: AgentToolResult<unknown>,
  isPartial: boolean,
  theme: Theme,
): Text {
  if (isPartial) return new Text(theme.fg("warning", "Sending work…"), 0, 0);

  const details = result.details;
  if (isRecord(details) && Array.isArray(details.workerIds)) {
    const count = details.workerIds.length;
    return new Text(
      theme.fg("success", `✓ Sent to ${count} worker${count === 1 ? "" : "s"}`) +
        theme.fg("dim", " · replies return automatically"),
      0,
      0,
    );
  }
  if (isRecord(details) && Array.isArray(details.results)) {
    const count = details.results.length;
    return new Text(
      theme.fg("success", `✓ ${count} worker${count === 1 ? " replied" : "s replied"}`),
      0,
      0,
    );
  }
  return renderSimpleResult(result, firstResultLine(result) || "Work sent", theme);
}

function renderSimpleResult(
  result: AgentToolResult<unknown>,
  message: string,
  theme: Theme,
): Text {
  const failed = "isError" in result && result.isError === true;
  return new Text(theme.fg(failed ? "error" : "success", failed ? firstResultLine(result) || message : message), 0, 0);
}

function firstResultLine(result: AgentToolResult<unknown>): string | undefined {
  const first = result.content[0];
  if (first?.type !== "text") return undefined;
  return first.text.split("\n").find((line) => line.trim())?.trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
