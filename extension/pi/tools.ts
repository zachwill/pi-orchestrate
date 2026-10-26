import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import {
  formatSize,
  getAgentDir,
  truncateHead,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import type {
  CatalogDiagnostic,
  WorkerCatalog,
  WorkerDefinition,
} from "../catalog/definition.ts";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  type RunRecord,
  type WorkerOutcome,
  type WorkerRecord,
  type WorkerUsage,
} from "../orchestration/model.ts";
import type {
  AbortTarget,
  OrchestrationContext,
} from "../orchestration/admission.ts";
import type {
  AcceptedRun,
  OwnerSnapshot,
} from "../orchestration/service.ts";
import type { DispatchDecision } from "../parent/dispatch-policy.ts";
import type { OrchestrationClient } from "../parent/process-host.ts";
import {
  interactiveCloseToolRenderer,
  interactiveSendToolRenderer,
  orchestrateToolRenderer,
  workerAbortToolRenderer,
  workerStatusToolRenderer,
} from "./tool-renderer.ts";

const STRICT_OBJECT = { additionalProperties: false } as const;
const shortTextSchema = Type.String({
  pattern: "\\S",
  maxLength: MAX_WORKER_TITLE_LENGTH,
});
const instructionsSchema = Type.String({
  pattern: "\\S",
  maxLength: MAX_WORKER_INSTRUCTIONS_LENGTH,
});
const workerIdSchema = Type.String({ pattern: "^worker-\\S+$" });

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

export interface OrchestrationToolDependencies {
  readonly orchestration: OrchestrationClient;
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
      "Start a fresh background worker. Choose a catalog name with worker, name the assignment with title, and give the complete brief in instructions. Returns a worker_id; results arrive separately. One-shot workers finish automatically; interactive workers become ready for follow-up.",
    promptSnippet: "Start a background worker with a complete assignment brief",
    executionMode: "parallel",
    parameters: taskSchema,
    ...orchestrateToolRenderer,
    async execute(toolCallId, params, signal, _onUpdate, ctx) {
      const decision = deps.getDispatchDecision(toolCallId);
      const orchestrationContext = buildOrchestrationContext(
        ctx,
        deps,
        decision.synthesisGroup,
      );
      const acceptedRun = await deps.orchestration.orchestrate(
        orchestrationContext,
        params,
        "async",
        signal,
      );
      const readable = acceptedRunSummary(acceptedRun);
      return readableToolResult(
        `Accepted ${JSON.stringify(params.title)} as ${readable.worker_id}. Results will arrive automatically.`,
        readable,
      );
    },
  });

  pi.registerTool({
    name: "worker_status",
    label: "Worker Status",
    description:
      "Inspect the worker catalog and this session's workers for diagnostics or recovery. Includes worker IDs, configuration, current state, latest outcomes, and available transcript paths.",
    promptSnippet: "Recover worker IDs and inspect configuration, state, and outcomes",
    parameters: statusSchema,
    ...workerStatusToolRenderer,
    async execute(_toolCallId, _params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const catalog = deps.getCatalog(ctx);
      const snapshot = await deps.orchestration.snapshot(ownerSessionId);
      const readable = statusSummary(catalog, snapshot);
      return readableToolResult(
        "Worker catalog and current state for this session.",
        readable,
      );
    },
  });

  pi.registerTool({
    name: "interactive_send",
    label: "Interactive Send",
    description:
      "Continue a ready interactive worker from this session. Pass its worker_id and follow-up instructions; it retains its conversation and returns the result in the background.",
    promptSnippet: "Continue a ready interactive worker using its worker_id",
    parameters: interactiveSendSchema,
    ...interactiveSendToolRenderer,
    async execute(toolCallId, params, signal, _onUpdate, ctx) {
      const workerId = params.worker_id;
      const decision = deps.getDispatchDecision(toolCallId);
      const orchestrationContext = buildOrchestrationContext(
        ctx,
        deps,
        decision.synthesisGroup,
      );
      const acceptedRun = await deps.orchestration.sendInteractive(
        orchestrationContext,
        workerId,
        params.instructions,
        "async",
        signal,
      );
      const readable = acceptedRunSummary(acceptedRun);
      return readableToolResult(
        `Accepted follow-up for ${readable.worker_id}. Results will arrive automatically.`,
        readable,
      );
    },
  });

  pi.registerTool({
    name: "worker_abort",
    label: "Worker Abort",
    description:
      "Stop active workers from this session. Supply worker_ids to select workers, or all: true to stop all active workers belonging to this session.",
    promptSnippet: "Stop selected active workers or all active workers from this session",
    parameters: workerAbortSchema,
    ...workerAbortToolRenderer,
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const target = normalizeAbortTarget(params);
      await deps.orchestration.abort(ownerSessionId, target);
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
    description: "Release a ready interactive worker from this session by worker_id when finished. Its conversation remains available for follow-up until closed.",
    promptSnippet: "Release a ready interactive worker when finished",
    parameters: interactiveCloseSchema,
    ...interactiveCloseToolRenderer,
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const workerId = params.worker_id;
      await deps.orchestration.closeInteractive(ownerSessionId, workerId);
      const readable = { worker_id: workerId };
      return readableToolResult(`Closed worker ${workerId}.`, readable);
    },
  });
}

function buildOrchestrationContext(
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

function normalizeAbortTarget(params: {
  worker_ids?: string[];
  all?: boolean;
}): AbortTarget {
  return {
    ...(params.worker_ids !== undefined ? { workerIds: params.worker_ids } : {}),
    ...(params.all !== undefined ? { all: params.all } : {}),
  };
}

function acceptedRunSummary(run: AcceptedRun) {
  return {
    mode: "async" as const,
    run_id: run.id,
    worker_id: run.workerId,
  };
}

function statusSummary(catalog: WorkerCatalog, snapshot: OwnerSnapshot) {
  return {
    catalog: {
      workers: catalog.workers.map(catalogWorkerSummary),
      diagnostics: catalog.diagnostics.map(diagnosticSummary),
    },
    state: {
      runs: snapshot.runs.map(runSummary),
      workers: snapshot.workers.map(workerSummary),
    },
  };
}

function catalogWorkerSummary(worker: WorkerDefinition) {
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

function diagnosticSummary(diagnostic: CatalogDiagnostic) {
  return {
    severity: diagnostic.severity,
    source: diagnostic.source,
    message: diagnostic.message,
    file_path: diagnostic.filePath,
  };
}

function runSummary(run: RunRecord) {
  return {
    run_id: run.id,
    owner_session_id: run.ownerSessionId,
    worker_id: run.workerId,
    mode: run.mode,
    state: run.state,
    created_at: run.createdAt,
  };
}

function workerSummary(worker: WorkerRecord) {
  return {
    worker_id: worker.id,
    worker: worker.worker,
    owner_session_id: worker.ownerSessionId,
    run_id: worker.runId,
    title: worker.title,
    lifecycle: worker.lifecycle,
    status: worker.status,
    activity: worker.activity,
    usage: usageSummary(worker.usage),
    outcome: worker.outcome ? outcomeSummary(worker.outcome) : undefined,
    session_file: worker.sessionFile,
  };
}

function usageSummary(usage: WorkerUsage) {
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

function outcomeSummary(outcome: WorkerOutcome) {
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

  return `${truncation.content}\n\n[Output truncated: ${formatSize(truncation.outputBytes)} of ${formatSize(truncation.totalBytes)}. Full output is retained in this tool result's session data.]`;
}
