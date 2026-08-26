import type { AgentToolResult } from "@earendil-works/pi-agent-core";
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
  CompletedRun,
  OwnerSnapshot,
  SettlementListener,
  WorkerRunResult,
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
import {
  encodeInlineWorkerToolDetails,
  type InlineWorkerSettlementDetails,
  type WorkerSettlement,
} from "../orchestration/settlement.ts";

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
    ...orchestrateToolRenderer,
    async execute(toolCallId, params, signal, onUpdate, ctx) {
      const decision = deps.getDispatchDecision(toolCallId);
      const mode = decision.mode;
      const orchestrationContext = buildOrchestrationContext(ctx, deps, decision.synthesisGroup);
      if (mode === "async") {
        const acceptedRun = await deps.orchestration.orchestrate(
          orchestrationContext,
          params,
          "async",
          signal,
        );
        const readable = acceptedRunSummary(acceptedRun);
        return {
          ...readableToolResult(`Accepted async run ${readable.run_id}.`, readable),
          terminate: true,
        };
      }

      const completedRun = await deps.orchestration.orchestrate(
        orchestrationContext,
        params,
        "inline",
        signal,
        createInlineSettlementListener(onUpdate),
      );
      const readable = completedRunSummary(completedRun);
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
      "Diagnostics and recovery only: inspect trusted catalog entries, catalog diagnostics, and this session's orchestration state. Never poll for completion.",
    promptSnippet: "Inspect owned worker state for diagnostics or recovery",
    promptGuidelines: [
      "Use worker_status only for diagnostics or recovery; never poll it for completion.",
    ],
    parameters: statusSchema,
    ...workerStatusToolRenderer,
    async execute(_toolCallId, _params, _signal, _onUpdate, ctx) {
      const ownerSessionId = ctx.sessionManager.getSessionId();
      const catalog = deps.getCatalog(ctx);
      const snapshot = await deps.orchestration.snapshot(ownerSessionId);
      const readable = statusSummary(catalog, snapshot);
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
    ...interactiveSendToolRenderer,
    async execute(toolCallId, params, signal, onUpdate, ctx) {
      const workerId = params.worker_id;
      const mode = deps.getDispatchDecision(toolCallId).mode;
      const orchestrationContext = buildOrchestrationContext(ctx, deps);
      if (mode === "async") {
        const acceptedRun = await deps.orchestration.sendInteractive(
          orchestrationContext,
          workerId,
          params.instructions,
          "async",
          signal,
        );
        const readable = acceptedRunSummary(acceptedRun);
        return {
          ...readableToolResult(`Accepted async run ${readable.run_id}.`, readable),
          terminate: true,
        };
      }

      const completedRun = await deps.orchestration.sendInteractive(
        orchestrationContext,
        workerId,
        params.instructions,
        "inline",
        signal,
        createInlineSettlementListener(onUpdate),
      );
      const readable = completedRunSummary(completedRun);
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
    description: "Close only an owned lifecycle interactive worker whose status is ready. Never use for one-shot or completed workers; one-shot sessions terminate automatically.",
    promptSnippet: "Use only for an owned lifecycle interactive worker with status ready; never one-shot/completed because one-shot sessions terminate automatically",
    promptGuidelines: [
      "Use interactive_close only for an owned lifecycle interactive worker whose status is ready; never use it for one-shot or completed workers because one-shot sessions terminate automatically.",
    ],
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

function acceptedRunSummary(run: AcceptedRun) {
  return {
    mode: "async" as const,
    run_id: run.id,
    worker_id: run.workerId,
  };
}

function completedRunSummary(run: CompletedRun) {
  return encodeInlineWorkerToolDetails({
    mode: "inline",
    runId: run.id,
    ownerSessionId: run.ownerSessionId,
    result: inlineResultValue(run.result),
  });
}

function inlineResultValue(
  result: WorkerRunResult | WorkerSettlement,
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

  return `${truncation.content}\n\n[Output truncated: ${formatSize(truncation.outputBytes)} of ${formatSize(truncation.totalBytes)}. Full structured details remain available.]`;
}
