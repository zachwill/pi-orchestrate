import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionFactory,
} from "@earendil-works/pi-coding-agent";
import {
  discoverWorkerCatalog,
  type DiscoverWorkerCatalogOptions,
} from "./catalog/discovery.ts";
import type { WorkerCatalog } from "./catalog/definition.ts";
import { applyOrchestratorContract } from "./parent/contract.ts";
import {
  attachProcessHost,
  createProcessHost,
  destroyProcessHost,
  detachProcessHost,
  type ProcessHost,
  type ProcessHostAttachment,
} from "./parent/process-host.ts";
import {
  createStatusController,
  registerOrchestrationPresentation,
  type StatusController,
  type WorkerStateSource,
} from "./pi/presentation.ts";
import {
  classifyParentDispatches,
  type DispatchDecision,
} from "./parent/dispatch-policy.ts";
import { registerOrchestrationTools } from "./pi/tools.ts";

interface StoredDispatchDecision extends DispatchDecision {
  readonly ownerSessionId: string;
}

interface OwnerBinding {
  readonly ownerSessionId: string;
  readonly generation: symbol;
}

export interface OrchestrationExtensionDependencies {
  getHost?(): ProcessHost;
  destroyHost?(host: ProcessHost): Promise<void>;
  discoverCatalog?(options: DiscoverWorkerCatalogOptions): WorkerCatalog;
  createStatusController?(workerState: WorkerStateSource): StatusController;
}

export function createOrchestrationExtension(
  dependencies: OrchestrationExtensionDependencies = {},
): ExtensionFactory {
  return (pi) => {
    const discoverCatalog = dependencies.discoverCatalog ?? discoverWorkerCatalog;
    const dispatchDecisions = new Map<string, StoredDispatchDecision>();
    let host: ProcessHost | undefined;
    let hostAttachment: ProcessHostAttachment | undefined;
    let statusController: StatusController | undefined;
    let activeBinding: OwnerBinding | undefined;
    let cachedCatalog: WorkerCatalog | undefined;

    const discoverCatalogFor = (ctx: ExtensionContext): WorkerCatalog =>
      discoverCatalog({
        cwd: ctx.cwd,
        projectTrusted: ctx.isProjectTrusted(),
      });

    const catalogFor = (ctx: ExtensionContext): WorkerCatalog => {
      if (cachedCatalog) return cachedCatalog;
      cachedCatalog = discoverCatalogFor(ctx);
      return cachedCatalog;
    };

    registerOrchestrationPresentation(pi);

    pi.on("session_start", (_event, ctx) => {
      host ??= dependencies.getHost?.() ?? createProcessHost();
      statusController ??=
        dependencies.createStatusController?.(host.orchestration) ??
        createStatusController(host.orchestration);
      registerOrchestrationTools(pi, {
        orchestration: host.orchestration,
        getCatalog: catalogFor,
        getDispatchDecision: (toolCallId) =>
          dispatchDecisions.get(toolCallId) ?? { mode: "inline" },
      });
      hostAttachment ??= attachProcessHost(host);

      dispatchDecisions.clear();
      cachedCatalog = undefined;
      const binding: OwnerBinding = {
        ownerSessionId: ctx.sessionManager.getSessionId(),
        generation: Symbol("pi-orchestrate-parent-binding"),
      };
      activeBinding = binding;
      host.delivery.bind({
        ownerSessionId: binding.ownerSessionId,
        generation: binding.generation,
        isIdle: ctx.isIdle,
        sendMessage: pi.sendMessage,
      });
      statusController.bind(binding.ownerSessionId, ctx);
    });

    pi.on("before_agent_start", (event, ctx) => {
      cachedCatalog = discoverCatalogFor(ctx);
      return {
        systemPrompt: applyOrchestratorContract(
          event.systemPrompt,
          cachedCatalog,
        ),
      };
    });

    pi.on("message_end", (event) => {
      if (event.message.role !== "assistant") return;
      const toolCalls = event.message.content.filter(
        (part) => part.type === "toolCall",
      );
      const ownerSessionId = activeBinding?.ownerSessionId;
      if (!ownerSessionId) return;
      for (const dispatch of classifyParentDispatches(toolCalls)) {
        dispatchDecisions.set(dispatch.toolCallId, {
          ...dispatch.decision,
          ownerSessionId,
        });
      }
    });

    pi.on("tool_execution_end", (event) => {
      const decision = dispatchDecisions.get(event.toolCallId);
      dispatchDecisions.delete(event.toolCallId);
      if (!event.isError || !decision?.synthesisGroup) return;
      // Group size is counted before admission, so a failed member must still be
      // accounted for as skipped or the final synthesis turn will never trigger.
      host?.delivery.skipSynthesisGroupMember(
        decision.ownerSessionId,
        decision.synthesisGroup.id,
        decision.synthesisGroup.size,
      );
    });

    pi.on("agent_start", () => {
      const binding = activeBinding;
      if (!binding) return;
      host?.delivery.markAgentStarted(
        binding.ownerSessionId,
        binding.generation,
      );
    });

    pi.on("agent_settled", () => {
      const binding = activeBinding;
      if (!binding) return;
      host?.delivery.markAgentSettled(
        binding.ownerSessionId,
        binding.generation,
      );
    });

    pi.on("session_shutdown", async (event) => {
      const binding = activeBinding;
      activeBinding = undefined;
      cachedCatalog = undefined;
      dispatchDecisions.clear();

      if (binding && host) {
        host.delivery.unbind(binding.ownerSessionId, binding.generation);
      }
      statusController?.dispose();

      const attachment = hostAttachment;
      hostAttachment = undefined;
      const wasLastAttachment = host && attachment
        ? detachProcessHost(host, attachment)
        : false;
      if (event.reason === "quit" && wasLastAttachment && host) {
        await (dependencies.destroyHost ?? destroyProcessHost)(host);
      }
    });
  };
}

export default function piOrchestrateExtension(pi: ExtensionAPI): void {
  createOrchestrationExtension()(pi);
}
