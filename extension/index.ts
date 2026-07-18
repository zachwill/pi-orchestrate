import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionFactory,
} from "@earendil-works/pi-coding-agent";
import {
  discoverWorkerCatalog,
  type DiscoverWorkerCatalogOptions,
} from "./catalog.js";
import { appendOrchestratorContract } from "./contract.js";
import type { WorkerCatalog } from "./domain.js";
import {
  attachProcessHost,
  createProcessHost,
  destroyProcessHost,
  detachProcessHost,
  type ProcessHost,
  type ProcessHostAttachment,
} from "./host.js";
import {
  createStatusController,
  registerOrchestrationPresentation,
  type StatusController,
} from "./presentation.js";
import {
  registerOrchestrationTools,
  type DispatchDecision,
} from "./tools.js";

const DISPATCH_TOOL_NAMES: ReadonlySet<string> = new Set([
  "orchestrate",
  "worker_send",
]);

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
  createStatusController?(runtime: ProcessHost["runtime"]): StatusController;
}

export function createOrchestrationExtension(
  dependencies: OrchestrationExtensionDependencies = {},
): ExtensionFactory {
  return (pi) => {
    const host = dependencies.getHost?.() ?? createProcessHost();
    const discoverCatalog = dependencies.discoverCatalog ?? discoverWorkerCatalog;
    const statusController =
      dependencies.createStatusController?.(host.runtime) ??
      createStatusController(host.runtime);
    const dispatchDecisions = new Map<string, StoredDispatchDecision>();
    let hostAttachment: ProcessHostAttachment | undefined;
    let activeBinding: OwnerBinding | undefined;
    let cachedCatalog: WorkerCatalog | undefined;

    const catalogFor = (ctx: ExtensionContext): WorkerCatalog => {
      if (cachedCatalog) return cachedCatalog;
      cachedCatalog = discoverCatalog({
        cwd: ctx.cwd,
        projectTrusted: ctx.isProjectTrusted(),
      });
      return cachedCatalog;
    };

    registerOrchestrationTools(pi, {
      runtime: host.runtime,
      getCatalog: catalogFor,
      getDispatchDecision: (toolCallId) =>
        dispatchDecisions.get(toolCallId) ?? { mode: "inline" },
    });
    registerOrchestrationPresentation(pi);

    pi.on("session_start", (_event, ctx) => {
      hostAttachment ??= attachProcessHost(host);
      if (activeBinding) {
        host.delivery.unbind(
          activeBinding.ownerSessionId,
          activeBinding.generation,
        );
        statusController.unbind(activeBinding.ownerSessionId);
      }

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
      cachedCatalog = discoverCatalog({
        cwd: ctx.cwd,
        projectTrusted: ctx.isProjectTrusted(),
      });
      return {
        systemPrompt: appendOrchestratorContract(
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
      const groupedOrchestration =
        toolCalls.length > 1 &&
        toolCalls.every((toolCall) => toolCall.name === "orchestrate");
      const group = groupedOrchestration
        ? { id: `orchestrate:${toolCalls[0]?.id ?? "group"}`, size: toolCalls.length }
        : undefined;

      for (const toolCall of toolCalls) {
        if (!DISPATCH_TOOL_NAMES.has(toolCall.name)) continue;
        const mode = groupedOrchestration || toolCalls.length === 1
          ? "async"
          : "inline";
        dispatchDecisions.set(toolCall.id, {
          mode,
          ownerSessionId,
          ...(toolCall.name === "orchestrate" && group ? { group } : {}),
        });
      }
    });

    pi.on("tool_execution_end", (event) => {
      const decision = dispatchDecisions.get(event.toolCallId);
      dispatchDecisions.delete(event.toolCallId);
      if (!event.isError || !decision?.group) return;
      host.delivery.skipDispatchGroupMember(
        decision.ownerSessionId,
        decision.group.id,
        decision.group.size,
      );
    });

    pi.on("agent_start", () => {
      const binding = activeBinding;
      if (!binding) return;
      host.delivery.markAgentStarted(
        binding.ownerSessionId,
        binding.generation,
      );
    });

    pi.on("agent_settled", () => {
      const binding = activeBinding;
      if (!binding) return;
      host.delivery.markAgentSettled(
        binding.ownerSessionId,
        binding.generation,
      );
    });

    pi.on("session_shutdown", async (event) => {
      const binding = activeBinding;
      activeBinding = undefined;
      cachedCatalog = undefined;
      dispatchDecisions.clear();

      if (binding) {
        host.delivery.unbind(binding.ownerSessionId, binding.generation);
      }
      statusController.dispose();

      const attachment = hostAttachment;
      hostAttachment = undefined;
      const wasLastAttachment = attachment
        ? detachProcessHost(host, attachment)
        : false;
      if (event.reason === "quit" && wasLastAttachment) {
        await (dependencies.destroyHost ?? destroyProcessHost)(host);
      }
    });
  };
}

export default function piOrchestrateExtension(pi: ExtensionAPI): void {
  createOrchestrationExtension()(pi);
}
