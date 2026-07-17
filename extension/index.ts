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
import { registerOrchestrationTools } from "./tools.js";

const DISPATCH_TOOL_NAMES: ReadonlySet<string> = new Set([
  "orchestrate",
  "worker_send",
]);

type DispatchMode = "async" | "inline";

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
    const dispatchModes = new Map<string, DispatchMode>();
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
      getDispatchMode: (toolCallId) => dispatchModes.get(toolCallId) ?? "inline",
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

      dispatchModes.clear();
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
      const mode: DispatchMode = toolCalls.length === 1 ? "async" : "inline";

      for (const toolCall of toolCalls) {
        if (DISPATCH_TOOL_NAMES.has(toolCall.name)) {
          dispatchModes.set(toolCall.id, mode);
        }
      }
    });

    pi.on("tool_execution_end", (event) => {
      dispatchModes.delete(event.toolCallId);
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
      dispatchModes.clear();

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
