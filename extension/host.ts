import { DeliveryCoordinator } from "./delivery.js";
import {
  createOrchestratorRuntime,
  type OrchestratorRuntime,
} from "./runtime.js";
import { createWorkerSessionFactory } from "./worker-session.js";

const PROCESS_HOST_KEY = Symbol.for("@zachwill/pi-orchestrate/process-host/v2");
const LEGACY_PROCESS_HOST_KEY = Symbol.for("@zachwill/pi-orchestrate/process-host/v1");

export interface ProcessHost {
  readonly runtime: OrchestratorRuntime;
  readonly delivery: DeliveryCoordinator;
}

export interface ProcessHostAttachment {
  readonly host: ProcessHost;
}

interface AttachmentAwareProcessHost extends ProcessHost {
  attachments?: Set<ProcessHostAttachment>;
}

interface OwnedProcessHost extends AttachmentAwareProcessHost {
  readonly unsubscribeSettlement?: () => void;
  destroyPromise?: Promise<void>;
}

interface LegacyProcessHost extends ProcessHost {
  unsubscribeCompletion?: () => void;
  unsubscribeSettlement?: () => void;
}

type ProcessGlobal = typeof globalThis & {
  [PROCESS_HOST_KEY]?: OwnedProcessHost;
  [LEGACY_PROCESS_HOST_KEY]?: LegacyProcessHost;
};

function processGlobal(): ProcessGlobal {
  return globalThis as ProcessGlobal;
}

export function getProcessHost(): ProcessHost | undefined {
  return processGlobal()[PROCESS_HOST_KEY];
}

export function createProcessHost(): ProcessHost {
  const global = processGlobal();
  const existing = global[PROCESS_HOST_KEY];
  if (existing) return existing;

  retireLegacyProcessHost(global);

  const runtime = createOrchestratorRuntime({
    workerSessionFactory: createWorkerSessionFactory(),
  });
  const delivery = new DeliveryCoordinator();
  const host: OwnedProcessHost = {
    runtime,
    delivery,
    attachments: new Set(),
    unsubscribeSettlement: runtime.subscribeSettlement((settlement) => {
      delivery.accept(settlement);
    }),
  };
  global[PROCESS_HOST_KEY] = host;
  return host;
}

export function attachProcessHost(host: ProcessHost): ProcessHostAttachment {
  const attachment: ProcessHostAttachment = { host };
  const attachmentAwareHost = host as AttachmentAwareProcessHost;
  attachmentAwareHost.attachments ??= new Set();
  attachmentAwareHost.attachments.add(attachment);
  return attachment;
}

export function detachProcessHost(
  host: ProcessHost,
  attachment: ProcessHostAttachment,
): boolean {
  if (attachment.host !== host) return false;

  const attachments = (host as AttachmentAwareProcessHost).attachments;
  if (!attachments?.delete(attachment)) return false;
  return attachments.size === 0;
}

export async function destroyProcessHost(host: ProcessHost): Promise<void> {
  const ownedHost = host as OwnedProcessHost;
  if ((ownedHost.attachments?.size ?? 0) > 0) return;
  if (ownedHost.destroyPromise) {
    await ownedHost.destroyPromise;
    return;
  }

  ownedHost.destroyPromise = (async () => {
    try {
      await ownedHost.runtime.shutdown();
    } finally {
      ownedHost.delivery.clear();
      ownedHost.unsubscribeSettlement?.();
      const global = processGlobal();
      if (global[PROCESS_HOST_KEY] === ownedHost) {
        delete global[PROCESS_HOST_KEY];
      }
    }
  })();
  await ownedHost.destroyPromise;
}

export async function quitProcessHost(): Promise<void> {
  const host = getProcessHost();
  if (!host) return;
  await destroyProcessHost(host);
}

function retireLegacyProcessHost(global: ProcessGlobal): void {
  const legacy = global[LEGACY_PROCESS_HOST_KEY];
  if (!legacy) return;

  delete global[LEGACY_PROCESS_HOST_KEY];
  try {
    legacy.unsubscribeCompletion?.();
  } catch {
    // A stale subscription cannot prevent installation of the current host.
  }
  try {
    legacy.unsubscribeSettlement?.();
  } catch {
    // A stale subscription cannot prevent installation of the current host.
  }
  try {
    legacy.delivery.close();
  } catch {
    // Legacy delivery cleanup is best-effort during reload migration.
  }
  try {
    void legacy.runtime.shutdown().catch(() => undefined);
  } catch {
    // Legacy runtime shutdown is best-effort during reload migration.
  }
}
