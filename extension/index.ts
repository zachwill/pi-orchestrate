import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import { installDefaultWorkersOnce } from "./default-workers.js";
import { registerCrewIntegration } from "./integration.js";
import { workerRuntime } from "./runtime/worker-runtime.js";
import { stopWidgetAnimation, updateWidget } from "./status-widget.js";

const extensionDir = dirname(fileURLToPath(import.meta.url));
const hooksKey = Symbol.for("pi-workers.processHooksSetup");
const globalHooks = globalThis as typeof globalThis & Record<symbol, boolean | undefined>;

function setupProcessHooks(): void {
  if (globalHooks[hooksKey]) return;

  globalHooks[hooksKey] = true;

  process.once("SIGINT", () => {
    workerRuntime.abortAll();
    process.exit(130);
  });

  process.on("beforeExit", () => {
    workerRuntime.abortAll();
  });
}

export default function registerPiWorkers(pi: ExtensionAPI) {
  let currentCtx: ExtensionContext | undefined;

  setupProcessHooks();

  const defaultWorkerNotes = installDefaultWorkersOnce(extensionDir);

  const refreshWidget = () => {
    if (currentCtx) updateWidget(currentCtx, workerRuntime);
  };

  pi.on("session_start", (_event, ctx) => {
    currentCtx = ctx;

    for (const note of defaultWorkerNotes) {
      ctx.ui.notify(note, "info");
    }

    workerRuntime.activateSession(
      {
        sessionId: ctx.sessionManager.getSessionId(),
        isIdle: () => ctx.isIdle(),
        sendMessage: pi.sendMessage.bind(pi),
      },
      refreshWidget,
    );
  });

  pi.on("session_shutdown", (event, ctx) => {
    const sessionId = ctx.sessionManager.getSessionId();
    workerRuntime.deactivateSession(sessionId);
    stopWidgetAnimation();

    if (event.reason === "quit") {
      workerRuntime.abortAll();
    }
  });

  registerCrewIntegration(pi, workerRuntime, extensionDir);
}
