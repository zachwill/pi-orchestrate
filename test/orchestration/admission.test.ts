import { describe, expect, test } from "bun:test";
import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Cause, Effect, Exit } from "effect";
import {
  createWorkerCatalog,
  type WorkerDefinition,
} from "../../extension/catalog/definition.js";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  type OrchestrateTaskInput,
} from "../../extension/orchestration/model.js";
import {
  OrchestrationActionRejected,
  accepted,
  actionRejection,
  rejected,
  validateAbortTarget,
  validateContextOwner,
  validateMode,
  validateOrchestrateRequest,
  validateText,
  validateWorkerId,
  type OrchestrationContext,
} from "../../extension/orchestration/admission.js";

function model(provider: string, id: string): Model<Api> {
  return {
    provider,
    id,
    name: id,
    api: "openai-responses",
    baseUrl: "https://example.test",
    reasoning: false,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 128_000,
    maxTokens: 16_000,
  };
}

function definition(
  name: string,
  configuredModel?: WorkerDefinition["model"],
): WorkerDefinition {
  return {
    name,
    source: { kind: "package", filePath: `/workers/${name}.md` },
    description: `${name} worker`,
    systemPrompt: `You are ${name}.`,
    lifecycle: "one-shot",
    tools: ["read"],
    skills: [],
    model: configuredModel,
  };
}

function context(
  workers: readonly WorkerDefinition[],
  overrides: Partial<OrchestrationContext> = {},
): OrchestrationContext {
  return {
    ownerSessionId: "owner",
    cwd: "/project",
    agentDir: "/agent",
    parentSessionFile: "/sessions/parent.jsonl",
    projectTrusted: true,
    catalog: createWorkerCatalog(workers),
    parentModel: model("parent", "selected"),
    modelRegistry: { find: () => undefined } as unknown as ModelRegistry,
    ...overrides,
  };
}

const task = (worker: string): OrchestrateTaskInput => ({
  worker,
  title: `Use ${worker}`,
  instructions: `Instructions for ${worker}`,
});

async function rejectionOf<A>(
  effect: Effect.Effect<A, OrchestrationActionRejected>,
): Promise<OrchestrationActionRejected> {
  const exit = await Effect.runPromiseExit(effect);
  if (!Exit.isFailure(exit)) throw new Error("Expected admission rejection");
  const error = Cause.squash(exit.cause);
  expect(error).toBeInstanceOf(OrchestrationActionRejected);
  return error as OrchestrationActionRejected;
}

describe("admission primitives", () => {
  test("constructs accepted and typed rejected decisions without losing error identity", () => {
    expect(accepted("value")).toEqual({ _tag: "accepted", value: "value" });
    const error = actionRejection("abort", "target", "Invalid abort target");
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toBe("Invalid abort target");
    const decision = rejected("abort", "target", "Invalid abort target");
    expect(decision).toMatchObject({ _tag: "rejected", error });
    if (decision._tag === "rejected") expect(decision.error).not.toBe(error);
  });

  test("validates context owners, worker IDs, run modes, and bounded text", async () => {
    await expect(Effect.runPromise(validateContextOwner("snapshot", "owner"))).resolves.toBeUndefined();
    expect(String(await Effect.runPromise(
      validateWorkerId("sendInteractive", "worker-1"),
    ))).toBe("worker-1");
    await expect(Effect.runPromise(validateMode("orchestrate", "inline"))).resolves.toBeUndefined();
    await expect(Effect.runPromise(validateText("orchestrate", "title", "x", 1))).resolves.toBeUndefined();

    const cases = [
      validateContextOwner("snapshot", " "),
      validateWorkerId("closeInteractive", "\n"),
      validateWorkerId("sendInteractive", "run-1"),
      validateMode("orchestrate", "background" as never),
      validateText("orchestrate", "title", "\t", 10),
      validateText("orchestrate", "title", "xx", 1),
    ];
    const expectedMessages = [
      "ownerSessionId must not be blank",
      "worker_id must not be blank",
      "worker_id must use the canonical worker- prefix",
      "Invalid orchestration mode",
      "title must not be blank",
      "title must be at most 1 characters",
    ];
    for (let index = 0; index < cases.length; index += 1) {
      expect((await rejectionOf(cases[index]!)).message).toBe(expectedMessages[index]!);
    }
  });

  test("normalizes valid abort targets and rejects malformed targets atomically", async () => {
    expect(await Effect.runPromise(validateAbortTarget({ all: true }))).toEqual({ _tag: "all" });
    const ids = await Effect.runPromise(validateAbortTarget({
      workerIds: ["worker-1", "worker-1", "worker-2"],
    }));
    expect(ids._tag).toBe("ids");
    if (ids._tag === "ids") expect(ids.workerIds.map(String)).toEqual(["worker-1", "worker-2"]);

    const cases = [
      [undefined, "Invalid abort target"],
      [{}, "Abort target must specify exactly one of workerIds or all: true"],
      [{ all: false }, "Abort target must specify exactly one of workerIds or all: true"],
      [{ all: true, workerIds: ["worker-1"] }, "Abort target must specify exactly one of workerIds or all: true"],
      [{ workerIds: [] }, "workerIds must contain at least one worker ID"],
      [{ workerIds: ["worker-1", " "] }, "worker_id must not be blank"],
    ] as const;
    for (const [target, message] of cases) {
      const error = await rejectionOf(validateAbortTarget(target as never));
      expect(error).toMatchObject({ operation: "abort", message });
    }
  });
});

describe("orchestrate request preflight", () => {
  test("returns the selected catalog definition and original task", async () => {
    const known = definition("known");
    const input = task("known");
    await expect(Effect.runPromise(
      validateOrchestrateRequest(context([known]), input, "async"),
    )).resolves.toEqual({ definition: known, task: input });
  });

  test("rejects malformed tasks, catalog misses, and unavailable models", async () => {
    const known = definition("known");
    const configured = definition("configured", {
      provider: "provider",
      modelId: "missing",
    });
    const cases = [
      [context([known]), undefined, "orchestrate requires one task object"],
      [context([known]), { ...task("known"), worker: " " }, "worker must not be blank"],
      [context([known]), { ...task("known"), title: "x".repeat(MAX_WORKER_TITLE_LENGTH + 1) }, `title must be at most ${MAX_WORKER_TITLE_LENGTH} characters`],
      [context([known]), { ...task("known"), instructions: "x".repeat(MAX_WORKER_INSTRUCTIONS_LENGTH + 1) }, `instructions must be at most ${MAX_WORKER_INSTRUCTIONS_LENGTH} characters`],
      [context([known]), task("missing"), "Unknown worker: missing"],
      [context([configured]), task("configured"), 'Worker "configured" configured model "provider/missing" was not found'],
      [context([known], { parentModel: undefined }), task("known"), 'Worker "known" has no configured model and no parent model is available'],
    ] as const;
    for (const [owner, input, message] of cases) {
      expect((await rejectionOf(
        validateOrchestrateRequest(owner, input as OrchestrateTaskInput, "async"),
      )).message).toBe(message);
    }
  });

  test("validates sibling synthesis metadata and async mode", async () => {
    const known = definition("known");
    const cases = [
      [{ id: " ", size: 2 }, "async", "synthesis group ID must not be blank"],
      [{ id: "group", size: 2 }, "inline", "Sibling synthesis requires an async task"],
      [{ id: "group", size: 1 }, "async", "Synthesis group size must be an integer of at least 2"],
      [{ id: "group", size: 2.5 }, "async", "Synthesis group size must be an integer of at least 2"],
    ] as const;
    for (const [synthesisGroup, mode, message] of cases) {
      const error = await rejectionOf(validateOrchestrateRequest(
        context([known], { synthesisGroup }),
        task("known"),
        mode,
      ));
      expect(error.message).toBe(message);
    }
  });

  test("keeps model registry exceptions as defects", async () => {
    const defect = new Error("registry defect");
    const configured = definition("configured", {
      provider: "provider",
      modelId: "available",
    });
    const registry = { find: () => { throw defect; } } as unknown as ModelRegistry;
    const exit = await Effect.runPromiseExit(validateOrchestrateRequest(
      context([configured], { modelRegistry: registry }),
      task("configured"),
      "async",
    ));

    if (!Exit.isFailure(exit)) throw new Error("Expected registry defect");
    expect(Cause.squash(exit.cause)).toBe(defect);
    expect(Cause.squash(exit.cause)).not.toBeInstanceOf(OrchestrationActionRejected);
  });
});
