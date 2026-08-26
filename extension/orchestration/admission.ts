import type { Api, Model } from "@earendil-works/pi-ai";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";
import { Effect, Schema } from "effect";
import {
  findWorkerByName,
  type WorkerCatalog,
  type WorkerDefinition,
} from "../catalog/definition.js";
import {
  MAX_WORKER_INSTRUCTIONS_LENGTH,
  MAX_WORKER_TITLE_LENGTH,
  OrchestrateTaskInput,
  WorkerId,
  type RunMode,
} from "./model.js";

/** Everything one orchestration request needs from its owning parent session. */
export interface OrchestrationContext {
  readonly ownerSessionId: string;
  readonly cwd: string;
  readonly agentDir: string;
  readonly parentSessionFile: string | undefined;
  readonly projectTrusted: boolean;
  readonly catalog: WorkerCatalog;
  readonly parentModel?: Model<Api>;
  readonly modelRegistry: ModelRegistry;
  readonly synthesisGroup?: {
    readonly id: string;
    readonly size: number;
  };
}

export interface AbortTarget {
  readonly workerIds?: readonly string[];
  readonly all?: boolean;
}

const OrchestrationOperation = Schema.Literals([
  "orchestrate",
  "sendInteractive",
  "abort",
  "closeInteractive",
  "snapshot",
]);
export type OrchestrationOperation = typeof OrchestrationOperation.Type;

const OrchestrationRejectionReason = Schema.Literals([
  "shutdown",
  "validation",
  "ownership",
  "target",
  "worker-state",
  "unknown-worker",
  "model-unavailable",
]);
export type OrchestrationRejectionReason = typeof OrchestrationRejectionReason.Type;

export class OrchestrationActionRejected extends Schema.TaggedError<OrchestrationActionRejected>()(
  "Orchestration.ActionRejected",
  {
    operation: OrchestrationOperation,
    reason: OrchestrationRejectionReason,
    message: Schema.String,
  },
) {}

/** Admission verdict shared by pure ingress checks and stateful orchestration decisions. */
export type Decision<A> =
  | {
      readonly _tag: "accepted";
      readonly value: A;
    }
  | {
      readonly _tag: "rejected";
      readonly error: OrchestrationActionRejected;
    };

export function accepted<A>(value: A): Decision<A> {
  return { _tag: "accepted", value };
}

export function rejected(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): Decision<never> {
  return {
    _tag: "rejected",
    error: actionRejection(operation, reason, message),
  };
}

export function actionRejection(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): OrchestrationActionRejected {
  return new OrchestrationActionRejected({ operation, reason, message });
}

export function rejectAction(
  operation: OrchestrationOperation,
  reason: OrchestrationRejectionReason,
  message: string,
): Effect.Effect<never, OrchestrationActionRejected> {
  return Effect.fail(actionRejection(operation, reason, message));
}

export function validateContextOwner(
  operation: OrchestrationOperation,
  ownerSessionId: string,
): Effect.Effect<void, OrchestrationActionRejected> {
  return typeof ownerSessionId !== "string" || ownerSessionId.trim() === ""
    ? rejectAction(
        operation,
        "validation",
        "ownerSessionId must not be blank",
      )
    : Effect.void;
}

export function validateWorkerId(
  operation: OrchestrationOperation,
  workerId: string,
): Effect.Effect<WorkerId, OrchestrationActionRejected> {
  if (typeof workerId !== "string" || workerId.trim() === "") {
    return rejectAction(
      operation,
      "validation",
      "worker_id must not be blank",
    );
  }
  return Schema.decodeUnknownEffect(WorkerId)(workerId).pipe(
    Effect.mapError(() => actionRejection(
      operation,
      "validation",
      "worker_id must use the canonical worker- prefix",
    )),
  );
}

export function validateMode(
  operation: OrchestrationOperation,
  mode: RunMode,
): Effect.Effect<void, OrchestrationActionRejected> {
  return mode !== "async" && mode !== "inline"
    ? rejectAction(operation, "validation", "Invalid orchestration mode")
    : Effect.void;
}

export function validateText(
  operation: OrchestrationOperation,
  name: string,
  value: string,
  maximumLength: number,
): Effect.Effect<void, OrchestrationActionRejected> {
  if (typeof value !== "string" || value.trim() === "") {
    return rejectAction(operation, "validation", `${name} must not be blank`);
  }
  return value.length > maximumLength
    ? rejectAction(
        operation,
        "validation",
        `${name} must be at most ${maximumLength} characters`,
      )
    : Effect.void;
}

/** Validates one orchestrate request against its context; no orchestration state is read. */
export function validateOrchestrateRequest(
  context: OrchestrationContext,
  task: OrchestrateTaskInput,
  mode: RunMode,
): Effect.Effect<
  { definition: WorkerDefinition; task: OrchestrateTaskInput },
  OrchestrationActionRejected
> {
  return Effect.gen(function* () {
    yield* validateContextOwner("orchestrate", context.ownerSessionId);
    yield* validateMode("orchestrate", mode);
    if (!task || typeof task !== "object" || Array.isArray(task)) {
      return yield* rejectAction(
        "orchestrate",
        "validation",
        "orchestrate requires one task object",
      );
    }
    if (context.synthesisGroup) {
      yield* validateText(
        "orchestrate",
        "synthesis group ID",
        context.synthesisGroup.id,
        MAX_WORKER_TITLE_LENGTH,
      );
      if (mode !== "async") {
        return yield* rejectAction(
          "orchestrate",
          "validation",
          "Sibling synthesis requires an async task",
        );
      }
      if (
        !Number.isSafeInteger(context.synthesisGroup.size) ||
        context.synthesisGroup.size < 2
      ) {
        return yield* rejectAction(
          "orchestrate",
          "validation",
          "Synthesis group size must be an integer of at least 2",
        );
      }
    }
    yield* validateText(
      "orchestrate",
      "worker",
      task.worker,
      MAX_WORKER_TITLE_LENGTH,
    );
    yield* validateText(
      "orchestrate",
      "title",
      task.title,
      MAX_WORKER_TITLE_LENGTH,
    );
    yield* validateText(
      "orchestrate",
      "instructions",
      task.instructions,
      MAX_WORKER_INSTRUCTIONS_LENGTH,
    );
    const definition = findWorkerByName(context.catalog, task.worker);
    if (!definition) {
      return yield* rejectAction(
        "orchestrate",
        "unknown-worker",
        `Unknown worker: ${task.worker}`,
      );
    }
    const configured = definition.model;
    if (!configured && !context.parentModel) {
      return yield* rejectAction(
        "orchestrate",
        "model-unavailable",
        `Worker "${definition.name}" has no configured model and no parent model is available`,
      );
    }
    if (
      configured &&
      !context.modelRegistry.find(configured.provider, configured.modelId)
    ) {
      return yield* rejectAction(
        "orchestrate",
        "model-unavailable",
        `Worker "${definition.name}" configured model "${configured.provider}/${configured.modelId}" was not found`,
      );
    }
    return { definition, task };
  });
}

export type ValidatedAbortTarget =
  | {
      readonly _tag: "ids";
      readonly workerIds: readonly WorkerId[];
    }
  | {
      readonly _tag: "all";
    };

export function validateAbortTarget(
  target: AbortTarget,
): Effect.Effect<ValidatedAbortTarget, OrchestrationActionRejected> {
  return Effect.gen(function* () {
    if (!target || typeof target !== "object") {
      return yield* rejectAction("abort", "target", "Invalid abort target");
    }
    const selected = [target.workerIds !== undefined, target.all !== undefined]
      .filter(Boolean).length;
    if (selected !== 1 || (target.all !== undefined && target.all !== true)) {
      return yield* rejectAction(
        "abort",
        "target",
        "Abort target must specify exactly one of workerIds or all: true",
      );
    }
    if (target.workerIds === undefined) return { _tag: "all" };
    if (!Array.isArray(target.workerIds) || target.workerIds.length === 0) {
      return yield* rejectAction(
        "abort",
        "target",
        "workerIds must contain at least one worker ID",
      );
    }
    const workerIds = yield* Effect.all(
      [...new Set(target.workerIds)].map((id) => validateWorkerId("abort", id)),
    );
    return { _tag: "ids", workerIds };
  });
}
