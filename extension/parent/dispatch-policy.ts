import type { SynthesisGroup } from "../orchestration/model.ts";

export interface ParentToolCall {
  readonly id: string;
  readonly name: string;
}

export interface DispatchDecision {
  readonly mode: "async" | "inline";
  readonly synthesisGroup?: SynthesisGroup;
}

export interface ClassifiedParentDispatch {
  readonly toolCallId: string;
  readonly decision: DispatchDecision;
}

const DISPATCH_TOOL_NAMES: ReadonlySet<string> = new Set([
  "orchestrate",
  "interactive_send",
]);

// Sole dispatches and homogeneous orchestrate waves detach so the parent turn can
// end while work continues. Mixed tools stay inline because their shared parent
// turn still has sibling work; one wave boundary defers one synthesis turn until
// every admitted member has settled.
export function classifyParentDispatches(
  toolCalls: readonly ParentToolCall[],
): readonly ClassifiedParentDispatch[] {
  const isOrchestrateGroup =
    toolCalls.length > 1 &&
    toolCalls.every((toolCall) => toolCall.name === "orchestrate");
  const synthesisGroup = isOrchestrateGroup
    ? { id: `orchestrate:${toolCalls[0]?.id ?? "group"}`, size: toolCalls.length }
    : undefined;

  return toolCalls.flatMap((toolCall): ClassifiedParentDispatch[] => {
    if (!DISPATCH_TOOL_NAMES.has(toolCall.name)) return [];
    return [{
      toolCallId: toolCall.id,
      decision: {
        mode: isOrchestrateGroup || toolCalls.length === 1 ? "async" : "inline",
        ...(toolCall.name === "orchestrate" && synthesisGroup
          ? { synthesisGroup }
          : {}),
      },
    }];
  });
}
