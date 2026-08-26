export interface ParentToolCall {
  readonly id: string;
  readonly name: string;
}

export interface ParentDispatchDecision {
  readonly mode: "async" | "inline";
  readonly synthesisGroup?: {
    readonly id: string;
    readonly size: number;
  };
}

export interface ClassifiedParentDispatch {
  readonly toolCallId: string;
  readonly decision: ParentDispatchDecision;
}

const DISPATCH_TOOL_NAMES: ReadonlySet<string> = new Set([
  "orchestrate",
  "interactive_send",
]);

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
