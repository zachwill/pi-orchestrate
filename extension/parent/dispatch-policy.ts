import type { SynthesisGroup } from "../orchestration/model.ts";

export interface ParentToolCall {
  readonly id: string;
  readonly name: string;
}

export interface DispatchDecision {
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

// Every public dispatch detaches so sibling parent tools can finish independently.
// Dispatches from one assistant response share a synthesis boundary; ordinary
// sibling tools are neither group members nor part of its expected size.
export function classifyParentDispatches(
  toolCalls: readonly ParentToolCall[],
): readonly ClassifiedParentDispatch[] {
  const dispatches = toolCalls.filter((toolCall) =>
    DISPATCH_TOOL_NAMES.has(toolCall.name)
  );
  const synthesisGroup = dispatches.length > 1
    ? { id: `dispatch:${dispatches[0]!.id}`, size: dispatches.length }
    : undefined;

  return dispatches.map((toolCall) => ({
    toolCallId: toolCall.id,
    decision: synthesisGroup ? { synthesisGroup } : {},
  }));
}
