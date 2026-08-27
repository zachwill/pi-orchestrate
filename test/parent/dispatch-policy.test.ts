import { describe, expect, test } from "bun:test";
import { classifyParentDispatches } from "../../extension/parent/dispatch-policy.ts";

function call(id: string, name: string) {
  return { id, name };
}

describe("parent dispatch policy", () => {
  test.each([
    ["filters non-dispatch tools", [call("read-1", "read")], []],
    ["detaches a sole orchestrate call", [call("dispatch-1", "orchestrate")], [
      { toolCallId: "dispatch-1", decision: { mode: "async" } },
    ]],
    ["detaches a sole interactive call", [call("interactive-1", "interactive_send")], [
      { toolCallId: "interactive-1", decision: { mode: "async" } },
    ]],
    ["groups a homogeneous orchestrate wave", [
      call("first-dispatch", "orchestrate"),
      call("second-dispatch", "orchestrate"),
    ], [
      {
        toolCallId: "first-dispatch",
        decision: {
          mode: "async",
          synthesisGroup: { id: "orchestrate:first-dispatch", size: 2 },
        },
      },
      {
        toolCallId: "second-dispatch",
        decision: {
          mode: "async",
          synthesisGroup: { id: "orchestrate:first-dispatch", size: 2 },
        },
      },
    ]],
    ["keeps dispatches inline beside other work", [
      call("dispatch-1", "orchestrate"),
      call("read-1", "read"),
    ], [
      { toolCallId: "dispatch-1", decision: { mode: "inline" } },
    ]],
    ["keeps mixed dispatch types inline", [
      call("dispatch-1", "orchestrate"),
      call("interactive-1", "interactive_send"),
    ], [
      { toolCallId: "dispatch-1", decision: { mode: "inline" } },
      { toolCallId: "interactive-1", decision: { mode: "inline" } },
    ]],
  ] as const)("%s", (_name, calls, expected) => {
    expect(classifyParentDispatches(calls)).toEqual(expected);
  });
});
