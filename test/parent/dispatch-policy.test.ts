import { describe, expect, test } from "bun:test";
import { classifyParentDispatches } from "../../extension/parent/dispatch-policy.ts";

function call(id: string, name: string) {
  return { id, name };
}

describe("parent dispatch policy", () => {
  test.each([
    ["filters non-dispatch tools", [call("read-1", "read")], []],
    ["classifies a sole orchestrate call", [call("dispatch-1", "orchestrate")], [
      { toolCallId: "dispatch-1", decision: {} },
    ]],
    ["classifies a sole interactive call", [call("interactive-1", "interactive_send")], [
      { toolCallId: "interactive-1", decision: {} },
    ]],
    ["groups dispatches while excluding ordinary sibling tools", [
      call("first-dispatch", "orchestrate"),
      call("read-1", "read"),
      call("second-dispatch", "orchestrate"),
    ], [
      {
        toolCallId: "first-dispatch",
        decision: {
          synthesisGroup: { id: "dispatch:first-dispatch", size: 2 },
        },
      },
      {
        toolCallId: "second-dispatch",
        decision: {
          synthesisGroup: { id: "dispatch:first-dispatch", size: 2 },
        },
      },
    ]],
    ["classifies a dispatch beside ordinary work without creating a group", [
      call("dispatch-1", "orchestrate"),
      call("read-1", "read"),
    ], [
      { toolCallId: "dispatch-1", decision: {} },
    ]],
    ["groups orchestrate and interactive dispatches together", [
      call("dispatch-1", "orchestrate"),
      call("interactive-1", "interactive_send"),
    ], [
      {
        toolCallId: "dispatch-1",
        decision: {
          synthesisGroup: { id: "dispatch:dispatch-1", size: 2 },
        },
      },
      {
        toolCallId: "interactive-1",
        decision: {
          synthesisGroup: { id: "dispatch:dispatch-1", size: 2 },
        },
      },
    ]],
  ] as const)("%s", (_name, calls, expected) => {
    expect(classifyParentDispatches(calls)).toEqual(expected);
  });

  test("later responses form separate synthesis groups", () => {
    const first = classifyParentDispatches([
      call("first-a", "orchestrate"),
      call("first-b", "orchestrate"),
    ]);
    const second = classifyParentDispatches([
      call("second-a", "orchestrate"),
      call("second-b", "interactive_send"),
    ]);

    expect(first[0]?.decision.synthesisGroup?.id).toBe("dispatch:first-a");
    expect(second[0]?.decision.synthesisGroup?.id).toBe("dispatch:second-a");
  });
});
