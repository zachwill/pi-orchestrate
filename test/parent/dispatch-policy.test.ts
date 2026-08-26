import { describe, expect, test } from "bun:test";
import { classifyParentDispatches } from "../../extension/parent/dispatch-policy.js";

function call(id: string, name: string) {
  return { id, name };
}

describe("parent dispatch policy", () => {
  test("classifies no calls and non-dispatch calls as no dispatches", () => {
    expect(classifyParentDispatches([])).toEqual([]);
    expect(classifyParentDispatches([call("read-1", "read")])).toEqual([]);
    expect(classifyParentDispatches([
      call("read-1", "read"),
      call("bash-1", "bash"),
    ])).toEqual([]);
  });

  test("classifies a sole orchestrate call as async without a synthesis group", () => {
    expect(classifyParentDispatches([call("dispatch-1", "orchestrate")])).toEqual([{
      toolCallId: "dispatch-1",
      decision: { mode: "async" },
    }]);
  });

  test("classifies a sole interactive_send call as async", () => {
    expect(classifyParentDispatches([call("interactive-1", "interactive_send")])).toEqual([{
      toolCallId: "interactive-1",
      decision: { mode: "async" },
    }]);
  });

  test("classifies orchestrate siblings as one async synthesis group", () => {
    expect(classifyParentDispatches([
      call("first-dispatch", "orchestrate"),
      call("second-dispatch", "orchestrate"),
      call("third-dispatch", "orchestrate"),
    ])).toEqual([
      {
        toolCallId: "first-dispatch",
        decision: {
          mode: "async",
          synthesisGroup: { id: "orchestrate:first-dispatch", size: 3 },
        },
      },
      {
        toolCallId: "second-dispatch",
        decision: {
          mode: "async",
          synthesisGroup: { id: "orchestrate:first-dispatch", size: 3 },
        },
      },
      {
        toolCallId: "third-dispatch",
        decision: {
          mode: "async",
          synthesisGroup: { id: "orchestrate:first-dispatch", size: 3 },
        },
      },
    ]);
  });

  test("classifies orchestrate calls mixed with non-dispatch calls as inline", () => {
    expect(classifyParentDispatches([
      call("dispatch-1", "orchestrate"),
      call("read-1", "read"),
    ])).toEqual([{
      toolCallId: "dispatch-1",
      decision: { mode: "inline" },
    }]);
  });

  test("classifies interactive_send mixed with siblings as inline", () => {
    expect(classifyParentDispatches([
      call("interactive-1", "interactive_send"),
      call("read-1", "read"),
    ])).toEqual([{
      toolCallId: "interactive-1",
      decision: { mode: "inline" },
    }]);
  });

  test("classifies mixed dispatch types as separate inline calls without a synthesis group", () => {
    expect(classifyParentDispatches([
      call("dispatch-1", "orchestrate"),
      call("interactive-1", "interactive_send"),
    ])).toEqual([
      { toolCallId: "dispatch-1", decision: { mode: "inline" } },
      { toolCallId: "interactive-1", decision: { mode: "inline" } },
    ]);
  });
});
