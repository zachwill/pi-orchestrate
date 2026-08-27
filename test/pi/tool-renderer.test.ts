import { beforeAll, describe, expect, test } from "bun:test";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import { initTheme, type Theme } from "@earendil-works/pi-coding-agent";
import {
  interactiveCloseToolRenderer,
  interactiveSendToolRenderer,
  orchestrateToolRenderer,
  workerAbortToolRenderer,
  workerStatusToolRenderer,
} from "../../extension/pi/tool-renderer.ts";

beforeAll(() => initTheme("dark", false));

const theme = {
  fg: (_color: string, text: string) => text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
  italic: (text: string) => text,
  underline: (text: string) => text,
  inverse: (text: string) => text,
  strikethrough: (text: string) => text,
} as Theme;

function result(details: unknown, text = "completed"): AgentToolResult<unknown> {
  return { content: [{ type: "text", text }], details };
}

function inlineDetails() {
  return {
    mode: "inline",
    run_id: "run-inline",
    owner_session_id: "owner-session",
    result: {
      worker_id: "worker-inline",
      worker: "scout",
      title: "Inspect",
      status: "completed",
      outcome: { status: "completed", assistant_text: "Inspection complete." },
      usage: {
        input: 11,
        output: 12,
        cache_read: 13,
        cache_write: 14,
        cost: 0.15,
        context_tokens: 16,
        turns: 2,
      },
      started_at: 1_000,
      settled_at: 6_000,
      session_file: "/sessions/worker-inline.jsonl",
    },
  };
}

const options = (overrides: Partial<{ isPartial: boolean; expanded: boolean }> = {}) => ({
  isPartial: false,
  expanded: false,
  ...overrides,
});
const context = (overrides: Partial<{ isError: boolean; lastComponent: unknown }> = {}) => ({
  isError: false,
  ...overrides,
});

describe("tool call renderers", () => {
  test("bounds previews and preserves a sanitized expanded control payload", () => {
    const instructions = `BEGIN\t\u001b[31mred\u0000\n${"UNBROKEN".repeat(12_500)}\nTAIL`;
    const task = { worker: "scout", title: "Inspect", instructions };
    const collapsed = orchestrateToolRenderer.renderCall(task, theme, options()).render(32);
    expect(collapsed.length).toBeLessThanOrEqual(5);
    expect(collapsed.every((line) => Bun.stringWidth(line) <= 32)).toBe(true);

    const expandedLines = orchestrateToolRenderer.renderCall(
      task,
      theme,
      options({ expanded: true }),
    ).render(120_000);
    const expanded = Bun.stripANSI(expandedLines.join("\n"));
    expect(expanded).toContain("BEGIN    ␛[31mred␀");
    expect(expanded).toContain("UNBROKEN".repeat(12_500));
    expect(expanded).toContain("TAIL");
    expect(expandedLines.every((line) => Bun.stringWidth(line) <= 120_000)).toBe(true);

    expect(workerAbortToolRenderer.renderCall(null, theme).render(1)
      .every((line) => Bun.stringWidth(line) <= 1)).toBe(true);
    expect(interactiveCloseToolRenderer.renderCall(null, theme).render(1)
      .every((line) => Bun.stringWidth(line) <= 1)).toBe(true);
  });

  test("preserves and sanitizes the expanded interactive follow-up payload", () => {
    const instructions = `FOLLOW_UP_BEGIN\t\u001b[31mred\u0000\n${"INTERIOR ".repeat(5_000)}\nFOLLOW_UP_TAIL`;
    const lines = interactiveSendToolRenderer.renderCall(
      { worker_id: "worker-interactive", instructions },
      theme,
      options({ expanded: true }),
    ).render(60_000);
    const output = Bun.stripANSI(lines.join("\n"));

    expect(output).toContain("worker-interactive");
    expect(output).toContain("FOLLOW_UP_BEGIN    ␛[31mred␀");
    expect(output).toContain("INTERIOR ".repeat(5_000));
    expect(output).toContain("FOLLOW_UP_TAIL");
    expect(output).not.toContain("\u001b");
    expect(output).not.toContain("\u0000");
    expect(lines.every((line) => Bun.stringWidth(line) <= 60_000)).toBe(true);
  });
});

describe("tool result renderers", () => {
  test("uses error context instead of optimistic success presentation", () => {
    const errorResult = result(
      undefined,
      "Invalid tool arguments: invalid-placeholder\nExpected worker_id to match ^worker-\\S+$",
    );
    const renderers = [
      orchestrateToolRenderer,
      workerStatusToolRenderer,
      interactiveSendToolRenderer,
      workerAbortToolRenderer,
      interactiveCloseToolRenderer,
    ] as const;
    for (const renderer of renderers) {
      const rendered = renderer.renderResult(
        errorResult,
        options(),
        theme,
        context({ isError: true }),
      );
      const output = Bun.stripANSI(rendered.render(120).join("\n"));
      expect(output).toContain("invalid-placeholder");
      expect(output).not.toContain("✓");
    }
  });

  test("preserves a canonical inline failure without optimistic success presentation", () => {
    const valid = inlineDetails();
    const failed = {
      ...valid,
      result: {
        ...valid.result,
        status: "failed",
        outcome: {
          status: "failed",
          message: "Canonical worker failure.",
        },
      },
    };
    const rendered = orchestrateToolRenderer.renderResult(
      result(failed), options(), theme, context(),
    );
    const output = Bun.stripANSI(rendered.render(80).join("\n"));
    expect(output).toContain("Canonical worker failure.");
    expect(output).not.toContain("✓");
  });

  test("renders a contradictory inline settlement neutrally", () => {
    const valid = inlineDetails();
    const contradictory = {
      ...valid,
      result: {
        ...valid.result,
        outcome: { status: "failed", message: "contradiction" },
      },
    };
    const rendered = orchestrateToolRenderer.renderResult(
      result(contradictory), options(), theme, context(),
    );
    const output = Bun.stripANSI(rendered.render(80).join("\n"));
    expect(output).toContain("details unavailable");
    expect(output).not.toContain("✓");
  });

  test("updates and reuses the inline result component for partial and expanded states", () => {
    const first = orchestrateToolRenderer.renderResult(
      result(inlineDetails()),
      options({ isPartial: true }),
      theme,
      context(),
    );

    const reused = orchestrateToolRenderer.renderResult(
      result({
        ...inlineDetails(),
        result: {
          ...inlineDetails().result,
          outcome: { status: "completed", assistant_text: "Expanded response." },
        },
      }),
      options({ expanded: true }),
      theme,
      context({ lastComponent: first }),
    );
    expect(reused).toBe(first);
    expect(Bun.stripANSI(reused.render(80).join("\n"))).toContain("Expanded response.");
    expect(() => (reused as { dispose?: () => void }).dispose?.()).not.toThrow();
  });

});
