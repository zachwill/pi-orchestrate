import { beforeAll, describe, expect, test } from "bun:test";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import { initTheme, type Theme } from "@earendil-works/pi-coding-agent";
import {
  interactiveCloseToolRenderer,
  interactiveSendToolRenderer,
  orchestrateToolRenderer,
  workerAbortToolRenderer,
  workerStatusToolRenderer,
} from "../../extension/pi/tool-renderer.js";

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
  test("renders malformed and partial calls without crashing", () => {
    const cases = [
      orchestrateToolRenderer.renderCall({ worker: "scout" }, theme, options()),
      orchestrateToolRenderer.renderCall(null, theme, options({ expanded: true })),
      interactiveSendToolRenderer.renderCall({ worker_id: "worker-ready" }, theme, options()),
      interactiveSendToolRenderer.renderCall(null, theme, options()),
      workerAbortToolRenderer.renderCall(null, theme),
      workerAbortToolRenderer.renderCall({ worker_ids: "not-an-array" }, theme),
      interactiveCloseToolRenderer.renderCall(null, theme),
    ];
    const names = [
      "orchestrate scout",
      "orchestrate",
      "interactive_send worker-ready",
      "interactive_send",
      "worker_abort",
      "worker_abort",
      "interactive_close",
    ];
    for (let index = 0; index < cases.length; index += 1) {
      const lines = cases[index]!.render(40);
      expect(lines.every((line) => Bun.stringWidth(line) <= 40)).toBe(true);
      expect(Bun.stripANSI(lines.join("\n"))).toContain(names[index]!);
    }
  });

  test("keeps collapsed instruction previews bounded and expanded instructions exact", () => {
    const instructions = `  First  exact\tline.\r\n\r\nUnicode 雪 \u001b[31mred\u0000\n${"UNBROKEN".repeat(12_500)}\nTAIL  `;
    const task = { worker: "scout", title: "Inspect", instructions };
    const collapsed = orchestrateToolRenderer.renderCall(task, theme, options()).render(32);
    expect(collapsed.every((line) => Bun.stringWidth(line) <= 32)).toBe(true);
    const collapsedText = Bun.stripANSI(collapsed.join("\n"));
    expect(collapsedText).toContain("First exact line.");
    expect(collapsedText).toContain("…");
    expect(collapsedText).toContain("to inspect full instructions");

    const expanded = Bun.stripANSI(orchestrateToolRenderer.renderCall(
      task,
      theme,
      options({ expanded: true }),
    ).render(120_000).join("\n"));
    expect(expanded).toContain("  First  exact    line.");
    expect(expanded).toContain("Unicode 雪 ␛[31mred␀");
    expect(expanded).toContain("UNBROKEN".repeat(12_500));
    expect(expanded).toContain("TAIL  ");

    const sendExpanded = Bun.stripANSI(interactiveSendToolRenderer.renderCall(
      { worker_id: "worker-1", instructions },
      theme,
      options({ expanded: true }),
    ).render(120_000).join("\n"));
    expect(sendExpanded).toContain("UNBROKEN".repeat(12_500));
    expect(sendExpanded).toContain("TAIL  ");
  });
});

describe("tool result renderers", () => {
  test("uses error context instead of optimistic success presentation", () => {
    const errorResult = result(
      undefined,
      "Invalid tool arguments: invalid-placeholder\nExpected worker_id to match ^worker-\\S+$",
    );
    const errorTheme = {
      ...theme,
      fg: (color: string, text: string) => `<${color}>${text}</${color}>`,
    } as Theme;
    const renderers = [
      [orchestrateToolRenderer, "Work sent"],
      [workerStatusToolRenderer, "Diagnostics unavailable"],
      [interactiveSendToolRenderer, "Work sent"],
      [workerAbortToolRenderer, "Worker stop requested"],
      [interactiveCloseToolRenderer, "Worker closed"],
    ] as const;
    for (const [renderer, successText] of renderers) {
      const rendered = renderer.renderResult(
        errorResult,
        options(),
        errorTheme,
        context({ isError: true }),
      );
      const output = Bun.stripANSI(rendered.render(120).join("\n"));
      expect(output).toContain("<error>Invalid tool arguments: invalid-placeholder</error>");
      expect(output).not.toContain(successText);
      expect(output).not.toContain("✓");
    }
  });

  test("renders accepted, unavailable, partial, and fallback orchestration results neutrally", () => {
    const accepted = orchestrateToolRenderer.renderResult(
      result({ mode: "async", run_id: "run-1", worker_id: "worker-1" }),
      options(), theme, context(),
    );
    expect(Bun.stripANSI(accepted.render(80).join("\n"))).toContain("Sent to worker");

    const unavailable = orchestrateToolRenderer.renderResult(
      result({ result: { bad: true } }), options(), theme, context(),
    );
    expect(Bun.stripANSI(unavailable.render(80).join("\n"))).toContain("details unavailable");

    const partial = orchestrateToolRenderer.renderResult(
      result(undefined), options({ isPartial: true }), theme, context(),
    );
    expect(Bun.stripANSI(partial.render(80).join("\n"))).toContain("Sending work");

    const fallback = orchestrateToolRenderer.renderResult(
      result(undefined, "Adapter response"), options(), theme, context(),
    );
    expect(Bun.stripANSI(fallback.render(80).join("\n"))).toContain("Adapter response");
  });

  test("renders inline failure details and rejects malformed details", () => {
    const valid = inlineDetails();
    const failed = {
      ...valid,
      result: {
        ...valid.result,
        status: "failed",
        outcome: { status: "failed", message: "Worker failed." },
      },
    };
    const output = Bun.stripANSI(orchestrateToolRenderer.renderResult(
      result(failed, "failed"), options(), theme, context(),
    ).render(80).join("\n"));
    expect(output).toContain("✗ Inspect · scout · failed · 5s");
    expect(output).toContain("Worker failed.");

    const malformed = [
      { ...valid, result: { ...valid.result, outcome: { status: "failed", message: "no" } } },
      { ...valid, result: { ...valid.result, worker_id: 42 } },
      { ...valid, result: { ...valid.result, status: "closed", outcome: { status: "closed" } } },
    ];
    for (const details of malformed) {
      const rendered = orchestrateToolRenderer.renderResult(
        result(details), options(), theme, context(),
      );
      const neutral = Bun.stripANSI(rendered.render(80).join("\n"));
      expect(neutral).toContain("details unavailable");
      expect(neutral).not.toContain("✓");
    }
  });

  test("updates and reuses the inline result component for partial and expanded states", () => {
    const first = orchestrateToolRenderer.renderResult(
      result(inlineDetails()),
      options({ isPartial: true }),
      theme,
      context(),
    );
    expect(Bun.stripANSI(first.render(80).join("\n"))).toContain("Receiving worker response");

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
    const output = Bun.stripANSI(reused.render(80).join("\n"));
    expect(output).toContain("Expanded response.");
    expect(output).not.toContain("Receiving worker response");
    expect(() => (reused as { dispose?: () => void }).dispose?.()).not.toThrow();
  });

  test("renders concrete diagnostics and simple action states", () => {
    const diagnostics = result({
      state: { workers: [{ status: "running" }, { status: "ready" }] },
      catalog: { diagnostics: [{ message: "bad worker" }] },
    });
    const diagnosticOutput = workerStatusToolRenderer.renderResult(
      diagnostics, options(), theme, context(),
    );
    expect(Bun.stripANSI(diagnosticOutput.render(80).join("\n")).trimEnd()).toBe(
      "1 active · 1 available for follow-up · 1 catalog diagnostic",
    );
    const partialDiagnostics = workerStatusToolRenderer.renderResult(
      diagnostics, options({ isPartial: true }), theme, context(),
    );
    expect(Bun.stripANSI(partialDiagnostics.render(80).join("\n")).trimEnd()).toBe(
      "Reading worker diagnostics…",
    );

    const abort = workerAbortToolRenderer.renderResult(
      result({}), options(), theme, context(),
    );
    expect(Bun.stripANSI(abort.render(80).join("\n"))).toContain("Worker stop requested");
    const close = interactiveCloseToolRenderer.renderResult(
      result({ worker_id: "worker-1" }), options(), theme, context(),
    );
    expect(Bun.stripANSI(close.render(80).join("\n"))).toContain("✓ Worker closed");
  });
});
