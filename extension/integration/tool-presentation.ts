import type { AgentToolResult } from "@mariozechner/pi-agent-core";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Box, Text } from "@mariozechner/pi-tui";

export type ToolTheme = Parameters<Exclude<Parameters<ExtensionAPI["registerTool"]>[0]["renderCall"], undefined>>[1];
export type ToolResult = AgentToolResult<unknown>;

export function toolSuccess(
  text: string,
  details: Record<string, unknown> = {},
  extra: { terminate?: boolean } = {},
) {
  return {
    content: [{ type: "text" as const, text }],
    details,
    ...(extra.terminate ? { terminate: true } : {}),
  };
}

export function toolError(text: string, details: Record<string, unknown> = {}) {
  return {
    content: [{ type: "text" as const, text }],
    isError: true,
    details: { ...details, error: true },
  };
}

export function renderCrewCall(
  theme: ToolTheme,
  tool: string,
  target?: string,
  detail?: string,
): Box {
  const box = new Box(1, 1);
  let header = theme.fg("toolTitle", theme.bold(`${tool} `));
  if (target) header += theme.fg("accent", target);
  box.addChild(new Text(header, 0, 0));

  if (detail) {
    box.addChild(new Text(theme.fg("dim", detail), 0, 0));
  }

  return box;
}

export function renderCrewResult(result: ToolResult, theme: ToolTheme): Text {
  const first = result.content[0];
  const details = result.details as { error?: boolean } | undefined;
  const content = first?.type === "text" && first.text ? first.text : "(no output)";
  return new Text(details?.error ? theme.fg("error", content) : theme.fg("success", content), 0, 0);
}
