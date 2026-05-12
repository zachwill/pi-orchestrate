import {
  type ExtensionAPI,
  getMarkdownTheme,
} from "@mariozechner/pi-coding-agent";
import { Box, Markdown, Text } from "@mariozechner/pi-tui";
import {
  type WorkerResultMessageDetails,
  STATUS_ICON,
  resultTitle,
} from "../worker-messages.js";

type MessageRenderer = Parameters<ExtensionAPI["registerMessageRenderer"]>[1];
type MessageRendererTheme = Parameters<MessageRenderer>[2];

function getStatusColor(status: WorkerResultMessageDetails["status"]): "success" | "error" | "warning" | "muted" {
  switch (status) {
    case "done":
      return "success";
    case "error":
    case "aborted":
      return "error";
    case "running":
    case "waiting":
      return "warning";
    default:
      return "muted";
  }
}

function getMessageContent(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return String(content ?? "");

  return content
    .map((part) => {
      if (part && typeof part === "object" && "type" in part && part.type === "text" && "text" in part) {
        return String(part.text);
      }
      return "[image]";
    })
    .join("\n");
}

function renderNoteMessage(content: unknown, theme: MessageRendererTheme): Box {
  const box = new Box(1, 1, (text) => theme.bg("customMessageBg", text));
  box.addChild(new Text(theme.fg("warning", getMessageContent(content)), 0, 0));
  return box;
}

export function registerCrewMessageRenderers(pi: ExtensionAPI): void {
  pi.registerMessageRenderer("pi-workers-result", (message, { expanded }, theme) => {
    const details = message.details as WorkerResultMessageDetails | undefined;
    const title = details ? resultTitle(details) : "Worker update";
    const icon = details
      ? theme.fg(getStatusColor(details.status), STATUS_ICON[details.status])
      : theme.fg("muted", "ℹ");
    const header = `${icon} ${theme.fg("toolTitle", theme.bold(title))}`;
    const body = details?.body ?? (!details && message.content ? getMessageContent(message.content) : undefined);

    const box = new Box(1, 1, (text) => theme.bg("customMessageBg", text));
    box.addChild(new Text(header, 0, 0));

    if (details?.sessionFile) {
      box.addChild(new Text(theme.fg("muted", `📁 ${details.sessionFile}`), 0, 0));
    }

    if (body) {
      if (expanded) {
        box.addChild(new Text("", 0, 0));
        box.addChild(new Markdown(body, 0, 0, getMarkdownTheme()));
      } else {
        const lines = body.split("\n");
        const preview = lines.slice(0, 5).join("\n");
        box.addChild(new Text(theme.fg("dim", preview), 0, 0));
        if (lines.length > 5) {
          box.addChild(new Text(theme.fg("muted", "(Ctrl+O to expand)"), 0, 0));
        }
      }
    }

    return box;
  });

  pi.registerMessageRenderer("pi-workers-note", (message, _options, theme) => {
    return renderNoteMessage(message.content, theme);
  });
}
