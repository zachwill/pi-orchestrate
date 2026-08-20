import { truncateToWidth, type Component } from "@earendil-works/pi-tui";

export class WidthBoundComponent implements Component {
  constructor(
    private readonly child: Component,
    private readonly maxLines?: number,
  ) {}

  render(width: number): string[] {
    const bounded = Math.max(1, Math.floor(width));
    const lines = this.child.render(bounded);
    return (this.maxLines === undefined ? lines : lines.slice(0, this.maxLines))
      .map((line) => truncateToWidth(line, bounded, "…"));
  }

  invalidate(): void { this.child.invalidate(); }
  dispose(): void { disposeComponent(this.child); }
}

export function disposeComponent(component: Component): void {
  (component as Component & { dispose?: () => void }).dispose?.();
}

export function resultAppearance(
  status: "completed" | "ready" | "failed" | "aborted",
  readyQualifier: string,
  failedQualifier = "failed",
): {
  readonly color: "success" | "error" | "warning";
  readonly icon: "✓" | "✗" | "■";
  readonly qualifier?: string;
} {
  if (status === "failed") {
    return { color: "error", icon: "✗", qualifier: failedQualifier };
  }
  if (status === "aborted") {
    return { color: "warning", icon: "■", qualifier: "aborted" };
  }
  if (status === "ready") {
    return { color: "success", icon: "✓", qualifier: readyQualifier };
  }
  return { color: "success", icon: "✓" };
}

export function formatElapsed(milliseconds: number): string {
  const seconds = Math.floor(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  return seconds % 60 === 0 ? `${minutes}m` : `${minutes}m ${seconds % 60}s`;
}
