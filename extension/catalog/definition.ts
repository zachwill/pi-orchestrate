import type { ThinkingLevel } from "@earendil-works/pi-agent-core";

export const SUPPORTED_TOOL_NAMES = [
  "read",
  "bash",
  "edit",
  "write",
  "grep",
  "find",
  "ls",
] as const;

export type SupportedToolName = (typeof SUPPORTED_TOOL_NAMES)[number];

const supportedToolNames: ReadonlySet<string> = new Set(SUPPORTED_TOOL_NAMES);

export function isSupportedToolName(value: unknown): value is SupportedToolName {
  return typeof value === "string" && supportedToolNames.has(value);
}

export type WorkerSourceKind = "package" | "user" | "project";

export interface WorkerSource {
  readonly kind: WorkerSourceKind;
  readonly filePath: string;
}

export interface WorkerModel {
  readonly provider: string;
  readonly modelId: string;
}

export interface WorkerCompaction {
  readonly enabled?: boolean;
  readonly reserveTokens?: number;
  readonly keepRecentTokens?: number;
}

export type WorkerLifecycle = "one-shot" | "interactive";

export interface WorkerDefinition {
  readonly name: string;
  readonly source: WorkerSource;
  readonly description: string;
  readonly systemPrompt: string;
  readonly lifecycle: WorkerLifecycle;
  readonly tools: readonly SupportedToolName[];
  readonly skills?: readonly string[];
  readonly model?: WorkerModel;
  readonly thinking?: ThinkingLevel;
  readonly compaction?: WorkerCompaction;
}

export type CatalogDiagnosticSeverity = "warning" | "error";

export interface CatalogDiagnostic {
  readonly severity: CatalogDiagnosticSeverity;
  readonly source: WorkerSourceKind;
  readonly message: string;
  readonly filePath?: string;
}

export interface WorkerCatalog {
  readonly workers: readonly WorkerDefinition[];
  readonly diagnostics: readonly CatalogDiagnostic[];
}

export function createWorkerCatalog(
  workers: readonly WorkerDefinition[],
  diagnostics: readonly CatalogDiagnostic[] = [],
): WorkerCatalog {
  return {
    workers: [...workers].sort(compareWorkersByName),
    diagnostics: [...diagnostics],
  };
}

export function findWorkerByName(
  catalog: WorkerCatalog,
  name: string,
): WorkerDefinition | undefined {
  return catalog.workers.find((worker) => worker.name === name);
}

function compareWorkersByName(left: WorkerDefinition, right: WorkerDefinition): number {
  if (left.name < right.name) return -1;
  if (left.name > right.name) return 1;
  return 0;
}
