import {
  CONFIG_DIR_NAME,
  getAgentDir,
  parseFrontmatter,
} from "@earendil-works/pi-coding-agent";
import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type {
  CatalogDiagnostic,
  SupportedToolName,
  WorkerCatalog,
  WorkerDefinition,
  WorkerLifecycle,
  WorkerSourceKind,
} from "./domain.js";
import { isSupportedToolName } from "./domain.js";

const MAX_WORKER_BYTES = 64 * 1024;
const KNOWN_FIELDS = new Set([
  "name",
  "description",
  "model",
  "thinking",
  "tools",
  "skills",
  "compaction",
  "lifecycle",
]);
const THINKING_LEVELS: ReadonlySet<string> = new Set([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);

function isThinkingLevel(value: string): value is NonNullable<WorkerDefinition["thinking"]> {
  return THINKING_LEVELS.has(value);
}

export interface CatalogFileStat {
  readonly size: number;
  isFile(): boolean;
  isSymbolicLink(): boolean;
}

export interface CatalogFileSystem {
  readDirectory(directory: string): readonly string[];
  inspect(path: string): CatalogFileStat;
  readText(path: string): string;
}

export interface DiscoverWorkerCatalogOptions {
  readonly cwd: string;
  readonly projectTrusted: boolean;
  readonly packageRoot?: string;
  readonly agentDir?: string;
}

interface CatalogSource {
  readonly kind: WorkerSourceKind;
  readonly directory: string;
}

interface WorkerFrontmatter {
  readonly name?: unknown;
  readonly description?: unknown;
  readonly model?: unknown;
  readonly thinking?: unknown;
  readonly tools?: unknown;
  readonly skills?: unknown;
  readonly compaction?: unknown;
  readonly lifecycle?: unknown;
  readonly [field: string]: unknown;
}

const productionFileSystem: CatalogFileSystem = {
  readDirectory: (directory) => readdirSync(directory),
  inspect: (path) => lstatSync(path),
  readText: (path) => readFileSync(path, "utf8"),
};

function diagnostic(
  source: WorkerSourceKind,
  filePath: string,
  message: string,
): CatalogDiagnostic {
  return { severity: "error", source, filePath, message };
}

function compareText(left: string, right: string): number {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function isMissingPath(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`frontmatter field '${field}' must be a non-empty string`);
  }
  return value.trim();
}

function optionalString(value: unknown, field: string): string | undefined {
  if (value === undefined) return undefined;
  return requiredString(value, field);
}

function stringList(value: unknown, field: string, required: boolean): string[] | undefined {
  if (value === undefined) {
    if (required) throw new Error(`frontmatter field '${field}' is required`);
    return undefined;
  }

  const values = typeof value === "string" ? value.split(",").map((item) => item.trim()) : value;
  if (!Array.isArray(values) || values.length === 0) {
    throw new Error(`frontmatter field '${field}' must be a non-empty comma string or string array`);
  }

  const strings: string[] = [];
  for (const item of values) {
    if (typeof item !== "string" || item === "") {
      throw new Error(`frontmatter field '${field}' must be a non-empty comma string or string array`);
    }
    strings.push(item);
  }
  return strings;
}

function parseTools(value: unknown): SupportedToolName[] {
  const tools: SupportedToolName[] = [];
  for (const tool of stringList(value, "tools", true) ?? []) {
    if (!isSupportedToolName(tool)) throw new Error(`unsupported tool '${tool}'`);
    tools.push(tool);
  }
  return tools;
}

function parseOptionalBoolean(value: unknown, field: string): boolean | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "boolean") {
    throw new Error(`frontmatter field '${field}' must be a boolean`);
  }
  return value;
}

function parseLifecycle(value: unknown): WorkerLifecycle {
  if (value !== "one-shot" && value !== "reusable") {
    throw new Error("frontmatter field 'lifecycle' must be 'one-shot' or 'reusable'");
  }
  return value;
}

function parseCompaction(value: unknown): WorkerDefinition["compaction"] {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("frontmatter field 'compaction' must be a mapping");
  }

  const fields = Object.keys(value).sort(compareText);
  const unknownFields = fields.filter(
    (field) => field !== "enabled" && field !== "reserveTokens" && field !== "keepRecentTokens",
  );
  if (unknownFields.length > 0) {
    throw new Error(
      `unknown compaction field${unknownFields.length === 1 ? "" : "s"}: ${unknownFields.join(", ")}`,
    );
  }

  const enabledValue = "enabled" in value ? value.enabled : undefined;
  const reserveTokens = "reserveTokens" in value ? value.reserveTokens : undefined;
  const keepRecentTokens = "keepRecentTokens" in value ? value.keepRecentTokens : undefined;
  const enabled = parseOptionalBoolean(enabledValue, "compaction.enabled");

  if (
    reserveTokens !== undefined &&
    (typeof reserveTokens !== "number" || !Number.isSafeInteger(reserveTokens) || reserveTokens < 0)
  ) {
    throw new Error("frontmatter field 'compaction.reserveTokens' must be a non-negative integer");
  }
  if (
    keepRecentTokens !== undefined &&
    (typeof keepRecentTokens !== "number" || !Number.isSafeInteger(keepRecentTokens) || keepRecentTokens < 0)
  ) {
    throw new Error("frontmatter field 'compaction.keepRecentTokens' must be a non-negative integer");
  }

  return { enabled, reserveTokens, keepRecentTokens };
}

function parseWorker(
  filePath: string,
  source: WorkerSourceKind,
  content: string,
): WorkerDefinition {
  let parsed: ReturnType<typeof parseFrontmatter<WorkerFrontmatter>>;
  try {
    parsed = parseFrontmatter<WorkerFrontmatter>(content);
  } catch {
    throw new Error("frontmatter is not valid YAML");
  }

  const { frontmatter, body } = parsed;
  if (typeof frontmatter !== "object" || frontmatter === null || Array.isArray(frontmatter)) {
    throw new Error("frontmatter must be a mapping");
  }

  const unknownFields = Object.keys(frontmatter)
    .filter((field) => !KNOWN_FIELDS.has(field))
    .sort(compareText);
  if (unknownFields.length > 0) {
    throw new Error(
      `unknown frontmatter field${unknownFields.length === 1 ? "" : "s"}: ${unknownFields.join(", ")}`,
    );
  }

  const name = requiredString(frontmatter.name, "name");
  const expectedName = basename(filePath, extname(filePath));
  if (name !== expectedName) {
    throw new Error(`frontmatter name '${name}' must match basename '${expectedName}'`);
  }

  const description = requiredString(frontmatter.description, "description");
  const model = optionalString(frontmatter.model, "model");
  if (model !== undefined && !/^[^/\s]+\/\S+$/.test(model)) {
    throw new Error("frontmatter field 'model' must use provider/model format");
  }

  const thinking = optionalString(frontmatter.thinking, "thinking");
  if (thinking !== undefined && !isThinkingLevel(thinking)) {
    throw new Error(`unsupported thinking level '${thinking}'`);
  }

  const tools = parseTools(frontmatter.tools);
  const skills = stringList(frontmatter.skills, "skills", false);
  const compaction = parseCompaction(frontmatter.compaction);
  const lifecycle = parseLifecycle(frontmatter.lifecycle);
  if (body.trim() === "") throw new Error("worker prompt body must not be empty");

  return {
    name,
    description,
    model:
      model === undefined
        ? undefined
        : {
            provider: model.slice(0, model.indexOf("/")),
            modelId: model.slice(model.indexOf("/") + 1),
          },
    thinking,
    tools,
    skills: skills ?? [],
    compaction,
    lifecycle,
    systemPrompt: body,
    source: { kind: source, filePath },
  };
}

function sourceDirectories(options: DiscoverWorkerCatalogOptions): CatalogSource[] {
  const packageRoot = options.packageRoot ?? fileURLToPath(new URL("..", import.meta.url));
  const agentDir = options.agentDir ?? getAgentDir();
  const sources: CatalogSource[] = [
    { kind: "package", directory: join(packageRoot, "examples", "workers") },
    { kind: "user", directory: join(agentDir, "pi-orchestrate", "workers") },
  ];

  if (options.projectTrusted) {
    sources.push({
      kind: "project",
      directory: join(options.cwd, CONFIG_DIR_NAME, "pi-orchestrate", "workers"),
    });
  }
  return sources;
}

function discoverSource(
  fileSystem: CatalogFileSystem,
  source: CatalogSource,
): { workers: WorkerDefinition[]; diagnostics: CatalogDiagnostic[] } {
  let entries: readonly string[];
  try {
    entries = fileSystem.readDirectory(source.directory);
  } catch (error) {
    if (isMissingPath(error)) return { workers: [], diagnostics: [] };
    return {
      workers: [],
      diagnostics: [diagnostic(source.kind, source.directory, "could not read worker directory")],
    };
  }

  const workers: WorkerDefinition[] = [];
  const diagnostics: CatalogDiagnostic[] = [];
  const markdownEntries = entries
    .filter((entry) => extname(entry) === ".md" && basename(entry) === entry)
    .sort(compareText);

  for (const entry of markdownEntries) {
    const filePath = join(source.directory, entry);
    let stat: CatalogFileStat;
    try {
      stat = fileSystem.inspect(filePath);
    } catch {
      diagnostics.push(diagnostic(source.kind, filePath, "could not inspect worker file"));
      continue;
    }

    if (stat.isSymbolicLink() || !stat.isFile()) {
      diagnostics.push(
        diagnostic(source.kind, filePath, "worker file must be a regular non-symlink file"),
      );
      continue;
    }
    if (stat.size > MAX_WORKER_BYTES) {
      diagnostics.push(
        diagnostic(source.kind, filePath, `worker file exceeds ${MAX_WORKER_BYTES} bytes`),
      );
      continue;
    }

    let content: string;
    try {
      content = fileSystem.readText(filePath);
    } catch {
      diagnostics.push(diagnostic(source.kind, filePath, "could not read worker file"));
      continue;
    }

    if (Buffer.byteLength(content, "utf8") > MAX_WORKER_BYTES) {
      diagnostics.push(
        diagnostic(source.kind, filePath, `worker file exceeds ${MAX_WORKER_BYTES} bytes`),
      );
      continue;
    }

    try {
      workers.push(parseWorker(filePath, source.kind, content));
    } catch (error) {
      const message = error instanceof Error ? error.message : "invalid worker definition";
      diagnostics.push(diagnostic(source.kind, filePath, message));
    }
  }

  return { workers, diagnostics };
}

export function createWorkerCatalogDiscovery(fileSystem: CatalogFileSystem) {
  return (options: DiscoverWorkerCatalogOptions): WorkerCatalog => {
    const workersByName = new Map<string, WorkerDefinition>();
    const diagnostics: CatalogDiagnostic[] = [];

    for (const source of sourceDirectories(options)) {
      const discovered = discoverSource(fileSystem, source);
      for (const worker of discovered.workers) workersByName.set(worker.name, worker);
      diagnostics.push(...discovered.diagnostics);
    }

    const workers = [...workersByName.values()].sort((left, right) =>
      compareText(left.name, right.name),
    );
    return { workers, diagnostics };
  };
}

export const discoverWorkerCatalog = createWorkerCatalogDiscovery(productionFileSystem);
