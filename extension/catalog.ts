import {
  CONFIG_DIR_NAME,
  getAgentDir,
  parseFrontmatter,
} from "@earendil-works/pi-coding-agent";
import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Effect, Result, Schema, SchemaGetter, type SchemaIssue } from "effect";
import type {
  CatalogDiagnostic,
  WorkerCatalog,
  WorkerDefinition,
  WorkerSourceKind,
} from "./domain.js";
import {
  createWorkerCatalog,
  isSupportedToolName,
  SUPPORTED_TOOL_NAMES,
} from "./domain.js";

const MAX_WORKER_BYTES = 64 * 1024;
const THINKING_LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh", "max"] as const;

const RequiredText = Schema.Trim.check(Schema.isNonEmpty());
const StringListInput = Schema.Union([Schema.String, Schema.Array(Schema.String)]);

function commaList<Item extends Schema.Constraint & { readonly Encoded: string }>(
  item: Item,
  allowEmpty = false,
) {
  const items = allowEmpty
    ? Schema.Array(item)
    : Schema.Array(item).check(Schema.isMinLength(1));
  return StringListInput.pipe(
    Schema.decodeTo(items, {
      decode: SchemaGetter.transform((value) =>
        typeof value === "string" ? value.split(",").map((entry) => entry.trim()) : value,
      ),
      encode: SchemaGetter.transform((value) => value),
    }),
  );
}

const ModelCoordinate = Schema.Trim.check(Schema.isPattern(/^[^/\s]+\/\S+$/)).pipe(
  Schema.decodeTo(
    Schema.Struct({ provider: Schema.NonEmptyString, modelId: Schema.NonEmptyString }),
    {
      decode: SchemaGetter.transform((coordinate) => {
        const separator = coordinate.indexOf("/");
        return {
          provider: coordinate.slice(0, separator),
          modelId: coordinate.slice(separator + 1),
        };
      }),
      encode: SchemaGetter.transform(({ provider, modelId }) => `${provider}/${modelId}`),
    },
  ),
);

const NonNegativeInteger = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
);
const Compaction = Schema.Struct({
  enabled: Schema.optionalKey(Schema.Boolean),
  reserveTokens: Schema.optionalKey(NonNegativeInteger),
  keepRecentTokens: Schema.optionalKey(NonNegativeInteger),
});
const ThinkingLevel = Schema.Trim.pipe(Schema.decodeTo(Schema.Literals(THINKING_LEVELS)));
const WorkerFrontmatter = Schema.Struct({
  name: RequiredText,
  description: RequiredText,
  model: Schema.optionalKey(ModelCoordinate),
  thinking: Schema.optionalKey(ThinkingLevel),
  tools: commaList(Schema.Literals(SUPPORTED_TOOL_NAMES)),
  skills: Schema.optionalKey(commaList(Schema.NonEmptyString, true)),
  compaction: Schema.optionalKey(Compaction),
  lifecycle: Schema.Literals(["one-shot", "interactive"]).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("one-shot")),
  ),
});
const decodeWorkerFrontmatter = Schema.decodeUnknownResult(WorkerFrontmatter, {
  errors: "all",
  onExcessProperty: "error",
});

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

interface IssuePath {
  readonly path: readonly PropertyKey[];
  readonly issue: SchemaIssue.Issue;
}

function collectIssuePaths(
  issue: SchemaIssue.Issue,
  parentPath: readonly PropertyKey[] = [],
): IssuePath[] {
  switch (issue._tag) {
    case "Pointer":
      return collectIssuePaths(issue.issue, [...parentPath, ...issue.path]);
    case "Composite":
      return issue.issues.flatMap((child) => collectIssuePaths(child, parentPath));
    case "AnyOf":
      return issue.issues.length === 0
        ? [{ path: parentPath, issue }]
        : issue.issues.flatMap((child) => collectIssuePaths(child, parentPath));
    case "Encoding":
    case "Filter":
      return collectIssuePaths(issue.issue, parentPath);
    case "InvalidType":
    case "InvalidValue":
    case "MissingKey":
    case "UnexpectedKey":
    case "Forbidden":
    case "OneOf":
      return [{ path: parentPath, issue }];
  }
}

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fieldValue(frontmatter: unknown, field: string): unknown {
  if (!isUnknownRecord(frontmatter)) return undefined;
  return field in frontmatter ? frontmatter[field] : undefined;
}

function hasIssuePath(issuePaths: readonly IssuePath[], ...path: readonly PropertyKey[]): boolean {
  return issuePaths.some((entry) => path.every((key, index) => entry.path[index] === key));
}

function unexpectedFieldsAt(
  issuePaths: readonly IssuePath[],
  parentPath: readonly PropertyKey[],
): string[] {
  return issuePaths
    .filter(({ path, issue }) =>
      issue._tag === "UnexpectedKey" &&
      path.length === parentPath.length + 1 &&
      parentPath.every((key, index) => path[index] === key) &&
      typeof path[parentPath.length] === "string"
    )
    .map(({ path }) => String(path[parentPath.length]))
    .sort(compareText);
}

function unknownFieldsDiagnostic(scope: string, fields: readonly string[]): string {
  return `unknown ${scope} field${fields.length === 1 ? "" : "s"}: ${fields.join(", ")}`;
}

function listItems(value: unknown): readonly unknown[] {
  if (typeof value === "string") return value.split(",").map((item) => item.trim());
  return Array.isArray(value) ? value : [];
}

const ORDERED_FRONTMATTER_FIELDS = [
  "name", "description", "model", "thinking", "tools", "skills", "compaction", "lifecycle",
] as const;

const FIXED_FIELD_DIAGNOSTICS: Partial<Record<(typeof ORDERED_FRONTMATTER_FIELDS)[number], string>> = {
  name: "frontmatter field 'name' must be a non-empty string",
  description: "frontmatter field 'description' must be a non-empty string",
  skills: "frontmatter field 'skills' must be a comma string or string array",
  lifecycle: "frontmatter field 'lifecycle' must be 'one-shot' or 'interactive'",
};

const COMPACTION_FIELD_DIAGNOSTICS = [
  ["enabled", "frontmatter field 'compaction.enabled' must be a boolean"],
  ["reserveTokens", "frontmatter field 'compaction.reserveTokens' must be a non-negative integer"],
  ["keepRecentTokens", "frontmatter field 'compaction.keepRecentTokens' must be a non-negative integer"],
] as const;

function schemaDiagnostic(issue: SchemaIssue.Issue, frontmatter: unknown): string {
  const issuePaths = collectIssuePaths(issue);
  const frontmatterFields = unexpectedFieldsAt(issuePaths, []);
  if (frontmatterFields.length > 0) {
    return unknownFieldsDiagnostic("frontmatter", frontmatterFields);
  }
  if (!isUnknownRecord(frontmatter)) return "frontmatter must be a mapping";

  const field = ORDERED_FRONTMATTER_FIELDS.find((candidate) =>
    hasIssuePath(issuePaths, candidate)
  );
  if (field === undefined) return "invalid worker definition";

  const fixedDiagnostic = FIXED_FIELD_DIAGNOSTICS[field];
  if (fixedDiagnostic !== undefined) return fixedDiagnostic;

  const value = fieldValue(frontmatter, field);
  if (field === "model") {
    return typeof value !== "string" || value.trim() === ""
      ? "frontmatter field 'model' must be a non-empty string"
      : "frontmatter field 'model' must use provider/model format";
  }
  if (field === "thinking") {
    return typeof value !== "string" || value.trim() === ""
      ? "frontmatter field 'thinking' must be a non-empty string"
      : `unsupported thinking level '${value.trim()}'`;
  }
  if (field === "tools") {
    const items = listItems(value);
    const validList = items.length > 0 && items.every(
      (item) => typeof item === "string" && item !== "",
    );
    const unsupported = validList
      ? items.find((item) => typeof item === "string" && !isSupportedToolName(item))
      : undefined;
    if (typeof unsupported === "string") return `unsupported tool '${unsupported}'`;
    return value === undefined
      ? "frontmatter field 'tools' is required"
      : "frontmatter field 'tools' must be a non-empty comma string or string array";
  }

  const compactionFields = unexpectedFieldsAt(issuePaths, ["compaction"]);
  if (compactionFields.length > 0) {
    return unknownFieldsDiagnostic("compaction", compactionFields);
  }
  const nestedDiagnostic = COMPACTION_FIELD_DIAGNOSTICS.find(([nestedField]) =>
    hasIssuePath(issuePaths, "compaction", nestedField)
  );
  return nestedDiagnostic?.[1] ?? "frontmatter field 'compaction' must be a mapping";
}

function parseWorker(
  filePath: string,
  source: WorkerSourceKind,
  content: string,
): WorkerDefinition {
  let parsed: ReturnType<typeof parseFrontmatter>;
  try {
    parsed = parseFrontmatter(content);
  } catch {
    throw new Error("frontmatter is not valid YAML");
  }

  const { frontmatter, body } = parsed;
  const decoded = decodeWorkerFrontmatter(frontmatter);
  if (Result.isFailure(decoded)) {
    throw new Error(schemaDiagnostic(decoded.failure.issue, frontmatter));
  }

  const worker = decoded.success;
  const expectedName = basename(filePath, extname(filePath));
  if (worker.name !== expectedName) {
    throw new Error(`frontmatter name '${worker.name}' must match basename '${expectedName}'`);
  }
  if (body.trim() === "") throw new Error("worker prompt body must not be empty");

  return {
    ...worker,
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

    return createWorkerCatalog([...workersByName.values()], diagnostics);
  };
}

export const discoverWorkerCatalog = createWorkerCatalogDiscovery(productionFileSystem);
