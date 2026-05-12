import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import type { ThinkingLevel } from "@mariozechner/pi-agent-core";
import { getAgentDir, parseFrontmatter } from "@mariozechner/pi-coding-agent";
import { type SupportedToolName, isSupportedToolName } from "./tool-registry.js";

export interface ParsedModel { provider: string; modelId: string }
export interface WorkerConfig {
  name: string;
  description: string;
  model?: string;
  parsedModel?: ParsedModel;
  thinking?: ThinkingLevel;
  tools?: SupportedToolName[];
  skills?: string[];
  compaction?: boolean;
  interactive?: boolean;
  systemPrompt: string;
  filePath: string;
  sourceLabel: string;
}
export interface WorkerDefaults {
  scope?: string;
  confirmProjectWorkers?: boolean;
  maxActiveWorkers?: number;
  maxConcurrentSpawns?: number;
}
export interface WorkerDiscoveryWarning { filePath: string; message: string }
export interface WorkerDiscoveryResult { workers: WorkerConfig[]; warnings: WorkerDiscoveryWarning[]; defaults: WorkerDefaults }

type WorkerOverride = Partial<Omit<WorkerConfig, "name" | "systemPrompt" | "filePath" | "sourceLabel">> & { prompt?: string; promptFile?: string; systemPrompt?: string };
interface Source { label: string; agentDirs: string[]; configPaths: string[]; compatibility: boolean }

const VALID_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh"]);
const ALLOWED_WORKER_FIELDS = new Set(["description", "prompt", "promptFile", "model", "thinking", "tools", "skills", "compaction", "interactive"]);

function warn(filePath: string, message: string): WorkerDiscoveryWarning { return { filePath, message }; }
function expandHome(value: string): string { return value.startsWith("~/") ? path.join(os.homedir(), value.slice(2)) : value; }
function hasOwn(record: Record<string, unknown>, key: string): boolean { return Object.prototype.hasOwnProperty.call(record, key); }

function parseList(value: unknown): string[] | undefined {
  if (value == null) return undefined;
  if (Array.isArray(value)) return value.map(String).map((v) => v.trim()).filter(Boolean);
  if (typeof value === "string") return value.split(",").map((v) => v.trim()).filter(Boolean);
  return undefined;
}

function parseModel(value: unknown): { model?: string; parsedModel?: ParsedModel; warning?: string } {
  if (value === undefined) return {};
  if (typeof value !== "string") return { warning: 'field "model" must be a string' };
  const slash = value.indexOf("/");
  if (slash <= 0 || slash === value.length - 1) return { warning: `invalid model format "${value}" (expected "provider/model-id")` };
  return { model: value, parsedModel: { provider: value.slice(0, slash), modelId: value.slice(slash + 1) } };
}

function parseSharedFields(name: string, record: Record<string, unknown>, filePath: string): { fields: WorkerOverride; warnings: WorkerDiscoveryWarning[] } {
  const fields: WorkerOverride = {};
  const warnings: WorkerDiscoveryWarning[] = [];

  const model = parseModel(record.model);
  if (model.warning) warnings.push(warn(filePath, `Worker "${name}": ${model.warning}, ignoring`));
  if (model.model) { fields.model = model.model; fields.parsedModel = model.parsedModel; }

  if (record.thinking !== undefined) {
    if (typeof record.thinking === "string" && VALID_THINKING_LEVELS.has(record.thinking)) fields.thinking = record.thinking as ThinkingLevel;
    else warnings.push(warn(filePath, `Worker "${name}": invalid thinking level, ignoring`));
  }

  if (hasOwn(record, "tools")) {
    const parsed = parseList(record.tools);
    if (!parsed) warnings.push(warn(filePath, `Worker "${name}": invalid tools field, expected array or comma-separated string`));
    else {
      const invalid = parsed.filter((tool) => !isSupportedToolName(tool));
      if (invalid.length > 0) warnings.push(warn(filePath, `Worker "${name}": unknown tools ${invalid.join(", ")}, ignoring those tools`));
      fields.tools = parsed.filter(isSupportedToolName);
    }
  }

  if (hasOwn(record, "skills")) {
    const parsed = parseList(record.skills);
    if (!parsed) warnings.push(warn(filePath, `Worker "${name}": invalid skills field, expected array or comma-separated string`));
    else fields.skills = parsed;
  }

  for (const booleanField of ["compaction", "interactive"] as const) {
    if (record[booleanField] !== undefined) {
      if (typeof record[booleanField] === "boolean") fields[booleanField] = record[booleanField];
      else warnings.push(warn(filePath, `Worker "${name}": field "${booleanField}" must be boolean, ignoring`));
    }
  }

  return { fields, warnings };
}

function loadMarkdown(filePath: string, sourceLabel: string): { worker?: WorkerConfig; warnings: WorkerDiscoveryWarning[] } {
  let content: string;
  try { content = fs.readFileSync(filePath, "utf8"); } catch (error) { return { warnings: [warn(filePath, `Worker file could not be read: ${String(error)}`)] }; }
  let parsed: { frontmatter: Record<string, unknown>; body: string };
  try { parsed = parseFrontmatter<Record<string, unknown>>(content); } catch (error) { return { warnings: [warn(filePath, `Ignored invalid worker definition. Frontmatter could not be parsed: ${String(error)}`)] }; }
  const name = typeof parsed.frontmatter.name === "string" ? parsed.frontmatter.name.trim() : "";
  const description = typeof parsed.frontmatter.description === "string" ? parsed.frontmatter.description.trim() : "";
  if (!name || !description) return { warnings: [warn(filePath, 'Ignored invalid worker definition. Required frontmatter fields "name" and "description" must be non-empty strings.')] };
  if (/\s/.test(name)) return { warnings: [warn(filePath, `Ignored worker "${name}". Worker names cannot contain whitespace.`)] };
  const { fields, warnings } = parseSharedFields(name, parsed.frontmatter, filePath);
  return { worker: { name, description, ...fields, systemPrompt: parsed.body, filePath, sourceLabel }, warnings };
}

function readMarkdownDir(dir: string, sourceLabel: string): { workers: WorkerConfig[]; warnings: WorkerDiscoveryWarning[] } {
  if (!fs.existsSync(dir)) return { workers: [], warnings: [] };
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch (error) { return { workers: [], warnings: [warn(dir, `Worker directory could not be read: ${String(error)}`)] }; }
  const workers: WorkerConfig[] = [];
  const warnings: WorkerDiscoveryWarning[] = [];
  const seen = new Set<string>();
  for (const entry of entries) {
    if (!entry.name.endsWith(".md") || !(entry.isFile() || entry.isSymbolicLink())) continue;
    const loaded = loadMarkdown(path.join(dir, entry.name), sourceLabel);
    warnings.push(...loaded.warnings);
    if (!loaded.worker) continue;
    if (seen.has(loaded.worker.name)) { warnings.push(warn(loaded.worker.filePath, `Duplicate worker name "${loaded.worker.name}" in ${dir}, skipping`)); continue; }
    seen.add(loaded.worker.name);
    workers.push(loaded.worker);
  }
  return { workers, warnings };
}

function resolvePromptFile(configDir: string, promptFile: string): string { return path.resolve(configDir, expandHome(promptFile)); }
function readPromptFile(configDir: string, promptFile: string, filePath: string, name: string): { prompt?: string; warning?: WorkerDiscoveryWarning } {
  const resolved = resolvePromptFile(configDir, promptFile);
  try { return { prompt: fs.readFileSync(resolved, "utf8") }; } catch (error) { return { warning: warn(filePath, `Worker "${name}": promptFile ${resolved} could not be read: ${String(error)}`) }; }
}

function loadConfig(filePath: string, legacy: boolean): { defaults: WorkerDefaults; overrides: Record<string, WorkerOverride>; warnings: WorkerDiscoveryWarning[] } {
  if (!fs.existsSync(filePath)) return { defaults: {}, overrides: {}, warnings: [] };
  const warnings: WorkerDiscoveryWarning[] = [];
  if (legacy || !filePath.endsWith(path.join("pi-workers", "config.json"))) warnings.push(warn(filePath, `Compatibility config path in use. Move this file to ${path.join(path.dirname(filePath), "pi-workers", "config.json")}.`));
  let root: Record<string, unknown>;
  try { root = JSON.parse(fs.readFileSync(filePath, "utf8")); } catch (error) { return { defaults: {}, overrides: {}, warnings: [warn(filePath, `Ignored config. JSON could not be parsed: ${String(error)}`)] }; }
  if (!root || typeof root !== "object" || Array.isArray(root)) return { defaults: {}, overrides: {}, warnings: [warn(filePath, "Ignored config. Root value must be a JSON object.")] };
  const defaults = (root.defaults && typeof root.defaults === "object" && !Array.isArray(root.defaults)) ? root.defaults as WorkerDefaults : {};
  const workerRoot = (root.workers ?? root.agents) as unknown;
  const overrides: Record<string, WorkerOverride> = {};
  if (workerRoot === undefined) return { defaults, overrides, warnings };
  if (!workerRoot || typeof workerRoot !== "object" || Array.isArray(workerRoot)) return { defaults, overrides, warnings: [...warnings, warn(filePath, 'Ignored config workers. Field "workers" must be a JSON object.')] };
  for (const [name, value] of Object.entries(workerRoot as Record<string, unknown>)) {
    if (!value || typeof value !== "object" || Array.isArray(value)) { warnings.push(warn(filePath, `Worker config "${name}" must be an object, ignoring`)); continue; }
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) if (!ALLOWED_WORKER_FIELDS.has(key)) warnings.push(warn(filePath, `Worker "${name}": unknown field "${key}", ignoring`));
    const { fields, warnings: fieldWarnings } = parseSharedFields(name, record, filePath);
    warnings.push(...fieldWarnings);
    const override: WorkerOverride = { ...fields };
    if (typeof record.description === "string") override.description = record.description.trim();
    const hasPrompt = typeof record.prompt === "string";
    const hasPromptFile = typeof record.promptFile === "string";
    if (hasPrompt && hasPromptFile) {
      warnings.push(warn(filePath, `Worker "${name}": use exactly one of prompt or promptFile; promptFile wins for compatibility.`));
    }
    if (hasPrompt && !hasPromptFile) override.prompt = record.prompt as string;
    if (hasPromptFile) {
      override.promptFile = record.promptFile as string;
      const loaded = readPromptFile(path.dirname(filePath), record.promptFile as string, filePath, name);
      if (loaded.warning) warnings.push(loaded.warning); else override.prompt = loaded.prompt;
    }
    if (!override.description && !override.prompt && !override.promptFile && Object.keys(override).length === 0) continue;
    overrides[name] = override;
  }
  return { defaults, overrides, warnings };
}

function pickCompatibilityConfigPaths(canonical: string, compat: string, legacyCrew: string): string[] {
  if (fs.existsSync(canonical)) return [];
  const paths: string[] = [];
  if (fs.existsSync(legacyCrew)) paths.push(legacyCrew);
  if (fs.existsSync(compat)) paths.push(compat);
  return paths;
}

function sources(cwd: string): Source[] {
  const agentDir = getAgentDir();
  const projectCanonical = path.join(cwd, ".pi", "pi-workers");
  const userCanonical = path.join(agentDir, "pi-workers");
  return [
    { label: "project canonical", agentDirs: [path.join(projectCanonical, "agents")], configPaths: [path.join(projectCanonical, "config.json")], compatibility: false },
    { label: "project compatibility", agentDirs: [path.join(cwd, ".pi", "agents")], configPaths: pickCompatibilityConfigPaths(path.join(projectCanonical, "config.json"), path.join(cwd, ".pi", "pi-workers.json"), path.join(cwd, ".pi", "pi-crew.json")), compatibility: true },
    { label: "user canonical", agentDirs: [path.join(userCanonical, "agents")], configPaths: [path.join(userCanonical, "config.json")], compatibility: false },
    { label: "user compatibility", agentDirs: [path.join(agentDir, "agents")], configPaths: pickCompatibilityConfigPaths(path.join(userCanonical, "config.json"), path.join(agentDir, "pi-workers.json"), path.join(agentDir, "pi-crew.json")), compatibility: true },
  ];
}

function applyOverride(base: WorkerConfig, override: WorkerOverride): WorkerConfig {
  const next = { ...base };
  if (override.description !== undefined) next.description = override.description;
  if (override.prompt !== undefined) next.systemPrompt = override.prompt;
  if (override.model !== undefined) { next.model = override.model; next.parsedModel = override.parsedModel; }
  if (override.thinking !== undefined) next.thinking = override.thinking;
  if (override.tools !== undefined) next.tools = override.tools;
  if (override.skills !== undefined) next.skills = override.skills;
  if (override.compaction !== undefined) next.compaction = override.compaction;
  if (override.interactive !== undefined) next.interactive = override.interactive;
  return next;
}

function configOnlyWorker(name: string, override: WorkerOverride, filePath: string, sourceLabel: string): WorkerConfig | undefined {
  if (!override.description) return undefined;
  const prompt = override.prompt;
  if (typeof prompt !== "string") return undefined;
  return { name, description: override.description, model: override.model, parsedModel: override.parsedModel, thinking: override.thinking, tools: override.tools, skills: override.skills, compaction: override.compaction, interactive: override.interactive, systemPrompt: prompt, filePath, sourceLabel };
}

export function discoverWorkers(cwd: string = process.cwd()): WorkerDiscoveryResult {
  const workers: WorkerConfig[] = [];
  const warnings: WorkerDiscoveryWarning[] = [];
  const defaults: WorkerDefaults = {};
  const seen = new Map<string, string>();

  for (const source of sources(cwd)) {
    const local = new Map<string, WorkerConfig>();
    for (const dir of source.agentDirs) {
      if (source.compatibility && fs.existsSync(dir)) warnings.push(warn(dir, "Compatibility worker path in use. Move worker Markdown to the pi-workers namespace."));
      const loaded = readMarkdownDir(dir, source.label);
      warnings.push(...loaded.warnings);
      for (const worker of loaded.workers) if (!local.has(worker.name)) local.set(worker.name, worker);
    }

    const sourceDefaults: WorkerDefaults = {};
    const overrides: Record<string, WorkerOverride> = {};
    const overrideSources: Record<string, string> = {};
    for (const configPath of source.configPaths) {
      const config = loadConfig(configPath, source.compatibility || configPath.endsWith("pi-crew.json"));
      Object.assign(sourceDefaults, config.defaults);
      warnings.push(...config.warnings);
      for (const [name, override] of Object.entries(config.overrides)) {
        overrides[name] = { ...(overrides[name] ?? {}), ...override };
        overrideSources[name] = configPath;
      }
    }
    for (const [key, value] of Object.entries(sourceDefaults)) {
      if ((defaults as Record<string, unknown>)[key] === undefined) {
        (defaults as Record<string, unknown>)[key] = value;
      }
    }

    for (const [name, override] of Object.entries(overrides)) {
      if (local.has(name)) local.set(name, applyOverride(local.get(name)!, override));
      else {
        const only = configOnlyWorker(name, override, overrideSources[name] ?? source.label, source.label);
        if (only) local.set(name, only);
        else if (override.description !== undefined || override.prompt !== undefined || override.promptFile !== undefined) {
          warnings.push(warn(overrideSources[name] ?? source.label, `Config-only worker "${name}" requires description and exactly one of prompt or promptFile.`));
        }
      }
    }

    for (const worker of local.values()) {
      if (seen.has(worker.name)) continue;
      seen.set(worker.name, worker.filePath);
      workers.push(worker);
    }
  }

  return { workers, warnings, defaults };
}
