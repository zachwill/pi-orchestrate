import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  DefaultResourceLoader,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { appendOrchestratorContract } from "../extension/contract.js";

const root = join(import.meta.dir, "..");
const manifestPath = join(root, "package.json");
const readmePath = join(root, "README.md");
const workerDirectory = join(root, "examples", "workers");
const workerNames = ["investigator", "scout", "web", "worker"] as const;
const workerPaths = workerNames.map((name) => join(workerDirectory, `${name}.md`));

const canonicalTools = [
  "orchestrate",
  "worker_status",
  "interactive_send",
  "worker_abort",
  "interactive_close",
] as const;
const supportedWorkerTools = new Set(["read", "bash", "edit", "write", "grep", "find", "ls"]);
const piPeerPackages = [
  "@earendil-works/pi-agent-core",
  "@earendil-works/pi-ai",
  "@earendil-works/pi-coding-agent",
  "@earendil-works/pi-tui",
] as const;

interface PackageManifest {
  readonly name: string;
  readonly version: string;
  readonly files: string[];
  readonly license: string;
  readonly repository: { readonly type: string; readonly url: string };
  readonly homepage: string;
  readonly bugs: { readonly url: string };
  readonly publishConfig: { readonly access: string };
  readonly pi: {
    readonly extensions: string[];
    readonly skills?: string[];
    readonly prompts?: string[];
  };
  readonly dependencies: Record<string, string>;
  readonly peerDependencies: Record<string, string>;
  readonly devDependencies: Record<string, string>;
}

interface ParsedWorker {
  readonly fields: ReadonlyMap<string, string>;
  readonly body: string;
}

async function readText(path: string): Promise<string> {
  return Bun.file(path).text();
}

async function readManifest(): Promise<PackageManifest> {
  return Bun.file(manifestPath).json() as Promise<PackageManifest>;
}

async function expectPath(path: string): Promise<void> {
  const pathStat = await stat(path);
  expect(pathStat.isFile() || pathStat.isDirectory()).toBe(true);
}

async function run(command: string[], cwd: string): Promise<string> {
  const process = Bun.spawn(command, { cwd, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ]);
  if (exitCode !== 0) {
    throw new Error(`${command.join(" ")} failed (${exitCode}): ${stderr.trim()}`);
  }
  return stdout;
}

function markdownSection(markdown: string, heading: string): string {
  const start = markdown.indexOf(`## ${heading}`);
  expect(start).toBeGreaterThanOrEqual(0);
  const end = markdown.indexOf("\n## ", start + heading.length + 3);
  return markdown.slice(start, end < 0 ? undefined : end);
}

function expectBlockWith(text: string, concepts: readonly RegExp[]): void {
  const blocks = text.split(/\n\s*\n/);
  expect(blocks.some((block) => concepts.every((concept) => concept.test(block)))).toBe(true);
}

function parseWorker(markdown: string): ParsedWorker {
  const normalized = markdown.replaceAll("\r\n", "\n");
  const match = normalized.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  expect(match).not.toBeNull();

  const fields = new Map<string, string>();
  for (const line of match?.[1]?.split("\n") ?? []) {
    const field = line.match(/^([a-z][a-z-]*):\s*(.*?)\s*$/);
    expect(field).not.toBeNull();
    const key = field?.[1] ?? "";
    expect(fields.has(key)).toBe(false);
    fields.set(key, field?.[2] ?? "");
  }

  return { fields, body: match?.[2] ?? "" };
}

describe("published package resources", () => {
  test("packs the declared Pi extension and package resources into the published artifact", async () => {
    const temporaryDirectory = await mkdtemp(join(tmpdir(), "pi-orchestrate-package-"));

    try {
      await run(
        ["bun", "pm", "pack", "--destination", temporaryDirectory, "--ignore-scripts", "--quiet"],
        root,
      );
      const artifacts = (await readdir(temporaryDirectory)).filter((path) => path.endsWith(".tgz"));
      expect(artifacts).toHaveLength(1);

      const artifactPath = join(temporaryDirectory, artifacts[0] ?? "missing.tgz");
      const archiveFiles = (await run(["tar", "-tzf", artifactPath], root))
        .split("\n")
        .filter(Boolean);
      expect(archiveFiles).toContain("package/package.json");
      expect(archiveFiles).toContain("package/README.md");
      expect(archiveFiles).toContain("package/LICENSE");
      for (const workerName of workerNames) {
        expect(archiveFiles).toContain(`package/examples/workers/${workerName}.md`);
      }
      expect(archiveFiles.some((path) => path.startsWith("package/skills/"))).toBe(false);

      const extractedDirectory = join(temporaryDirectory, "extracted");
      await mkdir(extractedDirectory);
      await run(["tar", "-xzf", artifactPath, "-C", extractedDirectory], root);
      const packageRoot = join(extractedDirectory, "package");
      const packedManifest = await Bun.file(join(packageRoot, "package.json")).json() as PackageManifest;

      expect(packedManifest.name).toBe("@zachwill/pi-orchestrate");
      expect(packedManifest.version).toBe("0.9.0");
      expect(packedManifest.files).toEqual(["extension/", "examples/", "README.md", "LICENSE"]);
      expect(packedManifest.pi).toEqual({ extensions: ["./extension/index.ts"] });
      expect(packedManifest.pi.skills).toBeUndefined();
      expect(packedManifest.pi.prompts).toBeUndefined();
      const declaredExtensionPaths = packedManifest.pi.extensions.map((extensionPath) => {
        const artifactEntry = `package/${extensionPath.replace(/^\.\//, "")}`;
        expect(archiveFiles).toContain(artifactEntry);
        return join(packageRoot, extensionPath);
      });
      for (const extensionPath of declaredExtensionPaths) {
        expect((await stat(extensionPath)).isFile()).toBe(true);
      }

      await symlink(
        join(root, "node_modules"),
        join(packageRoot, "node_modules"),
        process.platform === "win32" ? "junction" : "dir",
      );
      const loaderCwd = join(temporaryDirectory, "loader-cwd");
      const loaderAgentDir = join(temporaryDirectory, "loader-agent");
      await Promise.all([
        mkdir(loaderCwd),
        mkdir(loaderAgentDir),
      ]);
      const loader = new DefaultResourceLoader({
        cwd: loaderCwd,
        agentDir: loaderAgentDir,
        settingsManager: SettingsManager.inMemory({}, { projectTrusted: false }),
        additionalExtensionPaths: [packageRoot],
        noExtensions: true,
        noSkills: true,
        noPromptTemplates: true,
        noThemes: true,
        noContextFiles: true,
      });

      await loader.reload();
      const loadedExtensions = loader.getExtensions();
      expect(loadedExtensions.errors).toEqual([]);
      expect(loadedExtensions.extensions.map((extension) => extension.resolvedPath)).toEqual(
        declaredExtensionPaths,
      );
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  });

  test("declares public repository metadata and compatible Pi peers", async () => {
    const manifest = await readManifest();

    expect(manifest.license).toBe("MIT");
    expect(manifest.publishConfig).toEqual({ access: "public" });
    expect(manifest.repository).toEqual({
      type: "git",
      url: "git+https://github.com/zachwill/pi-orchestrate.git",
    });
    expect(manifest.homepage).toBe("https://github.com/zachwill/pi-orchestrate#readme");
    expect(manifest.bugs).toEqual({
      url: "https://github.com/zachwill/pi-orchestrate/issues",
    });
    expect(manifest.dependencies.effect).toBe("4.0.0-rc.111");
    for (const packageName of piPeerPackages) {
      expect(manifest.peerDependencies[packageName]).toBe("^0.80.10");
      expect(manifest.devDependencies[packageName]).toBe("0.80.10");
    }
    expect(manifest.peerDependencies.typebox).toBe("*");
  });

  test("includes exactly four fallback worker definitions", async () => {
    for (const workerPath of workerPaths) await expectPath(workerPath);

    const markdownFiles = (await readdir(workerDirectory))
      .filter((path) => path.endsWith(".md"))
      .sort();
    expect(markdownFiles).toEqual(workerNames.map((name) => `${name}.md`).sort());
  });
});

describe("fallback worker definitions", () => {
  test.each(workerPaths)("%s has strict canonical frontmatter and a body", async (workerPath) => {
    const definition = parseWorker(await readText(workerPath));
    const fields = definition.fields;
    const workerName = basename(workerPath, ".md");
    const allowedFields = new Set([
      "name",
      "description",
      "model",
      "thinking",
      "tools",
      "skills",
      "compaction",
      "lifecycle",
    ]);

    expect([...fields.keys()].every((field) => allowedFields.has(field))).toBe(true);
    expect(fields.get("name")).toBe(workerName);
    expect(fields.get("description")?.trim().length).toBeGreaterThan(0);
    expect(fields.get("thinking")?.trim().length).toBeGreaterThan(0);
    expect(fields.get("lifecycle")).toMatch(/^(one-shot|interactive)$/);
    expect(fields.get("lifecycle")).toBe("one-shot");
    expect(definition.body.trim().length).toBeGreaterThan(0);

    expect(fields.has("tools")).toBe(true);
    const tools = (fields.get("tools") ?? "")
      .split(",")
      .map((tool) => tool.trim())
      .filter(Boolean);
    expect(tools.length).toBeGreaterThan(0);
    expect(tools.every((tool) => supportedWorkerTools.has(tool))).toBe(true);

    if (workerName === "web") {
      expect(fields.get("model")).toBe("openai-codex/gpt-5.6-sol");
      expect(fields.get("tools")).toBe("bash");
      expect(fields.get("skills")).toBe("[]");
    } else {
      expect(fields.has("model")).toBe(false);
    }
  });
});

describe("published documentation", () => {
  test("README covers the current package and public boundaries", async () => {
    const [manifest, readme] = await Promise.all([readManifest(), readText(readmePath)]);
    const install = markdownSection(readme, "Install");
    const tools = markdownSection(readme, "Tools");
    const parent = markdownSection(readme, "Parent responsibilities");
    const definitions = markdownSection(readme, "Configure workers");
    const trust = markdownSection(readme, "Trust and isolation");

    expect(install).toContain(`pi install npm:${manifest.name}`);

    const documentedTools = [...tools.matchAll(/^### `([^`]+)`$/gm)].map(
      (match) => match[1],
    );
    expect(documentedTools).toEqual([
      "orchestrate",
      "worker_abort",
      "worker_status",
      "interactive_close",
      "interactive_send",
    ]);
    expect(tools).toContain("orchestrate({ worker, title, instructions })");
    expect(tools).toMatch(/complete wave[\s\S]*no other tool calls/i);
    expect(tools).toMatch(/multi_tool_use\.parallel[\s\S]*functions\.orchestrate/i);
    expect(tools).toMatch(/mixing another tool[\s\S]*inline and blocking/i);
    expect(tools).toMatch(/worker_status[\s\S]*diagnostics and recovery, not completion polling/i);
    expect(tools).toMatch(/worker_abort[\s\S]*active workers/i);
    expect(tools).toMatch(/interactive_close[\s\S]*status is `ready`/i);
    expect(tools).toMatch(/interactive_send[\s\S]*keeps its ID/i);

    expect(parent).toMatch(/bounded, independent scopes/i);
    expect(parent).toMatch(/small fixed count/i);
    expect(parent).toMatch(/floor unless the user sets an exact cap/i);
    expect(parent).toMatch(/review[\s\S]*verify/i);

    const precedence = definitions.match(/^\d+\. .*$/gm) ?? [];
    expect(precedence).toHaveLength(3);
    expect(precedence[0]).toMatch(/package/i);
    expect(precedence[1]).toMatch(/user/i);
    expect(precedence[2]).toMatch(/project.*trust/i);
    expect(definitions).toContain("lifecycle: interactive");
    expect(definitions).toMatch(/required fields are `name`, `description`, and a nonempty `tools` list/i);
    expect(definitions).toMatch(/Markdown body is the worker's nonempty system prompt/i);
    expect(definitions).toMatch(/`lifecycle` is optional, accepts `one-shot` or `interactive`, and defaults to `one-shot`/i);
    expect(definitions).toMatch(/supported Pi tools are `read`, `bash`, `edit`, `write`, `grep`, `find`, and `ls`/i);
    expect(definitions).toMatch(/only `interactive_send` continues an existing one/i);

    expect(trust).toMatch(/not security sandboxes/i);
    expect(trust).toMatch(/`bash` can start external processes, including agent CLIs/i);
    expect(trust).toMatch(/direct Pi children/i);
    expect(readme).not.toContain("orchestration_status");
    expect(readme).not.toMatch(/\bworker_(?:send|close)\b/);
  });

  test("shipped parent contract uses only the current tools and lifecycle semantics", () => {
    const contract = appendOrchestratorContract("", { workers: [], diagnostics: [] });
    const publicTools = contract
      .split("\n")
      .find((line) => line.includes("The public tools are"));

    expect(publicTools).toBeDefined();
    if (publicTools === undefined) throw new Error("missing public tools contract rule");
    expect([...publicTools.matchAll(/`([^`]+)`/g)].map((match) => match[1])).toEqual([
      ...canonicalTools,
    ]);
    expectBlockWith(contract, [
      /\bprefer one-shot workers\b/i,
      /\bterminate automatically\b/i,
      /\bnever use either tool for one-shot or completed workers\b/i,
    ]);
    expectBlockWith(contract, [
      /\bsame worker definition can be dispatched in multiple independent calls\b/i,
      /\beach call creates an independent worker session\b/i,
      /\binteractive session continuity\b/i,
      /\bone worker ID\b/i,
      /\bexplicit follow-up work\b/i,
      /`interactive_send`/,
      /`interactive_close`/,
    ]);
    expect(contract).not.toMatch(/\bworker_(?:send|close)\b/);
    expect(contract).not.toContain("orchestration_status");
    expect(contract).not.toMatch(/\breusable\b/i);
  });
});
