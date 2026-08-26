import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, realpath, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  DefaultResourceLoader,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { discoverWorkerCatalog } from "../extension/catalog/discovery.ts";
import { applyOrchestratorContract } from "../extension/parent/contract.ts";
import { PACKAGE_ROOT } from "../extension/package-root.ts";
import { isOrchestrationExtensionPath } from "../extension/worker/session.ts";

const root = join(import.meta.dir, "..");
const manifestPath = join(root, "package.json");
const workerDirectory = join(root, "examples", "workers");
const workerNames = ["investigator", "scout", "web", "worker"] as const;
const workerPaths = workerNames.map((name) => join(workerDirectory, `${name}.md`));

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
  readonly exports: Record<string, never>;
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

async function runExpectingFailure(command: string[], cwd: string): Promise<string> {
  const process = Bun.spawn(command, { cwd, stdout: "pipe", stderr: "pipe" });
  const [stderr, exitCode] = await Promise.all([
    new Response(process.stderr).text(),
    process.exited,
  ]);
  expect(exitCode).not.toBe(0);
  return stderr;
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
    const sourceManifest = await readManifest();
    const expectedPackedSources = (await readdir(join(root, "extension"), {
      recursive: true,
    }))
      .filter((path) => path.endsWith(".ts"))
      .map((path) => `extension/${path.replaceAll("\\", "/")}`)
      .sort();

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
      const extractedDirectory = join(temporaryDirectory, "extracted");
      await mkdir(extractedDirectory);
      await run(["tar", "-xzf", artifactPath, "-C", extractedDirectory], root);
      const packageRoot = join(extractedDirectory, "package");
      const packedManifest = await Bun.file(join(packageRoot, "package.json")).json() as PackageManifest;

      expect(packedManifest.name).toBe("@zachwill/pi-orchestrate");
      expect(packedManifest.version).toBe(sourceManifest.version);
      expect(packedManifest.files).toEqual(["extension/", "examples/", "README.md", "LICENSE"]);
      expect(packedManifest.exports).toEqual({});
      expect(packedManifest.pi).toEqual({ extensions: ["./extension/index.ts"] });
      expect(packedManifest.pi.skills).toBeUndefined();
      expect(packedManifest.pi.prompts).toBeUndefined();

      const packedSources = archiveFiles
        .filter((path) => path.startsWith("package/extension/") && path.endsWith(".ts"))
        .map((path) => path.slice("package/".length))
        .sort();
      expect(packedSources).toEqual(expectedPackedSources);
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
      const packedCatalogModule = await import(pathToFileURL(
        join(packageRoot, "extension", "catalog", "discovery.ts"),
      ).href);
      const packedCatalog = packedCatalogModule.discoverWorkerCatalog({
        cwd: loaderCwd,
        agentDir: loaderAgentDir,
        projectTrusted: false,
      });
      expect(packedCatalog.diagnostics).toEqual([]);
      expect(packedCatalog.workers.map((worker: { readonly name: string }) => worker.name)).toEqual(
        [...workerNames],
      );
      const canonicalPackageRoot = await realpath(packageRoot);
      expect(packedCatalog.workers.every((worker: {
        readonly source: { readonly kind: string; readonly filePath: string };
      }) =>
        worker.source.kind === "package" &&
        worker.source.filePath.startsWith(join(canonicalPackageRoot, "examples", "workers"))
      )).toBe(true);

      const consumerRoot = join(temporaryDirectory, "consumer");
      const packageScope = join(consumerRoot, "node_modules", "@zachwill");
      await mkdir(packageScope, { recursive: true });
      await symlink(
        packageRoot,
        join(packageScope, "pi-orchestrate"),
        process.platform === "win32" ? "junction" : "dir",
      );
      await Bun.write(join(consumerRoot, "package.json"), JSON.stringify({
        name: "package-boundary-consumer",
        private: true,
        type: "module",
      }));
      const blockedSpecifiers = [
        "@zachwill/pi-orchestrate",
        "@zachwill/pi-orchestrate/extension/index.ts",
        "@zachwill/pi-orchestrate/extension/catalog/discovery.ts",
        "@zachwill/pi-orchestrate/extension/parent/contract.ts",
      ] as const;
      for (const [index, specifier] of blockedSpecifiers.entries()) {
        const scriptName = `blocked-import-${index}.ts`;
        await Bun.write(join(consumerRoot, scriptName), `import ${JSON.stringify(specifier)};`);
        await runExpectingFailure(["bun", scriptName], consumerRoot);
      }
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

  test("shares the actual package root across fallback discovery and extension exclusion", async () => {
    const temporaryDirectory = await mkdtemp(join(tmpdir(), "pi-orchestrate-package-root-"));

    try {
      const manifest = await Bun.file(join(PACKAGE_ROOT, "package.json")).json() as PackageManifest;
      expect(manifest.name).toBe("@zachwill/pi-orchestrate");

      const catalog = discoverWorkerCatalog({
        cwd: temporaryDirectory,
        agentDir: temporaryDirectory,
        projectTrusted: false,
      });
      expect(catalog.diagnostics).toEqual([]);
      expect(catalog.workers.map((worker) => worker.name)).toEqual([...workerNames]);
      expect(catalog.workers.every((worker) =>
        worker.source.kind === "package" &&
        worker.source.filePath.startsWith(join(PACKAGE_ROOT, "examples", "workers"))
      )).toBe(true);

      expect(isOrchestrationExtensionPath(
        join(PACKAGE_ROOT, "extension", "worker", "session.ts"),
      )).toBe(true);
      expect(isOrchestrationExtensionPath(temporaryDirectory)).toBe(false);
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
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

describe("shipped parent contract", () => {
  test("prescribes fresh sessions and grouped parallel dispatch", () => {
    const contract = applyOrchestratorContract("", { workers: [], diagnostics: [] });

    expect(contract).toMatch(/each `orchestrate` call creates a fresh worker session/i);
    expect(contract).toMatch(/same worker definition and identical instructions/i);
    expect(contract).toMatch(/exactly one `multi_tool_use\.parallel` call/i);
    expect(contract).toMatch(/exactly N `functions\.orchestrate` entries and no other tools/i);
    expect(contract).toMatch(/not present.*native siblings in one assistant response/i);
    expect(contract).toMatch(/never dispatch a multi-worker wave as separate assistant responses/i);
    expect(contract).toMatch(/automatic delivery requires no keepalive activity/i);
    expect(contract).toMatch(/do not call `sleep`.*poll with any tool.*inspect files or processes.*no-op tool calls/i);
    expect(contract).toMatch(/only genuinely independent work.*otherwise end the turn/i);
    expect(contract).not.toMatch(/\bworker_(?:send|close)\b/);
    expect(contract).not.toContain("orchestration_status");
    expect(contract).not.toMatch(/\breusable\b/i);
  });
});
