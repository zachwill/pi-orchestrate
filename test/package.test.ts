import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, realpath, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  DefaultResourceLoader,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { discoverWorkerCatalog } from "../extension/catalog/discovery.ts";
import { PACKAGE_ROOT } from "../extension/package-root.ts";
import { isOrchestrationExtensionPath } from "../extension/worker/session.ts";

const root = join(import.meta.dir, "..");
const workerNames = ["investigator", "scout", "web", "worker"] as const;

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
      const extractedDirectory = join(temporaryDirectory, "extracted");
      await mkdir(extractedDirectory);
      await run(["tar", "-xzf", artifactPath, "-C", extractedDirectory], root);
      const packageRoot = join(extractedDirectory, "package");
      const packedManifest = await Bun.file(join(packageRoot, "package.json")).json();
      expect(packedManifest.peerDependencies).toMatchObject({
        "@earendil-works/pi-agent-core": "^0.85.0",
        "@earendil-works/pi-ai": "^0.85.0",
        "@earendil-works/pi-coding-agent": "^0.85.0",
        "@earendil-works/pi-tui": "^0.85.0",
      });

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
      expect(loadedExtensions.extensions.map((extension) => extension.resolvedPath)).toEqual([
        join(packageRoot, "extension", "index.ts"),
      ]);
      expect(loadedExtensions.extensions[0]?.handlers.has("context")).toBe(true);
      expect(await Bun.file(join(packageRoot, "extension", "parent", "worker-context.ts")).exists()).toBe(true);
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

  test("shares the actual package root across fallback discovery and extension exclusion", async () => {
    const temporaryDirectory = await mkdtemp(join(tmpdir(), "pi-orchestrate-package-root-"));

    try {
      const catalog = discoverWorkerCatalog({
        cwd: temporaryDirectory,
        agentDir: temporaryDirectory,
        projectTrusted: false,
      });
      expect(catalog.diagnostics).toEqual([]);
      expect(catalog.workers.length).toBeGreaterThan(0);
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
