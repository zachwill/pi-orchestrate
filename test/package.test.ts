import { describe, expect, test } from "bun:test";
import { readdir, stat } from "node:fs/promises";
import { basename, join } from "node:path";

const root = join(import.meta.dir, "..");
const manifestPath = join(root, "package.json");
const readmePath = join(root, "README.md");
const licensePath = join(root, "LICENSE");
const skillsPath = join(root, "skills");
const workerDirectory = join(root, "examples", "workers");
const workerNames = ["investigator", "scout", "worker"] as const;
const workerPaths = workerNames.map((name) => join(workerDirectory, `${name}.md`));

const canonicalTools = [
  "orchestrate",
  "orchestration_status",
  "worker_send",
  "worker_abort",
  "worker_close",
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
  test("publishes the extension, fallback examples, README, and MIT license without a skill", async () => {
    const manifest = await readManifest();

    expect(manifest.name).toBe("@zachwill/pi-orchestrate");
    expect(manifest.files).toEqual(["extension/", "examples/", "README.md", "LICENSE"]);
    expect(manifest.pi).toEqual({ extensions: ["./extension/index.ts"] });
    expect(manifest.pi.skills).toBeUndefined();
    expect(manifest.pi.prompts).toBeUndefined();

    for (const publishedPath of manifest.files) await expectPath(join(root, publishedPath));
    for (const resourcePath of manifest.pi.extensions) await expectPath(join(root, resourcePath));
    await expect(stat(skillsPath)).rejects.toThrow();
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
    expect(manifest.dependencies.effect).toBe("4.0.0-beta.99");
    for (const packageName of piPeerPackages) {
      expect(manifest.peerDependencies[packageName]).toBe("^0.80.10");
      expect(manifest.devDependencies[packageName]).toBe("0.80.10");
    }
    expect(manifest.peerDependencies.typebox).toBe("*");
  });

  test("ships a standard MIT license", async () => {
    const license = await readText(licensePath);

    expect(license).toStartWith("MIT License\n\nCopyright (c) 2026 Zach Williams\n");
    expect(license).toContain("Permission is hereby granted, free of charge");
    expect(license).toContain('THE SOFTWARE IS PROVIDED "AS IS"');
    expect(license).toContain("LIABILITY, WHETHER IN AN ACTION OF CONTRACT");
  });

  test("includes exactly three fallback worker definitions", async () => {
    for (const workerPath of workerPaths) await expectPath(workerPath);

    const markdownFiles = (await readdir(workerDirectory))
      .filter((path) => path.endsWith(".md"))
      .sort();
    expect(markdownFiles).toEqual(workerNames.map((name) => `${name}.md`).sort());
  });
});

describe("fallback worker definitions", () => {
  test.each(workerPaths)("%s has strict canonical frontmatter, no model, and a body", async (workerPath) => {
    const definition = parseWorker(await readText(workerPath));
    const fields = definition.fields;
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
    expect(fields.get("name")).toBe(basename(workerPath, ".md"));
    expect(fields.get("description")?.trim().length).toBeGreaterThan(0);
    expect(fields.has("model")).toBe(false);
    expect(fields.get("thinking")?.trim().length).toBeGreaterThan(0);
    expect(fields.get("lifecycle")).toMatch(/^(one-shot|reusable)$/);
    expect(definition.body.trim().length).toBeGreaterThan(0);

    expect(fields.has("tools")).toBe(true);
    const tools = (fields.get("tools") ?? "")
      .split(",")
      .map((tool) => tool.trim())
      .filter(Boolean);
    expect(tools.length).toBeGreaterThan(0);
    expect(tools.every((tool) => supportedWorkerTools.has(tool))).toBe(true);
  });
});

describe("published documentation", () => {
  test("README documents the exact public contract", async () => {
    const readme = await readText(readmePath);

    for (const tool of canonicalTools) expect(readme).toContain(tool);
    expect(readme).toContain("orchestrate({ worker, title, instructions })");
    expect(readme).toMatch(/atomic input, catalog, and model preflight/i);
    expect(readme).toMatch(/one rejected call does not prevent valid siblings from starting/i);
    expect(readme).toMatch(/resource startup failure becomes that worker's `failed` result/i);
    expect(readme).toMatch(/Pi executes sibling tool calls concurrently/i);
    expect(readme).toMatch(/without an extension-level group limit or hidden throttle/i);
    expect(readme).toMatch(/pure group of sibling `orchestrate` calls runs asynchronously/i);
    expect(readme).toMatch(/only the final response starts the parent's synthesis turn/i);
    expect(readme).toMatch(/Mixing `orchestrate` with another tool makes it inline and blocking/i);
    expect(readme).toMatch(/`worker_send` is asynchronous only as the sole tool call/i);
    expect(readme).toMatch(/inline work receives the parent turn's cancellation signal/i);
    expect(readme).toMatch(/accepted async work does not retain that signal/i);
    expect(readme).toMatch(/Do not poll/i);
    expect(readme).toMatch(/that exact owning session resumes/i);
    expect(readme).toMatch(/never delivered to another session/i);
    expect(readme).toMatch(/`worker_abort` only for active work/i);
    expect(readme).toMatch(/`worker_close` closes an owned reusable worker in the `ready` state/i);
  });

  test("README documents fallback inheritance and optional thinking without checkout-relative copying", async () => {
    const readme = await readText(readmePath);

    expect(readme).toMatch(/all three package fallbacks intentionally omit `model`/i);
    expect(readme).toMatch(/inherit the parent model active at dispatch/i);
    expect(readme).toMatch(/user and trusted project overrides are the model-specialization points/i);
    expect(readme).toMatch(/`thinking`, `skills`, and `compaction` are optional/i);
    expect(readme).toMatch(/omitted `skills` uses Pi's normal discovered skills/i);
    expect(readme).toMatch(/`skills: \[\]` disables skills/i);
    expect(readme).toMatch(/excludes its own package before child extension factories execute/i);
    expect(readme).toContain("@benvargas/pi-claude-code-use");
    expect(readme).toMatch(/create a Markdown definition manually/i);
    expect(readme).not.toMatch(/cp\s+examples\/workers/i);
    expect(readme).toMatch(/only after Pi trusts the project/i);
    expect(readme).toMatch(/run in-process and are not sandboxes/i);
    expect(readme).toMatch(/do not survive process exit/i);
    expect(readme).toMatch(/automatically injects the authoritative orchestration contract/i);
  });

  test("published Markdown contains no superseded vocabulary", async () => {
    const publishedText = (await Promise.all([readmePath, ...workerPaths].map(readText))).join("\n");

    for (const legacyPattern of [
      /crew_/i,
      /pi-workers/i,
      /\bsubagent\b/i,
      /worker_respond/i,
      /worker_status/i,
      /persistent\s*:/i,
    ]) {
      expect(publishedText).not.toMatch(legacyPattern);
    }
  });
});
