import { describe, expect, test } from "bun:test";
import { readdir, stat } from "node:fs/promises";
import { basename, join } from "node:path";

const root = join(import.meta.dir, "..");
const manifestPath = join(root, "package.json");
const readmePath = join(root, "README.md");
const skillsPath = join(root, "skills");
const workerDirectory = join(root, "examples", "workers");
const workerNames = ["investigator", "scout", "web", "worker"] as const;
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
    expect(fields.get("lifecycle")).toMatch(/^(one-shot|reusable)$/);
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
  test("README covers the current package and orchestration contract", async () => {
    const [manifest, readme] = await Promise.all([readManifest(), readText(readmePath)]);
    const install = markdownSection(readme, "Install");
    const tools = markdownSection(readme, "Tools");
    const dispatch = markdownSection(readme, "Dispatch");
    const results = markdownSection(readme, "Results and ownership");
    const lifecycle = markdownSection(readme, "Lifecycle");
    const catalog = markdownSection(readme, "Worker catalog");
    const trust = markdownSection(readme, "Trust and isolation");

    expect(install).toContain(`pi install npm:${manifest.name}`);

    const documentedTools = [...tools.matchAll(/^\| `([^`]+)` \|/gm)].map(
      (match) => match[1],
    );
    expect(documentedTools).toEqual([...canonicalTools]);
    expect(tools).toContain("orchestrate({ worker, title, instructions })");

    expectBlockWith(dispatch, [/\bpreflight|validates\b/i, /\bsibling\b/i, /\bindependently\b/i]);
    expectBlockWith(dispatch, [/\bpure group\b/i, /\basynchronous\b/i, /\bconcurrent\b/i]);
    expectBlockWith(dispatch, [/\bmixing\b/i, /\binline\b/i, /\bblocking\b/i]);
    expectBlockWith(dispatch, [/\bworker_send\b/i, /\bsole\b/i, /\basynchronous\b/i]);
    expectBlockWith(dispatch, [/\binline\b/i, /\bcancellation\b/i, /\bdetaches\b/i]);

    expectBlockWith(results, [/\bsibling\b/i, /\bfinal\b/i, /\bsynthesis\b/i]);
    expectBlockWith(results, [/\bowner-scoped\b/i, /\bqueue\b/i, /\bnever\b/i]);
    expectBlockWith(results, [/\bpolling\b/i, /\borchestration_status\b/i]);

    expectBlockWith(lifecycle, [/\bone-shot\b/i, /\bcompleted\b/i, /\breusable\b/i, /\bready\b/i]);
    expectBlockWith(lifecycle, [/\bworker_send\b/i, /\bworker_close\b/i, /\bworker_abort\b/i]);
    expectBlockWith(lifecycle, [/\bprocess\b/i, /\breloads?\b/i, /\bclose\b/i]);

    const precedence = catalog.match(/^\d+\. .*$/gm) ?? [];
    expect(catalog).toMatch(/precedence/i);
    expect(precedence).toHaveLength(3);
    expect(precedence[0]).toMatch(/package/i);
    expect(precedence[1]).toMatch(/user/i);
    expect(precedence[2]).toMatch(/project/i);
    expect(precedence[2]).toMatch(/trust/i);

    expectBlockWith(trust, [/\bdirect Pi children\b/i, /\bdescendant Pi worker sessions\b/i]);
    expectBlockWith(trust, [/\bbash\b/i, /\bexternal processes\b/i, /\bagent CLIs\b/i]);
  });
});
