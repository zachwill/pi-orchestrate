import { describe, expect, test } from "bun:test";
import { CONFIG_DIR_NAME } from "@earendil-works/pi-coding-agent";
import { join } from "node:path";
import type { WorkerCatalog } from "../../extension/catalog/definition.ts";
import {
  createWorkerCatalogDiscovery,
  type CatalogFileStat,
  type CatalogFileSystem,
  type DiscoverWorkerCatalogOptions,
} from "../../extension/catalog/discovery.ts";

interface FakeFile {
  readonly content: string;
  readonly kind?: "file" | "symlink" | "other";
  readonly size?: number;
}

class FakeFileSystem implements CatalogFileSystem {
  readonly calls: string[] = [];
  readonly directories = new Map<string, string[]>();
  readonly files = new Map<string, FakeFile>();

  readDirectory(directory: string): readonly string[] {
    this.calls.push(`directory:${directory}`);
    const entries = this.directories.get(directory);
    if (!entries) throw Object.assign(new Error("missing"), { code: "ENOENT" });
    return entries;
  }

  inspect(path: string): CatalogFileStat {
    this.calls.push(`inspect:${path}`);
    const file = this.files.get(path);
    if (!file) throw Object.assign(new Error("missing"), { code: "ENOENT" });
    return {
      size: file.size ?? Buffer.byteLength(file.content),
      isFile: () => (file.kind ?? "file") === "file",
      isSymbolicLink: () => file.kind === "symlink",
    };
  }

  readText(path: string): string {
    this.calls.push(`read:${path}`);
    const file = this.files.get(path);
    if (!file) throw Object.assign(new Error("missing"), { code: "ENOENT" });
    return file.content;
  }

  addDirectory(directory: string, files: Record<string, string | FakeFile>): void {
    this.directories.set(directory, Object.keys(files));
    for (const [name, value] of Object.entries(files)) {
      this.files.set(join(directory, name), typeof value === "string" ? { content: value } : value);
    }
  }
}

const roots = { packageRoot: "/package", agentDir: "/agent", cwd: "/workspace" };
const packageDirectory = join(roots.packageRoot, "examples", "workers");
const userDirectory = join(roots.agentDir, "pi-orchestrate", "workers");
const projectDirectory = join(roots.cwd, CONFIG_DIR_NAME, "pi-orchestrate", "workers");

function options(projectTrusted: boolean): DiscoverWorkerCatalogOptions {
  return { ...roots, projectTrusted };
}

function definition(name: string, body: string, extras = "", lifecycle = "one-shot"): string {
  return `---
name: ${name}
description: ${name} description
model: provider/model
tools: read
lifecycle: ${lifecycle}
${extras}---

${body}`;
}

function workers(catalog: WorkerCatalog) {
  return catalog.workers;
}

describe("worker catalog discovery", () => {
  test("performs no project I/O when the project is untrusted", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, { "package.md": definition("package", "package prompt") });
    fs.addDirectory(userDirectory, { "user.md": definition("user", "user prompt") });
    fs.addDirectory(projectDirectory, { "project.md": definition("project", "project prompt") });

    const catalog = createWorkerCatalogDiscovery(fs)(options(false));

    expect(workers(catalog).map((worker) => worker.name)).toEqual(["package", "user"]);
    expect(fs.calls.some((call) => call.includes(projectDirectory))).toBe(false);
  });

  test("uses package < user < trusted project precedence by worker name", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "shared.md": definition("shared", "package prompt"),
      "package.md": definition("package", "package prompt"),
    });
    fs.addDirectory(userDirectory, { "shared.md": definition("shared", "user prompt") });
    fs.addDirectory(projectDirectory, {
      "shared.md": definition("shared", "project prompt", "", "interactive"),
    });

    const catalog = createWorkerCatalogDiscovery(fs)(options(true));
    const shared = workers(catalog).find((worker) => worker.name === "shared");

    expect(shared?.source).toEqual({
      kind: "project",
      filePath: join(projectDirectory, "shared.md"),
    });
    expect(shared?.systemPrompt).toBe("project prompt");
    expect(shared?.lifecycle).toBe("interactive");
    expect(workers(catalog).map((worker) => worker.name)).toEqual(["package", "shared"]);
  });

  test("defaults an omitted lifecycle to one-shot", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "omitted.md": definition("omitted", "prompt").replace("lifecycle: one-shot\n", ""),
    });
    fs.addDirectory(userDirectory, {});

    const catalog = createWorkerCatalogDiscovery(fs)(options(false));

    expect(workers(catalog)[0]?.lifecycle).toBe("one-shot");
    expect(catalog.diagnostics).toEqual([]);
  });

  test("decodes comma lists and compaction through the frontmatter schema", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "schema.md": `---
name: schema
description: schema description
model: provider/nested/model
tools: read, grep, bash
skills: bun, effect
thinking: high
compaction:
  enabled: true
  reserveTokens: 1200
  keepRecentTokens: 400
lifecycle: interactive
---
prompt`,
    });
    fs.addDirectory(userDirectory, {});

    const catalog = createWorkerCatalogDiscovery(fs)(options(false));

    expect(catalog.diagnostics).toEqual([]);
    expect(workers(catalog)[0]).toMatchObject({
      model: { provider: "provider", modelId: "nested/model" },
      tools: ["read", "grep", "bash"],
      skills: ["bun", "effect"],
      thinking: "high",
      compaction: { enabled: true, reserveTokens: 1200, keepRecentTokens: 400 },
      lifecycle: "interactive",
    });
  });

  test("reports representative parsing, schema-path, excess, size, and regular-file failures", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "bad-lifecycle.md": definition("bad-lifecycle", "prompt", "", "temporary"),
      "bad-enabled.md": definition(
        "bad-enabled",
        "prompt",
        "compaction:\n  enabled: yes\n",
      ),
      "bad-compaction-extra.md": definition(
        "bad-compaction-extra",
        "prompt",
        "compaction:\n  zeta: true\n  alpha: true\n",
      ),
      "bad-second-tool.md": definition("bad-second-tool", "prompt").replace(
        "tools: read",
        "tools: [read, Read]",
      ),
      "top-extra.md": definition("top-extra", "prompt", "zeta: true\nalpha: true\n"),
      "invalid-yaml.md": "---\nname: [unterminated\n---\nprompt",
      "oversized.md": {
        content: definition("oversized", "x".repeat(64 * 1024)),
        size: 1,
      },
      "symlink.md": { content: definition("symlink", "prompt"), kind: "symlink" },
    });
    fs.addDirectory(userDirectory, {});

    const catalog = createWorkerCatalogDiscovery(fs)(options(false));
    const messages = new Map(
      catalog.diagnostics.map((item) => [item.filePath?.split("/").at(-1), item.message]),
    );

    expect(workers(catalog)).toEqual([]);
    expect(messages).toEqual(new Map([
      ["bad-compaction-extra.md", "unknown compaction fields: alpha, zeta"],
      ["bad-enabled.md", "frontmatter field 'compaction.enabled' must be a boolean"],
      ["bad-lifecycle.md", "frontmatter field 'lifecycle' must be 'one-shot' or 'interactive'"],
      ["bad-second-tool.md", "unsupported tool 'Read'"],
      ["invalid-yaml.md", "frontmatter is not valid YAML"],
      ["oversized.md", "worker file exceeds 65536 bytes"],
      ["symlink.md", "worker file must be a regular non-symlink file"],
      ["top-extra.md", "unknown frontmatter fields: alpha, zeta"],
    ]));
  });

  test("distinguishes omitted, explicit empty, and nonempty skill selection", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "omitted.md": definition("omitted", "prompt"),
      "empty.md": definition("empty", "prompt", "skills: []\n"),
      "selected.md": definition("selected", "prompt", "skills: [alpha, beta]\n"),
    });
    fs.addDirectory(userDirectory, {});

    const catalog = createWorkerCatalogDiscovery(fs)(options(false));

    expect(catalog.diagnostics).toEqual([]);
    expect(workers(catalog).map(({ name, skills }) => ({ name, skills }))).toEqual([
      { name: "empty", skills: [] },
      { name: "omitted", skills: undefined },
      { name: "selected", skills: ["alpha", "beta"] },
    ]);
  });

  test("allows a worker to inherit the parent model", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "inherited.md": definition("inherited", "prompt").replace("model: provider/model\n", ""),
    });
    fs.addDirectory(userDirectory, {});

    const catalog = createWorkerCatalogDiscovery(fs)(options(false));
    expect(catalog.diagnostics).toEqual([]);
    expect(workers(catalog)[0]?.model).toBeUndefined();
  });

  test("returns workers and diagnostics in stable lexical order", () => {
    const fs = new FakeFileSystem();
    fs.addDirectory(packageDirectory, {
      "zeta.md": definition("zeta", "zeta prompt"),
      "broken-z.md": definition("wrong", "prompt"),
      "alpha.md": definition("alpha", "alpha prompt"),
      "broken-a.md": definition("also-wrong", "prompt"),
    });
    fs.directories.set(packageDirectory, ["zeta.md", "broken-z.md", "alpha.md", "broken-a.md"]);
    fs.addDirectory(userDirectory, {});

    const discover = createWorkerCatalogDiscovery(fs);
    const first = discover(options(false));
    const second = discover(options(false));

    expect(workers(first).map((worker) => worker.name)).toEqual(["alpha", "zeta"]);
    expect(first.diagnostics.map((item) => item.filePath)).toEqual([
      join(packageDirectory, "broken-a.md"),
      join(packageDirectory, "broken-z.md"),
    ]);
    expect(first).toEqual(second);
  });
});
