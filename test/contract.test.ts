import { describe, expect, test } from "bun:test";
import type { WorkerCatalog, WorkerDefinition, WorkerLifecycle } from "../extension/domain.js";
import { appendOrchestratorContract } from "../extension/contract.js";

function worker(
  name: string,
  source: "package" | "user" | "project",
  lifecycle: WorkerLifecycle = "one-shot",
): WorkerDefinition {
  return {
    name,
    description: `${name} description`,
    model: { provider: "provider", modelId: "model" },
    tools: ["read"],
    skills: [],
    lifecycle,
    systemPrompt: `${name} prompt`,
    source: { kind: source, filePath: `/workers/${name}.md` },
  };
}

function catalog(workers: WorkerDefinition[]): WorkerCatalog {
  return { workers, diagnostics: [] };
}

describe("orchestrator contract", () => {
  test("appends exactly one idempotent section", () => {
    const snapshot = catalog([worker("scout", "package"), worker("expert", "user", "reusable")]);
    const once = appendOrchestratorContract("Base system prompt.", snapshot);
    const twice = appendOrchestratorContract(once, snapshot);

    expect(twice).toBe(once);
    expect(once.match(/<!-- pi-orchestrate:contract:start -->/g)).toHaveLength(1);
    expect(once.match(/## Pi Orchestrate Contract/g)).toHaveLength(1);
  });

  test("replaces the existing section when the trusted catalog changes", () => {
    const first = appendOrchestratorContract("Base", catalog([worker("old", "package")]));
    const updated = appendOrchestratorContract(first, catalog([worker("new", "project")]));

    expect(updated).not.toContain("`old`");
    expect(updated).toContain("`new` [project]");
  });

  test("defines async exclusivity, full concurrent waves, parent ownership, and lifecycle", () => {
    const result = appendOrchestratorContract(
      "",
      catalog([worker("zeta", "project", "reusable"), worker("alpha", "package")]),
    );

    expect(result).toContain("parent orchestrator");
    expect(result).toContain("{ tasks: [{ worker, title, instructions }] }");
    expect(result).toContain("at most 12 tasks");
    expect(result).toContain("all tasks execute concurrently with no hidden throttle");
    expect(result).toContain("full brief");
    expect(result).toContain("must be the sole tool call in its assistant message");
    expect(result).toContain("Mixing either with any sibling tool call");
    expect(result).toContain("inline and blocking");
    expect(result).toContain("yield the parent turn");
    expect(result).toContain("Do not duplicate delegated work");
    expect(result).toContain("poll `orchestration_status`");
    expect(result).toContain("Exact worker instructions remain visible");
    expect(result).toContain("can be expanded");
    expect(result).toContain("responses arrive individually as each worker settles");
    expect(result).toContain("final response starts parent synthesis");
    expect(result).toContain("widget shows only workers currently starting, running, or stopping");
    expect(result).toContain("Inline worker responses appear progressively in live tool output");
    expect(result).toContain("normal completion mechanism");
    expect(result).toContain("synthesizes worker results");
    expect(result).toContain("`worker_abort` only when active work must stop");
    expect(result).toContain("`worker_close` when that ready worker is finished");
    expect(result).toContain("Trusted worker catalog");
    for (const tool of [
      "orchestrate",
      "orchestration_status",
      "worker_send",
      "worker_abort",
      "worker_close",
    ]) {
      expect(result).toContain(`\`${tool}\``);
    }
    expect(result).toContain("`alpha` [package] (one-shot)");
    expect(result).toContain("`zeta` [project] (reusable)");
    expect(result.indexOf("`alpha`")).toBeLessThan(result.indexOf("`zeta`"));
  });
});
