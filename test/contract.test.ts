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

function expectContractRule(contract: string, concepts: readonly RegExp[]): void {
  const rules = contract.split("\n").filter((line) => line.startsWith("- "));
  expect(rules.some((rule) => concepts.every((concept) => concept.test(rule)))).toBe(true);
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

  test("teaches the parent orchestration contract with a sorted trusted catalog", () => {
    const result = appendOrchestratorContract(
      "",
      catalog([worker("zeta", "project", "reusable"), worker("alpha", "package")]),
    );

    expectContractRule(result, [
      /`orchestrate`/,
      /\{ worker, title, instructions \}/,
      /\bsibling\b/i,
      /\bone assistant message\b/i,
    ]);
    expectContractRule(result, [/`orchestrate`/, /\basynchronously\b/i, /\binline\b/i, /\bblocking\b/i]);
    expectContractRule(result, [/\bpoll\b/i, /`orchestration_status`/]);
    expectContractRule(result, [/\bparent\b/i, /\bsynthesi[sz]/i, /\breview/i, /\bverification\b/i]);
    expectContractRule(result, [/\breusable\b/i, /\bready\b/i, /`worker_send`/, /`worker_close`/]);

    expectContractRule(result, [/`alpha`/, /\bpackage\b/i, /\bone-shot\b/i]);
    expectContractRule(result, [/`zeta`/, /\bproject\b/i, /\breusable\b/i]);
    expect(result.indexOf("`alpha`")).toBeLessThan(result.indexOf("`zeta`"));
  });
});
