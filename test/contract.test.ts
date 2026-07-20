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
      /\bproactively identify\b/i,
      /\bevery useful bounded independent scope\b/i,
      /\bmaterially distinct\b/i,
      /\bevidence\b/i,
      /\bhypothesis\b/i,
      /\bvalidation\b/i,
    ]);
    expectContractRule(result, [
      /\bspin up as many workers as needed\b/i,
      /\bnever\b/i,
      /\bsmall fixed default\b/i,
    ]);
    expectContractRule(result, [
      /\bworker roles and counts named by the user\b/i,
      /\bminimum requirements\b/i,
      /\bnot ceilings\b/i,
      /\bexact cap\b/i,
    ]);
    expectContractRule(result, [
      /\bworker role is reusable\b/i,
      /\bsame catalog worker\b/i,
      /\bmany calls\b/i,
      /\bseparate scopes or perspectives\b/i,
    ]);
    expectContractRule(result, [
      /\bbefore dispatching\b/i,
      /\benumerate the full first parallel wave\b/i,
      /\bfrom the work itself\b/i,
    ]);
    expectContractRule(result, [
      /\bmandatory wave cardinality\b/i,
      /\bwave has N workers\b/i,
      /\bnext assistant response must contain exactly N separate, fully briefed\b/i,
      /`orchestrate` invocations/,
      /\bsingle invocation is valid only when N=1\b/i,
      /\bform all N invocations before emitting or finalizing\b/i,
      /\bsole asynchronous invocation ends the parent turn\b/i,
      /`terminate: true`/,
      /\bomitted siblings cannot be added afterward\b/i,
    ]);
    expectContractRule(result, [
      /\bmandatory response shape\b/i,
      /\bpure orchestration\b/i,
      /\bexactly those N\b/i,
      /\bno text or non-orchestration tools\b/i,
      /\bPi natively supports sibling tool calls\b/i,
      /\bFor N=3\b/i,
      /\bemit three native sibling calls in one response\b/i,
      /\{ worker, title, instructions \}/,
    ]);
    expectContractRule(result, [
      /\beach independent scope or distinct perspective\b/i,
      /\bfully briefed\b/i,
      /\bacceptance or completion\b/i,
      /\bdispatching the rest\b/i,
    ]);
    expectContractRule(result, [
      /\bdeliberate overlap\b/i,
      /\bonly\b/i,
      /\bdistinct evidence sources\b/i,
      /\bcompeting hypotheses\b/i,
      /\bvalidation perspectives\b/i,
      /\baccidental duplicate assignments are forbidden\b/i,
    ]);
    expectContractRule(result, [
      /\bthorough\b/i,
      /\bself-contained\b/i,
      /\bobjective\b/i,
      /\bscope\b/i,
      /\bsuccess criteria\b/i,
      /\bexpected output\b/i,
    ]);
    expectContractRule(result, [/`orchestrate`/, /\basynchronously\b/i, /\binline\b/i, /\bblocking\b/i]);
    expectContractRule(result, [
      /\bfull current wave\b/i,
      /\badmissions have resolved\b/i,
      /\brejected sibling does not block yielding\b/i,
    ]);
    expectContractRule(result, [/\bpoll\b/i, /`orchestration_status`/]);
    expectContractRule(result, [
      /\bresults\b/i,
      /\bmore useful independent scopes\b/i,
      /\bmaterially distinct perspectives\b/i,
      /\banother full parallel wave\b/i,
      /\bcontinue adaptive full waves\b/i,
      /\bwhole task\b/i,
    ]);
    expectContractRule(result, [/\bparent\b/i, /\bsynthesi[sz]/i, /\breview/i, /\bverification\b/i]);
    expect(result).not.toContain("active-work widget");
    expectContractRule(result, [/\breusable\b/i, /\bready\b/i, /`worker_send`/, /`worker_close`/]);

    expectContractRule(result, [/`alpha`/, /\bpackage\b/i, /\bone-shot\b/i]);
    expectContractRule(result, [/`zeta`/, /\bproject\b/i, /\breusable\b/i]);
    expect(result.indexOf("`alpha`")).toBeLessThan(result.indexOf("`zeta`"));
  });
});
