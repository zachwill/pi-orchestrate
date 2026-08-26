import { describe, expect, test } from "bun:test";
import {
  SUPPORTED_TOOL_NAMES,
  createWorkerCatalog,
  findWorkerByName,
  isSupportedToolName,
  type WorkerDefinition,
} from "../../extension/catalog/definition.js";

const workerDefinition = (name: string): WorkerDefinition => ({
  name,
  source: { kind: "package", filePath: `/workers/${name}.md` },
  description: `${name} worker`,
  systemPrompt: `You are ${name}.`,
  lifecycle: "one-shot",
  tools: ["read", "grep"],
  skills: [],
});

describe("supported tools and worker catalog", () => {
  test("accepts every exact built-in tool name", () => {
    for (const toolName of SUPPORTED_TOOL_NAMES) expect(isSupportedToolName(toolName)).toBe(true);
    expect(isSupportedToolName("Read")).toBe(false);
    expect(isSupportedToolName("shell")).toBe(false);
  });

  test("copies and sorts catalog workers", () => {
    const input = [workerDefinition("worker"), workerDefinition("investigator"), workerDefinition("scout")];
    const catalog = createWorkerCatalog(input);

    expect(catalog.workers.map((worker) => worker.name)).toEqual([
      "investigator",
      "scout",
      "worker",
    ]);
    expect(input.map((worker) => worker.name)).toEqual(["worker", "investigator", "scout"]);
    expect(findWorkerByName(catalog, "scout")?.description).toBe("scout worker");
    expect(findWorkerByName(catalog, "missing")).toBeUndefined();
  });
});

