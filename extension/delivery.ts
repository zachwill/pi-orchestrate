import type { CompletedWave } from "./runtime.js";

export const MAX_DELIVERY_MARKDOWN_BYTES = 50 * 1024;
export const DELIVERY_TRUNCATION_MARKER =
  "\n\n[Worker results truncated at 50KB. Full structured results remain available.]";
export const DELIVERY_PARENT_INSTRUCTIONS =
  "Parent: Synthesize all results, resolve conflicts, review changes and evidence, run integration checks, and continue the user's task. Do not merely forward worker reports.";

export type ParentBindingGeneration = string | number | symbol;

export interface WaveDeliveryDetails {
  readonly id: CompletedWave["id"];
  readonly ownerSessionId: CompletedWave["ownerSessionId"];
  readonly mode: CompletedWave["mode"];
  readonly results: CompletedWave["results"];
}

export interface WaveDeliveryMessage {
  readonly customType: "pi-orchestrate-wave";
  readonly content: string;
  readonly display: true;
  readonly details: WaveDeliveryDetails;
}

export interface WaveDeliveryOptions {
  readonly triggerTurn: boolean;
}

export interface ParentBinding {
  readonly ownerSessionId: string;
  readonly generation: ParentBindingGeneration;
  isIdle(): boolean;
  sendMessage(message: WaveDeliveryMessage, options: WaveDeliveryOptions): void;
}

interface BoundParent {
  readonly binding: ParentBinding;
  agentRunning: boolean;
}

export class DeliveryCoordinator {
  private readonly boundParents = new Map<string, BoundParent>();
  private readonly pendingWaves: CompletedWave[] = [];

  bind(binding: ParentBinding): void {
    this.boundParents.set(binding.ownerSessionId, {
      binding,
      agentRunning: !binding.isIdle(),
    });
    this.flush(binding.ownerSessionId, binding.generation);
  }

  unbind(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    this.boundParents.delete(ownerSessionId);
  }

  markAgentStarted(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    this.boundParents.get(ownerSessionId)!.agentRunning = true;
  }

  markAgentSettled(ownerSessionId: string, generation: ParentBindingGeneration): void {
    if (!this.matchesBinding(ownerSessionId, generation)) return;
    this.boundParents.get(ownerSessionId)!.agentRunning = false;
    this.flush(ownerSessionId, generation);
  }

  accept(completedWave: CompletedWave): boolean {
    if (completedWave.mode === "inline") return false;

    this.pendingWaves.push(completedWave);
    const generation = this.boundParents.get(completedWave.ownerSessionId)?.binding.generation;
    if (generation !== undefined) {
      this.flush(completedWave.ownerSessionId, generation);
    }
    return true;
  }

  pendingCount(ownerSessionId: string): number {
    return this.pendingWaves.filter((wave) => wave.ownerSessionId === ownerSessionId).length;
  }

  clear(): void {
    this.boundParents.clear();
    this.pendingWaves.length = 0;
  }

  close(): void {
    this.clear();
  }

  private matchesBinding(
    ownerSessionId: string,
    generation: ParentBindingGeneration,
  ): boolean {
    const binding = this.boundParents.get(ownerSessionId)?.binding;
    return binding?.generation === generation;
  }

  private canDeliver(
    ownerSessionId: string,
    generation: ParentBindingGeneration,
  ): boolean {
    const parent = this.boundParents.get(ownerSessionId);
    return (
      parent !== undefined &&
      parent.binding.generation === generation &&
      !parent.agentRunning &&
      parent.binding.isIdle()
    );
  }

  private flush(
    ownerSessionId: string,
    generation: ParentBindingGeneration,
  ): void {
    if (!this.canDeliver(ownerSessionId, generation)) return;

    const waves = this.pendingWaves.filter((wave) => wave.ownerSessionId === ownerSessionId);
    for (let index = 0; index < waves.length; index += 1) {
      if (!this.canDeliver(ownerSessionId, generation)) return;

      const wave = waves[index];
      if (!wave) return;

      try {
        // Pi 0.80.10 starts an idle turn only through triggerTurn. Omitting deliverAs keeps
        // streaming races as steering; nextTurn would queue without starting a turn.
        this.boundParents.get(ownerSessionId)!.binding.sendMessage(renderWaveMessage(wave), {
          triggerTurn: index === waves.length - 1,
        });
      } catch {
        return;
      }

      const pendingIndex = this.pendingWaves.indexOf(wave);
      if (pendingIndex >= 0) this.pendingWaves.splice(pendingIndex, 1);
    }
  }
}

function renderWaveMessage(wave: CompletedWave): WaveDeliveryMessage {
  const resultWord = wave.results.length === 1 ? "result" : "results";
  const sections = wave.results.map((result) => {
    const outcome = renderOutcome(result.outcome);
    const heading = `### ${result.worker} — ${result.title}`;
    const metadata = `Worker \`${result.workerId}\` · status \`${result.status}\``;
    return outcome.length > 0 ? `${heading}\n${metadata}\n\n${outcome}` : `${heading}\n${metadata}`;
  });
  const summary = `## Worker results — wave \`${wave.id}\`\n\n${wave.results.length} ${resultWord}`;
  const results = sections.length > 0 ? `${summary}\n\n${sections.join("\n\n")}` : summary;
  const instructions = `\n\n---\n\n${DELIVERY_PARENT_INSTRUCTIONS}`;

  return {
    customType: "pi-orchestrate-wave",
    content: capMarkdown(results, instructions),
    display: true,
    details: {
      id: wave.id,
      ownerSessionId: wave.ownerSessionId,
      mode: wave.mode,
      results: wave.results,
    },
  };
}

function capMarkdown(content: string, appendix: string): string {
  const complete = `${content}${appendix}`;
  const completeBytes = Buffer.from(complete, "utf8");
  if (completeBytes.byteLength <= MAX_DELIVERY_MARKDOWN_BYTES) return complete;

  const contentBytes = Buffer.from(content, "utf8");
  const reservedBytes = Buffer.byteLength(`${DELIVERY_TRUNCATION_MARKER}${appendix}`, "utf8");
  let prefixEnd = MAX_DELIVERY_MARKDOWN_BYTES - reservedBytes;
  while (prefixEnd > 0 && (contentBytes[prefixEnd]! & 0xc0) === 0x80) prefixEnd -= 1;
  return `${contentBytes.subarray(0, prefixEnd).toString("utf8")}${DELIVERY_TRUNCATION_MARKER}${appendix}`;
}

function renderOutcome(outcome: CompletedWave["results"][number]["outcome"]): string {
  switch (outcome.status) {
    case "completed":
    case "ready":
      return outcome.assistantText;
    case "failed":
      return outcome.assistantText
        ? `Failed: ${outcome.message}\n\n${outcome.assistantText}`
        : `Failed: ${outcome.message}`;
    case "aborted": {
      const reason = outcome.message ? `Aborted: ${outcome.message}` : "Aborted";
      return outcome.assistantText ? `${reason}\n\n${outcome.assistantText}` : reason;
    }
    case "closed":
      return "Closed";
  }
}
