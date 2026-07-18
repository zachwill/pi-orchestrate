import { describe, expect, test } from "bun:test";
import { createSequentialIdFactories, type WorkerOutcome } from "../extension/domain.ts";
import {
  DELIVERY_PARENT_INSTRUCTIONS,
  DELIVERY_TRUNCATION_MARKER,
  DeliveryCoordinator,
  MAX_DELIVERY_MARKDOWN_BYTES,
  MAX_WORKER_DELIVERY_MARKDOWN_BYTES,
  type ParentBinding,
  type ParentBindingGeneration,
  type WorkerDeliveryMessage,
  type WorkerDeliveryOptions,
} from "../extension/delivery.ts";
import type { WorkerSettlement } from "../extension/runtime.ts";

const usage = {
  input: 10,
  output: 5,
  cacheRead: 2,
  cacheWrite: 1,
  cost: 0.01,
  contextTokens: 17,
  turns: 1,
};

let settlementNumber = 0;
function settlement(overrides: Partial<WorkerSettlement> = {}): WorkerSettlement {
  settlementNumber += 1;
  const ids = createSequentialIdFactories(settlementNumber);
  const outcome: WorkerOutcome = { status: "completed", assistantText: "Inspection complete." };
  return {
    eventId: `event-${settlementNumber}`,
    sequence: settlementNumber,
    ownerSessionId: "owner-a",
    waveId: ids.waveId(),
    workerId: ids.workerId(),
    generation: 1,
    mode: "async",
    worker: "scout",
    title: "Inspect the code",
    lifecycle: "one-shot",
    status: "completed",
    outcome,
    usage,
    startedAt: 100,
    settledAt: 200,
    remainingActive: 0,
    waveSize: 1,
    waveComplete: true,
    sessionFile: "/sessions/worker.jsonl",
    ...overrides,
  };
}

interface SentMessage {
  readonly message: WorkerDeliveryMessage;
  readonly options: WorkerDeliveryOptions;
}

function createBinding(
  ownerSessionId: string,
  generation: ParentBindingGeneration,
  initiallyIdle = true,
) {
  let idle = initiallyIdle;
  let failedAttempt: number | undefined;
  const sent: SentMessage[] = [];
  const attempts: SentMessage[] = [];
  const binding: ParentBinding = {
    ownerSessionId,
    generation,
    isIdle: () => idle,
    sendMessage(message, options) {
      const delivery = { message, options };
      attempts.push(delivery);
      if (attempts.length === failedAttempt) throw new Error("send failed");
      sent.push(delivery);
    },
  };
  return {
    binding,
    sent,
    attempts,
    setIdle(value: boolean) { idle = value; },
    failOnAttempt(attempt: number | undefined) { failedAttempt = attempt; },
  };
}

describe("DeliveryCoordinator worker settlements", () => {
  test("delivers an intermediate result immediately without a turn and the final with one turn", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);
    const ids = createSequentialIdFactories(50);
    const waveId = ids.waveId();

    expect(coordinator.accept(settlement({
      waveId,
      workerId: ids.workerId(),
      eventId: "wave-event-1",
      sequence: 1,
      remainingActive: 1,
      waveComplete: false,
    }))).toBe(true);
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false]);

    coordinator.accept(settlement({
      waveId,
      workerId: ids.workerId(),
      eventId: "wave-event-2",
      sequence: 2,
      waveComplete: true,
    }));

    expect(parent.sent.map(({ message }) => message.customType)).toEqual([
      "pi-orchestrate-worker-result",
      "pi-orchestrate-worker-result",
    ]);
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false, true]);
    expect(parent.sent[0]?.message.content).not.toContain(DELIVERY_PARENT_INSTRUCTIONS);
    expect(parent.sent[1]?.message.content).toContain(DELIVERY_PARENT_INSTRUCTIONS);
  });

  test("queues while busy and flushes the ordered prefix through the latest final", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    const firstWave = createSequentialIdFactories(100).waveId();
    const secondWave = createSequentialIdFactories(200).waveId();
    coordinator.accept(settlement({ eventId: "a1", sequence: 10, waveId: firstWave, waveComplete: false, remainingActive: 1 }));
    coordinator.accept(settlement({ eventId: "b1", sequence: 11, waveId: secondWave, waveComplete: false, remainingActive: 1 }));
    coordinator.accept(settlement({ eventId: "a2", sequence: 12, waveId: firstWave, waveComplete: true }));
    coordinator.accept(settlement({ eventId: "b2", sequence: 13, waveId: secondWave, waveComplete: false, remainingActive: 1 }));

    parent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 1);

    expect(parent.sent.map(({ message }) => message.details.eventId)).toEqual(["a1", "b1", "a2"]);
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false, false, true]);
    expect(coordinator.pendingCount("owner-a")).toBe(1);
  });

  test("preserves owner isolation and ignores stale binding generations", () => {
    const coordinator = new DeliveryCoordinator();
    const oldParent = createBinding("owner-a", 1, false);
    const newParent = createBinding("owner-a", 2, false);
    const otherParent = createBinding("owner-b", 1);
    coordinator.bind(oldParent.binding);
    coordinator.bind(newParent.binding);
    coordinator.bind(otherParent.binding);
    coordinator.accept(settlement({ eventId: "owner-a", sequence: 20, ownerSessionId: "owner-a" }));
    coordinator.accept(settlement({ eventId: "owner-b", sequence: 21, ownerSessionId: "owner-b" }));

    coordinator.markAgentSettled("owner-a", 1);
    coordinator.unbind("owner-a", 1);
    expect(oldParent.sent).toEqual([]);
    expect(otherParent.sent[0]?.message.details.eventId).toBe("owner-b");

    newParent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 2);
    expect(newParent.sent[0]?.message.details.eventId).toBe("owner-a");
  });

  test("deduplicates events and retries a failed send without replaying successes", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    parent.failOnAttempt(2);
    coordinator.bind(parent.binding);
    const first = settlement({ eventId: "retry-1", sequence: 30, waveComplete: false, remainingActive: 1 });
    const second = settlement({ eventId: "retry-2", sequence: 31 });
    expect(coordinator.accept(first)).toBe(true);
    expect(coordinator.accept(first)).toBe(false);
    coordinator.accept(second);
    parent.setIdle(true);

    coordinator.markAgentSettled("owner-a", 1);
    expect(parent.sent.map(({ message }) => message.details.eventId)).toEqual(["retry-1"]);
    expect(coordinator.pendingCount("owner-a")).toBe(1);

    parent.failOnAttempt(undefined);
    coordinator.markAgentSettled("owner-a", 1);
    expect(parent.sent.map(({ message }) => message.details.eventId)).toEqual(["retry-1", "retry-2"]);
  });

  test("rejects inline settlements and counts pending worker results", () => {
    const coordinator = new DeliveryCoordinator();
    expect(coordinator.accept(settlement({ mode: "inline" }))).toBe(false);
    const busy = createBinding("owner-a", 1, false);
    coordinator.bind(busy.binding);
    coordinator.accept(settlement({ eventId: "pending", sequence: 40 }));
    expect(coordinator.pendingCount("owner-a")).toBe(1);
    coordinator.clear();
    expect(coordinator.pendingCount("owner-a")).toBe(0);
  });

  test("fairly caps twelve large workers while preserving identity, excerpts, and final instructions", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    const waveId = createSequentialIdFactories(500).waveId();

    for (let index = 0; index < 12; index += 1) {
      coordinator.accept(settlement({
        eventId: `fair-${index}`,
        sequence: 100 + index,
        waveId,
        workerId: `worker-fair-${index}` as WorkerSettlement["workerId"],
        title: `Fair worker ${index}`,
        outcome: { status: "completed", assistantText: `${index}:` + "x".repeat(30_000) },
        remainingActive: 11 - index,
        waveSize: 12,
        waveComplete: index === 11,
      }));
    }

    parent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 1);

    expect(parent.sent).toHaveLength(12);
    expect(parent.sent.reduce(
      (total, item) => total + Buffer.byteLength(item.message.content, "utf8"),
      0,
    )).toBeLessThanOrEqual(MAX_DELIVERY_MARKDOWN_BYTES);
    for (let index = 0; index < 12; index += 1) {
      const content = parent.sent[index]!.message.content;
      expect(content).toContain(`worker-fair-${index}`);
      expect(content).toContain(`${index}:`);
    }
    expect(parent.sent.at(-1)?.message.content).toEndWith(DELIVERY_PARENT_INSTRUCTIONS);
  });

  test("caps a busy-owner batch across many completed waves", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    for (let index = 0; index < 24; index += 1) {
      coordinator.accept(settlement({
        eventId: `batch-${index}`,
        sequence: 200 + index,
        workerId: `worker-batch-${index}` as WorkerSettlement["workerId"],
        title: `Batch ${index}`,
        outcome: { status: "completed", assistantText: `${index}:` + "z".repeat(20_000) },
      }));
    }

    parent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 1);
    expect(parent.sent).toHaveLength(24);
    expect(parent.sent.reduce(
      (total, item) => total + Buffer.byteLength(item.message.content, "utf8"),
      0,
    )).toBeLessThanOrEqual(MAX_DELIVERY_MARKDOWN_BYTES);
    expect(parent.sent.map((item) => item.options.triggerTurn).filter(Boolean)).toHaveLength(1);
    expect(parent.sent.at(-1)?.message.content).toEndWith(DELIVERY_PARENT_INSTRUCTIONS);
  });

  test("does not reenter a flush when sendMessage synchronously accepts another final", () => {
    const coordinator = new DeliveryCoordinator();
    const nested = settlement({ eventId: "nested", sequence: 301 });
    const sent: SentMessage[] = [];
    let idle = true;
    coordinator.bind({
      ownerSessionId: "owner-a",
      generation: 1,
      isIdle: () => idle,
      sendMessage(message, options) {
        sent.push({ message, options });
        if (message.details.eventId === "outer") coordinator.accept(nested);
      },
    });

    coordinator.accept(settlement({ eventId: "outer", sequence: 300 }));
    expect(sent.map((item) => item.message.details.eventId)).toEqual(["outer"]);
    expect(sent[0]?.options.triggerTurn).toBe(true);
    expect(coordinator.pendingCount("owner-a")).toBe(1);

    idle = true;
    coordinator.markAgentSettled("owner-a", 1);
    expect(sent.map((item) => item.message.details.eventId)).toEqual(["outer", "nested"]);
    expect(sent.map((item) => item.options.triggerTurn)).toEqual([true, true]);
  });

  test("accepts reused worker and wave IDs when the settlement sequence advances", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);
    const reused = settlement({ eventId: "old-identity", sequence: 400 });
    expect(coordinator.accept(reused)).toBe(true);
    parent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 1);
    expect(coordinator.accept({ ...reused, eventId: "new-identity", sequence: 401 })).toBe(true);
    expect(parent.sent.map((item) => item.message.details.sequence)).toEqual([400, 401]);
  });

  test("caps individual parent context while retaining the complete structured outcome", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);
    const fullBody = "🙂".repeat(20_000);
    coordinator.accept(settlement({
      eventId: "large",
      sequence: 50,
      outcome: { status: "completed", assistantText: fullBody },
    }));

    const delivered = parent.sent[0]?.message;
    expect(delivered).toBeDefined();
    expect(Buffer.byteLength(delivered?.content ?? "", "utf8")).toBeLessThanOrEqual(
      MAX_WORKER_DELIVERY_MARKDOWN_BYTES,
    );
    expect(delivered?.content).toContain(DELIVERY_TRUNCATION_MARKER);
    expect(delivered?.content).not.toContain("�");
    expect(delivered?.content).toEndWith(DELIVERY_PARENT_INSTRUCTIONS);
    expect(delivered?.details.outcome).toEqual({ status: "completed", assistantText: fullBody });
  });
});
