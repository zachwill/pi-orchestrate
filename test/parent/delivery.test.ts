import { describe, expect, test } from "bun:test";
import { createSequentialIdFactories, type WorkerOutcome } from "../../extension/orchestration/model.ts";
import {
  DELIVERY_TRUNCATION_MARKER,
  DeliveryCoordinator,
  MAX_DELIVERY_MARKDOWN_BYTES,
  MAX_WORKER_DELIVERY_MARKDOWN_BYTES,
  type ParentBinding,
  type ParentBindingGeneration,
  type ScheduleIdleRecheck,
  type WorkerDeliveryMessage,
  type WorkerDeliveryOptions,
} from "../../extension/parent/delivery.ts";
import type { WorkerSettlement } from "../../extension/orchestration/settlement.ts";

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
    runId: ids.runId(),
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

interface ScheduledRecheck {
  readonly callback: () => void;
  cancelled: boolean;
}

function createIdleRecheckScheduler() {
  const scheduled: ScheduledRecheck[] = [];
  const scheduleIdleRecheck: ScheduleIdleRecheck = (callback) => {
    const recheck = { callback, cancelled: false };
    scheduled.push(recheck);
    return () => { recheck.cancelled = true; };
  };
  return {
    scheduleIdleRecheck,
    scheduled,
    activeCount: () => scheduled.filter((recheck) => !recheck.cancelled).length,
    runNext() {
      const recheck = scheduled.find((candidate) => !candidate.cancelled);
      if (!recheck) throw new Error("No idle recheck is scheduled");
      recheck.cancelled = true;
      recheck.callback();
    },
  };
}

function flushQueuedSettlements(
  count: number,
  overridesFor: (index: number) => Partial<WorkerSettlement>,
) {
  const coordinator = new DeliveryCoordinator();
  const parent = createBinding("owner-a", 1, false);
  coordinator.bind(parent.binding);
  for (let index = 0; index < count; index += 1) {
    coordinator.accept(settlement(overridesFor(index)));
  }
  parent.setIdle(true);
  coordinator.markAgentSettled("owner-a", 1);
  return parent.sent;
}

describe("DeliveryCoordinator worker settlements", () => {
  test("treats an ungrouped async settlement as final", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);

    expect(coordinator.accept(settlement({ eventId: "ungrouped", sequence: 1 }))).toBe(true);
    expect(parent.sent[0]?.options.triggerTurn).toBe(true);
  });

  test("groups independent async runs behind one final synthesis boundary", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);

    coordinator.accept(settlement({
      eventId: "group-first",
      sequence: 3,
      synthesisGroupId: "synthesis-group",
      synthesisGroupSize: 2,
    }));
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false]);

    coordinator.accept(settlement({
      eventId: "group-second",
      sequence: 4,
      synthesisGroupId: "synthesis-group",
      synthesisGroupSize: 2,
    }));
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false, true]);
    expect(parent.sent.map(({ message }) => message.details)).toEqual([
      expect.objectContaining({
        synthesisGroupId: "synthesis-group",
        synthesisGroupSize: 2,
      }),
      expect.objectContaining({
        synthesisGroupId: "synthesis-group",
        synthesisGroupSize: 2,
      }),
    ]);
  });

  test("resumes each overlapping synthesis group at its own completion boundary", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);

    coordinator.accept(settlement({
      eventId: "group-a-first",
      sequence: 30,
      synthesisGroupId: "group-a",
      synthesisGroupSize: 2,
    }));
    coordinator.accept(settlement({
      eventId: "group-b-first",
      sequence: 31,
      synthesisGroupId: "group-b",
      synthesisGroupSize: 2,
    }));
    coordinator.accept(settlement({
      eventId: "group-b-final",
      sequence: 32,
      synthesisGroupId: "group-b",
      synthesisGroupSize: 2,
    }));

    expect(parent.sent.map(({ message }) => message.details.eventId)).toEqual([
      "group-a-first",
      "group-b-first",
      "group-b-final",
    ]);
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false, false, true]);

    coordinator.accept(settlement({
      eventId: "group-a-final",
      sequence: 33,
      synthesisGroupId: "group-a",
      synthesisGroupSize: 2,
    }));
    expect(coordinator.pendingCount("owner-a")).toBe(1);
    coordinator.markAgentSettled("owner-a", 1);

    expect(parent.sent.at(-1)?.message.details.eventId).toBe("group-a-final");
    expect(parent.sent.at(-1)?.options.triggerTurn).toBe(true);
  });

  test("shrinks an async synthesis group when a sibling call fails preflight", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    coordinator.accept(settlement({
      eventId: "group-valid",
      sequence: 5,
      synthesisGroupId: "partial-group",
      synthesisGroupSize: 2,
    }));
    coordinator.skipSynthesisGroupMember("owner-a", "partial-group", 2);

    parent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 1);

    expect(parent.sent).toHaveLength(1);
    expect(parent.sent[0]?.options.triggerTurn).toBe(true);
  });

  test("queues while busy and flushes the ordered prefix through the latest final", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    const firstRun = createSequentialIdFactories(100).runId();
    const secondRun = createSequentialIdFactories(200).runId();
    coordinator.accept(settlement({ eventId: "a1", sequence: 10, runId: firstRun}));
    coordinator.accept(settlement({ eventId: "b1", sequence: 11, runId: secondRun}));
    coordinator.accept(settlement({ eventId: "a2", sequence: 12, runId: firstRun}));
    coordinator.accept(settlement({ eventId: "b2", sequence: 13, runId: secondRun}));

    parent.setIdle(true);
    coordinator.markAgentSettled("owner-a", 1);

    expect(parent.sent.map(({ message }) => message.details.eventId)).toEqual(["a1", "b1", "a2", "b2"]);
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false, false, false, true]);
    expect(coordinator.pendingCount("owner-a")).toBe(0);
  });

  test("rechecks pending delivery blocked by non-agent activity until the parent is idle", () => {
    const scheduler = createIdleRecheckScheduler();
    const coordinator = new DeliveryCoordinator(scheduler.scheduleIdleRecheck);
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);

    expect(scheduler.scheduled).toHaveLength(0);
    coordinator.accept(settlement({ eventId: "compaction-result", sequence: 14 }));
    expect(scheduler.activeCount()).toBe(1);

    scheduler.runNext();
    expect(parent.sent).toEqual([]);
    expect(scheduler.activeCount()).toBe(1);

    parent.setIdle(true);
    scheduler.runNext();
    expect(parent.sent[0]?.message.details.eventId).toBe("compaction-result");
    expect(parent.sent[0]?.options.triggerTurn).toBe(true);
    expect(scheduler.activeCount()).toBe(0);
  });

  test("preserves grouped synthesis when an idle recheck resumes a flush", () => {
    const scheduler = createIdleRecheckScheduler();
    const coordinator = new DeliveryCoordinator(scheduler.scheduleIdleRecheck);
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    coordinator.accept(settlement({
      eventId: "compacted-group-first",
      sequence: 15,
      synthesisGroupId: "compacted-group",
      synthesisGroupSize: 2,
    }));
    coordinator.accept(settlement({
      eventId: "compacted-group-final",
      sequence: 16,
      synthesisGroupId: "compacted-group",
      synthesisGroupSize: 2,
    }));

    expect(scheduler.activeCount()).toBe(1);
    parent.setIdle(true);
    scheduler.runNext();

    expect(parent.sent.map(({ message }) => message.details.eventId)).toEqual([
      "compacted-group-first",
      "compacted-group-final",
    ]);
    expect(parent.sent.map(({ options }) => options.triggerTurn)).toEqual([false, true]);
    expect(scheduler.activeCount()).toBe(0);
  });

  test("stops idle rechecks when an agent starts and resumes delivery only after settlement", () => {
    const scheduler = createIdleRecheckScheduler();
    const coordinator = new DeliveryCoordinator(scheduler.scheduleIdleRecheck);
    const parent = createBinding("owner-a", 1, false);
    coordinator.bind(parent.binding);
    coordinator.accept(settlement({ eventId: "agent-race", sequence: 17 }));
    expect(scheduler.activeCount()).toBe(1);

    coordinator.markAgentStarted("owner-a", 1);
    expect(scheduler.activeCount()).toBe(0);
    parent.setIdle(true);
    expect(parent.sent).toEqual([]);

    coordinator.markAgentSettled("owner-a", 1);
    expect(parent.sent[0]?.message.details.eventId).toBe("agent-race");
    expect(scheduler.activeCount()).toBe(0);
  });

  test("fences and cancels idle rechecks across bind, rebind, unbind, and clear", () => {
    const scheduler = createIdleRecheckScheduler();
    const coordinator = new DeliveryCoordinator(scheduler.scheduleIdleRecheck);
    coordinator.accept(settlement({ eventId: "waiting-for-bind", sequence: 18 }));
    expect(scheduler.scheduled).toHaveLength(0);

    const oldParent = createBinding("owner-a", 1, false);
    coordinator.bind(oldParent.binding);
    const staleRecheck = scheduler.scheduled[0];
    expect(staleRecheck).toBeDefined();

    const newParent = createBinding("owner-a", 2, false);
    coordinator.bind(newParent.binding);
    expect(staleRecheck?.cancelled).toBe(true);
    expect(scheduler.activeCount()).toBe(1);

    newParent.setIdle(true);
    staleRecheck?.callback();
    expect(oldParent.sent).toEqual([]);
    expect(newParent.sent).toEqual([]);

    coordinator.unbind("owner-a", 2);
    expect(scheduler.activeCount()).toBe(0);

    const finalParent = createBinding("owner-a", 3, false);
    coordinator.bind(finalParent.binding);
    expect(scheduler.activeCount()).toBe(1);
    coordinator.clear();
    expect(scheduler.activeCount()).toBe(0);
    expect(coordinator.pendingCount("owner-a")).toBe(0);
  });

  test("does not schedule idle rechecks while the parent agent is running", () => {
    const scheduler = createIdleRecheckScheduler();
    const coordinator = new DeliveryCoordinator(scheduler.scheduleIdleRecheck);
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);
    coordinator.markAgentStarted("owner-a", 1);
    parent.setIdle(false);

    coordinator.accept(settlement({ eventId: "running-agent", sequence: 19 }));
    expect(scheduler.scheduled).toHaveLength(0);
    expect(parent.sent).toEqual([]);
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
    const first = settlement({ eventId: "retry-1", sequence: 30});
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

  test("fairly caps a busy owner's large backlog with one final triggering turn", () => {
    const sent = flushQueuedSettlements(24, (index) => ({
      eventId: `queued-${index}`,
      sequence: 200 + index,
      workerId: `worker-queued-${index}` as WorkerSettlement["workerId"],
      title: `Queued ${index}`,
      outcome: { status: "completed", assistantText: `${index}:` + "z".repeat(20_000) },
    }));

    expect(sent).toHaveLength(24);
    expect(sent.reduce(
      (total, item) => total + Buffer.byteLength(item.message.content, "utf8"),
      0,
    )).toBeLessThanOrEqual(MAX_DELIVERY_MARKDOWN_BYTES);
    expect(sent.map((item) => item.options.triggerTurn).filter(Boolean)).toHaveLength(1);
    for (let index = 0; index < 24; index += 1) {
      expect(sent[index]?.message.content).toContain(`worker-queued-${index}`);
      expect(sent[index]?.message.content).toContain(`${index}:`);
    }
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
    expect(delivered?.details.outcome).toEqual({ status: "completed", assistantText: fullBody });
  });
});
