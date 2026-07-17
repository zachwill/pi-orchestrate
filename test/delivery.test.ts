import { describe, expect, test } from "bun:test";
import {
  DELIVERY_PARENT_INSTRUCTIONS,
  DELIVERY_TRUNCATION_MARKER,
  DeliveryCoordinator,
  MAX_DELIVERY_MARKDOWN_BYTES,
  type ParentBinding,
  type ParentBindingGeneration,
  type WaveDeliveryMessage,
  type WaveDeliveryOptions,
} from "../extension/delivery.ts";

type CompletedWave = Parameters<DeliveryCoordinator["accept"]>[0];
type CompletedResult = CompletedWave["results"][number];

const usage = {
  input: 10,
  output: 5,
  cacheRead: 2,
  cacheWrite: 1,
  cost: 0.01,
  contextTokens: 17,
  turns: 1,
};

function result(
  workerId: string,
  worker: string,
  title: string,
  outcome: CompletedResult["outcome"],
  sessionFile?: string,
): CompletedResult {
  return {
    workerId,
    worker,
    title,
    status: outcome.status,
    outcome,
    usage,
    ...(sessionFile === undefined ? {} : { sessionFile }),
  } as CompletedResult;
}

function wave(
  id: string,
  ownerSessionId: string,
  results: readonly CompletedResult[] = [
    result("worker-1", "scout", "Inspect the code", {
      status: "completed",
      assistantText: "Inspection complete.",
    }),
  ],
  mode: CompletedWave["mode"] = "async",
): CompletedWave {
  return { id, ownerSessionId, mode, results } as CompletedWave;
}

interface SentMessage {
  readonly message: WaveDeliveryMessage;
  readonly options: WaveDeliveryOptions;
}

interface BindingHarness {
  readonly binding: ParentBinding;
  readonly sent: SentMessage[];
  readonly attempts: SentMessage[];
  setIdle(idle: boolean): void;
  failOnAttempt(attempt: number | undefined): void;
}

function createBinding(
  ownerSessionId: string,
  generation: ParentBindingGeneration,
  initiallyIdle = true,
): BindingHarness {
  let idle = initiallyIdle;
  let failedAttempt: number | undefined;
  const sent: SentMessage[] = [];
  const attempts: SentMessage[] = [];

  return {
    binding: {
      ownerSessionId,
      generation,
      isIdle: () => idle,
      sendMessage(message, options) {
        const delivery = { message, options };
        attempts.push(delivery);
        if (attempts.length === failedAttempt) throw new Error("send failed");
        sent.push(delivery);
      },
    },
    sent,
    attempts,
    setIdle(nextIdle) {
      idle = nextIdle;
    },
    failOnAttempt(attempt) {
      failedAttempt = attempt;
    },
  };
}

describe("DeliveryCoordinator", () => {
  test("holds a completed wave until the exact parent agent binding settles", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);
    coordinator.markAgentStarted("owner-a", 1);

    expect(coordinator.accept(wave("wave-1", "owner-a"))).toBe(true);
    expect(parent.sent).toEqual([]);
    expect(coordinator.pendingCount("owner-a")).toBe(1);

    coordinator.markAgentSettled("owner-a", 1);
    coordinator.markAgentSettled("owner-a", 1);

    expect(parent.sent).toHaveLength(1);
    expect(parent.sent[0]?.options).toEqual({ triggerTurn: true });
    expect("deliverAs" in parent.sent[0]!.options).toBe(false);
    expect(coordinator.pendingCount("owner-a")).toBe(0);
  });

  test("keeps simultaneous owner bindings and pending queues isolated", () => {
    const coordinator = new DeliveryCoordinator();
    const ownerA = createBinding("owner-a", 1);
    const ownerB = createBinding("owner-b", 1);
    coordinator.accept(wave("wave-a", "owner-a"));
    coordinator.accept(wave("wave-b", "owner-b"));

    coordinator.bind(ownerA.binding);
    coordinator.bind(ownerB.binding);
    coordinator.accept(wave("wave-a-2", "owner-a"));
    coordinator.accept(wave("wave-b-2", "owner-b"));

    expect(ownerA.sent.map(({ message }) => message.details.id)).toEqual([
      "wave-a",
      "wave-a-2",
    ]);
    expect(ownerB.sent.map(({ message }) => message.details.id)).toEqual([
      "wave-b",
      "wave-b-2",
    ]);
    expect(coordinator.pendingCount("owner-a")).toBe(0);
    expect(coordinator.pendingCount("owner-b")).toBe(0);
  });

  test("a stale generation cannot settle or unbind a newer binding", () => {
    const coordinator = new DeliveryCoordinator();
    const oldBinding = createBinding("owner-a", 1, false);
    const newBinding = createBinding("owner-a", 2, false);
    coordinator.bind(oldBinding.binding);
    coordinator.bind(newBinding.binding);
    coordinator.accept(wave("wave-1", "owner-a"));

    coordinator.markAgentSettled("owner-a", 1);
    coordinator.unbind("owner-a", 1);
    expect(newBinding.sent).toEqual([]);

    newBinding.setIdle(true);
    coordinator.markAgentSettled("owner-a", 2);
    expect(newBinding.sent.map(({ message }) => message.details.id)).toEqual(["wave-1"]);
  });

  test("flushes multiple waves in order with only the final message triggering a turn", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.accept(wave("wave-1", "owner-a"));
    coordinator.accept(wave("wave-2", "owner-a"));
    coordinator.accept(wave("wave-3", "owner-a"));

    coordinator.bind(parent.binding);

    expect(parent.sent.map(({ message }) => message.details.id)).toEqual([
      "wave-1",
      "wave-2",
      "wave-3",
    ]);
    expect(parent.sent.map(({ options }) => options)).toEqual([
      { triggerTurn: false },
      { triggerTurn: false },
      { triggerTurn: true },
    ]);
    expect(parent.sent.filter(({ options }) => options.triggerTurn)).toHaveLength(1);
    expect(parent.sent.some(({ options }) => "deliverAs" in options)).toBe(false);
  });

  test("renders one aggregate message with ordered results and structured details", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    const first = result(
      "worker-first",
      "scout",
      "Find relevant code",
      { status: "completed", assistantText: "First result body." },
      "/sessions/first.jsonl",
    );
    const second = result("worker-second", "worker", "Implement change", {
      status: "failed",
      message: "Verification failed",
      assistantText: "Second result body.",
    });
    const completed = wave("wave-ordered", "owner-a", [first, second]);
    coordinator.bind(parent.binding);

    coordinator.accept(completed);

    const delivered = parent.sent[0]!.message;
    expect(delivered.details).toEqual({
      id: completed.id,
      ownerSessionId: "owner-a",
      mode: "async",
      results: [first, second],
    });
    expect(delivered.content.startsWith("## Worker results — wave `wave-ordered`")).toBe(true);
    expect(delivered.content).toContain("First result body.");
    expect(delivered.content).toContain("Failed: Verification failed");
    expect(delivered.content.indexOf("worker-first")).toBeLessThan(
      delivered.content.indexOf("worker-second"),
    );
    expect(delivered.content).toEndWith(DELIVERY_PARENT_INSTRUCTIONS);
    expect(delivered.content).toContain("Synthesize all results");
    expect(delivered.content).toContain("resolve conflicts");
    expect(delivered.content).toContain("review changes and evidence");
    expect(delivered.content).toContain("run integration checks");
    expect(delivered.content).toContain("continue the user's task");
    expect(delivered.content).toContain("Do not merely forward worker reports");
  });

  test("caps aggregate Markdown at 50KB with a deterministic marker and full details", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    const fullBody = "🙂".repeat(20_000);
    const completed = wave("wave-large", "owner-a", [
      result("worker-large", "worker", "Large output", {
        status: "completed",
        assistantText: fullBody,
      }),
    ]);
    coordinator.bind(parent.binding);

    coordinator.accept(completed);
    const delivered = parent.sent[0]!.message;

    expect(Buffer.byteLength(delivered.content, "utf8")).toBeLessThanOrEqual(
      MAX_DELIVERY_MARKDOWN_BYTES,
    );
    expect(delivered.content).toContain(DELIVERY_TRUNCATION_MARKER);
    expect(delivered.content).toEndWith(DELIVERY_PARENT_INSTRUCTIONS);
    expect(delivered.content).not.toContain("�");
    expect(delivered.details.results[0]?.outcome).toEqual({
      status: "completed",
      assistantText: fullBody,
    });

    const secondCoordinator = new DeliveryCoordinator();
    const secondParent = createBinding("owner-a", 2);
    secondCoordinator.bind(secondParent.binding);
    secondCoordinator.accept(completed);
    expect(secondParent.sent[0]!.message.content).toBe(delivered.content);
  });

  test("retains a failed send and retries without redelivering successes", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1, false);
    parent.failOnAttempt(2);
    coordinator.bind(parent.binding);
    coordinator.accept(wave("wave-1", "owner-a"));
    coordinator.accept(wave("wave-2", "owner-a"));
    parent.setIdle(true);

    coordinator.markAgentSettled("owner-a", 1);
    expect(parent.sent.map(({ message }) => message.details.id)).toEqual(["wave-1"]);
    expect(coordinator.pendingCount("owner-a")).toBe(1);

    parent.failOnAttempt(undefined);
    coordinator.markAgentSettled("owner-a", 1);
    expect(parent.sent.map(({ message }) => message.details.id)).toEqual(["wave-1", "wave-2"]);
  });

  test("rejects inline waves and clear drops pending delivery", () => {
    const coordinator = new DeliveryCoordinator();
    const parent = createBinding("owner-a", 1);
    coordinator.bind(parent.binding);

    expect(coordinator.accept(wave("wave-inline", "owner-a", undefined, "inline"))).toBe(false);
    expect(parent.sent).toEqual([]);

    const running = createBinding("owner-a", 2, false);
    coordinator.bind(running.binding);
    coordinator.accept(wave("wave-pending", "owner-a"));
    coordinator.clear();
    expect(coordinator.pendingCount("owner-a")).toBe(0);
  });
});
