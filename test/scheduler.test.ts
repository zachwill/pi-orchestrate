import { describe, expect, test } from "bun:test";
import { Deferred, Effect, ManagedRuntime } from "effect";
import {
  CleanupSupervisor,
  processSupervisorLayer,
} from "../extension/scheduler.js";

async function expectPending(promise: Promise<unknown>): Promise<void> {
  let settled = false;
  void promise.finally(() => {
    settled = true;
  });
  await Promise.resolve();
  await Promise.resolve();
  expect(settled).toBe(false);
}

describe("cleanup supervision", () => {
  test("awaitEmpty waits for multiple concurrent cleanups and isolates cleanup failure", async () => {
    const runtime = ManagedRuntime.make(processSupervisorLayer);
    const cleanup = runtime.runSync(CleanupSupervisor);
    const first = Deferred.makeUnsafe<void>();
    const second = Deferred.makeUnsafe<void>();

    cleanup.supervise(Deferred.await(first));
    cleanup.supervise(Effect.die(new Error("cleanup failed")));
    cleanup.supervise(Deferred.await(second));

    const awaiting = runtime.runPromise(cleanup.awaitEmpty());
    await expectPending(awaiting);
    Deferred.doneUnsafe(first, Effect.void);
    await expectPending(awaiting);
    Deferred.doneUnsafe(second, Effect.void);
    await awaiting;

    await runtime.dispose();
  });

  test("ManagedRuntime disposal interrupts live cleanup and runs its finalizer", async () => {
    const runtime = ManagedRuntime.make(processSupervisorLayer);
    const cleanup = runtime.runSync(CleanupSupervisor);
    const started = Deferred.makeUnsafe<void>();
    const finalized = Deferred.makeUnsafe<void>();

    cleanup.supervise(
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Deferred.succeed(finalized, undefined)),
      ),
    );
    await Effect.runPromise(Deferred.await(started));

    const disposing = runtime.dispose();
    await Effect.runPromise(Deferred.await(finalized));
    await disposing;
  });

  test("cleanup requests after the root closes do not escape its FiberSet", async () => {
    const runtime = ManagedRuntime.make(processSupervisorLayer);
    const cleanup = runtime.runSync(CleanupSupervisor);
    let runs = 0;
    await runtime.dispose();

    cleanup.supervise(Effect.sync(() => {
      runs += 1;
    }));
    await Effect.runPromise(cleanup.awaitEmpty());

    expect(runs).toBe(0);
  });
});
