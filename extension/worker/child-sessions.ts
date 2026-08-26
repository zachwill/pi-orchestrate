import {
  Context,
  Deferred,
  Effect,
  Exit,
  FiberSet,
  Layer,
  Schema,
  SynchronizedRef,
} from "effect";
import {
  createWorkerSession,
  type ChildSessionOptions,
  type WorkerSessionCreationError,
  type WorkerSessionDependencies,
  type WorkerSessionHandle,
} from "./session.ts";

export class WorkerSessionAcquisitionClosedError extends Schema.TaggedError<WorkerSessionAcquisitionClosedError>()(
  "WorkerSession.AcquisitionClosedError",
  { message: Schema.String },
) {}

export type WorkerSessionAcquisitionError =
  | WorkerSessionCreationError
  | WorkerSessionAcquisitionClosedError;

export interface ChildSessionsService {
  /**
   * Acquires a child session through a process-owned producer. The adopter must
   * synchronously install ownership before returning a value. Returning undefined
   * rejects adoption and leaves ChildSessions responsible for disposal.
   */
  readonly acquire: <Adopted>(
    options: ChildSessionOptions,
    adopt: (session: WorkerSessionHandle) => Adopted | undefined,
  ) => Effect.Effect<Adopted | undefined, WorkerSessionAcquisitionError>;
  /** Closes every handoff without waiting for uncancellable Pi calls. */
  readonly shutdown: () => Effect.Effect<void>;
}

export class ChildSessions extends Context.Service<ChildSessions, ChildSessionsService>()(
  "@zachwill/pi-orchestrate/ChildSessions",
) {}

type AcquisitionHandoffState =
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Offered"; readonly session: WorkerSessionHandle }
  | { readonly _tag: "Adopting"; readonly session: WorkerSessionHandle }
  | { readonly _tag: "Adopted" }
  | { readonly _tag: "Abandoned"; readonly error?: WorkerSessionAcquisitionClosedError }
  | { readonly _tag: "Failed" };

interface AcquisitionHandoff {
  readonly state: SynchronizedRef.SynchronizedRef<AcquisitionHandoffState>;
  readonly result: Deferred.Deferred<WorkerSessionHandle, WorkerSessionAcquisitionError>;
}

type ChildSessionsState =
  | { readonly _tag: "Open"; readonly handoffs: ReadonlySet<AcquisitionHandoff> }
  | { readonly _tag: "Closed"; readonly error: WorkerSessionAcquisitionClosedError };

type AdoptionReservation =
  | { readonly _tag: "Reserved"; readonly session: WorkerSessionHandle }
  | { readonly _tag: "Closed"; readonly error: WorkerSessionAcquisitionClosedError }
  | { readonly _tag: "Abandoned" };

function disposeLateSession(session: WorkerSessionHandle): Effect.Effect<void> {
  return session.dispose().pipe(Effect.catchCause(() => Effect.void));
}

function admitReclamationOrJoinAfterClosure(
  fibers: FiberSet.FiberSet<void, never>,
  reclamation: Effect.Effect<void>,
  onOpenObserved: () => Effect.Effect<void>,
): Effect.Effect<void> {
  return Effect.suspend(() => {
    if (fibers.state._tag === "Closed") return reclamation;
    return Effect.gen(function* () {
      yield* onOpenObserved().pipe(Effect.catchCause(() => Effect.void));
      yield* FiberSet.run(fibers, reclamation, { startImmediately: true });
      if (fibers.state._tag === "Closed") {
        // FiberSet.run returns an interrupted sentinel when closure wins admission.
        // If admission won, closure interrupts the admitted fiber instead. Real
        // session disposal is cached and uninterruptible, so this fallback safely
        // joins that same disposal in both cases rather than starting cleanup twice.
        yield* reclamation;
      }
    });
  });
}

export function createChildSessionsLayer(
  overrides: Partial<WorkerSessionDependencies> = {},
): Layer.Layer<ChildSessions> {
  return Layer.effect(
    ChildSessions,
    Effect.gen(function* () {
      const dependencies = {
        beforeAdoptionReservation:
          overrides.beforeAdoptionReservation ?? (() => Effect.void),
        onReclamationOpenObserved:
          overrides.onReclamationOpenObserved ?? (() => Effect.void),
      };
      const fibers = yield* FiberSet.make<void, never>();
      const serviceState = yield* SynchronizedRef.make<ChildSessionsState>({
        _tag: "Open",
        handoffs: new Set(),
      });

      const withoutHandoff = (
        state: ChildSessionsState,
        handoff: AcquisitionHandoff,
      ): ChildSessionsState => {
        if (state._tag === "Closed" || !state.handoffs.has(handoff)) return state;
        const handoffs = new Set(state.handoffs);
        handoffs.delete(handoff);
        return { _tag: "Open", handoffs };
      };

      const removeHandoff = (handoff: AcquisitionHandoff): Effect.Effect<void> =>
        SynchronizedRef.update(serviceState, (state) => withoutHandoff(state, handoff));

      const runReclamation = (session: WorkerSessionHandle): Effect.Effect<void> =>
        admitReclamationOrJoinAfterClosure(
          fibers,
          disposeLateSession(session).pipe(Effect.uninterruptible),
          dependencies.onReclamationOpenObserved,
        );

      const abandonHandoff = Effect.fn("ChildSessions.abandonHandoff")(function* (
        handoff: AcquisitionHandoff,
      ) {
        const session = yield* SynchronizedRef.modifyEffect(serviceState, (service) => {
          const nextService = withoutHandoff(service, handoff);
          return SynchronizedRef.modify(handoff.state, (state): readonly [
            { readonly session: WorkerSessionHandle | undefined },
            AcquisitionHandoffState,
          ] => {
            if (state._tag === "Pending") {
              return [{ session: undefined }, { _tag: "Abandoned" }];
            }
            if (state._tag === "Offered") {
              return [{ session: state.session }, { _tag: "Abandoned" }];
            }
            return [{ session: undefined }, state];
          }).pipe(Effect.map(({ session }) => [session, nextService] as const));
        });
        if (session) yield* runReclamation(session);
      });

      const shutdown = Effect.fn("ChildSessions.shutdown")(() =>
        Effect.gen(function* () {
          const closed = yield* SynchronizedRef.modifyEffect(serviceState, (state) => {
            if (state._tag === "Closed") return Effect.succeed([undefined, state] as const);
            const error = new WorkerSessionAcquisitionClosedError({
              message: "Child sessions are shutting down",
            });
            return Effect.gen(function* () {
              const sessions: WorkerSessionHandle[] = [];
              for (const handoff of state.handoffs) {
                const session = yield* SynchronizedRef.modifyEffect(handoff.state, (handoffState) => {
                  if (handoffState._tag === "Pending") {
                    return Deferred.fail(handoff.result, error).pipe(
                      Effect.as([undefined, { _tag: "Abandoned", error }] as const),
                    );
                  }
                  if (handoffState._tag === "Offered") {
                    return Effect.succeed([
                      handoffState.session,
                      { _tag: "Abandoned", error },
                    ] as const);
                  }
                  // Failed already settled its Deferred. Adopting is an ownership
                  // reservation whose synchronous winner must be allowed to commit.
                  return Effect.succeed([undefined, handoffState] as const);
                });
                if (session) sessions.push(session);
              }
              return [{ sessions }, { _tag: "Closed", error }] as const;
            });
          });
          if (!closed) return;
          for (const session of closed.sessions) yield* runReclamation(session);
        }).pipe(Effect.uninterruptible)
      );

      yield* Effect.addFinalizer(() => shutdown());

      const acquire = Effect.fn("ChildSessions.acquire")(function* <Adopted>(
        options: ChildSessionOptions,
        adopt: (session: WorkerSessionHandle) => Adopted | undefined,
      ) {
        const handoff: AcquisitionHandoff = {
          state: yield* SynchronizedRef.make<AcquisitionHandoffState>({ _tag: "Pending" }),
          result: yield* Deferred.make<WorkerSessionHandle, WorkerSessionAcquisitionError>(),
        };
        return yield* Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
          const admissionError = yield* SynchronizedRef.modify(serviceState, (state) => {
            if (state._tag === "Closed") return [state.error, state] as const;
            return [undefined, {
              _tag: "Open",
              handoffs: new Set([...state.handoffs, handoff]),
            }] as const;
          });
          if (admissionError) return yield* Effect.fail(admissionError);

          const producer = createWorkerSession(options, overrides).pipe(
            Effect.matchCauseEffect({
              onFailure: (cause) => Effect.gen(function* () {
                const failed = yield* SynchronizedRef.modifyEffect(handoff.state, (state) =>
                  state._tag === "Pending"
                    ? Deferred.failCause(handoff.result, cause).pipe(
                        Effect.as([true, { _tag: "Failed" }] as const),
                      )
                    : Effect.succeed([false, state] as const));
                if (failed) yield* removeHandoff(handoff);
              }),
              onSuccess: (session) => Effect.gen(function* () {
                const offered = yield* SynchronizedRef.modifyEffect(handoff.state, (state) =>
                  state._tag === "Pending"
                    ? Deferred.succeed(handoff.result, session).pipe(
                        Effect.as([true, { _tag: "Offered", session }] as const),
                      )
                    : Effect.succeed([false, state] as const));
                if (!offered) {
                  yield* removeHandoff(handoff);
                  yield* runReclamation(session);
                }
              }),
            }),
            Effect.uninterruptible,
          );
          yield* FiberSet.run(fibers, producer, { startImmediately: true });

          const session = yield* restore(
            Deferred.await(handoff.result).pipe(
              Effect.tap(() => dependencies.beforeAdoptionReservation()),
            ),
          ).pipe(Effect.onInterrupt(() => abandonHandoff(handoff)));
          const reservation = yield* SynchronizedRef.modifyEffect(serviceState, (service) => {
            if (service._tag === "Closed") {
              return Effect.succeed([
                { _tag: "Closed", error: service.error } satisfies AdoptionReservation,
                service,
              ] as const);
            }
            return SynchronizedRef.modify(
              handoff.state,
              (state): readonly [AdoptionReservation, AcquisitionHandoffState] => {
                if (state._tag === "Abandoned") {
                  return state.error
                    ? [{ _tag: "Closed", error: state.error }, state]
                    : [{ _tag: "Abandoned" }, state];
                }
                if (state._tag !== "Offered" || state.session !== session) {
                  return [{ _tag: "Abandoned" }, state];
                }
                return [
                  { _tag: "Reserved", session },
                  { _tag: "Adopting", session },
                ];
              },
            ).pipe(Effect.map((result) => [result, service] as const));
          });
          if (reservation._tag === "Closed") return yield* Effect.fail(reservation.error);
          if (reservation._tag === "Abandoned") {
            return yield* Effect.die(new Error("Child session acquisition handoff was abandoned"));
          }

          // The adopter is arbitrary synchronous runtime code. The reservation
          // protects it from shutdown, but no SynchronizedRef semaphore is held.
          const adopted = yield* Effect.exit(Effect.sync(() => adopt(reservation.session)));
          const transferred = Exit.isSuccess(adopted) && adopted.value !== undefined;
          yield* SynchronizedRef.update(handoff.state, (state): AcquisitionHandoffState => {
            if (state._tag !== "Adopting" || state.session !== reservation.session) return state;
            return transferred ? { _tag: "Adopted" } : { _tag: "Abandoned" };
          });
          yield* removeHandoff(handoff);
          if (transferred) return adopted.value;

          yield* runReclamation(reservation.session);
          if (Exit.isFailure(adopted)) return yield* Effect.failCause(adopted.cause);
          return undefined;
        }));
      });

      return ChildSessions.of({ acquire, shutdown });
    }),
  );
}
