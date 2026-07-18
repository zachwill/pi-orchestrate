# HTTP Clients

Use this when writing outgoing HTTP calls, Effect HttpClient adapters, status classification, HTTP retries, or rate limiting.

The Effect HTTP modules used here are unstable and beta-sensitive:

- `effect/unstable/http/HttpClient`
- `effect/unstable/http/HttpClientRequest`
- `effect/unstable/http/HttpClientResponse`
- `effect/unstable/http/HttpClientError`

Verify every API against the repository-pinned Effect source before use.

Choose Effect HttpClient when its typed errors, Layers, schema helpers, request transforms, retries, or rate limiter materially simplify an Effect provider. Raw `fetch` inside a disciplined adapter remains reasonable for small boundaries, browser or edge constraints, platform transports, and code that should not depend on unstable Effect HTTP APIs.

Do not migrate an existing fetch adapter solely for stylistic consistency. Migrate when the Effect client removes real boundary, retry, rate-limit, or Layer machinery.

## Boundary Shape

HTTP adapter methods should be named effects that own the full boundary:

- construct request
- attach auth and headers
- execute request
- classify status
- decode response body
- map transport/status/decode failures to typed domain errors
- apply retry/rate-limit policy where idempotent

Keep raw provider/network effects outside business services and database transactions.

## Effect HttpClient

Useful APIs:

- `HttpClient.get(...)`, `post(...)`, `put(...)`, `patch(...)`, `del(...)`, `execute(...)` for service accessors.
- `HttpClient.mapRequest(...)` / `mapRequestEffect(...)` for configured client transforms.
- `HttpClientRequest.prependUrl(...)` for base URLs.
- `HttpClientRequest.bearerToken(...)` for bearer auth.
- `HttpClientRequest.acceptJson` for JSON accept headers.
- `HttpClientRequest.bodyJson(...)` for effectful JSON body encoding.
- `HttpClientRequest.schemaBodyJson(...)` for schema-backed JSON body encoding.
- `HttpClient.filterStatusOk` / `HttpClientResponse.filterStatusOk` before decoding when non-2xx responses are failures.
- `HttpClientResponse.schemaBodyJson(...)` for body-only decoding, `schemaJson(...)` for status/headers/body decoding, and `schemaNoBody(...)` for status/headers decoding.
- `HttpClient.retryTransient(...)` for common transient HTTP failures.
- `HttpClient.withRateLimiter(...)` for proactive pacing and learning from rate-limit headers. It requires a `RateLimiter` plus initial window, limit, and key options; it adds `RateLimiterError` to the error channel and retries `429` responses by default.

## Retry And Rate Limits

Use `HttpClient.retryTransient(...)` for common transient HTTP failures:

- transport errors
- timeouts
- `408`
- `429`
- `500`
- `502`
- `503`
- `504`

Use `HttpClient.withRateLimiter(...)` when the client should proactively pace requests and learn from rate-limit / `Retry-After` headers.

Use operation-level `Effect.retry(...)` when retry depends on domain-specific typed errors, provider payloads, or idempotency rules. Read `SCHEDULING.md` for custom schedules and `retryAfterMs` typed-provider patterns.

## Raw Fetch Exception

Use raw `fetch` deliberately when implementing a platform transport, adapting an API that cannot use Effect HttpClient, or targeting a runtime/library boundary where the unstable Effect HTTP modules are not an appropriate dependency.

If a temporary raw `fetch` boundary is unavoidable, keep it inside an adapter service and still use Effect boundary discipline.

```ts
const request = Effect.fn("Provider.request")(function* (input: RequestInput) {
  const response = yield* Effect.tryPromise({
    try: (signal) => fetch(input.url, { signal, headers: input.headers }),
    catch: (cause) => new ProviderError({ operation: "Provider.request", cause }),
  })

  if (!response.ok) {
    return yield* Effect.fail(new ProviderRejected({
      operation: "Provider.request",
      status: response.status,
    }))
  }

  const json = yield* Effect.tryPromise({
    try: () => response.json(),
    catch: (cause) => new ProviderError({ operation: "Provider.decodeJson", cause }),
  })

  return yield* Schema.decodeUnknownEffect(ResponseSchema)(json).pipe(
    Effect.mapError((cause) =>
      new ProviderError({ operation: "Provider.decodeResponse", cause }),
    ),
  )
})
```

Guidance:

- Prefer replacing this with Effect HttpClient before adding more behavior.
- Wire `AbortSignal` from `Effect.tryPromise` into `fetch` when raw fetch is unavoidable.
- Classify HTTP status before decoding successful payloads.
- Decode unknown response bodies with Schema at the boundary.
- Preserve provider evidence needed for diagnosis, but redact secrets and private payloads.
- Apply retry only for idempotent operations.
