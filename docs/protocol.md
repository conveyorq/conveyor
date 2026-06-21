# Conveyor wire protocol

|                 |                                                                      |
|-----------------|----------------------------------------------------------------------|
| Status          | Normative for the **`conveyor.v1`** protocol namespace; the wire is not yet frozen (pre-1.0); see §8 |
| Audience        | SDK authors implementing a Conveyor client or worker in any language |
| Source of truth | `protos/conveyor/v1/*.proto` + this document                         |

This is the contract every Conveyor SDK implements. The Go SDK (`sdks/go/`) is one
conforming implementation; nothing in it is privileged. Where this document and
the `.proto` files disagree, the `.proto` files win for message *shape* and this
document wins for *behavior* (ordering, defaults, when frames are sent).

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are used as in
RFC 2119.

---

## 1. Transport

- All three services are served on **one port** over ConnectRPC. A peer MAY
  speak any of the three wire formats ConnectRPC exposes: gRPC, gRPC-Web, or
  Connect's HTTP/JSON. The negotiated format does not change semantics.
- `WorkerService.Session` is a **bidirectional stream** and therefore requires a
  protocol that supports full-duplex streaming over HTTP/2: the gRPC protocol or
  the Connect streaming protocol. (gRPC-Web cannot carry a client/bidi stream and
  is not usable for the session.) The unary RPCs (`TaskService`, `AdminService`)
  work over HTTP/1.1 or HTTP/2 in any of the three formats.
- Plaintext endpoints use **HTTP/2 cleartext (h2c)**. TLS endpoints negotiate
  HTTP/2 via ALPN. An SDK MUST select the URL scheme (`http://` vs `https://`)
  accordingly.
- An SDK MUST send and accept binary protobuf (`application/grpc`,
  `application/proto`) at minimum; HTTP/JSON support is OPTIONAL but recommended
  for parity with the dashboard and CLI.

### 1.1 JSON encoding (HTTP/JSON format only)

When an SDK uses the HTTP/JSON format, message fields follow the standard
proto3 JSON mapping. SDK authors MUST account for:

| Proto type                  | JSON representation                                                                              |
|-----------------------------|--------------------------------------------------------------------------------------------------|
| `int32`, `uint32`           | JSON number                                                                                      |
| `int64`, `uint64`           | JSON **string** (e.g. `"42"`)                                                                    |
| `bytes`                     | base64 **string**                                                                                |
| `google.protobuf.Timestamp` | RFC 3339 string, e.g. `"2026-06-16T10:00:00Z"`                                                   |
| `google.protobuf.Duration`  | decimal seconds with an `s` suffix, e.g. `"1.500s"`, `"3600s"`                                   |
| `enum`                      | the enum value name string (e.g. `"TASK_OUTCOME_SUCCESS"`); the number is also accepted on input |
| `map<k,v>`                  | JSON object                                                                                      |

Note the two encoding layers that must not be confused: a task **payload** is a
`bytes` field, so over HTTP/JSON it is base64, and *inside* those bytes is the
payload codec named by `content_type` (§3). The transport encoding and the
payload encoding are independent.

---

## 2. Authentication

- Authentication is a **bearer token** carried in the HTTP `Authorization`
  header with the `Bearer ` prefix: `Authorization: Bearer <token>`.
- The token MUST be attached to every request, including the
  `WorkerService.Session` stream. For the stream, the header is validated **once
  at stream open**; there is no per-frame re-authentication.
- A missing or invalid token fails the call with Connect error code
  **`unauthenticated`** (gRPC code 16).
- A server running in development mode MAY disable authentication entirely, in
  which case the token is not required. SDKs SHOULD still send a token when one
  is configured; an unauthenticated server ignores it.

---

## 3. Payload codec & `content_type`

A task payload is **opaque bytes plus a `content_type`**. The server never
decodes a payload; it stores and forwards the bytes verbatim. Encoding and
decoding are entirely an SDK concern, governed by `content_type`. Two SDKs
interoperate on a task **only if they agree on the bytes** for its
`content_type`.

The built-in content types are:

| `content_type`             | Meaning                          | Byte contract                                                                                                                                                                   |
|----------------------------|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `application/json`         | JSON-encoded value (the default) | A UTF-8 JSON document. Producer and consumer MUST agree on the JSON shape (field names, casing, number representation). This is application-level, not imposed by the protocol. |
| `application/octet-stream` | Opaque binary                    | The bytes are used verbatim, unmodified.                                                                                                                                        |
| `application/x-protobuf`   | Protobuf-encoded message         | The wire-encoding of a protobuf message; consumer binds to the same message type.                                                                                               |

Rules:

- An SDK MUST treat `content_type` as the selector for how to decode
  `payload`, and MUST surface an error (rather than guess) when it has no codec
  for a received `content_type`.
- `application/json` is the **default** and the only codec a worker SDK MUST
  support to be useful with typical producers. The others are OPTIONAL.
- For `application/json`, the protocol does not mandate a field-naming
  convention. Cross-language producers and consumers of the *same* task type
  are responsible for agreeing on the JSON shape. SDKs SHOULD document their
  default (the Go SDK uses Go's `encoding/json` defaults).

---

## 4. Error model

Errors use ConnectRPC / gRPC status codes. The codes an SDK MUST handle:

| Code               | Meaning on this API                                                                                                        |
|--------------------|----------------------------------------------------------------------------------------------------------------------------|
| `invalid_argument` | Request or frame violated the contract (missing required field, value out of range, malformed Hello). Not retryable as-is. |
| `already_exists`   | Enqueue hit a duplicate `task_id` or an active `unique_key`.                                                               |
| `not_found`        | The referenced task or cron entry does not exist.                                                                          |
| `unauthenticated`  | Missing/invalid bearer token.                                                                                              |
| `unavailable`      | The server could not set up the session right now; retryable.                                                              |
| `internal`         | Server-side failure (storage, etc.).                                                                                       |

For a **worker session**, an SDK MUST classify the terminal stream error:

- `unauthenticated` / `permission_denied` → **fatal**: stop, do not reconnect.
- `invalid_argument` that is a *wire/protocol* error → **fatal**: the SDK is
  speaking the protocol wrong; reconnecting will not help.
- Any other error, or a clean stream end → **transient**: reconnect per §5.7.

---

## 5. WorkerService session protocol

A worker holds exactly one `Session` stream:
`rpc Session(stream WorkerMessage) returns (stream ServerMessage)`.

```
WorkerMessage.frame = { Hello | Credit | Result | Heartbeat | BatchResult }
ServerMessage.frame = { Welcome | Dispatch | Cancel | Ping | BatchDispatch }
```

### 5.1 Lifecycle

```
worker                         server
  │ ── Hello ──────────────────▶ │   (MUST be the first frame)
  │ ◀────────────────── Welcome ─│   (session_id, lease_ttl, heartbeat_interval)
  │                              │
  │ ◀───────────────── Dispatch ─│   (server pushes leased tasks, up to concurrency)
  │ ── Result ─────────────────▶ │   (one per dispatched task)
  │ ── Heartbeat ──────────────▶ │   (periodic, extends leases)
  │ ◀─────────────────── Cancel ─│   (best-effort, optional)
  │           ...                │
  │ ── (close request) ────────▶ │   (graceful drain / shutdown)
```

### 5.2 Hello (worker → server, required first frame)

The **first** frame on the stream MUST be `Hello`. Any other first frame fails
the stream with `invalid_argument`.

| Field                           | Requirement                                                                                                                                                                                    |
|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `queues` (`map<string,int32>`)  | MUST contain at least one entry. Each key MUST match `^[a-zA-Z0-9][a-zA-Z0-9-_.]*$`. Each weight MUST be > 0. The weight governs relative dispatch share across the queues this worker serves. |
| `concurrency` (`int32`)         | MUST be > 0. This is the worker's **total** simultaneous-execution capacity across all its queues (not per-queue). See §5.5; this value is the flow-control grant.                            |
| `labels` (`map<string,string>`) | OPTIONAL, informational. May be empty.                                                                                                                                                         |
| `sdk_version` (`string`)        | SHOULD be set (see §8). May be any string; an empty or non-semver value is accepted.                                                                                                           |
| `min_server_version` (`string`) | OPTIONAL. The minimum server version this worker requires, as semver (e.g. `"v1.2.0"`). Empty imposes no requirement; a non-semver value is ignored. The server fails the session with `invalid_argument` when its own version is older. See §8. |
| `batch_types` (`repeated string`) | OPTIONAL. Task types this worker handles as **batches** (aggregation groups). The server delivers a fired group's members as one `BatchDispatch` only to a worker that advertised the group's type here; a worker advertising none never receives a batch. See §5.11. |

A Hello that violates any MUST fails the stream with `invalid_argument`.

### 5.3 Welcome (server → worker)

Sent once, immediately after a valid Hello. The worker MUST wait for Welcome
before treating the session as established.

| Field                             | Meaning                                                                                  |
|-----------------------------------|------------------------------------------------------------------------------------------|
| `session_id` (`string`)           | Server-assigned session identifier (a ULID).                                             |
| `lease_ttl` (`Duration`)          | How long a dispatched task's lease lives before the server reclaims it. Default **60s**. |
| `heartbeat_interval` (`Duration`) | How often the worker SHOULD send `Heartbeat`. Default **lease_ttl / 3 = 20s**.           |
| `server_version` (`string`)       | The server's build version (semver, or a non-semver dev marker). Lets a worker detect version skew. |
| `min_sdk_version` (`string`)      | The oldest worker SDK version this server admits (semver). Surfaced so a worker can report the requirement clearly. |

The worker MUST drive its heartbeat cadence from `heartbeat_interval` rather
than hard-coding a value: a server MAY use a different lease TTL.

### 5.4 Dispatch (server → worker)

The server pushes one `Dispatch` per leased task. The worker does not request
tasks individually; it advertises capacity (§5.5) and the server streams work.

| Field                    | Meaning                                                                                                                                                             |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `task` (`TaskEnvelope`)  | The task to execute, including `payload`, `content_type`, `metadata`, `options`, `retried`, `id`, `queue`, `type`.                                                  |
| `deadline` (`Timestamp`) | The **effective** execution deadline: `min(lease expiry, task deadline if set, now + task timeout if set)`. The worker SHOULD cancel a handler that runs past this. |

### 5.5 Flow control: the credit model (READ THIS)

Concurrency control is **declared once in `Hello.concurrency` and managed by the
server**. The model is:

1. On Hello, the server grants the worker dispatch credits **equal to
   `concurrency`**, per the worker's declared queues.
2. The server dispatches a task by consuming one credit. It will never have more
   than `concurrency` tasks outstanding to a worker at once.
3. When the worker reports a `Result` for a task, the server **refills one
   credit**, which may trigger the next dispatch.

Therefore a conforming worker:

- MUST be able to execute up to `concurrency` tasks simultaneously, and MUST NOT
  assume the server throttles below that.
- MUST send exactly one `Result` per `Dispatch` (§5.8). The Result is also the
  flow-control signal; dropping it stalls dispatch for that slot until the lease
  expires.
- Does **not** need to send `Credit` frames. Credit is an OPTIONAL mechanism to
  grant *additional* dispatch credits dynamically; the server caps total credits
  at the declared `concurrency`, so a worker that simply declares its
  concurrency in Hello and answers every Dispatch with a Result is fully
  conformant. The reference Go SDK does not send `Credit` at all and instead
  gates locally on `concurrency`.

`Credit` frame (worker → server), if used: `n` MUST be > 0 and MUST NOT exceed
the declared `concurrency`; otherwise the stream fails with `invalid_argument`.

> Implementation note: this means an SDK's simplest correct strategy is a
> semaphore of size `concurrency`: acquire before accepting a Dispatch's work,
> release after sending its Result. No credit accounting is required on the SDK
> side.

### 5.6 Heartbeat & leases (worker → server)

- Every dispatched task carries a **lease** that expires `lease_ttl` after it was
  granted. If the lease expires before the worker reports a Result, the server
  considers the worker dead for that task, **increments its retry count**, and
  redelivers it (possibly to another worker). This is the at-least-once
  backbone, and a task MAY therefore run more than once.
- To keep long-running tasks alive, the worker MUST periodically send
  `Heartbeat` every `heartbeat_interval`, listing the ids of all tasks still
  executing in `active_task_ids`.
- Each id present in a Heartbeat has its lease extended to `now + lease_ttl`. An
  in-flight task **omitted** from a Heartbeat is *not* extended and will be
  reclaimed when its lease lapses. The worker MUST include every still-running
  task in every heartbeat.

### 5.7 Cancel & Ping (server → worker)

- `Cancel { task_id }` asks the worker to stop a running task. It is **best
  effort**: the worker SHOULD cancel the corresponding execution (e.g. cancel
  its context/coroutine) but the protocol does not require confirmation. The
  worker still reports a `Result` for the task when it finishes unwinding. The
  server emits Cancel both for operator-initiated cancellation and when a task's
  lease was lost.
- `Ping {}` is a reserved server→worker liveness probe. A worker MUST tolerate
  receiving a Ping and MUST NOT be required to reply. (The current server does
  not emit Ping; SDKs must still not break on one.)

### 5.8 Result & outcomes (worker → server)

The worker MUST send exactly one `Result` per dispatched task.

| Field       | Meaning                                                                  |
|-------------|--------------------------------------------------------------------------|
| `task_id`   | The dispatched task's id.                                                |
| `outcome`   | One of the `TaskOutcome` values below.                                   |
| `error_msg` | Human-readable failure reason; SHOULD be set for RETRY and SKIP_RETRY.   |
| `result`    | OPTIONAL result bytes, retained on SUCCESS for inspection. MAY be empty. |

| `TaskOutcome`     | Worker means                                      | Server does                                                                                                                                                                                         |
|-------------------|---------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SUCCESS` (1)     | Handler completed                                 | Marks completed; stores `result` bytes; retained per `retention`.                                                                                                                                   |
| `RETRY` (2)       | Transient failure                                 | If retries remain: re-queues with backoff and `retried+1`, storing `error_msg`. If exhausted (`retried >= max_retry`): archives (dead-letters). If the task was cancelled by an operator: archives. |
| `SKIP_RETRY` (3)  | Permanent failure; do not retry                   | Archives immediately with `error_msg`.                                                                                                                                                              |
| `RELEASED` (4)    | Giving the task back un-run (e.g. graceful drain) | Re-queues **without** incrementing `retried`; due immediately.                                                                                                                                      |
| `UNSPECIFIED` (0) | (never send)                                      | Treated defensively as RELEASED.                                                                                                                                                                    |

Mapping guidance for SDK authors (how the Go SDK derives the outcome):

- Handler returns success → `SUCCESS`.
- Handler returns a "skip retry" / permanent-failure sentinel → `SKIP_RETRY`.
- Handler returns any other error, **or panics/raises an uncaught exception**
  (which the SDK MUST recover and convert to a retryable error) → `RETRY`.
- Worker is draining and chooses to hand a task back un-run → `RELEASED`.

### 5.9 Reconnection

On a transient stream end (§4), the worker SHOULD reconnect with
**exponential backoff and full jitter**:

- delay before attempt *n* (0-based) = `uniform_random[0, min(max_delay, base * 2^n))`
- reference values: `base = 500ms`, `max_delay = 30s`.
- The failure counter MUST reset once a new session is established (Welcome
  received), so a flapping connection does not ratchet the delay to the ceiling.
- On a fatal error (§4), the worker MUST NOT reconnect.

### 5.10 Graceful drain / shutdown

On shutdown (e.g. SIGTERM), a worker SHOULD:

1. Stop accepting new Dispatches.
2. For each in-flight task, either finish it and report its real outcome, or,
   if it cannot finish in time, report `RELEASED`, which hands the task back
   with **no retry penalty and no backoff** (it becomes due immediately on
   another worker). A drain MUST NOT report `RETRY` for a task it is abandoning
   purely because the worker is stopping: that would consume the task's retry
   budget and delay it for a routine deploy.
3. Close the request side of the stream.

The distinction is deliberate and is what makes deploys cheap: a worker
abandoning a task *because it is shutting down* is **not** a failure, so it
SHOULD use `RELEASED`, whereas a task that failed, timed out, or was canceled by
the server uses `RETRY` (§5.8). The reference Go SDK implements this by tagging
the drain-induced cancellation distinctly from a deadline or a server `Cancel`,
so only true drain abandonment becomes `RELEASED`.

Safety net: whenever the stream closes for any reason, the server **releases all
of that session's still-leased tasks** for immediate redelivery (no retry
increment), the same penalty-free outcome as an explicit `RELEASED`. So even an
abrupt disconnect (crash of the stream, network loss) does not lose work and
does not burn a retry; it only risks a task running twice (at-least-once). A
genuinely *crashed* worker, by contrast, is detected by lease expiry, and that
redelivery **does** count as a retry (it is indistinguishable from a task that
hangs the worker, the poison-pill bound).

### 5.11 Batch delivery (aggregation groups)

A producer MAY tag a task with a **group** (`TaskOptions.group`, §6.1). Tasks
sharing a `(queue, group)` accumulate server-side (state `aggregating`, not
dispatched) until the group **fires**, whether by size, by max-delay since the first
member, or by grace period since the last (server-configured). A fired group is
delivered to one worker as a single batch:

- **`BatchDispatch` (server → worker):** `{ repeated TaskEnvelope tasks; Timestamp
  deadline; string group }`. All members share one lease; `deadline` is the
  tightest bound across members. The server only sends a `BatchDispatch` to a
  worker that advertised the group's type in `Hello.batch_types`.
- **`BatchResult` (worker → server):** `{ repeated Result results }`. The worker
  runs all members in **one** handler call and reports one `Result` per member.
  A member **omitted** from `results` is treated as `RELEASED` (redelivered, no
  penalty), the same safety net as a dropped single `Result`.

Rules an SDK MUST follow:

- A batch is **one concurrency slot**: it consumes a single dispatch credit
  regardless of member count, refunded once when the `BatchResult` is sent.
- **Heartbeats** MUST list every in-flight batch member's id (each member's lease
  is extended individually).
- A group is **single-type** (all members share a task type), so one handler
  serves the batch.
- A member returning `RETRY`/`RELEASED` redelivers **individually** (a plain
  `Dispatch`), not re-aggregated; an SDK SHOULD therefore let a batch handler
  also serve a single delivery (e.g. as a batch of one).

A worker that advertises no `batch_types` is unaffected: it never receives a
`BatchDispatch`, so grouping is fully back-compatible.

---

## 6. TaskService (enqueue side)

Unary RPCs. All inputs validated server-side; violations return
`invalid_argument` unless noted.

### 6.1 Enqueue / EnqueueBatch

`EnqueueRequest` fields and server defaults:

| Field                       | Rule / default                                                                                                                           |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `task_id`                   | OPTIONAL client-assigned ULID. Enqueue is **idempotent** on it: a repeat id returns `already_exists`. Omit to let the server assign one. |
| `queue`                     | Empty → `"default"`. MUST match `^[a-zA-Z0-9][a-zA-Z0-9-_.]*$`.                                                                          |
| `type`                      | **Required** (handler routing key). Empty → `invalid_argument`.                                                                          |
| `payload` / `content_type`  | Opaque bytes + codec (§3). Payload MUST be ≤ **1 MiB** (`1<<20`).                                                                        |
| `metadata`                  | OPTIONAL `map<string,string>`; carries user tags and trace propagation (e.g. `traceparent`).                                             |
| `max_retry`                 | `0` → server default (**25**). MUST NOT be negative.                                                                                     |
| `timeout`                   | OPTIONAL per-attempt bound (`Duration`).                                                                                                 |
| `deadline`                  | OPTIONAL absolute `Timestamp` after which the task MUST NOT run.                                                                         |
| `process_at` / `process_in` | OPTIONAL delay. **Mutually exclusive** (both set → error). `process_in` is resolved to `now + process_in` at enqueue.                    |
| `unique_key` / `unique_ttl` | OPTIONAL uniqueness claim among incomplete tasks; a conflicting enqueue returns `already_exists`.                                        |
| `priority`                  | `0` → default (**4**). Explicit range **1..9** (1 lowest, 9 highest). Out of `0..9` → error.                                             |
| `retention`                 | OPTIONAL how long to keep the completed row before purge.                                                                                |
| `group`                     | OPTIONAL aggregation group key (§5.11). The task accumulates as `aggregating` and is batch-delivered when its group fires. **Mutually exclusive** with `process_at`/`process_in`. |
| `expires_in` / `expires_at` | OPTIONAL pre-dispatch TTL: a task still waiting (scheduled/pending/retry) when it passes is **archived** instead of run. **Mutually exclusive** (both set → error); `expires_in` resolves to `now + expires_in`. Distinct from `deadline` (cancels a *running* task) and `retention` (purges a *completed* one). |

- `EnqueueResponse.task` is a `TaskInfo` reflecting the committed task and its
  initial state (`SCHEDULED` if delayed, else `PENDING`).
- `EnqueueBatch` accepts **1..1000** items. Items fail **independently**:
  `EnqueueBatchResponse.results[i]` carries either the committed `task` or a
  non-empty `error` string for item *i*, positionally. The RPC itself succeeds
  unless the batch is empty or oversized.

### 6.2 GetTask

`GetTaskRequest { id }` → `TaskInfo`. Empty id → `invalid_argument`; unknown id
→ `not_found`.

`TaskInfo` is the externally visible task view (id, queue, type, `TaskState`,
priority, retried, max_retry, last_error, timestamps, payload, content_type,
started_at). `TaskState` values are stable and MUST NOT be renumbered:
`SCHEDULED, PENDING, ACTIVE, RETRY, COMPLETED, ARCHIVED, CANCELED, AGGREGATING`.

---

## 7. AdminService (inspection & operations)

Unary RPCs for dashboards and operators: queue listing/pause/resume, task
listing (paged via `page_token`/`next_page_token`), single and batch task
actions (cancel/delete/run/archive), cron CRUD and pause/resume, cluster info,
worker-session listing, and broker info. These are operational surface, not
required for a minimal producer/worker SDK; an SDK MAY implement only the
subset it needs. Field-level details live in `protos/conveyor/v1/service.proto`.

Batch actions report per-id outcomes positionally in
`BatchTasksResponse.results[i] = { id, error }` (`error` empty on success).

---

## 8. Versioning & compatibility

- The protocol namespace is **`conveyor.v1`**. The project is pre-1.0 (current
  release `v0.1.0`), so the wire is **not yet frozen**: a breaking change
  remains possible before the 1.0.0 release. From **1.0.0** on, changes within
  `conveyor.v1` are **additive only**: new fields and messages, never renumbered
  or removed; enum values appended, never repurposed.
- The session opens with a **two-way version handshake**:
  - A worker advertises its SDK build in `Hello.sdk_version`. The server enforces
    a **minimum SDK version**: a value that parses as semver and is older than the
    minimum is rejected with `invalid_argument`; any non-semver value (dev builds,
    `"unknown"`, custom clients) is admitted. The current minimum is vacuous
    (`v0.0.0-0`, admits everything) and will be raised only if a future wire
    change leaves older SDKs behind.
  - A worker MAY demand a **minimum server version** in `Hello.min_server_version`.
    The server rejects the session with `invalid_argument` when its own version is
    older. The check fires only when both the requirement and the server version
    are comparable semver, so dev builds never trip it.
  - The server echoes its own build in `Welcome.server_version` and its admitted
    floor in `Welcome.min_sdk_version`, so a worker can detect skew and report a
    clear requirement without guessing.
- Unknown fields MUST be ignored by both sides (standard protobuf forward
  compatibility), so a newer server can add fields without breaking older SDKs
  and vice versa.

The release history and compatibility notes are tracked in the
[changelog](../CHANGELOG.md).

---

## 9. Conformance checklist for a new SDK

A new worker SDK is conformant when it:

- [ ] Opens the session over h2c/HTTP-2 (or TLS) with `Authorization: Bearer`.
- [ ] Sends `Hello` first, with ≥1 valid queue, positive weights, positive
      `concurrency`, and an `sdk_version`.
- [ ] Waits for `Welcome` and drives heartbeats from `heartbeat_interval`.
- [ ] Executes up to `concurrency` dispatches concurrently; needs no `Credit`.
- [ ] Sends exactly one `Result` per `Dispatch`, with the correct outcome
      mapping (success/skip-retry/retry, panics→retry).
- [ ] Heartbeats every in-flight task id each interval.
- [ ] Honors `Dispatch.deadline` and best-effort `Cancel`.
- [ ] Tolerates `Ping` without replying.
- [ ] Reconnects with full-jitter backoff on transient errors; stops on fatal.
- [ ] Closes the stream on shutdown (RELEASED/finish optional; server releases
      leases on close).
- [ ] Produces/consumes `application/json` payloads byte-compatibly with peers.

A client (producer) SDK is conformant when it implements `Enqueue`
(+ optionally `EnqueueBatch`, `GetTask`) with the defaults, limits, and
idempotency/uniqueness semantics of §6.

The cross-SDK **conformance suite** is the executable form of this checklist
and is the real gate.
