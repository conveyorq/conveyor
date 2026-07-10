# Webhook workers

Process Conveyor tasks with no SDK. Register an HTTP endpoint and Conveyor pushes each task to it as a signed [JSON-RPC 2.0](https://www.jsonrpc.org/specification) call; the endpoint runs the work and answers with the outcome. Any language that can serve an HTTP handler can be a worker.

Conveyor stays push-based. A webhook worker is not a queue you poll: you register a URL, and the server leases tasks to it and POSTs them, honoring the same concurrency, priorities, retries, and rate limits a streaming SDK worker gets. When the endpoint is full or unreachable, tasks wait in the queue instead of being pushed.

This is the operator-side counterpart of the [HTTP API](http-api.md), which covers producing tasks. Producing needs no SDK either; with both, a team can run Conveyor end to end over plain HTTP.

## Contents

- [When to use webhook workers](#when-to-use-webhook-workers)
- [Quick start](#quick-start)
- [Registering an endpoint](#registering-an-endpoint)
- [The delivery call](#the-delivery-call)
- [Completing a task](#completing-a-task)
- [Long-running tasks](#long-running-tasks)
- [Cancellation](#cancellation)
- [Group batches](#group-batches)
- [Verifying the signature](#verifying-the-signature)
- [Retries and circuit breaking](#retries-and-circuit-breaking)
- [Secret rotation](#secret-rotation)
- [Capabilities](#capabilities)
- [Sharp edges](#sharp-edges)



## When to use webhook workers

Use a webhook worker when a streaming SDK does not fit: a language without a Conveyor SDK, a serverless function, an existing HTTP service you want to feed tasks, or a team that prefers a request/response handler over a long-lived worker process.

Use an [SDK worker](usage.md) when you want typed handlers, payload codecs, in-process middleware, or end-to-end encryption. The webhook contract has the same queue capabilities (see [Capabilities](#capabilities)), but the SDK is nicer to hold.

## Quick start

Start a development server (in-memory broker, auth disabled, `http://` URLs allowed):

```sh
conveyord --dev
```

Run the [example endpoint](../examples/webhook), keyed with a signing secret:

```sh
WEBHOOK_SECRET=s3cret go run ./examples/webhook
```

Register the endpoint and enqueue a task:

```sh
conveyor webhooks add demo-hooks http://localhost:9090/tasks \
  --queue email=1 --secret s3cret

curl -s http://localhost:8080/conveyor.v1.TaskService/Enqueue \
  -H 'Content-Type: application/json' \
  -d '{"queue":"email","type":"email:send","payload":"eyJ0byI6ImFAYi5jIn0=","contentType":"application/json"}'
```

The endpoint receives the delivery and completes the task.

## Registering an endpoint

Registration is an operator action, persisted server-side like a cron schedule, not an application API. Manage it with the CLI, the dashboard's **Webhooks** panel, or static server config.

```sh
conveyor webhooks add billing-hooks https://hooks.example.com/tasks \
  --queue billing=3 --queue default=1 \
  --secret "$WEBHOOK_SECRET" \
  --concurrency 8

conveyor webhooks list
conveyor webhooks pause billing-hooks
conveyor webhooks resume billing-hooks
conveyor webhooks delete billing-hooks
```


| Field           | Flag                | Meaning                                                           |
| --------------- | ------------------- | ----------------------------------------------------------------- |
| Name            | (first argument)    | Unique handle for the registration, e.g. `billing-hooks`.         |
| URL             | (second argument)   | Delivery URL. `https` is required unless the server runs `--dev`. |
| Queues          | `--queue name=w`    | Served queues and weights, like an SDK worker's. Repeatable.      |
| Concurrency     | `--concurrency`     | Max in-flight tasks (sync requests plus accepted async). Min 1.   |
| Secrets         | `--secret`          | Signing secret, newest first. Repeatable (two during rotation).   |
| Batch types     | `--batch-type`      | Your own task-type names (not a fixed set) delivered as one batch when their group fires. Repeatable. |
| Request timeout | `--request-timeout` | Synchronous response wait; server default (30s) when unset.       |
| Paused          | `--paused`          | Register without delivering.                                      |


A registration with no secret is unsigned; provide at least one secret in any environment where the endpoint is reachable by anyone but you.

Server config may declare registrations statically; declared entries are upserted by name at boot, so config and CLI/dashboard changes compose.

## The delivery call

Each task attempt is one [JSON-RPC 2.0](https://www.jsonrpc.org/specification) request `POST`ed to the registered URL. Every request carries these headers:


| Header                 | Value                                                                                  |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `Content-Type`         | `application/json`                                                                     |
| `X-Conveyor-Timestamp` | Unix seconds when the request was sent.                                                |
| `X-Conveyor-Signature` | `v1=` followed by the hex HMAC-SHA256 of `"{timestamp}.{body}"`, keyed by your secret. |


The two `X-Conveyor-*` headers authenticate the delivery; verify them before trusting the body (see [Verifying the signature](#verifying-the-signature)). A registration with no secret sends them empty/omitted.

### Request envelope

The body is a JSON-RPC 2.0 request: `method` is always `conveyor.task.execute`, and `params` is the task.

```json
{
  "jsonrpc": "2.0",
  "id": "01KX3E7T2M4Q9RZC8B1JW5HD6P",
  "method": "conveyor.task.execute",
  "params": {
    "taskId": "01KX3CAWA7WMGN3R5EA7JJCFA0",
    "queue": "email",
    "type": "email:send",
    "attempt": 1,
    "maxRetry": 25,
    "deadline": "2026-07-09T13:00:00Z",
    "contentType": "application/json",
    "payload": "eyJ0byI6ImFAYi5jIn0=",
    "metadata": {"tenant": "acme"},
    "lease": {
      "token": "opaque; authenticates callbacks for this delivery only",
      "heartbeatInterval": "30s"
    }
  }
}
```


| Field                | Type              | Meaning                                                                                                                                                                                                       |
| -------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                 | string            | The delivery's lease id. **Echo it back** in the response `id`.                                                                                                                                               |
| `params.taskId`      | string            | Stable task ULID. Use it as the idempotency key (delivery is at-least-once).                                                                                                                                  |
| `params.queue`       | string            | Queue the task was leased from.                                                                                                                                                                               |
| `params.type`        | string            | Task type; your handler-routing key.                                                                                                                                                                          |
| `params.attempt`     | number            | `1` on first delivery, `+1` per retry.                                                                                                                                                                        |
| `params.maxRetry`    | number            | The task's retry budget.                                                                                                                                                                                      |
| `params.deadline`    | string (RFC 3339) | Execution deadline; omitted when unbounded.                                                                                                                                                                   |
| `params.contentType` | string            | Codec of the payload bytes (e.g. `application/json`).                                                                                                                                                         |
| `params.payload`     | string (base64)   | The task payload; the envelope is JSON, so the raw bytes are base64-encoded, delivered exactly as stored.                                                                                                     |
| `params.metadata`    | object            | User-set string tags; omitted when none.                                                                                                                                                                      |
| `params.lease`       | object            | Present only when the task may complete asynchronously (see [Long-running tasks](#long-running-tasks)): `token` authenticates its callbacks, `heartbeatInterval` (e.g. `"30s"`) is the required beat cadence. |




### Response envelope

Answer with a JSON-RPC 2.0 response on **HTTP 200**, echoing the request `id`, with exactly one of `result` or `error` set.

Success:

```json
{"jsonrpc": "2.0", "id": "01KX3E7T2M4Q9RZC8B1JW5HD6P", "result": {"status": "completed"}}
```

Failure:

```json
{"jsonrpc": "2.0", "id": "01KX3E7T2M4Q9RZC8B1JW5HD6P", "error": {"code": -32000, "message": "smtp timeout"}}
```

`result.status` is `completed` or `accepted`; `error.code` selects the retry behavior. The next section covers the full outcome vocabulary and what counts as a transport failure.

## Completing a task

Finish the work inside the request and answer with the outcome. Success:

```json
{"jsonrpc": "2.0", "id": "01KX3E7T...", "result": {"status": "completed"}}
```

A failure is a JSON-RPC error whose code selects the retry behavior. This gives webhook workers the same outcome vocabulary an SDK handler has:


| Error code     | Meaning           | Server action                          |
| -------------- | ----------------- | -------------------------------------- |
| `-32000`       | Retryable failure | Retry with backoff.                    |
| `-32001`       | Permanent failure | Archive without retrying (skip retry). |
| Any other code | Endpoint fault    | Retry with backoff, like a crash.      |


```json
{"jsonrpc": "2.0", "id": "01KX3E7T...", "error": {"code": -32000, "message": "smtp timeout"}}
```

The error `message` is captured (truncated) into the task's last error.

Every JSON-RPC response, error included, rides an **HTTP 200**. Anything else is a transport failure, not an outcome: a non-200 status, a malformed envelope, a connection error, or a timeout all retry the task and feed the endpoint's [circuit breaker](#retries-and-circuit-breaking). Redirects are never followed. A synchronous response must arrive within `min(task timeout, request_timeout)`.

## Long-running tasks

When the work outlives the request, accept the task and return immediately:

```json
{"jsonrpc": "2.0", "id": "01KX3E7T...", "result": {"status": "accepted"}}
```

From there the endpoint completes the task out of band, over the server's plain HTTP/JSON surface, authenticated by the delivery's **lease token**, with no API bearer token needed. Use the `lease.token` from the delivery `params`:

- **Heartbeat** at least every `heartbeatInterval`, or the lease expires and the task is reclaimed and retried elsewhere (exactly like a crashed worker):
  ```sh
  curl -s http://localhost:8080/conveyor.v1.WebhookService/Heartbeat \
    -H 'Content-Type: application/json' \
    -d '{"leaseToken":"<lease token>"}'
  ```
- **Report the outcome** when done:
  ```sh
  curl -s http://localhost:8080/conveyor.v1.WebhookService/ReportResult \
    -H 'Content-Type: application/json' \
    -d '{"leaseToken":"<lease token>","outcome":"TASK_OUTCOME_SUCCESS"}'
  ```

`outcome` is `TASK_OUTCOME_SUCCESS`, `TASK_OUTCOME_RETRY`, or `TASK_OUTCOME_SKIP_RETRY`; a failure may add `"errorMsg":"..."`.

An accepted task holds its concurrency slot until the result is reported, so it counts against the registration's `concurrency` like any running task.

## Cancellation

Cancellation is best-effort, the same contract SDK workers have; the endpoint may already have done the work.

- **Synchronous:** the server aborts the open HTTP request.
- **Asynchronous:** the server POSTs a JSON-RPC notification (no `id`, no response expected):
  ```json
  {"jsonrpc": "2.0", "method": "conveyor.task.cancel", "params": {"taskId": "01KX3C..."}}
  ```



## Group batches

An [aggregation group](grouping.md) fires as one delivery: a JSON-RPC batch (a JSON array), one `conveyor.task.execute` call per member, one POST. Answer with the response array; each member's `id` is its `taskId`. Members complete individually (any mix of `completed`, `accepted`, and errors), and the group's credit refills when every member resolves.

A registration only receives a group batch for a task type listed in its `--batch-type` set. The values are your own task-type identifiers (the same free-form strings you set when enqueuing tasks), not a predefined vocabulary; `report:batch` used throughout these examples is only illustrative. A task type not listed here is still delivered, just one call per task rather than batched.

## Verifying the signature

Every delivery and cancel notification is signed. Verify it before trusting the body:

- `X-Conveyor-Timestamp`: unix seconds when the request was sent.
- `X-Conveyor-Signature`: `v1=` followed by the hex HMAC-SHA256 of `"{timestamp}.{body}"`, keyed by the registration secret.

To verify: recompute the HMAC over the exact raw body with your secret, compare it to the header in constant time, and reject a timestamp outside a small window (5 minutes is a good default) to bound replay. The [example endpoint](../examples/webhook) shows the whole check in ~15 lines of Go. During [rotation](#secret-rotation) a registration holds two secrets; verify against either.

## Retries and circuit breaking

Both are handled server-side; the endpoint writes no code for either.

- **Retries** are fully inherited. `max_retry`, per-task retry-policy backoff, and the retry/archive state machine apply to webhook deliveries exactly as to SDK dispatches. A retryable error, a transport failure, and an expired async lease all take the same path.
- **Per-endpoint circuit breaker.** Repeated transport failures (connection errors, non-200, timeouts, malformed envelopes) open a breaker that withholds capacity: the server stops leasing to the endpoint, so tasks wait as `pending` instead of churning through failed attempts. It probes on a backoff and restores capacity on the first success. JSON-RPC *outcome* errors do not trip it: a reachable endpoint whose handler fails is a task problem, not an endpoint problem.
- **Slowness** needs no mechanism. In-flight tasks hold credits, so a slow endpoint is naturally capped at its `concurrency` and the queue backs up in `pending`, exactly like a slow SDK worker.



## Secret rotation

A registration holds up to two secrets. The server signs with the newest; receivers verify against either. To rotate with no missed deliveries:

1. Add the new secret alongside the old (`--secret new --secret old`).
2. Deploy the endpoint so it verifies against both.
3. Remove the old secret (`--secret new`).



## Capabilities

Capability parity, not library parity: the SDK is more ergonomic, but the webhook contract exposes every queue capability.


| Capability                          | Webhook                         | SDK |
| ----------------------------------- | ------------------------------- | --- |
| Process tasks, retries, priorities  | ✓                               | ✓   |
| Skip retry (permanent failure)      | ✓ (error code `-32001`)         | ✓   |
| Long-running tasks + heartbeats     | ✓ (accepted mode)               | ✓   |
| Cancel (best-effort)                | ✓ (abort / cancel notification) | ✓   |
| Aggregation-group batches           | ✓ (JSON-RPC batch)              | ✓   |
| Rate/concurrency limits, scheduling | ✓ (server-side, applies as-is)  | ✓   |
| End-to-end encrypted queues         | manual (bring your own crypto)  | ✓   |




## Sharp edges

- **At-least-once.** A transport failure can follow completed work, so a task can be delivered more than once. Make handlers idempotent, keyed on `taskId`.
- **Encrypted queues need your own crypto.** The payload arrives exactly as stored; for an [encrypted queue](encryption.md) that is ciphertext, and no webhook decryption library ships. In practice, encrypted queues are SDK territory.
- **HTTP is dev-only.** `http://` URLs are rejected unless the server runs unauthenticated (`--dev`). Production endpoints must be `https`.
- **Lease tokens are single-delivery.** A token authorizes one task's heartbeat and result and expires with the lease; a leaked token grants nothing else. It is not an API credential.

