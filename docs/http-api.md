# HTTP API

Enqueue tasks into Conveyor from any language, with nothing but an HTTP client. The server exposes its task API over plain HTTP/JSON on the same port the SDKs use, so anything that can send a POST (curl, a cron job, a webhook, a language without an SDK) can produce work.

This page covers **producing and inspecting** tasks. Consuming is different: Conveyor pushes work to connected workers, and holding that delivery stream is what the [SDKs](usage.md) do. There is no endpoint to poll for work. To consume without an SDK, register a [webhook worker](webhook-workers.md): Conveyor pushes each task to your HTTP endpoint as a signed JSON-RPC call.

The wire is not frozen yet (Conveyor is pre-1.0); breaking changes remain possible before 1.0.0. The normative contract, including everything this page summarizes, is the [wire protocol spec](protocol.md).

## Contents

- [Quick start](#quick-start)
- [How requests work](#how-requests-work)
- [JSON encoding rules](#json-encoding-rules)
- [Enqueue](#enqueue)
- [EnqueueBatch](#enqueuebatch)
- [EnqueueTx](#enqueuetx)
- [GetTask](#gettask)
- [Errors](#errors)
- [Limits and validation](#limits-and-validation)
- [Sharp edges](#sharp-edges)

## Quick start

Start a development server (in-memory broker, auth disabled):

```sh
conveyord --dev
```

Enqueue a task:

```sh
curl -sS http://localhost:8080/conveyor.v1.TaskService/Enqueue \
  -H "Authorization: Bearer $CONVEYOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "queue": "email",
        "type": "email:send",
        "payload": "'"$(printf '{"to":"a@b.c"}' | base64)"'",
        "contentType": "application/json"
      }'
```

The server answers with the committed task:

```json
{"task":{"id":"01KX3CAWA7WMGN3R5EA7JJCFA0","queue":"email","type":"email:send","state":"TASK_STATE_PENDING","priority":4,"maxRetry":25,"enqueuedAt":"2026-07-09T12:06:12.551994Z","payload":"eyJ0byI6ImFAYi5jIn0=","contentType":"application/json"}}
```

The task is now pending; any worker serving the `email` queue picks it up.

## How requests work

Every call is an HTTP **POST** to `http(s)://<server>/<service>/<method>`, with a JSON body:

| Method                                  | Purpose                                            |
|-----------------------------------------|----------------------------------------------------|
| `/conveyor.v1.TaskService/Enqueue`      | Enqueue one task                                   |
| `/conveyor.v1.TaskService/EnqueueBatch` | Enqueue many, each succeeds or fails independently |
| `/conveyor.v1.TaskService/EnqueueTx`    | Enqueue many atomically, all or none               |
| `/conveyor.v1.TaskService/GetTask`      | Fetch one task by id                               |

- `Content-Type: application/json` is required.
- Authentication is a bearer token: `Authorization: Bearer <token>`. A `--dev` server has authentication disabled and ignores the header.
- No other headers are needed; this is the [Connect protocol](https://connectrpc.com), which plain HTTP clients can speak directly. The same endpoints also serve gRPC, which is what the SDKs use.

## JSON encoding rules

Fields follow the standard proto3 JSON mapping. The ones that surprise people:

| Field type                                           | JSON form                                          |
|------------------------------------------------------|----------------------------------------------------|
| `payload` (bytes)                                    | base64 string                                      |
| durations (`processIn`, `timeout`, `uniqueTtl`, ...) | decimal seconds with `s` suffix: `"30s"`, `"1.5s"` |
| timestamps (`processAt`, `deadline`, ...)            | RFC 3339 string: `"2026-07-10T09:00:00Z"`          |
| 64-bit integers                                      | JSON string, not number                            |
| enums (`state`, ...)                                 | value name string: `"TASK_STATE_PENDING"`          |

Field names are lowerCamelCase (`contentType`, `maxRetry`); the original snake_case proto names are also accepted on input.

Mind the two encoding layers on `payload`: the JSON transport encoding is base64, and the bytes inside must match the codec named by `contentType`. `"payload": "eyJ0byI6ImFAYi5jIn0=", "contentType": "application/json"` means "these bytes, decoded from base64, are UTF-8 JSON". Workers decode with the codec the `contentType` names, so a producer and its workers must agree on it.

## Enqueue

`type` is the only required field: it is the routing key a worker registers a handler for. Everything else has a server default or is optional. The commonly used options:

```sh
curl -sS http://localhost:8080/conveyor.v1.TaskService/Enqueue \
  -H "Authorization: Bearer $CONVEYOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "queue": "email",
        "type": "email:send",
        "payload": "'"$(printf '{"to":"a@b.c"}' | base64)"'",
        "contentType": "application/json",
        "processIn": "30s",
        "priority": 9,
        "maxRetry": 5,
        "timeout": "60s",
        "uniqueKey": "welcome-a@b.c",
        "uniqueTtl": "3600s",
        "metadata": {"tenant": "acme"}
      }'
```

- `queue` empty selects `default`. `priority` runs 1 (lowest) to 9 (highest), default 4. `maxRetry` defaults to 25.
- `processIn` (or the absolute `processAt`, not both) delays the task; it is committed in state `TASK_STATE_SCHEDULED` instead of `TASK_STATE_PENDING`.
- `uniqueKey` plus `uniqueTtl` suppresses duplicates: a second enqueue with the same key while one is incomplete fails with `already_exists`.
- `taskId` (a client-assigned ULID) makes the call idempotent: retrying the same request cannot double-enqueue; the repeat fails with `already_exists`.
- The full option list (deadlines, expiry, groups, dependencies, retry policies, concurrency keys) is in [`protocol.md` §6.1](protocol.md).

## EnqueueBatch

Up to 1000 tasks in one call. Items succeed or fail **independently**; the call itself succeeds and reports per-item results positionally:

```sh
curl -sS http://localhost:8080/conveyor.v1.TaskService/EnqueueBatch \
  -H "Authorization: Bearer $CONVEYOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "tasks": [
          {"queue": "email", "type": "email:send", "payload": "'"$(printf '{"to":"a@b.c"}' | base64)"'", "contentType": "application/json"},
          {"queue": "email", "payload": "bm8gdHlwZQ=="}
        ]
      }'
```

The second item is missing its `type`, so the response carries a committed `task` for item 0 and an `error` for item 1:

```json
{"results":[{"task":{"id":"01KX3D...","queue":"email","type":"email:send","state":"TASK_STATE_PENDING",...}},{"error":"task type is required"}]}
```

Check every `results[i]`; the HTTP status alone does not tell you whether all items landed.

## EnqueueTx

Same shape as `EnqueueBatch`, atomic semantics: either every task commits or none do. Any invalid item, duplicate `uniqueKey`, or key collision inside the request fails the whole call and commits nothing:

```sh
curl -sS http://localhost:8080/conveyor.v1.TaskService/EnqueueTx \
  -H "Authorization: Bearer $CONVEYOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "tasks": [
          {"queue": "orders", "type": "order:charge", "payload": "'"$(printf '{"order":42}' | base64)"'", "contentType": "application/json"},
          {"queue": "orders", "type": "order:confirm", "payload": "'"$(printf '{"order":42}' | base64)"'", "contentType": "application/json"}
        ]
      }'
```

On success the response lists the committed tasks in request order; there are no per-item results, which is the difference from `EnqueueBatch`.

## GetTask

Fetch the current view of a task (state, retry count, timestamps, last error):

```sh
curl -sS http://localhost:8080/conveyor.v1.TaskService/GetTask \
  -H "Authorization: Bearer $CONVEYOR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "01KX3CAWA7WMGN3R5EA7JJCFA0"}'
```

States are `TASK_STATE_` + one of `SCHEDULED, PENDING, ACTIVE, RETRY, COMPLETED, ARCHIVED, CANCELED, AGGREGATING`.

## Errors

Failures return a non-2xx status and a JSON body with a Connect error code:

```json
{"code":"not_found","message":"broker: task not found"}
```

| Code               | Meaning                                                          |
|--------------------|------------------------------------------------------------------|
| `invalid_argument` | The request violated the contract; fix it before retrying        |
| `already_exists`   | Duplicate `taskId`, or an active `uniqueKey` suppressed the task |
| `not_found`        | Unknown task id                                                  |
| `unauthenticated`  | Missing or invalid bearer token                                  |
| `unavailable`      | Transient server condition; retry                                |
| `internal`         | Server-side failure                                              |

## Limits and validation

| Rule                                                                                                        | Violation          |
|-------------------------------------------------------------------------------------------------------------|--------------------|
| `type` is required                                                                                          | `invalid_argument` |
| `payload` at most 1 MiB                                                                                     | `invalid_argument` |
| `queue` matches `^[a-zA-Z0-9][a-zA-Z0-9-_.]*$`                                                              | `invalid_argument` |
| `priority` in 1..9 (0 selects the default)                                                                  | `invalid_argument` |
| Batch and Tx take 1 to 1000 tasks                                                                           | `invalid_argument` |
| `processAt`/`processIn` mutually exclusive; likewise `expiresAt`/`expiresIn`, and `group` with either delay | `invalid_argument` |

## Sharp edges

- **Producing only.** There is no HTTP endpoint that hands out work. Conveyor is push-based: the server delivers tasks to connected workers, and workers are built with the [SDKs](usage.md).
- **Encrypted queues.** [End-to-end encryption](encryption.md) seals payloads in the producer before enqueue. The server cannot tell ciphertext from plaintext, so nothing stops a raw HTTP producer from enqueueing plaintext into a queue whose workers expect sealed payloads; those tasks fail at the worker. Do not produce over HTTP into encrypted queues unless you implement the same sealing scheme.
- **At-least-once.** A timeout on your POST does not mean the task was not committed. Use `taskId` (or `uniqueKey`) so retrying the request is safe.
