# Webhook worker example

Process Conveyor tasks with no SDK, from any language that can serve HTTP.
This endpoint receives pushed tasks over the [JSON-RPC 2.0 delivery
protocol](../../docs/webhook-workers.md), verifies the delivery signature,
and completes each task inside the request.

The server still pushes: you register a URL, Conveyor leases tasks to it and
POSTs each one. There is no queue to poll.

## Run it

Terminal 1 — the server (in-memory broker, auth off):

```sh
go run ./cmd/conveyord --dev
```

Terminal 2 — this endpoint, keyed with a signing secret:

```sh
WEBHOOK_SECRET=s3cret go run ./examples/webhook
```

Terminal 3 — register the endpoint, then enqueue a task:

```sh
go run ./cmd/conveyor webhooks add demo-hooks http://localhost:9090/tasks \
  --queue email=1 --secret s3cret

curl -s http://localhost:8080/conveyor.v1.TaskService/Enqueue \
  -H 'Content-Type: application/json' \
  -d '{"queue":"email","type":"email:welcome","payload":"eyJ1c2VyX2lkIjo5OX0=","contentType":"application/json"}'
```

The endpoint prints one line per delivered task. (The payload is
base64-encoded `{"user_id":99}`; `--dev` accepts `http://` URLs, which are
rejected on an authenticated server.)

## What it shows

- **Signature verification.** Every delivery carries `X-Conveyor-Timestamp`
  and `X-Conveyor-Signature: v1=<hmac>`; the endpoint recomputes the HMAC
  over `"{timestamp}.{body}"` with the shared secret and rejects a mismatch
  or a stale timestamp.
- **Synchronous completion.** The handler answers `{"result":{"status":
  "completed"}}`; a failure would answer a JSON-RPC error (`-32000` to retry,
  `-32001` to skip retry).
- **At-least-once delivery.** A transport failure can follow completed work,
  so key idempotency on `taskId`.

Long-running work uses **asynchronous completion** instead (answer
`accepted`, then heartbeat and report the result with the delivery's lease
token). The [webhook workers guide](../../docs/webhook-workers.md) covers it.
