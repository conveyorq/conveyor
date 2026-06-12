# Standalone example

A minimal Conveyor deployment: one `conveyord` node, one worker process,
one enqueueing client.

## Run it

Terminal 1 — the server (in-memory broker, auth off, debug logs):

```sh
go run ./cmd/conveyord --dev
```

Terminal 2 — the worker:

```sh
go run ./examples/standalone/worker
```

Terminal 3 — enqueue ten welcome emails:

```sh
go run ./examples/standalone/client
```

The worker prints one line per processed task. Kill the worker (`kill -9`)
mid-run and restart it: every in-flight task is redelivered immediately
with no retry penalty — that is the at-least-once contract.

Both programs read `CONVEYOR_ADDR` (default `http://localhost:8080`) and
`CONVEYOR_TOKEN` (empty for `--dev` servers, which disable auth).

## Enqueue with curl

The same API is plain HTTP/JSON:

```sh
curl -s http://localhost:8080/conveyor.v1.TaskService/Enqueue \
  -H 'Content-Type: application/json' \
  -d '{"type":"email:welcome","payload":"eyJ1c2VyX2lkIjo5OX0=","content_type":"application/json"}'
```

(The payload is base64-encoded `{"user_id":99}`.)
