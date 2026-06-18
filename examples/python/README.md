# Conveyor Python example — email delivery

A minimal but realistic use of the `conveyorq` SDK: a producer enqueues welcome
and reminder emails, and a worker delivers them. It mirrors the
[TypeScript example](../typescript) so you can compare the two SDKs.

## Run it

Start a Conveyor server (from the repo root):

```bash
go run ./cmd/conveyord --dev      # standalone, in-memory broker, auth disabled
```

Install the SDK and run the worker and producer in two terminals:

```bash
cd examples/python
python -m venv .venv && . .venv/bin/activate
pip install -e ../../sdks/python   # or: pip install conveyorq

python worker.py                                   # terminal 1
python client.py welcome ada@example.com Ada       # terminal 2
python client.py reminder ada@example.com "Pay invoice" 1
```

The worker prints each delivery; the producer prints the enqueued task id. Stop
the worker with Ctrl-C — it drains in-flight work gracefully.

## Configuration

Both programs read the environment:

| Variable             | Default                  | Meaning                          |
|----------------------|--------------------------|----------------------------------|
| `CONVEYOR_ADDR`      | `http://localhost:8080`  | Server base URL                  |
| `CONVEYOR_TOKEN`     | _(empty)_                | Bearer token, if auth is enabled |
| `CONVEYOR_MAX_RETRY` | `10`                     | Retry budget per task            |

## What to notice

- **`tasks.py` is the contract.** The producer and worker share the task-type
  names and payload shapes; that agreement on the JSON bytes is all the queue
  requires of two peers — they could be in different languages.
- **`welcome` is idempotent.** It enqueues with a `unique_key` and a 24h TTL, so
  a retry or a double submit never sends two welcomes.
- **`reminder` is delayed.** `process_in` schedules it for the future.
- **Bad payloads dead-letter.** The worker raises `SkipRetry` for a permanently
  invalid email instead of retrying it forever.
