# Conveyor — Python SDK

`conveyorq` is the Python SDK for [Conveyor](https://github.com/conveyorq/conveyor),
a durable, distributed task queue. It speaks the same wire protocol as the Go and
TypeScript SDKs, so a task enqueued from any of them runs on a worker written in
any other.

Requires Python 3.9+. A worker connects to a running Conveyor server
(`conveyord`); see the [project README](https://github.com/conveyorq/conveyor)
to start one.

## Install

`conveyorq` is not yet on PyPI, so install it from source with
[uv](https://docs.astral.sh/uv/). Pick whichever fits your setup:

```bash
# Straight from the repository — no clone needed (recommended):
uv add "conveyorq @ git+https://github.com/conveyorq/conveyor.git#subdirectory=sdks/python"

# Or from a local checkout, so you can edit the SDK and have changes apply live:
git clone https://github.com/conveyorq/conveyor.git
uv add --editable ./conveyor/sdks/python
```

Not using uv? The same git URL works with pip:

```bash
pip install "git+https://github.com/conveyorq/conveyor.git#subdirectory=sdks/python"
```

## Enqueue a task

```python
import asyncio
from conveyorq import Client, new_task, json


async def main() -> None:
    async with Client("http://localhost:8080", token="my-token") as client:
        info = await client.enqueue(
            new_task("email:welcome", json({"user_id": 42})),
            queue="critical",
            max_retry=5,
        )
        print(info.id, info.state)


asyncio.run(main())
```

Every field of `EnqueueOptions` is accepted as a keyword argument: `queue`,
`task_id`, `max_retry`, `priority`, `timeout`, `deadline`, `process_at`,
`process_in`, `retention`, `unique`, `unique_key`, `group`, `expires_in`,
`expires_at`, and `metadata`. Durations are `datetime.timedelta`; absolute times
are `datetime.datetime`.

## Process tasks

```python
import asyncio
from conveyorq import Worker, Mux


async def main() -> None:
    mux = Mux()

    @mux.handler("email:welcome")
    async def send_welcome(task, ctx):
        payload = task.json()
        # ... do the work; raise to retry, raise SkipRetry to dead-letter ...

    worker = Worker(
        "http://localhost:8080",
        queues={"default": 1, "critical": 5},
        concurrency=10,
        token="my-token",
    )
    await worker.run(mux)  # drains gracefully on SIGTERM/SIGINT


asyncio.run(main())
```

A handler that returns normally marks the task completed. Raising
`conveyorq.SkipRetry(...)` archives (dead-letters) it immediately; any other
exception retries it with backoff. The `ctx` argument exposes `ctx.cancelled`
(an `asyncio.Event` set on the task's deadline or an operator cancel) so a
long-running handler can stop early.

## Synchronous API

If your code is not async, use `SyncClient` and `SyncWorker`. Handlers may be
plain functions (run on a thread pool) or `async def`:

```python
from conveyorq import SyncClient, SyncWorker, Mux, new_task, json

with SyncClient("http://localhost:8080", token="my-token") as client:
    client.enqueue(new_task("email:welcome", json({"user_id": 42})))


def send_welcome(task, ctx):
    ...


SyncWorker(
    "http://localhost:8080",
    queues={"default": 1},
    concurrency=8,
    token="my-token",
).run(Mux().handle("email:welcome", send_welcome))
```

## Payload codecs

A payload is opaque bytes plus a content type. Build one with `json(value)` (the
default), `binary(data)` for raw bytes, or `text(value)` for a string. Decode a
received task with `task.json()`, `task.text()`, or `task.payload` for the raw
bytes. Producer and consumer must agree on the JSON shape of a given task type;
the bytes are interoperable across the Go, TypeScript, and Python SDKs.

## End-to-end encryption

Pass an `Encryptor` to seal payloads before they leave the process; the server
stores only ciphertext. The built-in AES-256-GCM codec is byte-compatible with
the other SDKs:

```python
from conveyorq import Client, Worker, Key, new_aes_gcm

enc = new_aes_gcm("k1", Key("k1", secret_32_bytes))
client = Client("http://localhost:8080", token="my-token", encryptor=enc)
worker = Worker("http://localhost:8080", queues={"default": 1}, concurrency=4, encryptor=enc)
```

Pass several keys to `new_aes_gcm` to rotate: the active key seals new data while
retired keys still open existing data.

## For Celery / RQ users

Conveyor is a server-backed queue like Celery or RQ, but the durable state lives
in the server and its broker (Postgres), not in your worker processes. You
enqueue with a `Client` (a thin gRPC producer — no broker connection in your
app) and consume with a `Worker` that holds one streaming session and is pushed
work up to its `concurrency`. There are no per-worker broker credentials, no
result backend to configure, and a deploy is free: a worker draining on SIGTERM
hands its in-flight tasks back with no retry penalty.

## License

Apache-2.0.
