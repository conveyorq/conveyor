# Usage guide

The two halves of using Conveyor: a **worker** registers a handler per task type and processes work, and a **client** enqueues tasks. Both shapes are the same in every SDK. For the concepts behind task, queue, client, server, worker, and broker, see [Concepts](concepts.md).

## Contents

- [Writing a worker](#writing-a-worker)
- [Enqueueing work](#enqueueing-work)

## Writing a worker

A worker registers a handler per task type, then runs. The shape is the same in every SDK.

### Go

```go
w, _ := conveyor.NewWorker("http://localhost:8080",
    conveyor.WithQueues(map[string]int{"critical": 6, "default": 3}),
    conveyor.WithConcurrency(20),
)

mux := conveyor.NewMux()
mux.HandleFunc("email:welcome", func(ctx context.Context, t *conveyor.Task) error {
    var p WelcomeEmail
    if err := t.Bind(&p); err != nil {
        return conveyor.SkipRetry(err) // a payload that cannot decode never will
    }

    return sendEmail(ctx, p)
})

_ = w.Run(ctx, mux) // blocks; reconnects with jitter; drains on ctx cancel
```

### TypeScript

The shape of [`examples/typescript`](../examples/typescript/src/worker.ts):

```ts
import { Mux, skipRetry, type Task, Worker } from "@conveyorq/conveyor";

const mux = new Mux().handle("email:welcome", async (task: Task, { signal }) => {
  const email = task.json<{ to: string; name: string }>();
  if (!email.to) throw skipRetry("missing recipient");          // permanent → dead-letter
  await sendEmail(email.to, `Welcome, ${email.name}!`, signal); // any other throw → retried
});

const stop = new AbortController();
for (const sig of ["SIGTERM", "SIGINT"] as const) process.on(sig, () => stop.abort());

const worker = new Worker("http://localhost:8080", {
  queues: { email: 1 },
  concurrency: 8,
  token: process.env.CONVEYOR_TOKEN,
});

await worker.run(mux, stop.signal); // blocks; reconnects with jitter; drains on abort
```

### Python

The shape of [`examples/python`](../examples/python/worker.py); a synchronous `SyncWorker` exists too:

```python
import asyncio
import os

from conveyorq import Mux, SkipRetry, Worker


async def main() -> None:
    mux = Mux()

    @mux.handler("email:welcome")
    async def send_welcome(task, ctx) -> None:
        email = task.json()
        if not email.get("to"):
            raise SkipRetry("missing recipient")             # permanent → dead-letter
        await send_email(email["to"], f"Welcome, {email['name']}!")  # else retried


    worker = Worker(
        "http://localhost:8080",
        queues={"email": 1},
        concurrency=8,
        token=os.environ.get("CONVEYOR_TOKEN") or None,
    )
    await worker.run(mux)  # drains gracefully on SIGTERM/SIGINT


asyncio.run(main())
```

Handlers must be idempotent and should honor cancellation (`ctx.Done()` in Go, the abort signal in TypeScript and Python). A handler that panics or throws is recovered and reported as a retryable failure, and it never kills the worker.

## Enqueueing work

### Go

```go
client, _ := conveyor.NewClient("http://localhost:8080")

info, _ := client.Enqueue(ctx,
    conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: 42})),
    conveyor.Queue("critical"),
    conveyor.ProcessIn(5*time.Minute),
    conveyor.MaxRetry(10),
)
```

### TypeScript

The shape of [`examples/typescript`](../examples/typescript/src/producer.ts), where durations are milliseconds:

```ts
import { Client, json, newTask } from "@conveyorq/conveyor";

const client = new Client("http://localhost:8080", { token: process.env.CONVEYOR_TOKEN });

const info = await client.enqueue(newTask("email:welcome", json({ to: "ada@example.com", name: "Ada" })), {
  queue: "email",
  processIn: 5 * 60_000, // milliseconds
  maxRetry: 10,
});
```

### Python

The shape of [`examples/python`](../examples/python/client.py), where durations are `timedelta`:

```python
import asyncio
from datetime import timedelta

from conveyorq import Client, json, new_task


async def main() -> None:
    async with Client("http://localhost:8080") as client:
        info = await client.enqueue(
            new_task("email:welcome", json({"to": "ada@example.com", "name": "Ada"})),
            queue="email",
            process_in=timedelta(minutes=5),
            max_retry=10,
        )
        print(info.id, info.state.value)


asyncio.run(main())
```

### Command line

```sh
go run ./cmd/conveyor enqueue email:welcome --queue critical --json '{"user_id":42}' --in 5m
```
