# Conveyor

Conveyor is a distributed task processing system for Go: a persistent task
queue with push-based dispatch, at-least-once execution, retries with
backoff, scheduling, and priorities — backed by Postgres or an in-memory
broker, with no Redis and no polling.

## Quickstart

Terminal 1 — start a development server (in-memory broker, auth off):

```sh
go run ./cmd/conveyord --dev
```

Terminal 2 — run a worker:

```sh
go run ./examples/standalone/worker
```

Terminal 3 — enqueue ten welcome emails:

```sh
go run ./examples/standalone/client
```

The worker prints one line per processed task. The whole flow is scripted
in [`hack/quickstart.sh`](hack/quickstart.sh) (`make quickstart`), which CI
runs on every change under a 60-second budget.

## Writing a worker

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

Handlers must be idempotent and should honor `ctx.Done()`. Panics are
recovered and reported as retryable failures — a panicking handler never
kills the worker.

## Enqueueing work

```go
client, _ := conveyor.NewClient("http://localhost:8080")

info, _ := client.Enqueue(ctx,
    conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: 42})),
    conveyor.Queue("critical"),
    conveyor.ProcessIn(5*time.Minute),
    conveyor.MaxRetry(10),
)
```

Or from the command line:

```sh
go run ./cmd/conveyor enqueue email:welcome --queue critical --json '{"user_id":42}' --in 5m
```

## Embedded mode

The whole system inside one process — no server, no infrastructure:

```go
system, _ := embedded.Start(ctx, embedded.Config{Broker: embedded.Memory()})
client := system.Client()
worker := system.Worker(conveyor.WithQueues(map[string]int{"default": 1}), conveyor.WithConcurrency(8))
```

See [`examples/embedded`](examples/embedded). Moving to a real cluster is
swapping the constructors; handler and enqueue code is identical.

## How it works

Conveyor has three moving parts: your **client** enqueues tasks, the
**server** (`conveyord`) owns them, and your **workers** process them. A
durable **broker** — Postgres in production, in-memory for dev — is the source
of truth. Tasks are persisted *before* they're dispatched, so they survive any
crash.

```
 ┌──────────┐  enqueue   ┌─────────────────────────┐   dispatch ↓  ┌──────────┐
 │  Client  │ ──────────▶│        conveyord        │ ─────────────▶│  Worker  │
 │ (or CLI) │            │  · accepts enqueues     │   results  ↑  │ (handler │
 └──────────┘            │  · pushes work to ready │◀───────────── │  code)   │
                         │    workers (no polling) │               └──────────┘
                         │  · retries, backoff,    │
                         │    scheduling, cron     │
                         └────────────┬────────────┘
                                      │ persists before dispatch
                              ┌───────▼────────┐
                              │     Broker     │  durable source of truth
                              │ Postgres / mem │  (tasks survive crashes)
                              └────────────────┘
```

**Push, not poll.** The server pushes tasks to workers the instant work
exists and a worker has free capacity — there's no poll interval to tune and
no Redis. Each worker opens one persistent connection, tells the server which
queues it serves and how much it can handle, and receives work over that
stream. When a worker is saturated it simply stops accepting more, and the
extra work waits safely in the broker.

**At-least-once execution.** A task is delivered until a worker acknowledges
it. If a worker dies mid-task, the task is redelivered — so **handlers must be
idempotent**. Return `conveyor.SkipRetry(err)` to dead-letter immediately;
panics are recovered and treated as retryable failures.

**What the server gives you:** named queues with weights, bounded worker
concurrency, retries with exponential backoff, per-task priorities, delayed
and scheduled tasks, cron, unique tasks, retention/archival, and a read-only
admin/inspection API — all enforced server-side. Your code only writes
handlers and enqueues tasks.

The internal design (coordination, flow control, failover, guarantees G1–G7)
is documented in [`.claude/DESIGN.md`](.claude/DESIGN.md).

## Development

```sh
make help        # all targets
make test        # race-enabled tests (Postgres tests need Docker)
make lint        # golangci-lint via the pinned tools image
make quickstart  # the scripted README quickstart
```
