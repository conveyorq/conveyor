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

## Development

```sh
make help        # all targets
make test        # race-enabled tests (Postgres tests need Docker)
make lint        # golangci-lint via the pinned tools image
make quickstart  # the scripted README quickstart
```
