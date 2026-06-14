# Conveyor

[![CI](https://img.shields.io/github/actions/workflow/status/conveyorq/conveyor/ci.yml?branch=main&label=build)](https://github.com/conveyorq/conveyor/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/conveyorq/conveyor.svg)](https://pkg.go.dev/github.com/conveyorq/conveyor)

Conveyor is a distributed task processing system for Go: a persistent task
queue with push-based dispatch, at-least-once execution, retries with
backoff, scheduling, and priorities — backed by Postgres or an in-memory
broker, with no Redis and no polling.

## Features

- **Push-based dispatch** — the server streams work to connected workers the
  instant it exists, with credit-based flow control. No polling.
- **At-least-once with crash safety** — tasks are persisted before dispatch and
  survive server and worker crashes; a dead worker's task is redelivered.
- **Retries** with exponential backoff, **delayed** and **scheduled** tasks,
  per-task **priorities** and weighted queues.
- **Unique tasks**, **dead-letter/archive**, **retention**, per-queue
  **pause/resume**, and a per-task-type **circuit breaker**.
- **Cron** — server-persisted schedules that survive restarts and failover.
- **Built-in clustering / HA** — multi-node by default; a lost node's work
  re-activates elsewhere with zero task loss.
- **Four ways to run it** — standalone, cluster, Kubernetes, or
  [embedded](#embedded-mode) in a Go process.
- **Prometheus metrics** and **OpenTelemetry traces** out of the box.

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

### Try it with Docker

Prebuilt multi-arch images are published to the GitHub Container Registry on
every push to `main` (`:edge`) and every release (`:vX.Y.Z`, `:latest`).

Run a throwaway server (in-memory broker, auth off) in one line:

```sh
docker run --rm -p 8080:8080 ghcr.io/conveyorq/conveyor:edge --dev
```

Or bring up a realistic stack (server + Postgres) with Compose:

```sh
docker compose -f deploy/compose/quickstart.yaml up
```

Either way the API is on `http://localhost:8080` (with `/healthz` and
`/readyz`); the Compose stack also serves metrics on
`http://localhost:9464/metrics`.

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

The server coordinates all of this across a cluster of `conveyord` nodes:
queues, scheduling, and lease recovery rebalance automatically when a node is
lost, and no task is dropped. Scale by adding nodes; the broker is the only
stateful dependency.

## Documentation

- [Operations guide](docs/operations.md) — deployment modes, configuration,
  scaling, broker sizing, security, observability, and upgrades.
- [Migrating from asynq](docs/migrate-from-asynq.md) — side-by-side API mapping.
- [Migrating from River](docs/migrate-from-river.md) — side-by-side API mapping,
  and the one trade-off to decide first (transactional enqueue).
- [Benchmark harness](benchmark/README.md) — reproducible throughput/latency
  harness (`make benchmark`) and its honesty notes.
- Deployment artifacts live under [`deploy/`](deploy): Docker, Helm, systemd,
  Compose, and Grafana.

## Development

```sh
make help        # all targets
make test        # race-enabled tests (Postgres tests need Docker)
make lint        # golangci-lint via the pinned tools image
make quickstart  # the scripted README quickstart
make chaos       # 3-node kill test, repeated for the zero-loss gate (CHAOS_COUNT=20)
make e2e         # kind-based end-to-end deployment test
```

### End-to-end deployment test

`make e2e` runs `hack/e2e-kind.sh`, which stands up a throwaway [kind](https://kind.sigs.k8s.io)
cluster close to a production setup: a Postgres broker, three server replicas in
kubernetes mode discovering each other through the Kubernetes API, the database
DSN and API tokens delivered as Secrets, and metrics on their own port. It
builds the image, loads it into kind, installs the Helm chart, and asserts the
rollout completes, the three nodes form one cluster, and the metrics endpoint
serves — then deletes the cluster. It needs `docker`, `kind`, `kubectl`, and
`helm`, and runs the same way locally and in CI.
