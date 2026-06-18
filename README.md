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
- **Deploys are free** — a worker shutting down hands its in-flight tasks back
  with no retry penalty and no backoff; they resume immediately elsewhere, so
  rolling out a new build never burns a task's retry budget.
- **Retries** with exponential backoff, **delayed** and **scheduled** tasks,
  per-task **timeouts/deadlines**, per-task **priorities** and weighted queues.
- **Unique tasks**, **dead-letter/archive**, **retention**, per-queue
  **pause/resume**, and a per-task-type **circuit breaker**.
- **Expiring tasks** — a pre-dispatch TTL (`ExpiresIn`/`ExpiresAt`): a task not
  dispatched in time is archived instead of run, for work that goes stale.
- **Group aggregation** — coalesce many tasks into one batch and process them in
  a single handler call (debounce/digest, or bulk processing); fires on size,
  delay, or grace period.
- **Rate limiting** — cap a queue's dispatch rate (token bucket: rate + burst) to
  protect a downstream; a global default plus per-queue overrides, tunable live
  from the CLI, dashboard, or API.
- **SDK middleware** — wrap both sides: decorate enqueues
  (`WithEnqueueMiddleware`) and handlers (`Mux.Use`, `Mux.UseBatch`) for
  logging, metrics, or policy, without touching task code.
- **End-to-end encryption** — seal task payloads in the SDK/CLI
  (`WithEncryption`) so the server stores ciphertext only and holds no keys;
  built-in AES-256-GCM or bring your own KMS/HSM codec.
- **Cron** — server-persisted schedules that survive restarts and failover,
  pausable at runtime.
- **Built-in clustering / HA** — multi-node by default; a lost node's work
  re-activates elsewhere with zero task loss.
- **Four ways to run it** — standalone, cluster, Kubernetes, or
  [embedded](#embedded-mode) in a Go process.
- **Secure by default** — bearer-token auth that fails closed: outside `--dev`
  the server refuses to start unauthenticated unless you opt in explicitly.
- **Built-in operations dashboard** — an embedded web console to inspect and
  operate queues, tasks, cron, and connected workers; host it anywhere.
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
`http://localhost:9464/metrics`. Both run the API without authentication for
local evaluation; a real deployment sets `api.auth_tokens` (see the
[operations guide](docs/operations.md#security)).

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

## SDKs

One wire protocol, three SDKs — a task enqueued from any of them runs on a
worker written in any other.

- **Go** — `import conveyor "github.com/conveyorq/conveyor/sdks/go"` (the worker
  and enqueue examples above). The reference implementation.
- **TypeScript** — [`sdks/typescript`](sdks/typescript), npm package
  `@conveyorq/conveyor`, Node 20+.
- **Python** — [`sdks/python`](sdks/python), PyPI package `conveyorq`,
  Python 3.9+, with both async and synchronous APIs.

The TypeScript and Python SDKs match the Go SDK feature-for-feature: a producer
client, a worker runtime (push-based dispatch, heartbeats, graceful drain),
JSON/binary codecs, and AES-256-GCM end-to-end encryption that is byte-compatible
across all three. The npm and PyPI packages publish with the `v1.1.0` release;
until then, install from the in-repo source (see each SDK's README). The wire
contract that any new SDK implements is specified in
[`docs/protocol.md`](docs/protocol.md).

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

## Dashboard

`conveyord` embeds a web operations console, served at the API root and **enabled
by default in every mode, production included** — open the server's API URL in a
browser. In production it sits behind the same bearer-token auth as the API: you
enter a token in the UI, which is kept client-side and sent on each call.
`conveyord --dev` is just the local convenience — auth off, zero config.

It is a full read **and write** console: inspect queues, drill into tasks by
state with a detail view, manage cron, and see the worker sessions connected to
each node — and act: run/cancel/delete tasks, pause/resume queues, edit cron.
Auto-refresh keeps it live, and a configurable link deep-links to Grafana for
time-series metrics.

The UI is just an API client, so you can run the embedded copy, serve the same
bundle from your own host/CDN, or front both behind one ingress. Set
`api.dashboard: false` to disable the embedded copy, `api.cors_origins` to allow
a different-origin UI, and `api.grafana_url` for the metrics link. See the
[operations guide](docs/operations.md#dashboard).

## Documentation

- [Operations guide](docs/operations.md) — deployment modes, configuration,
  scaling, broker sizing, security, observability, and upgrades.
- [Group aggregation](docs/grouping.md) — how to enqueue grouped tasks, write
  batch handlers, and tune the firing policy.
- [Rate limiting](docs/rate-limiting.md) — cap per-queue dispatch rate with a
  global default and live per-queue overrides.
- [End-to-end encryption](docs/encryption.md) — seal task payloads in the
  SDK/CLI so the server stores ciphertext only and holds no keys.
- [Expiring tasks](docs/expiring-jobs.md) — a pre-dispatch TTL, and how it
  differs from a deadline and from retention.
- [Wire protocol](docs/protocol.md) — the normative protocol spec for SDK
  authors building a Conveyor client or worker in another language.
- [TypeScript SDK](sdks/typescript/README.md) — enqueue and process tasks from
  Node (the `@conveyorq/conveyor` npm package).
- [Python SDK](sdks/python/README.md) — async and sync clients and workers, with
  a "Conveyor for Celery/RQ users" intro (the `conveyorq` PyPI package).
- [Migrating from asynq](docs/migrate-from-asynq.md) — side-by-side API mapping.
- [Migrating from River](docs/migrate-from-river.md) — side-by-side API mapping,
  and the one trade-off to decide first (transactional enqueue).
- [Benchmark harness](benchmark/README.md) — reproducible throughput/latency
  harness (`make benchmark`) and its honesty notes.
- Deployment artifacts live under [`deploy/`](deploy): Docker, Helm, systemd,
  Compose, and Grafana.
- [Contributing](CONTRIBUTING.md) — build, test, conventions, and how to submit
  changes.
- [Changelog](CHANGELOG.md) — release history.

## Development

```sh
make help        # all targets
make test        # race-enabled tests (Postgres tests need Docker)
make lint        # golangci-lint via the pinned tools image
make quickstart  # the scripted README quickstart
make chaos       # 3-node kill test, repeated for the zero-loss gate (CHAOS_COUNT=20)
make e2e         # kind-based end-to-end deployment test (KEEP=1 keeps the cluster)
make e2e-demo    # live playground: cluster + continuous load + dashboard
make dashboard   # rebuild the embedded dashboard bundle (needs Node)
```

### End-to-end deployment test

`make e2e` runs `hack/e2e-kind.sh`, which stands up a throwaway [kind](https://kind.sigs.k8s.io)
cluster close to a production setup: a Postgres broker, three server replicas in
kubernetes mode discovering each other through the Kubernetes API, the database
DSN and API tokens delivered as Secrets, and metrics on their own port. It
builds the image, loads it into kind, installs the Helm chart, and asserts the
rollout completes, the three nodes form one cluster, and the metrics endpoint
serves. It then drives load through a **rolling restart** — an in-cluster
producer/worker enqueues and processes tasks through the API Service while the
StatefulSet is rolled one pod at a time — and asserts the cluster reforms and
every task completes with zero loss. It needs `docker`, `kind`, `kubectl`, and
`helm`, and runs the same way locally and in CI.

The cluster is torn down automatically on exit. To watch it **live** instead —
stand up the cluster, run a continuous producer/worker, and open the dashboard
so you can see tasks flow — use the one-command playground:

```sh
make e2e-demo      # cluster + continuous load + live dashboard at http://localhost:8080/ (token: e2e-token)
make e2e-clean     # tear the cluster down when finished
```

It runs the same health checks first, then goes live and blocks until Ctrl-C;
turn on **Auto-refresh** in the UI to watch the queues, tasks, and workers
update. The pieces are also available on their own: `KEEP=1 make e2e` (the
assert-and-exit test, kept), `make e2e-dashboard` (port-forward + open an
existing cluster's dashboard), and `make e2e-clean` (remove a cluster, including
one left by an interrupted run).

### Dashboard development

The dashboard (`web/dashboard/`) is a React + TypeScript app built with Vite.
Its built bundle (`dist/`) is **not committed** — it's built in CI and baked
into the Docker image. `go build`/`go test` don't need Node (the dashboard
tests skip when the bundle is absent, and the binary serves an empty dashboard
until built). Build it locally with:

```sh
make dashboard       # build web/dashboard/dist (embedded by conveyord)
make dashboard-test  # run the frontend unit tests (Vitest)
make dashboard-gen   # regenerate the TypeScript API client from the protos
```

For a fast edit loop, run a dev server against a local `conveyord --dev`:

```sh
go run ./cmd/conveyord --dev                       # API + dashboard on :8080
cd web/dashboard && npm install && npm run dev      # hot-reloading UI on :5173
```

Open the Vite dev server with `?api=http://localhost:8080` so it targets the
running server. After changing the UI, run `make dashboard` to refresh the
committed `dist/` that ships in the binary.

## License

Conveyor is **fully open source** under the [Apache License 2.0](LICENSE) — the
entire project, with no separate enterprise, commercial, or closed-source
edition. Third-party dependency licenses are inventoried in
[docs/licenses.md](docs/licenses.md).
