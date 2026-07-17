# Operations guide

How to deploy, configure, scale, secure, observe, and upgrade `conveyord`.

Administration is driven by the **`conveyor` CLI** and the **dashboard**: rescheduling, running, canceling, deleting, and archiving tasks, pausing and resuming queues, setting rate and concurrency limits, and managing cron are operator actions, kept deliberately out of the SDK. The SDK is the produce and consume surface for application code; operating the system is the CLI and dashboard. For the full command-by-command CLI reference, see the [CLI reference](cli.md).

## Deployment modes

One binary, selected by `mode` (or `--mode`):

| Mode         | Discovery                              | Broker                               | Use                              |
|--------------|----------------------------------------|--------------------------------------|----------------------------------|
| `standalone` | self                                   | Postgres (or in-memory with `--dev`) | a single node, dev, edge         |
| `cluster`    | static peer list                       | Postgres                             | VMs / bare metal                 |
| `kubernetes` | pod-label discovery via the API server | Postgres                             | the flagship mode                |
| embedded     | self                                   | memory or Postgres                   | a Go package run in your process |

Clustering is always compiled in: `standalone` is a cluster of one running the same code path. Artifacts ship under [`deploy/`](../deploy): a distroless [Dockerfile](../deploy/docker/Dockerfile), a [Helm chart](../deploy/helm/conveyor), a [systemd unit](../deploy/systemd/conveyord.service), and Compose files. For an end-to-end walkthrough that ties the server, Postgres, and worker tiers together, see [high availability](high-availability.md).

## Configuration

Precedence, lowest to highest: **defaults → config file → `CONVEYOR_*` environment → flags**. `${VAR}` in the file is expanded from the environment.

```sh
conveyord --config=/etc/conveyor/conveyor.yaml
conveyord --mode=kubernetes --config=/etc/conveyor/conveyor.yaml
conveyord --dev   # standalone + in-memory broker + auth off + debug logs
```

Environment keys mirror the file with `CONVEYOR_` and `__` between levels. `broker.dsn` is `CONVEYOR_BROKER__DSN`, `cluster.bind_addr` is `CONVEYOR_CLUSTER__BIND_ADDR`.

Key groups:

- `broker.driver` (`postgres` | `memory`) and `broker.dsn`.
- `api.listen` (default `:8080`), `api.auth_tokens`, `api.tls`.
- `cluster.discovery`, `cluster.bind_addr`, the remoting/discovery/peers ports, `cluster.tls`, and `cluster.kubernetes` (namespace + pod labels).
- `engine.lease_ttl`, `reap_interval`, `lease_batch_max`, `promote_interval`, `passivate_after`, `default_max_retry`, `shutdown_timeout`.
- `engine.rate_limit_enabled` (master switch, default `true`), `engine.rate_limit_rate_per_sec` and `engine.rate_limit_burst` (the global default per-queue dispatch limit; per-queue overrides are set at runtime, see [rate limiting](rate-limiting.md)).
- `metrics.listen` (default `:9464`; empty disables the endpoint).
- `otel.endpoint` (OTLP push for metrics + traces), `otel.service_name`.
- `log.level`, `log.format`.

The Helm chart renders the full configuration into a ConfigMap from [`deploy/helm/conveyor/files/conveyor.yaml`](../deploy/helm/conveyor/files/conveyor.yaml), so that file is also a complete annotated reference.

## Scaling

**The server is stateless.** Durable state lives in the broker. Scale it horizontally:

- **More server nodes** spread queue ownership and worker sessions across the cluster and survive node loss (a lost node's queues re-activate elsewhere and its in-flight tasks are redelivered). On Kubernetes raise `replicaCount`. Run at least three nodes in production; see the [high-availability guide](high-availability.md#server-tier) for why three is the floor.
- **More worker capacity** comes from running more worker processes or raising a worker's `WithConcurrency`. Workers are independent of the server cluster.
- **The broker is the throughput ceiling.** Conveyor commits every task to Postgres before dispatch, so sustained throughput is bounded by the database, not the server. Size the connection pool and the database accordingly, and measure the broker first when tuning.

Priorities and weights shape *what* runs first: per-task `Priority(1..9)` orders within a queue, and per-queue weights bias a worker that serves several queues.

## Broker sizing (Postgres)

- Give `conveyord` a connection pool sized for its concurrency; every replica opens its own pool against the same database.
- Tasks accumulate rows in the task log. Use `Retention` so completed tasks are purged, and inspect archived (dead-lettered) tasks via the Admin API/CLI.
- `engine.lease_ttl` bounds how long a crashed worker's task waits before redelivery; `engine.reap_interval` is how often the reaper reclaims expired leases (recovery time after a failure is roughly `2 × reap_interval`).
- `engine.lease_batch_max` caps how many tasks one dispatch cycle claims. Raise it for high-throughput queues, lower it to smooth load.

## Security

- **Authentication.** `api.auth_tokens` are accepted bearer tokens. Auth is on by default: with no tokens, conveyord **refuses to start** unless you set `api.allow_unauthenticated: true`, so a deployment never serves an open API by accident. The `--dev` preset sets that flag for you; in production set `api.auth_tokens` instead (the Helm chart's `auth.tokensSecret`), and only use `allow_unauthenticated` when a gateway, mTLS, or a private network fronts the API. Clients and workers pass a token with `conveyor.WithToken` (or `CONVEYOR_TOKEN` / the CLI `--token`).
- **TLS.** `api.tls` serves the API over TLS; `cluster.tls` turns on mutual TLS between cluster peers (set `ca_file` for peer verification).
- **Network.** The Helm chart ships an opt-in NetworkPolicy example and keeps the metrics port off the public API listener. Never expose the metrics port (`:9464`) publicly, since it carries internal topology.

## Dashboard

`conveyord` embeds a read+write operations console, served at the API root.

- **Enable/disable.** On by default (`api.dashboard: true`); set it `false` to expose the API without the UI. The static shell is served unauthenticated (it holds no secrets); the data calls it makes go through the bearer-token- authenticated API, so with auth on, enter a token in the UI.
- **Hosting models.** (1) *Embedded*, served by `conveyord`, same origin, no CORS. (2) *Same-origin behind a proxy*, your own UI and the API behind one ingress, no CORS. (3) *Different origin*, a separately hosted UI (CDN/your host); set `api.cors_origins` to the UI's origin(s) (empty disables CORS; `*` allows any). The same built bundle works in all three; it reads its API base URL at runtime (defaults to same-origin, overridable via `?api=` or a global).
- **Metrics link.** Set `api.grafana_url` to surface a "Metrics" link to your Grafana; the dashboard owns task-level inspection and operations, Grafana owns the time-series charts.

## Observability

- **Health.** `/healthz` (liveness) and `/readyz` (readiness: broker reachable and engine running) on the API port. Wired into the chart's probes.
- **Metrics.** Prometheus exposition at `/metrics` on `metrics.listen` (`:9464`): `conveyor_enqueued_total`, `…_completed_total`, `…_failed_total`, `…_retried_total`, `…_archived_total`, `…_released_total`, `conveyor_active`, `conveyor_sessions_active`, `conveyor_pending`, plus runtime metrics. The chart stamps `prometheus.io/scrape` annotations and ships an opt-in ServiceMonitor; `deploy/grafana/` has a dashboard and scrape config.
- **Tracing.** Set `otel.endpoint` to push OTLP traces to a collector. Each enqueue opens a span and stamps a W3C `traceparent` into the task; if your worker process has OpenTelemetry configured, its execution span links back to the enqueue.
- **Lifecycle events.** A push stream of per-task state transitions for live dashboards, alerting, audit logs, and event-driven chaining; see [lifecycle events](events.md).
- `conveyor cluster info` reports cluster membership.

## Upgrades & restarts

- **Graceful shutdown.** On `SIGTERM` the node drains live worker sessions (releasing in-flight tasks for redelivery) before stopping, bounded by `engine.shutdown_timeout`. On Kubernetes, `terminationGracePeriodSeconds` must exceed `shutdown_timeout` (the chart sets this) so the drain completes before SIGKILL.
- **Worker deploys are free.** When a *worker* process shuts down (cancel its `Run` context, e.g. on `SIGTERM`), any task it was running is handed back with **no retry penalty and no backoff**, so it becomes due immediately on another worker rather than counting as a failed attempt. So rolling out a new worker build does not eat into tasks' retry budgets or delay them. A genuine worker *crash* is different: it is recovered by lease expiry and **does** count as a retry, which bounds a task that repeatedly kills its worker.
- **Rolling restart.** Because execution is **at-least-once**, redelivery during a restart is always safe, so design handlers to be idempotent. The StatefulSet rolls one pod at a time; a PodDisruptionBudget keeps a quorum available. Workers reconnect with jitter to the API Service and keep processing while a node is replaced; tasks held by a restarting node are reclaimed by lease expiry and redelivered. The kind e2e (`make e2e`) drives load through a full rolling restart and asserts zero task loss.
- **Version-skew policy.** The wire protocol is additive, so a newer server serves older workers: **roll the server tier first, then workers.** During a rolling restart the cluster runs mixed server versions briefly; keep upgrades to one minor version at a time. Full mixed-version cluster testing is deferred past v1, so do not run a cluster on more than one server version longer than a rollout takes.
- **Schema migrations** run automatically on Postgres connect; no manual step.
