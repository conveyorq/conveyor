# Changelog

All notable changes to Conveyor are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

First public release — a distributed task queue for Go: a persistent,
push-based queue with at-least-once execution, backed by Postgres or an
in-memory broker, with no Redis and no polling.

### Added

- **Expiring tasks (pre-dispatch TTL)**: a task that must not run if it was not
  dispatched in time. Set `conveyor.ExpiresIn(d)` or `conveyor.ExpiresAt(t)`
  (or the CLI `--expires-in`/`--expires-at` flags) and a task still waiting past
  its expiry is archived with `task expired before dispatch` instead of run.
  Distinct from a deadline (which cancels a running task) and retention (which
  purges a completed one). The lease query skips expired tasks and a reaper
  sweep archives them. See `docs/expiring-jobs.md`.
- **End-to-end payload encryption**: seal task payloads in the SDK/CLI with
  `conveyor.WithEncryption(...)` so the server stores ciphertext only and holds
  no keys. Ships the `encryption` package — an `Encryptor` seam with a built-in
  AES-256-GCM implementation (fresh nonce per call, key-id-framed ciphertext for
  rotation) and bring-your-own as the extension point. A metadata marker gates
  decryption, so encrypted and plaintext tasks share a queue; a wrong-key or
  keyless worker fails the task without running the handler. The CLI gains
  `conveyor enqueue --encryption-key <id>:<base64-secret>` (or
  `$CONVEYOR_ENCRYPTION_KEY`). See `docs/encryption.md`.
- **Group aggregation**: tag tasks with `conveyor.Group(...)` to accumulate them
  by `(queue, group)` and deliver the whole group to a worker as one batch via
  `Mux.HandleBatch`. Fires on size, max-delay, or grace period (server-configured);
  one slot and one lease per batch; per-member outcomes via `BatchError`. Members
  show as the `aggregating` state in the dashboard. See `docs/grouping.md`.
- **SDK middleware** on both sides of the queue: decorate the enqueue path with
  `WithEnqueueMiddleware` (client) and handlers with `Mux.Use` (single-task) and
  `Mux.UseBatch` (batch). The first middleware registered runs outermost; a group
  member redelivered as a batch of one runs the single-task chain.
- **Per-queue rate limiting**: cap a queue's dispatch rate with a token bucket
  (`rate` + `burst`). A global default in server config (`rate_limit_enabled`,
  `rate_limit_rate_per_sec`, `rate_limit_burst`) plus per-queue overrides set live
  via `conveyor ratelimit set|rm|ls`, the dashboard's Limits tab, or the
  `AdminService` `SetQueueRateLimit`/`DeleteQueueRateLimit`/`ListRateLimits` RPCs.
  Over-rate tasks wait without a retry penalty; the `conveyor_ratelimit_throttled`
  metric tracks throttling. See `docs/rate-limiting.md`.
- **Normative wire-protocol spec** (`docs/protocol.md`): the language-agnostic
  contract for non-Go SDKs, covering transport, auth, the `content_type` codec
  contract, flow control, the session frames, and a conformance checklist.
- **Session version handshake**: workers may require a minimum server version
  (`Hello.min_server_version`, `WithMinServerVersion`), and the server now
  advertises its build and admitted SDK floor in `Welcome` (`server_version`,
  `min_sdk_version`). All fields are additive and optional.
- **Push-based dispatch** over a ConnectRPC worker-session protocol with
  credit-based flow control — the server streams work to ready workers; no
  poll interval to tune.
- **At-least-once execution** with crash safety across server nodes and worker
  disconnects: tasks are persisted before dispatch, and lease expiry
  redelivers a dead worker's in-flight tasks.
- **Queues**: named queues with weights, per-task priority, and bounded worker
  concurrency.
- **Task lifecycle**: retries with exponential backoff, delayed/scheduled
  tasks, per-task timeouts/deadlines, unique tasks, retention, dead-letter
  (archive), per-queue pause/resume, and a per-task-type circuit breaker.
- **Cron**: cluster-singleton scheduler; schedules survive restart and
  singleton failover, fire without double-firing across relocation, and are
  pausable at runtime; CLI/Admin management.
- **Clustering / HA**, on by default: multi-node with static and Kubernetes
  discovery, queue-grain relocation and lease recovery on node loss with zero
  task loss, and cluster-singleton scheduler/reaper.
- **Pluggable brokers**: a `Broker` interface with Postgres and in-memory
  implementations behind a shared conformance suite.
- **Four run modes** from one codebase: standalone, cluster, Kubernetes, and
  embedded (the whole server in-process).
- **Go SDK** free of protobuf and GoAkt types, a **CLI** (`conveyor`), and a
  ConnectRPC wire protocol intended as the public contract.
- **TypeScript SDK** (`sdks/typescript`, npm `@conveyorq/conveyor`): a producer
  `Client` and a `Worker` implementing the full session protocol (push-based
  dispatch, heartbeats, full-jitter reconnect, graceful drain), `Mux` routing
  with batch handlers and middleware, JSON/binary/text codecs, and AES-256-GCM
  end-to-end encryption byte-compatible with the Go SDK. ESM, Node 20+.
- **Python SDK** (`sdks/python`, PyPI `conveyorq`): an asyncio-native `Client`
  and `Worker` — plus synchronous `SyncClient`/`SyncWorker` wrappers over the
  same core — with `Mux` routing (batch handlers and middleware), JSON/binary/text
  codecs, and AES-256-GCM encryption byte-compatible with the Go SDK. Full type
  hints (`py.typed`), Python 3.9+. A task enqueued from any SDK runs on a worker
  written in any other.
- **Operations dashboard** embedded in `conveyord`: queues, tasks (filter,
  pagination, detail), cron, and a worker-topology view, with mutations
  (run/cancel/delete, pause/resume, cron edit), live auto-refresh, light/dark
  themes, and host-anywhere portability (configurable CORS).
- **Observability**: Prometheus `/metrics` (engine counters, timing histograms,
  health canaries, and GoAkt metrics), OpenTelemetry traces with enqueue→worker
  propagation and OTLP push export, plus a Grafana dashboard.
- **Security**: bearer-token auth that fails closed outside `--dev`, mutual TLS
  on cluster remoting, and hardened container `securityContext` defaults.
- **Deployment**: a multi-arch, cosign-signed image on GHCR, a Helm chart
  (StatefulSet), a Docker Compose quickstart, and a systemd unit.

### Changed

- **Go SDK relocated** from `sdk/` to `sdks/go/`, so all three SDKs live under
  `sdks/<language>/`. The import path is now
  `github.com/conveyorq/conveyor/sdks/go` (the package name is unchanged:
  `conveyor`). Done before the first tag, so no released version is affected.
- **Deploys are free**: a worker draining on shutdown (SIGTERM) now hands its
  in-flight tasks back with no retry penalty and no backoff — they become due
  immediately on another worker instead of consuming a retry. The drain-induced
  cancellation is reported as `RELEASED`, distinct from a genuine failure,
  deadline, or server cancel (which still count as a retry). A crashed worker is
  still recovered via lease expiry and does count, bounding poison tasks.

### Positioning

Verified against asynq, Faktory, and River: Conveyor matches their core
task-queue features and adds push-based dispatch (no polling), built-in
clustering/HA, an embeddable mode, and an operations dashboard that goes beyond
asynqmon's read-only inspection — mutations, a live worker-topology view, and
host-anywhere hosting.
