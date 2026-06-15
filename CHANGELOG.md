# Changelog

All notable changes to Conveyor are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

First public release — a distributed task queue for Go: a persistent,
push-based queue with at-least-once execution, backed by Postgres or an
in-memory broker, with no Redis and no polling.

### Added

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

### Positioning

Verified against asynq, Faktory, and River: Conveyor matches their core
task-queue features and adds push-based dispatch (no polling), built-in
clustering/HA, an embeddable mode, and an operations dashboard that goes beyond
asynqmon's read-only inspection — mutations, a live worker-topology view, and
host-anywhere hosting.
