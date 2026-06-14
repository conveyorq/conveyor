# Benchmark harness

A reproducible harness that drives a fixed workload of no-op tasks through an
**embedded** Conveyor engine (the full server in-process, minus the network hop)
and reports end-to-end throughput and latency. It is the foundation for the
asynq/River comparison; read the honesty notes before quoting any number.

## Running

```sh
make benchmark                         # 20k tasks, in-memory broker (no infra)

# Against a real Postgres broker:
go run ./benchmark --tasks=20000 --concurrency=50 \
  --broker=postgres --dsn="postgres://user:pass@host:5432/db?sslmode=disable"
```

Flags: `--tasks`, `--concurrency` (worker), `--producers` (concurrent enqueuers),
`--broker` (`memory` | `postgres`), `--dsn`.

## What it measures

The harness enqueues all N tasks as fast as it can, then drains them. So:

- **Throughput** = N ÷ (first enqueue → last completion). This is the headline
  number: tasks per second the engine sustains under saturation.
- **Latency** (p50/p95/p99) is **end-to-end under saturation** — it includes
  time spent waiting in the backlog, so it reflects drain behavior, not idle
  per-task latency. Do not read it as "how long one task takes."

## Honesty notes — read before quoting

1. **Full-pipeline, not engine-internal.** This measures the whole client→worker
   path — enqueue RPC, durable commit, dispatch stream, and completion report.
   It is heavier than the engine's internal per-queue grain dispatch, which
   sustains ~10k tasks/s on this hardware (the `TestQueueGrainDispatchThroughput`
   gate; skipped in the suite only because the CI pass runs under `-race`, where
   instrumentation slows it ~10×). So the headline here is the realistic end-to-
   end rate, and the grain is not the bottleneck.
2. **Hardware- and workload-specific.** These numbers are from one machine with a
   trivial handler. They are not comparable to vendor-published numbers measured
   on other hardware with other workloads.
3. **No head-to-head yet.** A fair asynq/River comparison needs all three on
   identical hardware with the same workload; publishing one before that would be
   misleading. The harness ships the Conveyor runner today; asynq (Redis) and
   River (Postgres) runners will be added as a separate module (to isolate their
   dependencies). Expect a raw-throughput comparison to favor an in-process
   library (asynq, River) over Conveyor's durable server + wire protocol — that
   overhead is the price of the architecture, not a defect.

**Conveyor's case is not raw throughput** — it is the architecture: Postgres-first
with no Redis, a clustered server tier with built-in HA, an embeddable mode, and
a language-neutral protocol. Those gains are documented in the migration guides
([asynq](../docs/migrate-from-asynq.md), [River](../docs/migrate-from-river.md)),
not in this number.

## Indicative results

One local run, 20k tasks, concurrency 50 (Apple Silicon) — re-run on your own
hardware before drawing conclusions:

| Broker           | Throughput    | Notes                                           |
|------------------|---------------|-------------------------------------------------|
| memory           | ~4.5k tasks/s | full client→worker pipeline (grain itself ~10k) |
| postgres (local) | ~1.8k tasks/s | bounded by durable commit to Postgres           |
