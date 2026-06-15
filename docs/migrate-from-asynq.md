# Migrating from asynq

[asynq](https://github.com/hibiken/asynq) is a Redis-backed Go task queue
embedded as a library in your application. Conveyor covers the same core
features but with a different shape, so the migration is mostly mechanical.

## What changes conceptually

|          | asynq                                    | Conveyor                                                                                                                        |
|----------|------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| Storage  | Redis                                    | Postgres (or in-memory for dev)                                                                                                 |
| Topology | A library in your worker process         | A **server** (`conveyord`) your clients and workers connect to — or an in-process [embedded](../README.md#embedded-mode) engine |
| Dispatch | Workers **poll** Redis                   | The server **pushes** tasks to connected workers over a stream                                                                  |
| Cron     | Registered in code via `asynq.Scheduler` | Persisted on the server; managed via the API/CLI                                                                                |
| Web UI   | asynqmon                                 | None yet (Grafana dashboard for metrics)                                                                                        |

The important practical consequence: with asynq your worker binary talks to
Redis directly; with Conveyor your client and worker talk to `conveyord`, which
owns the broker. Run one `conveyord` (plus Postgres) and point both at it.

## Enqueuing

```go
// asynq
client := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})
payload, _ := json.Marshal(WelcomeEmail{UserID: 42})
task := asynq.NewTask("email:welcome", payload)
client.Enqueue(task, asynq.Queue("critical"), asynq.MaxRetry(5), asynq.ProcessIn(5*time.Minute))
```

```go
// Conveyor
client, _ := conveyor.NewClient("http://localhost:8080")
task := conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: 42}))
client.Enqueue(ctx, task, conveyor.Queue("critical"), conveyor.MaxRetry(5), conveyor.ProcessIn(5*time.Minute))
```

### Option mapping

| asynq                              | Conveyor                                        | Notes                                                                                                                                                                                                    |
|------------------------------------|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `asynq.Queue(name)`                | `conveyor.Queue(name)`                          |                                                                                                                                                                                                          |
| `asynq.MaxRetry(n)`                | `conveyor.MaxRetry(n)`                          | retries after the first attempt                                                                                                                                                                          |
| `asynq.ProcessIn(d)`               | `conveyor.ProcessIn(d)`                         |                                                                                                                                                                                                          |
| `asynq.ProcessAt(t)`               | `conveyor.ProcessAt(t)`                         |                                                                                                                                                                                                          |
| `asynq.TaskID(id)`                 | `conveyor.TaskID(id)`                           | client-assigned id for idempotent retries                                                                                                                                                                |
| `asynq.Retention(d)`               | `conveyor.Retention(d)`                         | keep the completed task visible                                                                                                                                                                          |
| `asynq.Unique(ttl)`                | `conveyor.Unique(ttl)`                          | dedup by type+payload; add `conveyor.UniqueKey(k)` for an explicit key                                                                                                                                   |
| weighted priority queues           | `conveyor.Priority(1..9)` **and** queue weights | Conveyor also has a per-task priority                                                                                                                                                                    |
| `asynq.Timeout(d)` / `asynq.Deadline(t)` | `conveyor.Timeout(d)` / `conveyor.Deadline(t)`  | the handler `ctx` is canceled at the earliest of the timeout, the deadline, and the lease expiry (`engine.lease_ttl`); honor it. |
| `asynq.Group` (aggregation)        | —                                               | task aggregation is not in v1                                                                                                                                                                            |

Payloads: asynq takes raw `[]byte`; Conveyor takes a `conveyor.Payload` —
`conveyor.JSON(v)`, `conveyor.Bytes(b)`, or `conveyor.Proto(m)` — which also
records the content type.

## Processing

```go
// asynq
mux := asynq.NewServeMux()
mux.HandleFunc("email:welcome", func(ctx context.Context, t *asynq.Task) error {
    var email WelcomeEmail
    json.Unmarshal(t.Payload(), &email)
    return send(email)
})
srv := asynq.NewServer(asynq.RedisClientOpt{Addr: "localhost:6379"},
    asynq.Config{Concurrency: 10, Queues: map[string]int{"critical": 6, "default": 3}})
srv.Run(mux)
```

```go
// Conveyor
worker, _ := conveyor.NewWorker("http://localhost:8080",
    conveyor.WithConcurrency(10),
    conveyor.WithQueues(map[string]int{"critical": 6, "default": 3}))

mux := conveyor.NewMux()
mux.HandleFunc("email:welcome", func(ctx context.Context, task *conveyor.Task) error {
    var email WelcomeEmail
    if err := task.Bind(&email); err != nil {
        return conveyor.SkipRetry(err) // a payload that can't decode now never will
    }
    return send(email)
})
worker.Run(ctx, mux)
```

| asynq                                | Conveyor                                                                                  |
|--------------------------------------|-------------------------------------------------------------------------------------------|
| `asynq.NewServeMux()`                | `conveyor.NewMux()`                                                                       |
| `mux.HandleFunc(type, fn)`           | `mux.HandleFunc(type, fn)`                                                                |
| `asynq.NewServer(...).Run(mux)`      | `conveyor.NewWorker(...).Run(ctx, mux)`                                                   |
| `Config{Concurrency, Queues}`        | `conveyor.WithConcurrency(n)`, `conveyor.WithQueues(map)`                                 |
| `t.Payload()` + `json.Unmarshal`     | `task.Bind(&v)` (decodes by content type)                                                 |
| return `asynq.SkipRetry`             | return `conveyor.SkipRetry(err)`                                                          |
| `asynq.GetTaskID(ctx)` / retry count | `conveyor.GetTaskID(ctx)`, `conveyor.GetRetryCount(ctx)`, or `task.ID()`/`task.Retried()` |

A crashed worker mid-task is safe in both: asynq recovers via its lease, Conveyor
releases the in-flight task on disconnect (no retry penalty) and redelivers it.

## Cron

asynq registers periodic tasks in code with `asynq.Scheduler`. Conveyor persists
cron entries on the server, so they survive restarts and run from whichever node
holds the scheduler. Manage them with the CLI (or the Admin API):

```sh
conveyor cron add nightly-report "0 0 2 * * *" report:daily --queue reports
conveyor cron list
conveyor cron pause nightly-report
```

The spec is a 6-field cron expression (seconds first).

## Inspection

asynqmon and the `asynq` CLI map to the `conveyor` CLI and the Admin API:

```sh
conveyor stats                 # per-queue counts and pause flags
conveyor tasks list --state retry
conveyor tasks run <id>        # make a scheduled/retry task due now
conveyor queues pause critical
```

A read-only web dashboard is planned; today, metrics are exported in Prometheus
format (see the [operations guide](operations.md)).

## What you gain, what you give up

Honest framing — asynq is mature and excellent at what it does; switch only for
reasons that actually apply to you.

**Genuinely gain:**

- **No Redis.** If you already run Postgres, Conveyor needs no extra datastore,
  and you avoid sizing Redis persistence/eviction for durable jobs. (The reverse
  is also true: if you run Redis and not Postgres, asynq is the simpler choice.)
- **A clustered server tier with built-in HA.** asynq has no server process —
  coordination lives entirely in Redis, which is the single point of failure
  unless you run Redis Sentinel (Redis Cluster is only partially supported).
  Conveyor's server tier is clustered by default and a lost node's queues
  re-activate elsewhere.
- **Server-controlled dispatch.** The server pushes work with credit-based
  backpressure instead of every worker contending on the broker — this is about
  flow control and broker load, **not** raw latency (asynq's blocking dequeue is
  already near-real-time; don't switch expecting lower latency).
- **Per-task priority** (asynq orders by queue weight only) and an **embeddable**
  in-process mode.

**Give up:**

- **Maturity and ecosystem** — asynq is battle-tested with a large community and
  the asynqmon UI. Conveyor is pre-1.0.
- **Task aggregation/groups** and a **built-in web UI**.
- **The simplicity** of a single library plus Redis.

## Not yet covered

Task aggregation/groups and a built-in web UI are not in v1. Everything else in
asynq's core has a direct equivalent above.
