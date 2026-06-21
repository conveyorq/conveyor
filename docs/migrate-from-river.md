# Migrating from River

[River](https://github.com/riverqueue/river) is a Postgres-backed Go job queue
embedded as a library in your application, with type-safe job args. Conveyor is
also Postgres-first but ships as a **server** with a wire protocol, so the
shape differs more than with asynq. Read the trade-off below before migrating.

## The one thing to decide first: transactional enqueue

River's defining feature is **transactional insertion**: `InsertTx` enqueues a
job inside *your* database transaction, so the job and your business data commit
atomically (no lost or orphaned jobs).

**Conveyor does not do this.** Clients enqueue over the API to `conveyord`, which
owns the broker; the enqueue is durable on the server but is **not part of your
application's database transaction**. If transactional enqueue is load-bearing
for you, Conveyor is not a drop-in replacement. Keep River, or adopt the
[outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
(write an intent row in your transaction, enqueue to Conveyor from a relay).

Everything below assumes you've accepted that difference.

## What changes conceptually

|             | River                                                  | Conveyor                                                                                                 |
|-------------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Topology    | A library embedded in your app                         | A **server** clients/workers connect to (or an in-process [embedded](../README.md#embedded-mode) engine) |
| Job args    | Generic Go type implementing `JobArgs`                 | Opaque payload + content type (`conveyor.JSON`/`Bytes`/`Proto`), decoded with `task.Bind`                |
| Enqueue     | `Insert` / `InsertTx` (transactional)                  | `Enqueue` over the API (non-transactional)                                                               |
| Dispatch    | Poll + `LISTEN`/`NOTIFY`                               | Server pushes over a stream                                                                              |
| HA          | Postgres advisory-lock leader election                 | Built-in application-tier clustering with grain relocation on node loss                                  |
| Dead-letter | OSS: exhausted jobs go to a terminal `discarded` state | OSS: exhausted jobs are archived (inspectable), at parity                                                |
| Form factor | Library only                                           | Library (embedded) **and** standalone server with a language-neutral protocol                            |
| Web UI      | riverui                                                | Embedded operations dashboard                                                                            |

## Defining work

```go
// River: typed args + a typed worker
type WelcomeArgs struct{ UserID int }
func (WelcomeArgs) Kind() string { return "email:welcome" }

type WelcomeWorker struct{ river.WorkerDefaults[WelcomeArgs] }
func (w *WelcomeWorker) Work(ctx context.Context, job *river.Job[WelcomeArgs]) error {
    return send(job.Args.UserID)
}

workers := river.NewWorkers()
river.AddWorker(workers, &WelcomeWorker{})
```

```go
// Conveyor: a payload type you own + a handler keyed by task type
type WelcomeEmail struct{ UserID int }

mux := conveyor.NewMux()
mux.HandleFunc("email:welcome", func(ctx context.Context, task *conveyor.Task) error {
    var email WelcomeEmail
    if err := task.Bind(&email); err != nil {
        return conveyor.SkipRetry(err)
    }
    return send(email.UserID)
})
```

River binds the type to the job kind via `Kind()`; Conveyor routes by the task
type string you pass to `HandleFunc` and `NewTask`. `task.Bind(&v)` decodes the
payload the same way River unmarshals `job.Args`.

## Enqueuing

```go
// River
client.Insert(ctx, WelcomeArgs{UserID: 42}, &river.InsertOpts{
    Queue: "critical", Priority: 2, MaxAttempts: 5, ScheduledAt: time.Now().Add(5*time.Minute),
})
```

```go
// Conveyor
client.Enqueue(ctx, conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: 42})),
    conveyor.Queue("critical"), conveyor.Priority(2), conveyor.MaxRetry(4),
    conveyor.ProcessAt(time.Now().Add(5*time.Minute)))
```

### Option mapping

| River `InsertOpts`  | Conveyor                                         | Notes                                                                                                        |
|---------------------|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `Queue`             | `conveyor.Queue(name)`                           |                                                                                                              |
| `Priority` (1-4)    | `conveyor.Priority(1..9)`                        | different range; lower number = higher priority in River, **higher** number = higher priority in Conveyor    |
| `ScheduledAt`       | `conveyor.ProcessAt(t)` (or `ProcessIn(d)`)      |                                                                                                              |
| `MaxAttempts`       | `conveyor.MaxRetry(n)`                           | River counts the first try; Conveyor counts **retries after** the first, so `MaxAttempts: 5` ≈ `MaxRetry(4)` |
| `UniqueOpts`        | `conveyor.Unique(ttl)` / `conveyor.UniqueKey(k)` |                                                                                                              |
| client-assigned id  | `conveyor.TaskID(id)`                            |                                                                                                              |
| retention           | `conveyor.Retention(d)`                          |                                                                                                              |
| `Tags` / `Metadata` | *(none yet)*                                     | attaching arbitrary metadata at enqueue is not exposed in the SDK yet                                        |

## Periodic jobs

River registers periodic jobs in code (`PeriodicJobs`), held in the leader's
memory. Conveyor persists cron entries on the server, so they survive restarts
and failover. Manage them with the CLI or Admin API:

```sh
conveyor cron add nightly-report "0 0 2 * * *" report:daily --queue reports
```

## Inspection

riverui maps to the `conveyor` CLI and Admin API (a web dashboard is planned):

```sh
conveyor stats
conveyor tasks list --state retry
conveyor tasks run <id>
```

## What you gain, what you give up

These are the **honest** differences. Both are capable Postgres-first queues, and
several things people assume differ actually don't.

**Genuinely gain:**

- **Application-tier clustering.** River coordinates through Postgres (row locks
  plus an advisory-lock leader for maintenance); there is no clustered River
  process. Conveyor's *server tier* is itself a cluster (membership, singletons,
  and automatic relocation of a queue's owner when a node dies), independent of
  the database. This matters when you want coordinated distributed dispatch
  beyond what row-locking provides; it is operational surface you don't need
  otherwise.
- **Two form factors from one codebase.** Embed the whole engine in a Go process,
  or run a standalone server that any language can talk to over the protocol.
  River is a Go library.

**Roughly equal (don't switch for these):** retries/backoff, scheduling, unique
jobs, priorities, multiple queues, and dead-lettering (River's `discarded` vs
Conveyor's archived) are all present in both OSS.

**Give up:**

- **Transactional enqueue**, River's signature feature (see the top of this
  page). Conveyor has no equivalent.
- **Type-safe generic job args.** Conveyor uses opaque payloads plus `Bind`.
- **A more mature, commercially backed project** with wider adoption and the
  polished **riverui**. Conveyor ships its own operations dashboard, but River is
  further along, so weigh that seriously for production.
