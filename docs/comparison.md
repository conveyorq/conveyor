# How it compares

**Conveyor** is a durable task queue. Its closest peers are
[asynq](https://github.com/hibiken/asynq) (Redis-backed) and
[River](https://github.com/riverqueue/river) (Postgres-backed), both Go
libraries. **Conveyor** is Postgres-first like River, but ships as a clustered
**server** with a language-neutral wire protocol and **push-based** dispatch,
serves Go, TypeScript, and Python SDKs, and also runs embedded inside a Go
process.

| Capability                    |                                                                            Conveyor                                                                            |            asynq             |                 River                  |
|-------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------:|:----------------------------:|:--------------------------------------:|
| Primary store                 |                                                                     Postgres or in-memory                                                                      |            Redis             |                Postgres                |
| Runs as                       |                                                                  Server and embedded library                                                                   |           Library            |                Library                 |
| Dispatch                      |                                                                        Push (streaming)                                                                        |             Poll             |      Poll plus `LISTEN`/`NOTIFY`       |
| HA / failover                 | Built-in clustering; a lost node's work re-activates elsewhere. Kubernetes and static discovery out of the box, or a pluggable provider interface | Via Redis (Sentinel/Cluster) | Postgres advisory-lock leader election |
| Transactional enqueue         |                                                                               ✗                                                                                |              ✗               |                  ✓ ¹                   |
| Atomic multi-task enqueue     |                                                                        ✓ (`EnqueueTx`)                                                                         |              ✗               |            ✓ (`InsertMany`)            |
| SDK languages                 |                                                                     Go, TypeScript, Python                                                                     |              Go              |                   Go                   |
| Weighted queues               |                                                                               ✓                                                                                |              ✓               |                   ✗                    |
| Per-task priority             |                                                                           ✓ (1 to 9)                                                                           |      ✗ (queue weights)       |                   ✓                    |
| Delayed / scheduled           |                                                                               ✓                                                                                |              ✓               |                   ✓                    |
| Reschedule a task's run time  |                                                                               ✓                                                                                |              ✗               |                   ✗                    |
| Cron / periodic               |                                                            ✓ (server-persisted, survives failover)                                                             |         ✓ (in code)          |              ✓ (in code)               |
| Unique tasks                  |                                                                               ✓                                                                                |              ✓               |                   ✓                    |
| Retries with backoff          |                                                     ✓ (exponential/linear/fixed, server-wide or per task)                                                      |              ✓               |                   ✓                    |
| Dead-letter / archive         |                                                                               ✓                                                                                |              ✓               |                   ✓                    |
| Pause / resume queues         |                                                                               ✓                                                                                |              ✓               |                   ✓                    |
| Rate limiting                 |                                                                      ✓ (per-queue, live)                                                                       |            DIY ²             |                   ✗                    |
| Per-key concurrency           |                                                                               ✓                                                                                |              ✗               |                   ✗                    |
| Circuit breaker               |                                                                       ✓ (per task type)                                                                        |              ✗               |                   ✗                    |
| Task dependencies (workflows) |                                                                     ✓ (chains, fan-out/in)                                                                     |              ✗               |                 Pro ³                  |
| Group aggregation / batching  |                                                                      ✓ (per-group, live)                                                                       |              ✓               |                   ✗                    |
| End-to-end payload encryption |                                                                               ✓                                                                                |              ✗               |                   ✗                    |
| Lifecycle events / webhooks   |                                                                               ✓                                                                                |              ✗               |                   ✗                    |
| Task progress reporting       |                                                                               ✓                                                                                |              ✗               |                   ✗                    |
| Web operations UI             |                                                                    Embedded, read and write                                                                    |           asynqmon           |                riverui                 |

¹ River commits the job inside *your* database transaction (`InsertTx`), so the
job and your data commit atomically. Conveyor's broker is owned by the server,
so an enqueue is durable but not part of your application transaction. If you
need that atomicity, use the
[outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
or stay on River. It is the main reason to pick River; see
[migrating from River](migrate-from-river.md).

² asynq rate limiting is the `RateLimitError` pattern (you supply the limiter),
not a built-in server-side limiter.

³ Workflow DAGs are a commercial River Pro feature, not part of the OSS core.

The table reflects each project's open-source core at the time of writing; all
three are actively developed, so check upstream for changes (corrections welcome
via a PR). For durable, long-running *business* workflows (sagas, multi-day
orchestrations), a workflow engine such as Temporal or Cadence is a different and
complementary category: Conveyor is a task queue, not a workflow engine.

For the API-level mapping when moving an existing codebase, see [migrating from asynq](migrate-from-asynq.md) and [migrating from River](migrate-from-river.md).
