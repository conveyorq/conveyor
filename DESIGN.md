# Conveyor — Design Document

**A distributed, durable task processing system built on the GoAkt actor framework**

|                |                                                                                                                                                                   |
|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Status         | Draft v2.2 — ready for implementation; GoAkt usage verified against v4.2.8 source; implementation plan carries competitive launch gates                           |
| Supersedes     | v1 (library-only design). Key changes: ships as an application with deployment modes; protobuf removed from the public API; workers connect over a wire protocol. |
| Target runtime | Go 1.26+, GoAkt v4 (`github.com/tochemey/goakt/v4`, verified at v4.2.8), ConnectRPC                                                                               |
| Working name   | `conveyor` (server binary `conveyord`, CLI `conveyor`, module `github.com/ORG/conveyor`)                                                                          |
| Audience       | Implementing agent (Claude Code) + human reviewers                                                                                                                |

---

## 1. Overview

Conveyor is a background task processing **system**: a clustered server application (`conveyord`) plus thin SDKs. Applications enqueue tasks through a client SDK or HTTP/gRPC API; worker processes register handlers and receive pushed work over a persistent stream. The server provides retries, priorities, scheduling, uniqueness, dead-lettering, inspection, and metrics.

Three layers, strictly separated:

- **Durable task log** (pluggable `Broker`: Postgres first, in-memory for dev/tests, SQLite fast-follow) is the *source of truth*. Tasks are persisted **before** dispatch and survive any crash.
- **GoAkt actor cluster** (inside `conveyord`) is the *coordination layer*: queue dispatch, credit-based flow control, cron, lease reaping, dead letters. Clustering is **always on** — standalone mode is a cluster of one, running the identical code path.
- **Worker plane**: user processes running the worker SDK, connected to the server over one bidirectional ConnectRPC stream each. User code never joins the actor cluster and never sees protobuf.

This inverts asynq's model (workers poll Redis): Conveyor is **push-based** end to end. Queue grains notify gateways the instant work exists; gateways stream tasks to workers with free capacity.

### 1.1 Positioning

| Capability       | asynq                   | Faktory                   | River                          | Conveyor                                            |
|------------------|-------------------------|---------------------------|--------------------------------|-----------------------------------------------------|
| Form factor      | Go library              | Server + polyglot clients | Go library (+ riverui)         | Server + SDKs **and** embeddable Go package         |
| Storage          | Redis                   | Embedded RocksDB          | Postgres only                  | Pluggable (`Broker`): Postgres, memory; SQLite next |
| Dispatch         | Worker polls            | Worker fetch loop         | Poll + LISTEN/NOTIFY assist    | Server pushes over stream (credit-based)            |
| Clustering / HA  | N/A (Redis is the SPOF) | Enterprise only           | N/A (Postgres coordinates)     | Built in, default; grain relocation on node loss    |
| Priority queues  | Weighted polling        | Strict ordering           | Per-job priority               | Weighted credit dispatch + per-task priority        |
| Cron             | Scheduler process       | Enterprise                | Periodic jobs (in-process)     | Cluster-singleton scheduler, pausable at runtime    |
| Uniqueness       | Redis SETNX             | Enterprise                | Built-in unique jobs           | Broker constraint + grain fast path                 |
| Polyglot workers | No (Go)                 | Yes                       | Insert-only clients            | Protocol-ready; Go SDK in v1, Python/TS later       |
| Zero-infra mode  | Needs Redis             | Needs server              | Needs Postgres                 | Embedded mode: whole server in-process              |
| Inspection UI    | asynqmon                | Web UI                    | riverui                        | Read-only dashboard in v1 (§17 P7); full UI v2      |

### 1.2 Goals (v1)

1. Core feature parity with asynq: named queues with weights, bounded worker concurrency, retries with backoff, deadlines/timeouts, delayed tasks, cron, unique tasks, retention/archive, dead-letter inspection, OTel metrics — delivered through the server + Go SDK.
2. **At-least-once** execution with crash safety across server nodes *and* worker disconnects.
3. Four deployment modes from one codebase: **standalone**, **cluster**, **kubernetes**, **embedded** (§4).
4. A wire protocol (ConnectRPC) clean enough to be a public contract: future SDKs, CLI, and web UI all consume it.
5. Public Go API free of protobuf and free of GoAkt types. Payloads are bytes + content type with codec helpers; JSON is the default codec.

### 1.3 Non-goals (v1)

- Exactly-once execution (idempotent-handler contract instead, documented).
- Workflow orchestration / DAGs.
- Group aggregation (v2, design hook §16).
- Full web UI (v2). v1 ships a **minimal read-only dashboard** (§17 Phase 7) so launch day isn't conceded to asynqmon/riverui; all mutations stay CLI/API until v2.
- Multi-datacenter routing (v2; design stays DC-compatible).
- Non-Go SDKs (v2; protocol is the enabler, Go SDK is the reference implementation).
- Multi-tenancy/namespaces (v2; auth is single-tenant token-based in v1).

---

## 2. Semantics & guarantees

Invariants every component must preserve. Unchanged from v1 except G7 (new trust boundary).

**G1 — Durable before dispatched.** `Enqueue` returns success only after the broker commit. Actor wake-ups are best-effort hints; the sweep loop (§8.4) recovers lost ones.

**G2 — At-least-once.** A `pending`/`retry` task is eventually executed; execution is recorded by a durable ack. Worker death between lease and ack → lease expiry → re-delivery. **Handlers must be idempotent** (documented user contract).

**G3 — Single active execution per task (best effort).** Leasing is atomic in the broker. Double-run is possible only when a lease expires under a still-running stalled handler — identical to asynq. Handler contexts carry the effective deadline.

**G4 — No cross-task ordering guarantees.** Priority-then-FIFO best effort within a queue; retries reorder. Documented.

**G5 — Actor and session state is disposable.** Any grain, singleton, or gateway session may die at any time; on (re)activation state is rebuilt from the broker. No correctness depends on in-memory state.

**G6 — Uniqueness.** `Unique(ttl)` tasks are rejected with `ErrDuplicateTask` while an incomplete task with the same key exists. Enforced by broker constraint; grain checks are an optimization.

**G7 — Workers are untrusted-ish.** Worker processes hold only a stream session and a bearer token. They never touch the broker, never receive lease credentials usable outside their session, and a misbehaving worker can at worst stall its own leased tasks until expiry. All state transitions are executed server-side by the gateway.

---

## 3. System architecture

```
            (HTTP/JSON or gRPC via ConnectRPC, bearer token, TLS)
┌─────────────┐  Enqueue/Admin  ┌──────────────────────────────────────────┐
│ Client SDK  │ ───────────────▶│            conveyord node(s)             │
│ or curl/CLI │                 │  ┌────────────────────────────────────┐  │
└─────────────┘                 │  │ API layer (stateless)              │  │
                                │  │ TaskService · AdminService         │  │
┌─────────────┐  Session        │  │ WorkerService (bidi stream)        │  │
│ Worker SDK  │◀───────────────▶│  └───────┬───────────────▲────────────┘  │
│ (user code, │   stream:       │          │ Tell          │ stream        │
│  handlers)  │   dispatch ↓    │  ════════▼═══ GoAkt cluster ═══════════  │
└─────────────┘   results ↑     │  ┌──────────────┐  ┌─────────────────┐   │
                                │  │ QueueGrain   │◀─│ SchedulerSinglt │   │
       ┌────────────────────┐   │  │ (per queue)  │  │ cron + delayed  │   │
       │ Durable task log   │◀──┼──│ leases,      │  ├─────────────────┤   │
       │ (Broker: Postgres) │   │  │ credits      │  │ ReaperSingleton │   │
       └────────────────────┘   │  └──────┬───────┘  └─────────────────┘   │
              ▲                 │         │ dispatch                       │
              │ Ack/Fail/       │  ┌──────▼────────┐                       │
              │ Archive/Release │  │ WorkerGateway │ one per worker        │
              └─────────────────┼──│ (per session) │ stream session        │
                                │  └───────────────┘                       │
                                └──────────────────────────────────────────┘
```

### 3.1 Planes

- **API plane** — stateless ConnectRPC services on every node (one port serves gRPC, gRPC-Web, and HTTP/JSON). Any node accepts any request; enqueue writes to the broker then `Tell`s the owning grain (location-transparent).
- **Control plane** — the GoAkt cluster: `QueueGrain` per queue, `WorkerGateway` per connected worker session, `SchedulerSingleton`, `ReaperSingleton`. Remoting + discovery per deployment mode.
- **Data plane** — the broker (§7). The only durable state.
- **Worker plane** — SDK processes owned by the user. Connect, declare queues + concurrency, receive dispatches, return results.

### 3.2 Task lifecycle flow

1. `Enqueue` (SDK/HTTP/CLI) → API node validates, assigns ULID if absent, commits to broker (`pending` or `scheduled`), then `Tell`s `queue/<name>` grain `TaskEnqueued`.
2. `QueueGrain` runs a lease cycle when it has credits: `Broker.Lease(queue, n, ttl, leaseID)`, then distributes `ExecuteTask` to gateways with credits (weighted round-robin).
3. `WorkerGateway` forwards the task down its stream as a `Dispatch` frame; decrements session credits.
4. Worker SDK runs the matched handler with deadline = min(task deadline, lease expiry, task timeout); sends `Result` frame (success / retryable error / skip-retry / canceled).
5. Gateway executes the durable transition: `Ack`, `Fail(backoff)`, or `Archive`; `Tell`s the grain `TaskCompleted` (refill signal).
6. Worker heartbeats list active task IDs; gateway extends those leases. Stream death → gateway `Release`s that session's in-flight tasks (immediate re-delivery, no retry increment) and stops.

### 3.3 Worker session protocol (the public wire contract)

One bidi stream per worker process (`WorkerService.Session`). Frames (proto on the wire; SDK users never see them):

```
worker → server:  Hello{queues, concurrency, labels, sdk_version}
                  Credit{n}                       // free slots opened
                  Result{task_id, outcome, error_msg, result_bytes}
                  Heartbeat{active_task_ids}      // every lease_ttl/3
server → worker:  Welcome{session_id, lease_ttl, heartbeat_interval}
                  Dispatch{task}                  // task = id, type, queue, payload bytes,
                                                  //   content_type, metadata, deadline, retried
                  Cancel{task_id}                 // Inspector-initiated cancellation
                  Ping{}
```

Outcomes: `SUCCESS`, `RETRY` (server computes backoff), `SKIP_RETRY` (archive now), `RELEASED` (graceful shutdown re-queue, no retry increment). Flow control is credits-only; the server never buffers more dispatches than granted credits (G5: undispatched work stays in the broker, not in memory).

---

## 4. Deployment modes

One codebase; modes differ only in config defaults, discovery provider, and packaging. **Clustering is always compiled in and always active** — standalone is a one-node cluster with static self-discovery, so there is no separate non-clustered path to test or maintain.

| Mode           | Discovery                                            | Broker default                                | Packaging                                                                                                                                                | Intended for                           |
|----------------|------------------------------------------------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| **standalone** | static (self)                                        | Postgres DSN required; `--dev` uses in-memory | single binary, Docker image, systemd unit                                                                                                                | dev, small prod, edge                  |
| **cluster**    | static list, NATS, Consul, etcd, mDNS, DNS-SD        | Postgres                                      | binary/Docker + config file                                                                                                                              | VMs, bare metal                        |
| **kubernetes** | GoAkt k8s provider (pod-label discovery via API server) | Postgres                                   | Helm chart: Deployment (server is stateless), Role+RoleBinding (pods list/watch), named container ports (remoting/discovery/peers), PDB, ServiceMonitor; HPA-safe | the flagship mode                      |
| **embedded**   | static (self)                                        | memory or Postgres                            | Go package `github.com/ORG/conveyor/embedded`                                                                                                            | Go apps wanting asynq-style zero infra |

Mode selection: `conveyord --mode=kubernetes --config=/etc/conveyor/conveyor.yaml` (mode also settable in config/env). Embedded mode starts the same server components in-process and hands back SDK objects wired over an in-memory transport (`bufconn`-style), so user code is identical whether embedded or remote — the migration path from embedded to a real cluster is changing one constructor.

Scaling notes (document in ops guide): `conveyord` nodes are stateless (G5) → scale horizontally freely; grains rebalance. The broker is the throughput ceiling; sizing guidance ships with benchmarks (§18.6). Rolling upgrades: workers tolerate server restarts via SDK reconnect-with-jitter; in-flight tasks released on disconnect are re-delivered.

---

## 5. Repository layout

```
conveyor/
├── go.mod
├── cmd/
│   ├── conveyord/main.go        # server: flags, config load, mode wiring
│   └── conveyor/                # CLI (cobra): enqueue, stats, queues, tasks, cron
├── sdk/                          # PUBLIC Go API — no protobuf, no GoAkt types
│   ├── conveyor.go              # Client, Worker, Mux, Task, options, sentinel errors
│   ├── codec.go                 # JSON (default), Bytes, Proto codecs; Task.Bind
│   └── internal/transport/      # ConnectRPC client + stream session management
├── embedded/embedded.go         # in-process server + loopback SDK constructors
├── server/                       # application assembly (importable, but not the public API)
│   ├── server.go                # boots broker + actor system + API listeners
│   ├── config.go                # yaml/env/flags (koanf or similar), validation
│   └── api/                     # ConnectRPC service implementations
│       ├── taskservice.go  ├── workerservice.go  └── adminservice.go
├── internal/
│   ├── broker/                  # storage layer — internal: only sdk/ and embedded/ are public API
│   │   ├── broker.go            # Broker interface + CronEntry type
│   │   ├── brokertest/          # conformance suite (§18.1), run by every broker impl
│   │   ├── memory/memory.go
│   │   └── postgres/{postgres.go, migrate.go, migrations/}
│   ├── actors/{queuegrain.go, gateway.go, scheduler.go, reaper.go}
│   ├── backoff/backoff.go
│   ├── clock/clock.go           # injectable clock; time.Now() forbidden elsewhere
│   └── proto/                   # generated (wire + actor messages) — never exported
├── protos/conveyor/v1/          # task.proto, messages.proto (actors), service.proto (wire)
├── buf.yaml / buf.gen.yaml
├── deploy/
│   ├── helm/conveyor/           # chart: values for replicas, broker DSN secret, TLS, tokens
│   ├── docker/Dockerfile        # distroless, nonroot
│   ├── compose/dev.yaml         # conveyord + postgres for local dev
│   ├── grafana/                 # dashboard JSON + Prometheus scrape example (ships with the chart)
│   └── systemd/conveyord.service
├── web/dashboard/               # v1 read-only UI: static SPA, go:embed'd into conveyord, AdminService JSON only
├── examples/{standalone, kubernetes, embedded}/
└── Makefile                     # test lint proto integration chaos release helm-lint
```

Hard rules: `sdk/` and `embedded/` import nothing from `internal/proto` or GoAkt in their exported signatures (`go vet` custom check in CI). `protos/` is versioned wire contract — additive changes only after v1.0.

---

## 6. Data model

### 6.1 Task states

```
                    ┌──────────── retry due ───────────┐
                    ▼                                  │
scheduled ──due──▶ pending ──lease──▶ active ──fail──▶ retry
                    ▲                  │ │ │
   release/expiry ──┘          ack ────┘ │ └─exhaust──▶ archived (dead-letter)
                                         └────────────▶ completed (retained, purged)
```

`canceled` reachable from `scheduled|pending|retry` via Admin API; active tasks get a best-effort `Cancel` frame to the executing worker.

### 6.2 Internal task envelope (proto, `protos/conveyor/v1/task.proto`)

Protobuf is internal/wire only. The payload is opaque bytes with a content type — no `Any`, no user-visible proto:

```proto
message TaskEnvelope {
  string id = 1;                  // ULID
  string queue = 2;
  string type = 3;                // handler routing key, "email:welcome"
  bytes  payload = 4;             // opaque; SDK codecs own encoding
  string content_type = 5;        // "application/json" (default), "application/octet-stream", ...
  map<string,string> metadata = 6;// traceparent, user tags
  TaskOptions options = 7;
  int32 retried = 8;
  string last_error = 9;
  google.protobuf.Timestamp enqueued_at = 10;
}
message TaskOptions {
  int32 max_retry = 1;            // default 25
  google.protobuf.Duration timeout = 2;
  google.protobuf.Timestamp deadline = 3;
  google.protobuf.Timestamp process_at = 4;
  string unique_key = 5;
  google.protobuf.Duration unique_ttl = 6;
  google.protobuf.Duration retention = 7;
  int32 priority = 8;             // 0..9, default 4
  reserved 9 to 15;               // v2: groups, rate limits
}
```

### 6.3 Postgres schema

Identical to v1 design with one addition (`content_type` lives inside the serialized envelope; no new column) plus a small queue-state table:

```sql
CREATE TABLE conveyor_tasks (
  id TEXT PRIMARY KEY, queue TEXT NOT NULL DEFAULT 'default', type TEXT NOT NULL,
  state SMALLINT NOT NULL, priority SMALLINT NOT NULL DEFAULT 4,
  payload BYTEA NOT NULL,                      -- serialized TaskEnvelope
  unique_key TEXT, unique_expires_at TIMESTAMPTZ,
  process_at TIMESTAMPTZ NOT NULL DEFAULT now(), deadline TIMESTAMPTZ,
  max_retry INT NOT NULL DEFAULT 25, retried INT NOT NULL DEFAULT 0, last_error TEXT,
  lease_id TEXT, lease_expires_at TIMESTAMPTZ,
  result BYTEA, retention INTERVAL NOT NULL DEFAULT '0',
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(), completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX conveyor_tasks_dispatch_idx ON conveyor_tasks (queue, priority DESC, process_at, id) WHERE state IN (2,4);
CREATE INDEX conveyor_tasks_lease_idx     ON conveyor_tasks (lease_expires_at) WHERE state = 3;
CREATE INDEX conveyor_tasks_scheduled_idx ON conveyor_tasks (process_at) WHERE state = 1;
CREATE UNIQUE INDEX conveyor_tasks_unique_idx ON conveyor_tasks (unique_key)
  WHERE unique_key IS NOT NULL AND state IN (1,2,3,4);

CREATE TABLE conveyor_queue_state (
  queue TEXT PRIMARY KEY, paused BOOLEAN NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE conveyor_cron_entries (
  id TEXT PRIMARY KEY, spec TEXT NOT NULL, task_type TEXT NOT NULL, queue TEXT NOT NULL,
  payload BYTEA NOT NULL, options BYTEA NOT NULL,
  paused BOOLEAN NOT NULL DEFAULT false, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Leasing remains a single `FOR UPDATE SKIP LOCKED` statement (one round trip). Time-dependent statements take *now* as a bind parameter ($5) from the injected clock — never `now()` — so the broker conformance suite can fast-forward time identically on every broker (§18.4) and app/db clock skew can't distort lease expiry (column `DEFAULT now()` stays only as a fallback for ad-hoc inserts):

```sql
WITH due AS (
  SELECT id FROM conveyor_tasks
  WHERE queue = $1 AND state IN (2,4) AND process_at <= $5
  ORDER BY priority DESC, process_at, id LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE conveyor_tasks t SET state = 3, lease_id = $3,
  lease_expires_at = $5 + $4, updated_at = $5
FROM due WHERE t.id = due.id RETURNING t.payload;
```

---

## 7. Broker layer

Unchanged from v1 except one new method (`Release`, for graceful worker disconnects) and queue-state persistence. Actors and API never touch SQL; everything goes through this interface. Every implementation takes its notion of *now* from the injected `clock.Clock` (passed into SQL as a bind parameter, §6.3) — this is what lets one conformance suite drive time-based behavior (lease expiry, unique TTL, retention) on all brokers without sleeps.

```go
type Broker interface {
    Enqueue(ctx context.Context, t *conveyorv1.TaskEnvelope) error      // ErrDuplicateTask on unique conflict (G6)
    Lease(ctx context.Context, queue string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error)
    ExtendLease(ctx context.Context, taskID, leaseID string, ttl time.Duration) error // ErrLeaseLost
    Ack(ctx context.Context, taskID, leaseID string, result []byte) error
    Fail(ctx context.Context, taskID, leaseID, errMsg string, processAt time.Time) error // retried++
    Release(ctx context.Context, taskID, leaseID string) error          // -> pending now, NO retried++ (graceful disconnect)
    Archive(ctx context.Context, taskID, leaseID, errMsg string) error  // leaseID "" allowed for reaper

    ReapExpiredLeases(ctx context.Context, limit int) (queues []string, err error)
    PromoteScheduled(ctx context.Context, limit int) (queues []string, err error)
    PurgeCompleted(ctx context.Context, limit int) (int, error)
    PendingCount(ctx context.Context) (map[string]int64, error)

    // queue state + inspection (Admin API)
    SetQueuePaused(ctx context.Context, queue string, paused bool) error
    QueuePaused(ctx context.Context, queue string) (bool, error)
    GetTask(ctx context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error)
    ListTasks(ctx context.Context, q TaskQuery) ([]*conveyorv1.TaskEnvelope, error)
    CancelTask(ctx context.Context, id string) error
    DeleteTask(ctx context.Context, id string) error
    RunTaskNow(ctx context.Context, id string) error

    // cron persistence
    UpsertCronEntry(ctx context.Context, e *CronEntry) error
    ListCronEntries(ctx context.Context) ([]*CronEntry, error)
    SetCronPaused(ctx context.Context, id string, paused bool) error
    DeleteCronEntry(ctx context.Context, id string) error

    Close() error
}
```

In-memory broker: mutex-guarded maps + per-queue priority heap on `(priority desc, process_at, id)`; passes the same conformance suite as Postgres (§18.1).

---

## 8. Actor topology (server-internal)

Actor messages are protobuf (`protos/conveyor/v1/messages.proto`); design rule unchanged: **messages carry hints and identities; the broker carries data.** The only data-bearing message is `ExecuteTask`, whose task is already durably leased.

```proto
message TaskEnqueued   { string queue = 1; }
message TasksAvailable { string queue = 1; int64 hint = 2; }
message RegisterGateway{ string queue = 1; string gateway_address = 2; int32 capacity = 3; }
message GatewayCredit  { string queue = 1; string gateway_address = 2; int32 credits = 3; }
message ExecuteTask    { conveyor.v1.TaskEnvelope task = 1; string lease_id = 2;
                         google.protobuf.Timestamp lease_expires_at = 3; }
message TaskCompleted  { string task_id = 1; string queue = 2; bool success = 3; }
message DrainQueue     { string queue = 1; }
message ResumeQueue    { string queue = 1; }
message CancelActive   { string task_id = 1; }   // grain -> gateway -> worker Cancel frame
```

### 8.1 QueueGrain (one per named queue)

GoAkt **Grain** (implements `OnActivate`/`OnReceive`/`OnDeactivate`), identity `queue/<name>` via `ActorSystem.GrainIdentity` — virtual-actor single activation (cluster-registry CAS ownership claim) gives exactly one live dispatcher per queue cluster-wide, no leader election. Grain kinds register in `ClusterConfig.WithGrains(...)`. The grain reaches the broker through a system **extension** (`GrainContext.Extension("broker")`) — extensions are system-wide and need no serialization, unlike grain `Dependencies` (which must be `BinaryMarshaler`s for relocation). State (rebuilt in `OnActivate`, G5): registered gateways with credit counts; `paused` (loaded from `conveyor_queue_state`); a `leasing` guard.

- Wake-ups (`TaskEnqueued`, `TasksAvailable`, `GatewayCredit`, `TaskCompleted`) trigger a **lease cycle** when unpaused and credits > 0: `Broker.Lease(queue, min(totalCredits, batchMax), ttl, newLeaseID())` via `GrainContext.PipeToSelf` (broker I/O never blocks the grain's turn), then weighted round-robin `ExecuteTask` to gateways (`TellActor`), decrementing credits; a full batch sends itself another wake-up.
- `DrainQueue` → set the `paused` flag and persist via `SetQueuePaused`. While paused, wake-up hints are **dropped, not stashed** — grains have no `Become`/`Stash` (those are actor `ReceiveContext` APIs, not `GrainContext`). Dropping is safe: hints carry no data (G1), undispatched work stays in the broker, `ResumeQueue` clears the flag and triggers an immediate lease cycle, and the reaper sweep (§8.4) backstops any gap.
- The grain mailbox is FIFO, optionally bounded via `WithGrainMailboxCapacity` (no priority mailbox exists for grains) — fine, since grain traffic is small control hints only.
- Passivation via `WithGrainDeactivateAfter(5m)` (GoAkt's default is 2m); gateways heartbeat `RegisterGateway` every 30s, which also heals grain relocation. Relocation on node loss is automatic for grains; a `RelocationFailed` event is logged + alerted, and the next message to the identity re-activates the grain regardless.

Credits-not-push-blindly rationale: a saturated worker plane simply stops granting credits and work stays safely in the broker — never buffered in mailboxes.

### 8.2 WorkerGateway (one per worker stream session)

Spawned by `WorkerService.Session` on `Hello` with `WithLongLived()` (never passivates while the stream lives) and `WithRelocationDisabled()` (a gateway is bound to its node-local stream and must die with its node, never relocate); it is the bridge between the actor world and one worker's stream, and the **only** component that executes durable transitions for that worker's tasks (G7).

- On spawn: `RegisterGateway` to each declared queue grain with capacity = worker concurrency; grants initial credits.
- On `ExecuteTask`: forward `Dispatch` frame; track `taskID → (leaseID, queue, deadline)` in session state.
- On worker `Result`: map outcome → `Broker.Ack` / `Fail(now+backoff(retried))` / `Archive` / `Release`; backoff is server-side (full-jitter exponential, base 2s, cap 15m, configurable). Then `TaskCompleted` + `GatewayCredit{1}` to the grain.
- On worker `Heartbeat{active_ids}`: `ExtendLease` each; `ErrLeaseLost` → send `Cancel` frame for that task (another delivery owns it).
- Breaker integration: per-task-type circuit breaker around the *outcome stats* — GoAkt's `breaker.CircuitBreaker` (sliding-window failure rate; `WithFailureRate`, `WithMinRequests`, `WithOpenTimeout(30s)`), not hand-rolled. RETRY results count as failures; while open, the gateway withholds credits for that type's queue so a dead downstream doesn't burn retries at full speed.
- On stream close/error: `Release` all in-flight session tasks (immediate redelivery, no retry penalty — covers graceful worker shutdown and crashes alike; crash-released tasks may also have been mid-handler, which is exactly the at-least-once contract), deregister, stop self.

### 8.3 SchedulerSingleton

GoAkt cluster singleton — `SpawnSingleton(ctx, "conveyor-scheduler", ...)` (placed on the cluster leader, relocated to the new leader on failover):
- On start: `ListCronEntries`; for each unpaused entry `ScheduleWithCron(FireCron{id}, self, spec, WithReference(id))`; runtime control via `PauseSchedule(id)` / `ResumeSchedule(id)` / `CancelSchedule(id)`. GoAkt cron specs are 6-field (go-quartz); the Admin API validates with the same parser. The quartz registry is **node-local state**, which is exactly why every (re)start rebuilds it from the broker.
- `FireCron`: materialize a TaskEnvelope (fresh ULID; uniqueness key template makes double-fires harmless via G6), `Broker.Enqueue`, wake grain.
- Admin cron mutations: API writes broker first, then `Tell`s the singleton `CronEntriesChanged`.
- **Promotion loop**: `Schedule` every 1s → `PromoteScheduled(limit)` → wake affected grains. Delayed tasks live in the broker, never as per-task GoAkt timers (unbounded timers don't survive relocation; the broker is the timer store).
- Singleton failover: GoAkt relocates; rebuild from broker; missed ticks ≤ failover time (documented).

### 8.4 ReaperSingleton

Cluster singleton on a `Schedule` tick (default 15s):
1. `ReapExpiredLeases` → wake affected grains.
2. `PurgeCompleted` (retention + unique-key tombstones).
3. **Sweep backstop**: `PendingCount`; any queue with due work gets `TasksAvailable`. Lost wake-ups (G1) self-heal with staleness bounded by the reap interval.

### 8.5 Dead letters & event stream

Subscribe via `ActorSystem.EventStream()`: `Deadletter` (undeliverable actor messages — ours are hints, so loss is benign but counted), actor lifecycle (`ActorStarted`/`ActorStopped`/`ActorRestarted`/`ActorPassivated`/`ActorSuspended`), cluster topology (`NodeJoined`/`NodeLeft`), and `RelocationFailed` (alert-worthy; affected grains still re-activate on next message) → logs + metrics. Task-level dead-lettering is the broker `archived` state, surfaced via Admin API.

---

## 9. API surface (ConnectRPC, `protos/conveyor/v1/service.proto`)

One port per node serves gRPC + gRPC-Web + HTTP/JSON (ConnectRPC). All services bearer-token authenticated (§15). This protocol is the public contract for SDKs, CLI, and the future UI.

```proto
service TaskService {
  rpc Enqueue(EnqueueRequest) returns (TaskInfo);          // also POST /conveyor.v1.TaskService/Enqueue (JSON)
  rpc EnqueueBatch(EnqueueBatchRequest) returns (EnqueueBatchResponse);
  rpc GetTask(GetTaskRequest) returns (TaskInfo);
}
service WorkerService {
  rpc Session(stream WorkerMessage) returns (stream ServerMessage);   // §3.3
}
service AdminService {
  rpc ListQueues(...) returns (...);          // counts by state, latency stats
  rpc PauseQueue(...) / ResumeQueue(...);
  rpc ListTasks(...) / CancelTask(...) / DeleteTask(...) / RunTask(...);
  rpc ListCron(...) / UpsertCron(...) / PauseCron(...) / DeleteCron(...);
  rpc ClusterInfo(...) returns (...);         // nodes, grains placement (debug)
}
```

`EnqueueRequest` mirrors the SDK options (queue, type, payload bytes, content_type, max_retry, process_at/in, deadline, unique key/ttl, priority, retention, metadata). HTTP/JSON examples ship in the README — `curl`-only enqueue is a first-class supported path.

---

## 10. Go SDK (`sdk/` — the public API)

No protobuf, no GoAkt, no generated types in any exported signature.

```go
// ---- enqueue side ----
client, err := conveyor.NewClient("https://conveyor.internal:8080",
    conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")),
)
info, err := client.Enqueue(ctx,
    conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: 42})), // default codec
    conveyor.Queue("critical"),
    conveyor.MaxRetry(10),
    conveyor.ProcessIn(5*time.Minute),
    conveyor.Unique(24*time.Hour),           // key = type + payload hash; or UniqueKey("user:42:welcome")
    conveyor.Priority(7),
    conveyor.Retention(48*time.Hour),
)

// ---- worker side ----
w, err := conveyor.NewWorker("https://conveyor.internal:8080",
    conveyor.WithToken(token),
    conveyor.WithQueues(map[string]int{"critical": 6, "default": 3, "low": 1}),
    conveyor.WithConcurrency(20),
)
mux := conveyor.NewMux()
mux.Use(loggingMiddleware)                                   // asynq-style middleware
mux.HandleFunc("email:welcome", func(ctx context.Context, t *conveyor.Task) error {
    var p WelcomeEmail
    if err := t.Bind(&p); err != nil { return conveyor.SkipRetry(err) } // decode via content_type
    return sendEmail(ctx, p)                                  // respect ctx.Done()
})
err = w.Run(ctx, mux)   // blocks; reconnects with jitter; graceful drain on ctx cancel/SIGTERM
```

Codecs: `conveyor.JSON(v)` (default, content-type `application/json`), `conveyor.Bytes(b)`, `conveyor.Proto(m)` (opt-in convenience; still just bytes on the wire). `Task` exposes `ID()`, `Type()`, `Queue()`, `Payload() []byte`, `ContentType()`, `Retried()`, `Metadata()`, `Bind(any) error`. Context helpers: `conveyor.GetTaskID(ctx)`, `GetRetryCount(ctx)`, `GetMaxRetry(ctx)`.

Handler contract (documented prominently): idempotent (G2); honor `ctx.Done()`; `conveyor.SkipRetry(err)` archives immediately; panics are recovered by the SDK and reported as retryable failures with the stack in `last_error` — a panicking handler never kills the worker process.

Embedded mode:

```go
sys, err := embedded.Start(ctx, embedded.Config{Broker: embedded.Memory()}) // or Postgres(dsn)
client := sys.Client()
w := sys.Worker(conveyor.WithQueues(...), conveyor.WithConcurrency(8))
go w.Run(ctx, mux)
```

Identical SDK types over a loopback transport; moving to a remote cluster = swap `embedded.Start` for `conveyor.NewClient/NewWorker` with a URL.

---

## 11. CLI (`conveyor`)

Thin client over the API; reads `CONVEYOR_ADDR`/`CONVEYOR_TOKEN` or `--addr/--token`.

```
conveyor enqueue email:welcome --queue critical --json '{"user_id":42}' --in 5m --unique 24h
conveyor stats                       # queue table: pending/active/retry/archived, p50/p99 latency
conveyor queues pause critical | resume critical
conveyor tasks list --state archived --queue default --limit 50
conveyor tasks run <id> | cancel <id> | delete <id>
conveyor cron list | pause <id> | resume <id>
conveyor cluster info
```

---

## 12. Configuration (`conveyor.yaml` + `CONVEYOR_*` env + flags; env > file)

```yaml
mode: kubernetes                # standalone | cluster | kubernetes
broker:
  driver: postgres              # postgres | memory
  dsn: ${DATABASE_URL}
api:
  listen: :8080
  tls: {cert_file: ..., key_file: ...}        # optional; required for non-loopback in prod docs
  auth_tokens: [${CONVEYOR_TOKEN}]            # bearer tokens; empty list = auth disabled (dev only, loud warning)
cluster:
  discovery: kubernetes         # static | nats | consul | etcd | mdns | dnssd | kubernetes
  static_peers: []              # for discovery=static
  remoting_port: 9000           # GoAkt remoting (bind addr auto-detected; overridable)
  discovery_port: 9001          # gossip bootstrap
  peers_port: 9002              # cluster peers
  tls: {...}                    # remoting mTLS (pass-through to GoAkt tls.Info{ServerConfig, ClientConfig})
engine:
  lease_ttl: 60s
  lease_batch_max: 100
  reap_interval: 15s
  promote_interval: 1s
  passivate_after: 5m
  default_max_retry: 25
  shutdown_timeout: 30s
log: {level: info, format: json}
otel: {endpoint: ..., service_name: conveyord}
```

`conveyord --dev` = standalone + memory broker + auth off + debug logs: the 10-second quickstart.

---

## 13. Observability

Metrics (OTel, attributes `queue`/`task_type` where relevant): `conveyor.enqueued|completed|failed|archived|retried|released` counters; `conveyor.active` updown; `conveyor.pending` observable gauge (from `PendingCount`); `conveyor.process.duration` and `conveyor.queue.latency` histograms; health canaries `conveyor.lease.expired`, `conveyor.wakeups.swept`; `conveyor.breaker.open`; API plane `conveyor.sessions.active`, RPC durations; GoAkt's own OTel metrics enabled (`actor.WithMetrics()`).

Tracing: enqueue RPC span → W3C `traceparent` stored in envelope metadata → linked execution span emitted by the worker SDK (`conveyor.process <type>`). Structured logs on every state transition (Debug) and error (Warn/Error); never log payloads. Health endpoints: `/healthz` (process), `/readyz` (broker ping + cluster joined) — wired into the Helm probes.

## 14. Failure matrix

| Failure                                                | Detection                                   | Recovery                                                                                                                   | Guarantee |
|--------------------------------------------------------|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|-----------|
| Crash after Enqueue commit, before wake-up Tell        | reaper sweep                                | `TasksAvailable` within reap interval                                                                                      | G1        |
| Worker process crash mid-handler                       | stream close → gateway                      | `Release` in-flight → immediate redelivery                                                                                 | G2/G3     |
| Worker stalls (no heartbeat, stream alive)             | lease expiry                                | reaper resets to retry; gateway's later result hits `ErrLeaseLost` and is discarded                                        | G3        |
| conveyord node dies (grains/gateways/singletons on it) | GoAkt relocation + worker reconnect         | grains rebuild in activation; workers reconnect to another node, new gateways; released/expired leases redeliver           | G5        |
| API node dies mid-Enqueue                              | client error before commit OR success after | SDK retries idempotently via client-supplied ULID (Enqueue is upsert-on-id)                                                | G1        |
| Broker down                                            | lease/ack errors                            | enqueue fails loudly; grains back off lease cycles; leases freeze (no false expiry processing since reaper also can't run) | G1        |
| Duplicate wake-ups                                     | lease atomicity                             | empty lease cycle, no-op                                                                                                   | G3        |
| Handler ignores ctx past deadline                      | lease expiry                                | re-delivery; double-run documented (asynq parity)                                                                          | G2/G3     |
| Poison task                                            | retry counter + breaker                     | backoff → archived at max_retry; breaker caps blast radius per type                                                        | G2        |
| Worker on old SDK version                              | `Hello.sdk_version`                         | server rejects below min version with clear error frame                                                                    | —         |

## 15. Security

- API auth: static bearer tokens v1 (config/secret); constant-time compare; per-token scopes deferred to v2.
- TLS on the API port; mTLS optional. Cluster remoting mTLS via GoAkt options.
- Workers hold tokens + session only (G7); lease IDs are scoped server-side to the session and never exported.
- Payloads opaque; decode only in user code via `Bind`. Helm chart: secrets for DSN/tokens, distroless nonroot image, NetworkPolicy example.

## 16. Deferred (v2) — hooks preserved

- **Group aggregation**: `GroupGrain` per (queue, group key) using GoAkt's `stream` package (`Batch(n, maxWait)` / `Throttle(n, per)` flows); broker `state=aggregating`; `TaskOptions` fields 9–15 reserved.
- **Web UI (full)**: v1's read-only dashboard (§17 Phase 7) grows mutations + auth UX; still consumes AdminService only (already JSON over HTTP via ConnectRPC).
- **Polyglot SDKs** (Python, TS): WorkerService protocol is the contract; keep frames additive.
- **SQLite broker** (standalone durability without Postgres): conformance suite makes it additive.
- **Per-type rate limiting** (token bucket in gateways), **multi-DC** (broker per DC + GoAkt's `datacenter` package: per-DC clusters with a NATS-JetStream or etcd control plane, wired via `ClusterConfig.WithDataCenter`), **namespaces/scoped tokens**.

## 17. Implementation plan (phases for Claude Code)

The bar for v1 is not "feature-complete" — it is **demonstrably ahead of asynq, Faktory, and River on the axes §1.1 claims, on launch day**. Three competitive properties are release gates, not aspirations, each anchored to a phase below:

- **Latency — the push-based payoff.** p99 enqueue→handler-start < 50 ms at 1k tasks/s on one node (an order of magnitude under any polling interval). First measured in Phase 3a, recalibrated against measured asynq/River baselines in Phase 6 — gates may tighten from measurement, never loosen.
- **Reliability as a reproducible demo, not a claim.** The chaos suite is an in-repo artifact (`make chaos`); the README links to green CI runs of kill -9, node-loss, and partition tests. Competitors assert durability; Conveyor proves it on every commit.
- **Time-to-first-task ≤ 60 s.** From `docker run`/`go install` to a processed task, measured in CI by scripting the README quickstart verbatim.

Strict order; every phase ends with `make test lint` green. Throughput/latency gates are provisional until Phase 6 recalibration and run on pinned CI hardware.

> **Status (2026-06-12):** Phases 0–2, 3a, 3b, and 4 are ✅ COMPLETE. Next up: **Phase 5**. One open caveat: the Phase 2 5k dispatches/s gate is skip-blocked on the upstream GoAkt TellGrain local fast path (see `TestQueueGrainDispatchThroughput`); bump the GoAkt pin and unskip when it ships. Phase 4 notes: the CLI moved to spf13/cobra and ships every §11 command except `cron` mutations beyond pause/resume (cron *materialization* is Phase 6 — the Admin cron RPCs persist entries today but the scheduler does not fire them yet, and Admin cron writes do not Tell the scheduler `CronEntriesChanged` until then); canceling an *active* task sends the best-effort Cancel frame and the task lands in `retry` (asynq parity), where a second cancel is durable; the SDK version gate ships vacuous (`minSDKVersion = v0.0.0-0`) — bump it on the first breaking wire change. One deviation: server-side session drain runs in `server.Stop` *before* the engine stops, not via GoAkt `WithCoordinatedShutdown` hooks as originally specified — GoAkt rejects every user message the instant its stop sequence begins (`PID.doReceive` returns `ErrSystemShuttingDown`), so gateways cannot process drain requests, or execute any durable transition, from inside a shutdown hook.

**Phase 0 — Scaffolding.** ✅ COMPLETE — Module, buf + proto gen (envelope, actor messages, services), Makefile (test/lint/proto/integration via compose with Postgres 16/chaos/helm-lint — chaos and helm targets stubbed), CI, config loader + validation, `sdk/` skeleton with sentinel errors, `--dev` no-op boot. *Accept:* `conveyord --dev` starts and serves `/healthz`; `make proto test` green.

**Phase 1 — Broker.** ✅ COMPLETE — Interface, in-memory impl, **conformance suite in `internal/broker/brokertest`** (run by every impl; drives all time-based behavior through the injected clock / `$now` bind parameter, §6.3 — no sleeps), Postgres impl (pgx/v5, embedded migrations) green under testcontainers, concurrency tests (N goroutines leasing one queue never double-lease, `-race`). *Accept:* suite green on both brokers; **perf floor:** `Lease` of a 100-task batch ≤ 2 ms on local Postgres (the broker is the throughput ceiling — measure it before building on it).

**Phase 2 — Engine core (no wire protocol yet).** ✅ COMPLETE (perf gate skip-blocked upstream, see status note) — ActorSystem boot in cluster mode (single node, static self-discovery — the only code path), QueueGrain, Scheduler + Reaper as plain actors (conversion to `SpawnSingleton` in Phase 5 is intentional, near-zero rework), structured transition logs + core counters (enqueued/completed/failed/active) from day one, an internal in-process test harness standing in for gateways. *Accept:* harness drives 10k tasks across 3 weighted queues on the memory broker; priorities respected statistically; retry counting correct; kill -9 + restart on Postgres loses zero tasks; **perf gate:** one QueueGrain sustains ≥ 5k dispatches/s on the memory broker (the grain is a per-queue serialization point — prove it isn't the bottleneck before the protocol calcifies around it); **cluster smoke:** 2 nodes in-process, kill the node hosting a loaded grain → grain re-activates on the survivor, zero loss (de-risks Phase 5 four phases early).

**Phase 3a — Wire protocol + gateway (the public contract).** ✅ COMPLETE (latency gate measured: p99 = 231µs @ 1k tasks/s; protocol declared additive-only) — ConnectRPC services, WorkerGateway, session protocol (credits, heartbeats, release-on-disconnect), bearer-token auth (constant-time compare; `--dev` disables with loud warning), minimal worker SDK (connect, handle, result — enough for e2e). *Accept:* example app (separate worker process) processes tasks end-to-end; SIGKILL the worker mid-task → task redelivered with no retry penalty ≤ 1s; frame state machine fuzz tests green (Hello-less Result, double Result, credit overflow — §18.3); all services reject missing/bad tokens; **latency gate** measured for the first time (p99 enqueue→start < 50 ms @ 1k tasks/s, one node); **protocol freeze review:** wire contract walked end-to-end and declared additive-only from here.

**Phase 3b — SDK ergonomics + CLI + embedded.** ✅ COMPLETE — Full Go SDK polish (Mux middleware, codecs, reconnect-with-jitter, panic recovery surface), CLI basics, embedded mode over loopback transport, godoc on every exported `sdk/`/`embedded/` symbol. *Accept:* embedded example behaves identically to remote; asynq-user porting test — the §10 sample compiles and runs as written; **DX gate:** scripted README quickstart completes in ≤ 60 s in CI.

**Phase 4 — Semantics + API hardening.** ✅ COMPLETE — Uniqueness, deadlines/timeouts, lease-extension heartbeats, SkipRetry, pause/resume (paused flag + persisted state, §8.1), graceful drain on both server and worker shutdown (server side via `WithCoordinatedShutdown` hooks), panic recovery in SDK, archived flow, Admin API complete, breaker; input hardening — payload size cap, `EnqueueBatch` limits, request validation with actionable errors. *Accept:* one test per failure-matrix row simulable on a single node.

**Phase 5 — Cluster + deployment modes.** Multi-node GoAkt clustering (static + kubernetes discovery), cluster singletons, gateway/grain relocation handling, worker reconnect-with-jitter under server restart, remoting mTLS, Helm chart + Docker + compose + systemd, Grafana dashboard + Prometheus scrape config in `deploy/grafana/`, `conveyor cluster info`. *Accept:* 3-node chaos test — kill the node hosting a loaded grain and the node holding worker sessions — zero task loss, **recovery (kill → first successful dispatch) < 2× reap interval**, 20 consecutive green runs; `helm template` lints; kind-based e2e in CI.

**Phase 6 — Cron, observability, docs, benchmarks.** Cron via CLI/Admin with pause/resume, full OTel exporter wiring + trace propagation, README + migration guides (**asynq and River**, side-by-side API tables) + ops guide (scaling, broker sizing, upgrades), benchmark harness vs **asynq and River** on identical hardware with reproducible scripts in-repo. *Accept:* cron e2e — a 1s-interval entry fires N±1 times in N seconds with no double-fire across a singleton failover; an enqueue trace reaches the worker span in a real collector; benchmark numbers published in README; **latency/throughput gates recalibrated** against measured asynq/River baselines and re-pinned.

**Phase 7 — Launch readiness.** The read-only dashboard (`web/dashboard/`: static SPA, `go:embed`'d, consumes AdminService JSON only — queues, task drill-down, cron, cluster view; zero mutations, so zero new attack surface beyond existing Admin reads); security defaults audit (auth on by default outside `--dev`, TLS docs, NetworkPolicy example); rolling-restart-under-load test (workers keep processing across a server rolling upgrade; version-skew policy documented — full mixed-version testing is explicitly deferred past v1); **competitive checklist:** walk §1.1 row by row and verify each Conveyor cell is true, demoable, and documented. *Accept:* dashboard serves from a bare `conveyord --dev` with zero config; checklist signed off in the release notes.

**Definition of done (v1):** all acceptance criteria including the three competitive gates; lint/vet/race clean; failure-matrix tests in CI; no GoAkt or generated-proto types in exported SDK signatures (CI check); wire protocol documented as stable; benchmarks vs asynq and River published with reproduction scripts.

## 18. Testing strategy

1. **Broker conformance suite** (`brokertest.Run(t, factory)`) — the most valuable asset; every broker (incl. future SQLite/Redis) runs it.
2. **Actor unit tests** with GoAkt `testkit` (grain drops-hints-while-paused then resumes cleanly, credit accounting, gateway release-on-close).
3. **Protocol tests**: in-memory ConnectRPC (loopback) driving full sessions; fuzz the frame state machine (Hello-less Result, double Result, credit overflow).
4. **Engine integration**: real ActorSystem + memory broker; injectable `clock.Clock` everywhere (`time.Now()` banned by lint outside `internal/clock`).
5. **Durability/chaos**: Postgres + kill -9 (server and worker), SIGSTOP (stalled-worker lease-expiry path), 3-node cluster partitions in CI (kind).
6. **Benchmarks**: enqueue throughput; e2e latency p50/p99 at 1 and 3 nodes; head-to-head vs asynq and River on identical hardware, reproducible scripts in-repo (these back the §17 competitive gates).

## 19. Decisions & open questions

Decided (do not relitigate):
- Application-first; clustering always on (standalone = cluster of one); embedded mode preserves the library audience.
- Workers outside the actor cluster, attached via one bidi ConnectRPC stream; gateway executes all durable transitions (G7).
- No protobuf or GoAkt types in the public SDK; payload = bytes + content type; JSON default codec.
- Postgres-first, `SKIP LOCKED` leasing, no LISTEN/NOTIFY (actors are the notification layer; sweep is the backstop).
- Credits-based flow control; zero per-delayed-task timers (broker is the timer store).
- ULID task IDs, client-assignable; Enqueue idempotent on ID (safe SDK retries).

Open (implementer decides, documents in code + CHANGELOG):
- ConnectRPC vs vanilla grpc-go + grpc-gateway (recommendation: ConnectRPC — one port, native HTTP/JSON, smaller surface).
- Gateway-actor-per-session vs gateway-goroutine-per-session bridged by one actor (start actor-per-session; revisit only if benchmarks demand).

Resolved (was open in v2): exact GoAkt v4 identifiers — verified against the v4.2.8 source; see §20. Two findings forced design changes, both folded into §8: grains have **no** `Become`/`Stash` and **no** priority mailbox (queue pause now uses a flag + drop-hints instead of stash; grain mailbox is FIFO), and GoAkt ships a circuit breaker (`breaker` package) so the gateway breaker is not hand-rolled.

## 20. GoAkt v4 capability map (verified at v4.2.8)

Every GoAkt API this design depends on, with the exact identifier. Architecture **and** identifiers are now binding.

| Design need                          | GoAkt v4 API                                                                                                                                          |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| Queue dispatcher (§8.1)              | `Grain` (`OnActivate`/`OnReceive`/`OnDeactivate`), `ActorSystem.GrainIdentity` / `TellGrain` / `AskGrain`; kinds via `ClusterConfig.WithGrains(...)`   |
| Broker access from grains/actors     | `actor.WithExtensions(...)` at system boot; `GrainContext.Extension("broker")` / `ReceiveContext.Extension("broker")`                                  |
| Non-blocking broker I/O in grain     | `GrainContext.PipeToSelf(task, opts...)` (also `PipeToGrain`/`PipeToActor`); `PipeOption`: `WithTimeout`, `WithCircuitBreaker`                          |
| Grain passivation                    | `WithGrainDeactivateAfter(d)` (default 2m), `WithLongLivedGrain()`; bounded mailbox via `WithGrainMailboxCapacity(n)`                                  |
| Gateway actor (§8.2)                 | `Spawn` with `WithLongLived()`, `WithRelocationDisabled()`; supervision via `supervisor.NewSupervisor(WithStrategy, WithDirective)`                     |
| Outcome circuit breaker (§8.2)       | `breaker.NewCircuitBreaker(WithFailureRate, WithMinRequests, WithOpenTimeout, WithWindow)`; `Execute` / `State`                                         |
| Cluster singletons (§8.3, §8.4)      | `ActorSystem.SpawnSingleton(ctx, name, actor, opts...)` — leader-placed, relocated on failover; `WithSingletonRole` available for pinning               |
| Cron + ticks                         | `ScheduleWithCron(msg, pid, spec, WithReference(id))` (6-field go-quartz specs), `Schedule(msg, pid, interval)`, `ScheduleOnce`; `PauseSchedule` / `ResumeSchedule` / `CancelSchedule` by reference — registry is node-local (rebuild from broker on failover) |
| Event stream (§8.5)                  | `ActorSystem.EventStream()`; events: `Deadletter`, `ActorStarted/Stopped/Restarted/Passivated/Suspended`, `NodeJoined`/`NodeLeft`, `RelocationFailed`   |
| Clustering                           | `actor.WithCluster(NewClusterConfig().WithDiscovery(p).WithDiscoveryPort(n).WithPeersPort(n).WithKinds(...).WithGrains(...))`                            |
| Discovery providers                  | `discovery/{static, nats, consul, etcd, mdns, dnssd, kubernetes}` (also `selfmanaged` — LAN broadcast, unused). k8s provider: pod labels + named container ports + RBAC pods list/watch; headless Service optional |
| Remoting + mTLS                      | `actor.WithRemote(remote.NewConfig(...))` (compression, frame size, conn pool tunables); `actor.WithTLS(&tls.Info{ServerConfig, ClientConfig})`         |
| Metrics                              | `actor.WithMetrics()` (OTel; exporter wiring is ours)                                                                                                   |
| Graceful drain (server shutdown)     | `actor.WithCoordinatedShutdown(hooks...)` — drain gateways/grains before the actor system stops                                                         |
| v2 group aggregation (§16)           | `stream` package: `Batch(n, maxWait)`, `Throttle(n, per)` flows                                                                                          |
| v2 multi-DC (§16)                    | `datacenter.Config` + `ClusterConfig.WithDataCenter`; control planes: NATS JetStream, etcd                                                              |

Deliberately **not** used (exists in GoAkt, out of scope here): `WithPubSub()`/TopicActor and the CRDT replicator (the broker is the only shared state, G5), routers, the external cluster `client` package (our API layer runs in-process on every node), reentrancy modes, grain `Dependencies` (extensions suffice; dependencies require binary serialization for relocation).

## 21. References

- GoAkt: https://github.com/Tochemey/goakt · https://docs.goakt.dev · examples: https://github.com/Tochemey/goakt-examples
- asynq (parity target): https://github.com/hibiken/asynq · Faktory (server model): https://github.com/contribsys/faktory
- River (`SKIP LOCKED` prior art): https://github.com/riverqueue/river · ConnectRPC: https://connectrpc.com
- ULID: https://github.com/oklog/ulid · pgx: https://github.com/jackc/pgx