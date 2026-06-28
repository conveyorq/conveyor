# CLI reference (`conveyor`)

`conveyor` is the command-line client for a Conveyor server. It is two things in
one binary:

- a **producer**: enqueue tasks (`enqueue`, `enqueue-tx`), and
- an **operator console**: inspect and drive the system (queues, tasks, limits,
  cron, cluster, and the live event stream).

Operating the system lives here and in the dashboard, deliberately kept out of
the SDKs: the SDKs are the produce-and-consume surface for application code, while
rescheduling, running, canceling, pausing, limiting, and cron management are
operator actions. See the [operations guide](operations.md) for the wider
deployment picture.

> The CLI talks to a running `conveyord`. It does not start a server. To run one,
> see the [operations guide](operations.md); for an in-process server, see
> [embedded mode](../README.md#embedded-mode).

## Installing

Build it from the repository:

```sh
go build -o conveyor ./cmd/conveyor
```

Or run it without installing:

```sh
go run ./cmd/conveyor <command> [flags]
```

## Connecting: global flags and environment

Every command accepts these, and the same two settings cover the whole session:

| Setting | Flag | Environment | Default |
|---------|------|-------------|---------|
| Server URL | `--addr` | `CONVEYOR_ADDR` | `http://localhost:8080` |
| Bearer token | `--token` | `CONVEYOR_TOKEN` | empty (dev servers only) |

A flag wins over its environment variable. Outside `--dev`, a server requires a
token, so set `--token`/`CONVEYOR_TOKEN`. When the server runs with
`api.read_only`, the mutating commands return `permission denied` while reads,
enqueue, and the event stream still work.

```sh
export CONVEYOR_ADDR=https://conveyor.internal:8080
export CONVEYOR_TOKEN=$(cat /run/secrets/conveyor-token)

conveyor stats
```

## Command overview

| Command | Purpose |
|---------|---------|
| `enqueue` | Commit one task |
| `enqueue-tx` | Commit many tasks atomically (all-or-nothing) |
| `tasks` | Inspect and operate on tasks (get, list, run, cancel, delete, reschedule) |
| `stats` | Per-queue state counts and pause flags |
| `queues` | Pause and resume queues |
| `ratelimit` | Per-queue dispatch rate limits (set, rm, ls) |
| `concurrency` | Per-queue per-key concurrency limits (set, rm, ls) |
| `group` | Per-group aggregation overrides (set, rm, ls) |
| `cron` | Cron entries (add, list, pause, resume) |
| `cluster` | Cluster membership (info) |
| `events` | Stream task lifecycle events until interrupted |
| `completion` | Generate a shell autocompletion script |

Run `conveyor <command> --help` (or `conveyor <command> <subcommand> --help`) for
the authoritative, version-matched flags.

## Producing work

### `enqueue`: commit one task

```sh
conveyor enqueue <type> [flags]
```

`<type>` is the handler routing key. The payload is JSON passed with `--json`.

| Flag | Meaning |
|------|---------|
| `--queue` | Target queue (server default when empty) |
| `--json` | JSON payload |
| `--id` | Client-assigned task id for idempotent retries |
| `--in` | Delay execution by a duration, e.g. `5m` |
| `--at` | Delay execution until an RFC3339 time |
| `--expires-in` | Archive the task if not dispatched within this duration |
| `--expires-at` | Archive the task if not dispatched by this RFC3339 time |
| `--max-retry` | Retry budget (server default when 0) |
| `--priority` | Dispatch priority 1..9 (server default when 0) |
| `--retention` | Keep the completed task visible for this long |
| `--unique` | Reject duplicates of this task for the given TTL |
| `--unique-key` | Explicit uniqueness key (default: type + payload hash) |
| `--retry-strategy` | Retry backoff: `exponential`, `linear`, or `fixed` |
| `--retry-base` | First-retry delay ceiling |
| `--retry-max` | Overall retry delay cap |
| `--encryption-key` | Seal the payload with AES-256-GCM, as `<id>:<base64-secret>` (default `CONVEYOR_ENCRYPTION_KEY`) |

```sh
conveyor enqueue email:welcome --queue critical --json '{"user_id":42}' --in 5m
```

### `enqueue-tx`: commit many tasks atomically

```sh
conveyor enqueue-tx --file <path> [--encryption-key <id>:<secret>]
```

`enqueue-tx` commits a set of tasks **all-or-nothing**: either every task is
enqueued or none is. If any task fails (a duplicate unique key, a unique-key
collision between two tasks in the file, or an invalid task), nothing is
committed. This is atomic multi-task enqueue, distinct from the best-effort
behavior a per-task loop of `enqueue` would give. The tasks may span queues,
priorities, and schedules.

`--file` is a JSON array of task specs. Each spec mirrors the `enqueue` flags:

| Field | Maps to | Notes |
|-------|---------|-------|
| `type` | `<type>` | Required |
| `queue` | `--queue` | |
| `json` | `--json` | A JSON value used as the payload |
| `id` | `--id` | |
| `in` | `--in` | Duration string, e.g. `"5m"` |
| `at` | `--at` | RFC3339 time |
| `expires_in` | `--expires-in` | Duration string |
| `expires_at` | `--expires-at` | RFC3339 time |
| `max_retry` | `--max-retry` | |
| `priority` | `--priority` | |
| `retention` | `--retention` | Duration string |
| `unique` | `--unique` | Duration string |
| `unique_key` | `--unique-key` | |

```jsonc
// tasks.json
[
  {"type": "order:charge",  "queue": "billing", "json": {"id": "order-42"}, "priority": 7},
  {"type": "email:receipt", "queue": "mail",    "json": {"id": "order-42"}},
  {"type": "ledger:post",                        "json": {"id": "order-42"}}
]
```

```sh
conveyor enqueue-tx --file tasks.json
```

`--encryption-key` seals every payload in the file before it leaves the CLI, the
same as `enqueue`. For the model behind this, see
[end-to-end encryption](encryption.md).

## Inspecting

### `stats`

```sh
conveyor stats
```

Prints each queue with its per-state counts (scheduled, pending, active, retry,
completed, archived, aggregating, blocked) and pause flag.

### `tasks get` / `tasks list`

```sh
conveyor tasks get <id>
conveyor tasks list [--queue NAME] [--state STATE] [--limit N]
```

`tasks list` shows tasks newest first. `--state` is one of `scheduled`,
`pending`, `active`, `retry`, `completed`, `archived`, `canceled`.

```sh
conveyor tasks list --state retry --queue critical --limit 50
```

### `cluster info`

```sh
conveyor cluster info
```

Reports the nodes in the cluster (a debugging aid; a single-node server reports
one node).

### `events`

```sh
conveyor events [--queue NAME]... [--type TYPE]...
```

Streams task lifecycle transitions live until interrupted. `--queue` and `--type`
are repeatable filters; `--type` is one of `enqueued`, `scheduled`, `leased`,
`completed`, `retried`, `archived`, `canceled`, `released`. See
[lifecycle events](events.md) for the delivery semantics.

```sh
conveyor events --queue billing --type completed --type archived
```

## Operating tasks

| Command | Effect |
|---------|--------|
| `conveyor tasks run <id>` | Make a scheduled or retry task due immediately |
| `conveyor tasks cancel <id>` | Cancel a task (best-effort for an executing one) |
| `conveyor tasks delete <id>` | Delete a non-active task |
| `conveyor tasks reschedule <id> --in DUR` (or `--at RFC3339`) | Move a scheduled, pending, or retry task's due time |

```sh
conveyor tasks reschedule 01J... --in 30m
conveyor tasks run 01J...
```

## Queues, limits, and aggregation

### `queues`

```sh
conveyor queues pause <name>
conveyor queues resume <name>
```

A paused queue keeps its work durable and stops dispatching it.

### `ratelimit`

```sh
conveyor ratelimit set <queue> --rate N [--burst N]
conveyor ratelimit rm <queue>
conveyor ratelimit ls
```

Caps a queue's dispatch rate (token bucket). See [rate limiting](rate-limiting.md).

```sh
conveyor ratelimit set email --rate 50 --burst 10
```

### `concurrency`

```sh
conveyor concurrency set <queue> --max N
conveyor concurrency rm <queue>
conveyor concurrency ls
```

Caps how many tasks sharing a concurrency key run at once. See
[concurrency limits](concurrency.md).

```sh
conveyor concurrency set email --max 5
```

### `group`

```sh
conveyor group set <queue> [--group KEY] --max-size N --max-delay DUR --grace DUR
conveyor group rm <queue> [--group KEY]
conveyor group ls
```

Overrides a group's aggregation thresholds. An empty `--group` sets the
queue-wide default applied to every group on the queue without its own override.
See [group aggregation](grouping.md).

```sh
conveyor group set email --group welcome --max-size 20 --max-delay 2m --grace 5s
```

## Cron

```sh
conveyor cron add <id> "<spec>" <type> [--queue NAME] [--json PAYLOAD] [--priority N] [--max-retry N]
conveyor cron list
conveyor cron pause <id>
conveyor cron resume <id>
```

`<spec>` is a 6-field cron expression. Cron entries are server-persisted, so they
survive restarts and failover.

```sh
conveyor cron add nightly-report "0 0 2 * * *" report:daily --queue reports
```

## Shell completion

```sh
conveyor completion bash|zsh|fish|powershell
```

Generates a completion script for the named shell; follow that command's own
output for where to install it.

## See also

- [Operations guide](operations.md): deployment, configuration, security, and observability.
- [Concepts](concepts.md): the vocabulary the commands operate on.
- [End-to-end encryption](encryption.md), [rate limiting](rate-limiting.md),
  [concurrency limits](concurrency.md), [group aggregation](grouping.md),
  [lifecycle events](events.md).
