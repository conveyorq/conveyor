# Lifecycle events

Conveyor can **push** every task state transition as it happens, so external systems react without polling `GetTask`/`ListTasks`. Use it for live dashboards, alerting, audit logs, and event-driven chaining — for example, "archive this record the moment its task is dead-lettered."

There are two ways to consume the stream, and they share the same events:

- **`WatchEvents`** — a server-streaming Admin API the CLI exposes as `conveyor events`. A client subscribes and receives events live.
- **A webhook** — the server POSTs each event as JSON to a URL you configure.

Events are **off by default** in production (`events.enabled: false`) — nothing consumes the stream out of the box, so a node pays nothing until you opt in. Turn it on with `events.enabled: true` (the `--dev` preset already does). Setting a webhook URL also requires `events.enabled: true`.

## What an event carries

Each event is a small notification — the task's identity and its new state, not its payload:

| Field         | Meaning                                                        |
|---------------|---------------------------------------------------------------|
| `id`          | the task id                                                   |
| `queue`       | the task's queue                                              |
| `type`        | the handler routing key, e.g. `email:welcome`                 |
| `state`       | the task's state after the transition                         |
| `event_type`  | the transition that occurred (see below)                      |
| `occurred_at` | when the transition was recorded                              |
| `attempt`     | the retry count after the transition                          |
| `last_error`  | the most recent error (set on `retried` and `archived`)       |

### Event types

| Event       | When                                                              |
|-------------|------------------------------------------------------------------|
| `enqueued`  | a task was committed and is runnable (or became runnable again)  |
| `scheduled` | a task was committed with a future run time                      |
| `leased`    | a task was handed to a worker (now active)                       |
| `completed` | a task executed successfully                                     |
| `retried`   | an attempt failed and a retry was scheduled                      |
| `archived`  | a task was dead-lettered (retries exhausted, skipped, expired, or canceled mid-run) |
| `canceled`  | a waiting task was canceled (admin action or a failed dependency)|
| `released`  | an active task was returned to pending without a retry penalty   |

## Watching the stream

Tail every transition:

```sh
conveyor events
```

Filter by queue and/or event type (both repeatable; a filter narrows the stream server-side):

```sh
# only dead-letterings on the payments queue
conveyor events --queue payments --type archived

# enqueues and completions across all queues
conveyor events --type enqueued --type completed
```

The command blocks until the first matching event arrives, then prints one row per event until interrupted.

## Webhook sink

Point the server at an HTTPS endpoint and it POSTs each event as JSON:

```yaml
events:
  webhook:
    url: https://example.com/conveyor/events
    timeout: 10s          # per-attempt timeout
    max_retries: 3        # retries after a failed delivery, with backoff
    secret: s3cret        # sent as: Authorization: Bearer s3cret
    queues: [payments]    # optional filter; empty = every queue
    event_types: [TASK_EVENT_TYPE_ARCHIVED]   # optional; empty = every type
```

Deliveries retry on transport errors, `5xx`, and `429` with exponential backoff; a `4xx` (other than `429`) is treated as a permanent client error and not retried. The webhook runs on every server node, so in a multi-node cluster the endpoint receives the union of all nodes' events.

## Delivery semantics

Events are **best-effort and non-durable** (fire-and-forget):

- A watcher receives events from the moment it subscribes — there is **no replay** of past transitions and no durable history. For the authoritative current state of a task, read it with `GetTask`.
- Delivery is **at-least-once to connected listeners**: a rare duplicate is possible; consumers should be idempotent (the `id` plus `event_type` identifies a transition).
- **Backpressure never reaches the dispatcher.** Each watcher and the webhook have a bounded buffer; a consumer too slow to keep up has events **dropped** rather than stalling task processing. Drops are counted by the `conveyor_events_dropped_total` metric. Raise `events.buffer_size` if a fast, bursty stream overruns a consumer that is normally able to keep up.

## Configuration

| Key                          | Default | Meaning                                        |
|------------------------------|---------|------------------------------------------------|
| `events.enabled`             | `false` | master switch for the stream and the webhook (on in `--dev`) |
| `events.buffer_size`         | `1024`  | per-consumer buffer depth before events drop   |
| `events.webhook.url`         | —       | webhook endpoint; empty disables the webhook   |
| `events.webhook.timeout`     | `10s`   | per-delivery timeout                           |
| `events.webhook.max_retries` | `3`     | retries after a failed delivery                |
| `events.webhook.secret`      | —       | bearer token sent on each delivery             |
| `events.webhook.queues`      | —       | queue filter; empty = all                      |
| `events.webhook.event_types` | —       | event-type filter (enum names); empty = all    |

With `events.enabled: false`, `WatchEvents` returns `Unavailable` and no webhook runs.
