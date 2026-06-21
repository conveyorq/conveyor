# Rate limiting

Rate limiting caps **how fast a queue dispatches tasks**, for example "run
`email:send` at most 50 per second", so a queue can't overwhelm a fragile
downstream (a
third-party API quota, a database, an email provider) no matter how much worker
capacity is free. Over-rate tasks **wait in the queue** and dispatch the instant
capacity refills; nothing is dropped and no retry is spent.

A limit is a **token bucket**: a sustained `rate` (tasks/second) plus a `burst`
(how many may go out in one instantaneous spike before the rate applies).

This is distinct from concurrency: worker concurrency bounds *how many tasks run
at once*; a rate limit bounds *how many start per second*. They compose.

## Two layers

- **Global default**: set in server config. Every queue uses it unless it has
  its own limit.
- **Per-queue override**: set at runtime (CLI, dashboard, or API). When present,
  it **replaces** the default for that queue.

So the effective limit for a queue is its own override if set, otherwise the
global default, otherwise unlimited.

## Set a per-queue limit

Limits are set at runtime and stored durably (shared across servers, surviving
restarts), so there is no redeploy to change one.

**CLI:**

```sh
conveyor ratelimit set email --rate 50 --burst 10   # 50/s, burst 10
conveyor ratelimit ls                                # list overrides
conveyor ratelimit rm email                          # revert to the default
```

**Dashboard:** the **Limits** tab has an editor (queue, rate, burst) and a table
of overrides with edit/remove.

**API:** the `AdminService` RPCs `SetQueueRateLimit`, `DeleteQueueRateLimit`, and
`ListRateLimits`. These are the same calls the CLI and dashboard make, usable from any
ConnectRPC client (script them in CI to manage limits as code).

`rate` must be greater than zero and `burst` at least one.

## Global default and kill-switch (server config)

In `conveyor.yaml` (engine config) or `CONVEYOR_*` env:

| Setting | Meaning | Default |
|---|---|---|
| `rate_limit_enabled` | master switch; `false` disables all rate limiting (overrides are kept but ignored) | `true` |
| `rate_limit_rate_per_sec` | global default rate in tasks/second; `0` means no default (queues unlimited unless overridden) | `0` |
| `rate_limit_burst` | global default burst, used when the default rate is set | `0` |

`rate_limit_enabled: false` is the production safety valve: if a misconfigured
limit is starving a queue, flip it and every queue runs unthrottled again, with
the override rows left intact for when you turn it back on.

## Behavior

- **Over-rate tasks wait, they are not dropped.** They stay queued and dispatch
  as soon as the bucket refills. Average throughput settles at the configured
  rate, with bursts up to `burst`.
- **An override replaces the default** for its queue (the two do not stack).
  Removing an override reverts the queue to the global default.
- **Changes take effect immediately**, with no worker or server restart.
- **Unaffected when unused.** A queue with no effective limit pays nothing; rate
  limiting adds no overhead to queues you haven't limited.

## Observability

A throttled queue increments the `conveyor_ratelimit_throttled_total` counter
(labeled by queue) each time it defers dispatch on an exhausted limit. Watch it to see
which queues are hitting their ceiling. You will also see a limited queue's
**Pending** count rise on the dashboard's Queues tab as work waits its turn.
