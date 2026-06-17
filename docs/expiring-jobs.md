# Expiring tasks (pre-dispatch TTL)

An expiring task is one that **must not run if it wasn't dispatched in time**.
Set an expiry and a task still waiting in the queue when that moment passes is
**archived instead of run** — useful for work that loses its value once a
deadline slips: a one-time login code, a "your ride is arriving" push, a
flash-sale reminder. Sending it late is worse than not sending it at all.

```go
// Relative: drop it if not dispatched within 5 minutes of enqueue.
client.Enqueue(ctx, conveyor.NewTask("sms:code", conveyor.JSON(code)),
    conveyor.ExpiresIn(5*time.Minute))

// Absolute: drop it if not dispatched by a specific instant.
client.Enqueue(ctx, conveyor.NewTask("push:ride-arriving", conveyor.JSON(ride)),
    conveyor.ExpiresAt(ride.PickupTime))
```

`ExpiresIn` is relative to enqueue time and resolved against the server clock;
`ExpiresAt` is an absolute time. They are mutually exclusive.

From the CLI:

```sh
conveyor enqueue sms:code --json '{"code":"123456"}' --expires-in 5m
conveyor enqueue push:ride-arriving --json '{...}' --expires-at 2026-06-17T18:30:00Z
```

## How it behaves

- A task whose expiry has passed is **never leased** to a worker — the handler
  does not run.
- A background sweep moves expired-but-still-waiting tasks to the **archived**
  (dead-letter) state, where they are visible for inspection and purged by their
  retention like any other archived task. The recorded error is
  `task expired before dispatch`.
- Expiry applies while a task is **waiting** — scheduled (delayed), pending, or
  between retries. Once a task has been dispatched and is running, expiry no
  longer applies; use a deadline or timeout to bound a running task.

## Expiry vs. deadline vs. retention

Conveyor has three distinct time limits; they cover different phases of a task's
life and compose freely:

| Limit | Phase it acts on | What it does |
|-------|------------------|--------------|
| **Expiry** (`ExpiresIn`/`ExpiresAt`) | waiting (not yet dispatched) | archives the task instead of running it |
| **Deadline** (`Deadline`) / **Timeout** (`Timeout`) | running | cancels the handler's context mid-execution |
| **Retention** (`Retention`) | completed | keeps the finished task row visible before purge |

A task can carry all three: dispatch it before its expiry, cancel it if it runs
past its deadline, and keep the record around for its retention.

## See also

- [Operations guide](operations.md) — deployment and configuration.
