# Group aggregation (batch processing)

Group aggregation lets you **coalesce many tasks into one batch** and process
them together in a single handler call. It is the answer to "100 of these events
arrived in a minute — handle them once" (debounce/digest) and to "process 1,000
of these in one bulk API call" (batching).

A producer tags tasks with a **group**; the server accumulates members of the
same `(queue, group)` and, when the group *fires*, delivers the whole group to a
worker as one batch.

## Enqueue grouped tasks

Add the `Group` option:

```go
client.Enqueue(ctx,
    conveyor.NewTask("digest:send", conveyor.JSON(event)),
    conveyor.Group("user:42:digest"),
)
```

A grouped task lands in the **`aggregating`** state instead of `pending`; it is
not dispatched until its group fires. `Group` is mutually exclusive with
`ProcessAt`/`ProcessIn`, and a group is **single-type** — every member shares the
task's type.

## Handle a batch

Register a batch handler with `HandleBatch`; it receives all the group's members
in one call:

```go
mux := conveyor.NewMux()

mux.HandleBatch("digest:send", func(ctx context.Context, batch []*conveyor.Task) error {
    // Coalesce: send one digest for the whole group.
    return sendDigest(ctx, batch)
})
```

- Return `nil` to acknowledge **every** member.
- Return any error to retry **every** member with backoff.
- Return a `*conveyor.BatchError` to mark specific members:

```go
return &conveyor.BatchError{Errs: map[string]error{
    failedID:      err,                       // retried
    badPayloadID:  conveyor.SkipRetry(err),   // archived, not retried
}}
// members not listed succeed
```

A batch handler also serves **single deliveries** of its type (a retried or
released member is redelivered individually, as a batch of one), so you don't
need a separate `HandleFunc` for the same type.

## When a group fires

A group fires when **any** threshold trips, configured server-side (engine
config / `conveyor.yaml`):

| Setting | Meaning | Default |
|---|---|---|
| `group_max_size` | fire once this many members accumulate | 100 |
| `group_max_delay` | fire this long after the **first** member (latency cap) | 1m |
| `group_grace_period` | fire this long after the **last** member (debounce) | 10s |
| `group_sweep_interval` | how often the server evaluates firing | 1s |

The grace period gives coalescing (it resets on each new member); the max-delay
bounds worst-case latency; the max-size bounds batch size.

## Semantics & guarantees

- **At-least-once, like everything else.** A batch shares one lease; heartbeats
  keep it alive while the handler runs. If the worker dies, the whole batch
  redelivers.
- **One slot per batch.** A batch counts as one unit of the worker's
  concurrency, regardless of how many members it carries.
- **Per-member outcomes are individual.** A member that retries or is released
  comes back on its own (not re-aggregated into a new group).
- **Opt-in and back-compatible.** Workers that register no batch handlers never
  receive a batch; ungrouped tasks are completely unaffected.

## Observability

Grouped tasks show up as the **Aggregating** count per queue in the dashboard
and in `AdminService.ListQueues`, and as `conveyor_aggregating`-style state
counts — so you can see a group filling up before it fires.
