# Task dependencies (workflows)

Task dependencies let you **order work**: run one task only after others finish. It is the answer to "send the receipt after the payment settles" (a chain) and to "process every shard, then write the summary" (fan-out then fan-in).

A producer declares that a task **depends on** other tasks. The dependent lands in the **`blocked`** state and is not eligible to run until every task it depends on reaches a terminal success. Completing the last dependency promotes the dependent to `pending` automatically.

## Declare dependencies

Add the `DependsOn` option with the ids of the tasks to wait for:

```go
first, _ := client.Enqueue(ctx, conveyor.NewTask("order:charge", conveyor.JSON(order)))

client.Enqueue(ctx,
    conveyor.NewTask("order:receipt", conveyor.JSON(order)),
    conveyor.DependsOn(first.ID), // runs only after order:charge succeeds
)
```

The dependent is committed in the `blocked` state and stays there until `order:charge` completes; then it becomes `pending` and is dispatched like any other task. Dependencies are referenced by **task id**, so assign your own ids (or use the server-assigned id returned from `Enqueue`, as above).

## Chains

A chain is a dependency per step — each task depends on the one before it:

```go
payload := conveyor.JSON(record)

a, _ := client.Enqueue(ctx, conveyor.NewTask("step:extract", payload))
b, _ := client.Enqueue(ctx, conveyor.NewTask("step:transform", payload), conveyor.DependsOn(a.ID))
client.Enqueue(ctx, conveyor.NewTask("step:load", payload), conveyor.DependsOn(b.ID))
```

`extract` runs first; `transform` unblocks when it succeeds; `load` unblocks when `transform` succeeds.

## Fan-out / fan-in

Fan-out is enqueuing N independent children; fan-in is a **continuation** that depends on all of them. The continuation runs once — after every child succeeds:

```go
var shardIDs []string

for _, shard := range shards {
    child, _ := client.Enqueue(ctx, conveyor.NewTask("report:shard", conveyor.JSON(shard)))
    shardIDs = append(shardIDs, child.ID)
}

// The join: blocked until every shard completes, then runs once.
client.Enqueue(ctx,
    conveyor.NewTask("report:summary", conveyor.JSON(report)),
    conveyor.DependsOn(shardIDs...),
)
```

The shards run in parallel (subject to your workers and queue limits); the summary stays `blocked` until the last shard succeeds.

## Failure policies

By default a dependent that depends on a task which **fails terminally** (its retries are exhausted, it is skipped, or it is canceled) stays blocked forever — the dependency never succeeded. Choose a different policy per dependency with `DependsOnTasks`:

```go
client.Enqueue(ctx,
    conveyor.NewTask("report:summary", conveyor.JSON(report)),
    conveyor.DependsOnTasks(
        conveyor.Dependency{TaskID: requiredID},                                       // BlockOnFailure (default)
        conveyor.Dependency{TaskID: optionalID, OnFailure: conveyor.ContinueOnFailure}, // proceed even if it failed
        conveyor.Dependency{TaskID: childID, OnFailure: conveyor.CascadeCancelOnFailure},
    ),
)
```

- **`BlockOnFailure`** (default) — the dependent stays blocked; the dependency never succeeded, so the dependent never becomes eligible.
- **`ContinueOnFailure`** — the failed dependency is treated as satisfied, so the dependent proceeds once its remaining dependencies clear.
- **`CascadeCancelOnFailure`** — the dependent is canceled, and the cancellation cascades to *its* dependents in turn.

## Semantics & guarantees

- A task with dependencies starts in **`blocked`**, distinct from `pending`, `scheduled`, and `aggregating`. It cannot be leased while blocked.
- A dependency that has **already completed** when the dependent is enqueued is treated as satisfied immediately — the dependent does not block on it. A dependency that already failed is applied through its failure policy at enqueue time.
- Resolution is **at-least-once and eventually consistent**: completing a dependency promotes its dependents promptly, and a background sweep is the safety net, so a dependent is never stranded by a lost wake-up. A dependent may briefly remain `blocked` after its last dependency finishes before it is promoted.
- Dependencies must be **acyclic**. A cycle (A depends on B, B depends on A) leaves every task in it blocked forever — it is never detected or rejected.
- A dependency on a **task id that is never enqueued** blocks the dependent indefinitely; the dependent waits for a task that will never finish.
- A task may depend on at most **1000** tasks.

## Other SDKs

The option exists in every SDK. TypeScript accepts a task id string or a `{ taskId, onFailure }` object:

```ts
await client.enqueue(newTask("order:receipt", json(order)), {
  dependsOn: [chargeId, { taskId: optionalId, onFailure: "continue" }],
});
```

Python takes `depends_on` with strings or `Dependency` values:

```python
await client.enqueue(
    new_task("order:receipt", json(order)),
    depends_on=[charge_id, Dependency("optional", DependencyFailure.CONTINUE)],
)
```

## Observability

Blocked tasks are reported per queue in the management API and dashboard queue stats (the `blocked` count), alongside `pending`, `active`, and the rest, so you can see work waiting on its dependencies at a glance.
