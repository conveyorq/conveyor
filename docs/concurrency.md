# Per-key concurrency limits

Concurrency limits cap **how many tasks run at the same time per key** — for example "at most 5 reports in flight per `customer_id`", or "exactly one active task per external resource" (a mutex). Held-back tasks wait **pending** and dispatch the moment a slot frees; nothing is dropped and no retry is spent.

## The two parts: a key and a limit

A concurrency limit is **two things working together** — neither does anything alone:

1. **The concurrency key** — a label *you put on a task*, e.g. `ConcurrencyKey("customer:42")`. It is just a string and **carries no number of its own**. It only answers *which tasks share a budget*: all tasks on a queue with the same key value are counted together; a different key (or no key) is a separate budget.
2. **The limit** — a single number *set on the queue*, e.g. `--max 5`. It answers *how large each key's budget is*: the most tasks of any one key the queue runs at once.

Put together, the rule is:

> For each distinct value of the concurrency key, the queue dispatches at most
> `max` of its tasks at the same time, holding the rest pending until one
> finishes.

So the **key is the grouping** and the **queue's limit is the number**. A key with no limit set on its queue is inert; a limit with no keyed tasks does nothing.

**Worked example** — queue `reports` with `--max 2`:

| Enqueued on the queue        | Active at once             | Held pending      |
|------------------------------|----------------------------|-------------------|
| 3 tasks, key `customer:1`    | 2 of `customer:1`          | 1 of `customer:1` |
| 3 tasks, key `customer:2`    | 2 of `customer:2`          | 1 of `customer:2` |
| 2 tasks, no key              | both                       | —                 |

Each key gets its own budget of 2, so the two customers run fully in parallel (4 active), while keyless tasks are never throttled.

## What the key does not change

The concurrency key is used **only** for this count. It does not change anything else about the task or borrow any other setting's value:

- **It is independent of rate limiting.** A rate limit (tasks/second, a token bucket) and a concurrency limit (simultaneous active per key) are *separate settings that both apply* — a queue may have either, both, or neither. The rate limit bounds *how many tasks start per second*; the concurrency limit bounds *how many are active at once* per key. The key plays no part in rate limiting.
- **It does not touch priority, retries, deadlines, uniqueness, or routing** — those come from their own options. In particular it is **not** the unique key: `UniqueKey` forbids a duplicate task outright, whereas a concurrency key lets duplicates exist but throttles how many run together.
- **It is not a queue or a group.** Counting happens per key *within one queue*; `ConcurrencyKey` is mutually exclusive with `Group`, which batches members on a separate dispatch path.

## Declare a concurrency key

Tag the task with a key; tasks sharing it on the same queue count against that key's budget.

```go
client.Enqueue(ctx,
    conveyor.NewTask("report:build", conveyor.JSON(job)),
    conveyor.ConcurrencyKey("customer:42"), // counts against customer 42's budget
)
```

The key is any string you choose — a customer id, a resource name, a tenant. Use the **same** key for the work you want to throttle or serialize together.

## Set a per-queue limit

The limit is the most tasks sharing **any one key** the queue runs at once. It is set at runtime and stored durably (shared across servers, surviving restarts), so there is no redeploy to change one.

**CLI:**

```sh
conveyor concurrency set reports --max 5   # ≤ 5 active per key on the "reports" queue
conveyor concurrency ls                    # list per-queue limits
conveyor concurrency rm reports            # clear it; the queue's keys go unbounded
```

**Dashboard:** the **Concurrency** tab lists, sets, and clears per-queue limits live.

**API:** `AdminService.SetQueueConcurrencyLimit` / `DeleteQueueConcurrencyLimit` / `ListConcurrencyLimits`.

A queue with no limit leaves its keys unbounded — a concurrency key is then just an inert tag.

> **No global default.** Unlike rate limiting — which has a server-config default
> (`rate_limit_rate_per_sec` / `rate_limit_burst`) that every queue falls back to
> — concurrency has **no `conveyord` config setting and no server-wide default**.
> The limit is set *per queue* only, at runtime. A queue you have not given a
> limit is simply unlimited; there is no fallback value to inherit.

## Other SDKs

```ts
await client.enqueue(newTask("report:build", json(job)), { concurrencyKey: "customer:42" });
```

```python
await client.enqueue(new_task("report:build", json(job)), concurrency_key="customer:42")
```

## Behavior

- The limit is enforced **per key**, not per queue: `--max 5` lets each distinct key run up to 5 tasks at once, in parallel across keys.
- A task held back for a saturated key goes back to **pending** and dispatches the moment a same-key task completes and frees a slot. It keeps its place by priority; no retry is spent.
- Enforcement is **per queue grain**, so a key's limit is counted within one queue. A limit is not shared across queues or across an aggregation group's batch.
- Changing or clearing a limit takes effect on the live queue immediately; the in-flight counts are preserved, so lowering a limit simply holds new dispatches until a key drains below it.
- Limits are advisory under failover: a queue grain that relocates rebuilds its per-key counts from new dispatches, like all of its disposable state. Briefly after a relocation a key may run slightly above its limit until the counts re-converge.
- A key's slot is freed when its task completes (including a normal failure or skip), and a task whose worker crashes is re-dispatched without losing its place. One edge does not self-correct immediately: a task **dead-lettered by the reaper** after its retries are exhausted through repeated lease expiry (sustained worker crashes) holds its slot until the queue grain next passivates. The effect is conservative — it only over-restricts that one key, never over-admits — and clears on the next passivation or relocation.

## Fairness

Enforcement is grain-side: the queue leases tasks in priority order and holds back the ones whose key is saturated. A saturated **high-priority** key can therefore sit at the head of the line and delay lower-priority tasks of other keys until it drains or the next maintenance sweep re-leases. If strict cross-key fairness matters, give the contended work its own queue.

## Observability

The `conveyor_concurrency_throttled` metric counts lease cycles in which a queue held a task back because its key was saturated, labeled by queue — a signal that a key is contended.
