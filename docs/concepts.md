# Concepts

The vocabulary you need to use Conveyor, in plain terms. Conveyor moves units
of work from the code that creates them to the code that runs them, durably and
in order of priority. Five pieces do that:

| Term                     | One line                                                                               |
|--------------------------|----------------------------------------------------------------------------------------|
| **Task**                 | A single unit of work (a type name plus a payload) to run later.                       |
| **Queue**                | A named, weighted channel that tasks flow through.                                     |
| **Client** (producer)    | Your code that enqueues tasks.                                                         |
| **Server** (`conveyord`) | Owns the queues and decides who runs what, when.                                       |
| **Worker**               | Your code that receives tasks from the server and executes them.                       |
| **Broker**               | The durable store (Postgres, or in-memory for dev) that is the single source of truth. |

A **client** enqueues a **task** onto a **queue**; the **server** persists it in
the **broker**, then pushes it to a **worker** that has free capacity; the
worker runs it and reports the result. Everything is persisted *before* it is
dispatched, so no crash loses work.

## Task

A task is one job to do: a **type** (a string like `"email:welcome"` that selects
the handler) and a **payload** (the bytes the handler needs, whether JSON,
protobuf, or raw). Tasks may be enqueued to run now, after a delay, at a scheduled time, or on
a cron expression, and carry options such as a priority, a retry budget, a
timeout, and a uniqueness key. The server stores every task and tracks its state
(pending, active, completed, failed, …) for its whole lifecycle.

## Queue

A queue is a named channel (`"default"`, `"emails"`, `"billing"`) that tasks
are enqueued onto. Queues are **weighted**: when several queues have work
waiting, the server dispatches from them in proportion to their weights, so a
busy low-priority queue never starves a high-priority one. Queues can be paused,
resumed, and rate-limited at runtime. They are created on demand; there is no
separate "declare a queue" step.

## Client (producer)

The client is the half of an SDK that **enqueues** tasks. It opens a connection
to the server, hands over a task (type, payload, options), and gets back a task
id. That is the end of its involvement: the client does not run the task and
does not wait for it. A client never talks to a worker directly; the server sits
between them. Any number of clients can enqueue into the same queues.

## Server (`conveyord`)

The server owns the queues and all the scheduling logic: it persists tasks,
enforces priorities and weights, applies retries with backoff, fires delayed and
cron tasks when due, recovers leases when a worker dies, and exposes the
admin/inspection API and dashboard. It holds no durable state of its own;
everything authoritative lives in the broker, so a server node can be lost and
its work re-activates on another node with nothing dropped. You run one process
(possibly embedded) for development, or a cluster of nodes over a shared broker
for high availability.

## Worker

**A worker is your process that runs tasks.** It is the counterpart to the
client: where the client *puts work in*, the worker *takes work out and executes
it*. You build a worker with an SDK, register a **handler** for each task type it
knows how to run, and start it; from then on the server streams matching tasks to
it and the worker invokes the right handler for each one.

A worker is **external** to the server and **stateless**: it holds no durable
state, so you can start, stop, scale, and redeploy workers freely. Run as many
as you like; together they form a **fleet**, and the server spreads work across
whichever workers have spare capacity. A worker is *not* a thread or a goroutine;
it is a whole process (or a long-lived object inside one) that may run many tasks
concurrently.

### What a worker declares

When a worker connects it tells the server two things:

- **Which queues it serves**, with a weight per queue. The server only ever sends
  it tasks from those queues.
- **Its concurrency**, the total number of tasks it can execute at once across
  all its queues.

### How a worker receives work: push, not poll

The worker opens **one long-lived connection** and then waits. It never polls and
never asks for tasks. The server pushes due tasks down that connection, **up to
the worker's declared concurrency**, and it will never have more than that many
tasks outstanding to one worker at a time. Each time the worker finishes a task
and reports the result, one slot frees up and the server may push the next task.
A saturated worker simply stops being sent more; the extra work waits safely in
the broker. This is the core of Conveyor's model: there is no poll interval to
tune and no Redis.

### The lifecycle of a worker session

1. **Connect & declare.** The worker opens its connection and announces its
   queues and concurrency.
2. **Receive.** The server pushes leased tasks, never exceeding the declared
   concurrency.
3. **Execute.** The worker runs the matching handler for each task, up to
   concurrency tasks at once.
4. **Heartbeat.** While tasks are in flight the worker periodically heartbeats so
   the server knows it is alive; this keeps each in-flight task's **lease** from
   expiring. (Defaults: a lease lives 60s and is renewed every 20s.)
5. **Report.** When a handler returns, the worker reports the outcome (success,
   retryable failure, or skip-retry) and the slot reopens.
6. **Drain.** On graceful shutdown the worker stops accepting new tasks, finishes
   (or hands back) its in-flight ones, and disconnects. Anything not completed is
   redispatched to another worker, so **rolling deploys cost no work**.

If the connection drops, the worker **reconnects automatically** with jittered
backoff, so a restarting server does not stampede the fleet.

### Execution guarantees

- **At-least-once.** A task is delivered until a worker acknowledges it. If a
  worker dies mid-task, its lease expires and the server **redelivers** the task
  (possibly to another worker). Because a task can therefore run more than once,
  **handlers must be idempotent.**
- **Failures and retries.** A handler that returns an error is retried with
  exponential backoff up to the task's retry budget, then dead-lettered. Wrap an
  error in `SkipRetry` to dead-letter it immediately without retrying. A handler
  that panics is recovered and treated as a retryable failure, so one bad task
  never takes the worker down.
- **Timeouts.** Each dispatched task carries an effective deadline (the smaller of
  its lease, its own deadline, and its timeout); a handler that runs past it is
  canceled.

## Broker

The broker is the **only durable thing** in the system: every task, lease,
schedule, and queue setting lives there. In production it is Postgres; for
development it can be in-memory. Because the broker is the single source of
truth, the server and workers are both disposable: lose either and the
authoritative state is untouched. Payloads can optionally be
[end-to-end encrypted](encryption.md) so the broker stores ciphertext only.

## How they fit together

```
  Client ──enqueue──▶ Server ──persist──▶ Broker
                        │  ▲
                  push  │  │  result
                        ▼  │
                      Worker ──runs your handler
```

The client and the worker are **your** code (built with an SDK); the server and
broker are the infrastructure you run. A client and a worker for the same queues
need never know about each other; the server and broker decouple them in space
(different processes/hosts) and in time (a task can wait in the broker until a
worker is free).

## Administering

The SDK is the **produce and consume** surface: a client enqueues, a worker runs
your handlers. It deliberately does not include operational actions on tasks or
queues. Those are operator concerns, and you drive them with the **`conveyor`
CLI** and the **dashboard**: reschedule, run-now, cancel, delete, or archive a
task; pause and resume queues; set rate and concurrency limits; manage cron.
Keeping administration out of the SDK keeps application code small and draws a
clear line between running work and operating the system.

## Where to go next

- [Writing a worker](../README.md#writing-a-worker): the minimal Go worker, with
  handler registration and graceful shutdown.
- Build a client and worker in your language:
  - [Go SDK](../sdks/go/README.md): the reference client and worker.
  - [TypeScript SDK](../sdks/typescript/README.md): enqueue and process tasks from Node.
  - [Python SDK](../sdks/python/README.md): async and sync clients and workers.
- [Architecture](architecture.md): how the server implements all of this
  internally (for contributors and SDK authors).
- [Wire protocol](protocol.md): the normative session protocol for building a
  worker in another language.
