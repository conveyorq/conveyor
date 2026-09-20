# Embedded mode

Run the whole system (broker, server, and dispatch) inside your own Go
process, with no separate `conveyord` to deploy and no network hop. Your handler
and enqueue code is identical to a remote deployment. The package is
`github.com/conveyorq/conveyor/embedded`:

```go
system, _ := embedded.Start(ctx, embedded.Config{Broker: embedded.Memory()})
defer system.Stop(ctx)

client := system.Client()
worker := system.Worker(conveyor.WithQueues(map[string]int{"default": 1}), conveyor.WithConcurrency(8))
```

**Use it for:**

- **Tests**: a full-fidelity node in a plain `go test`, with no Docker or
  external infrastructure.
- **Single-process apps**: a CLI, an edge/desktop binary, or a small service
  that wants durable background jobs without operating a separate server.
- **Local development and demos**: producer, worker, and server in one process
  (see [`examples/embedded`](../examples/embedded)).
- **A zero-friction start**: adopt Conveyor now; graduating to a cluster later
  is just swapping `embedded.Start` for `conveyor.NewClient`/`NewWorker` with a
  URL, leaving handler and enqueue code unchanged.

**Durability vs. availability: read this before production.** Embedded is a
single process, which has two distinct consequences people often conflate:

- **Durability is your choice of broker.** `embedded.Memory()` keeps nothing
  across a restart, which is right for tests and disposable work. `embedded.Postgres(dsn)`
  gives the same durability as a remote deployment: queued and in-flight work
  survives a restart.
- **It is not highly available.** One process is a single point of failure;
  while it is down, nothing is dispatched. Embedded always runs as a cluster of
  one on a private loopback port and cannot join or form a multi-node cluster.

So embedded can be **durable** (with Postgres) but never **HA**. For high
availability (no single point of failure, automatic failover) run the real
deployment: multiple `conveyord` nodes clustered over a shared Postgres broker,
where a lost node's work re-activates elsewhere with no task loss. See the
[operations guide](operations.md).
