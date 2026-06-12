# Embedded example

The whole Conveyor system — server, worker, client — inside one Go
process, asynq-style: zero external infrastructure.

## Run it

```sh
go run ./examples/embedded
```

The program starts an embedded node on a loopback port, enqueues ten
welcome emails, processes them with an in-process worker, and exits.

## Moving to a real cluster

The handler and enqueue code is identical to the
[standalone example](../standalone); the migration is swapping
constructors:

```go
// embedded
system, _ := embedded.Start(ctx, embedded.Config{Broker: embedded.Memory()})
client := system.Client()
worker := system.Worker(conveyor.WithQueues(map[string]int{"default": 1}))

// remote
client, _ := conveyor.NewClient("https://conveyor.internal:8080", conveyor.WithToken(token))
worker, _ := conveyor.NewWorker("https://conveyor.internal:8080", conveyor.WithToken(token),
    conveyor.WithQueues(map[string]int{"default": 1}), conveyor.WithConcurrency(8))
```

Durability follows the broker: `embedded.Memory()` loses queued work when
the process dies; `embedded.Postgres(dsn)` keeps the same at-least-once
guarantees as a remote deployment.
