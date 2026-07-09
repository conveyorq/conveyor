# Conveyor Go SDK

`github.com/conveyorq/conveyor/sdks/go` is the Go SDK for
[Conveyor](../../README.md), a distributed, push-based task queue. It provides a
producer `Client` for enqueuing tasks and a `Worker` for processing them, and
speaks the same wire protocol as the TypeScript and Python SDKs — so producers
and workers written in any of the three interoperate on the same queues.

Requires Go 1.26+. The worker opens a bidirectional gRPC stream to a running
Conveyor server (`conveyord`); see the [project README](../../README.md) to
start one.

## Install

The SDK is distributed as a Go module, so no package registry is involved —
`go get` resolves it straight from the repository:

```sh
go get github.com/conveyorq/conveyor/sdks/go
```

Import it under a short alias, since the package name is `conveyor`:

```go
import conveyor "github.com/conveyorq/conveyor/sdks/go"
```

## Enqueue (producer)

```go
client, err := conveyor.NewClient("http://localhost:8080",
	conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")),
)
if err != nil {
	log.Fatal(err)
}

info, err := client.Enqueue(ctx,
	conveyor.NewTask("email:welcome", conveyor.JSON(map[string]any{"user_id": 42})),
	conveyor.Queue("email"),
	conveyor.MaxRetry(5),
	conveyor.Unique(24*time.Hour), // dedup identical welcomes for 24h
)
if err != nil {
	log.Fatal(err)
}

fmt.Println(info.ID, info.State)
```

Every enqueue option is a functional option: `TaskID`, `Queue`, `MaxRetry`,
`Priority`, `Timeout`, `Deadline`, `ProcessAt`, `ProcessIn`, `ExpiresAt`,
`ExpiresIn`, `Retention`, `Unique`, `UniqueKey`, and `Group`. Durations are
`time.Duration`; absolute times are `time.Time`.

## Process (worker)

```go
worker, err := conveyor.NewWorker("http://localhost:8080",
	conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")),
	conveyor.WithQueues(map[string]int{"email": 1}),
	conveyor.WithConcurrency(20),
)
if err != nil {
	log.Fatal(err)
}

mux := conveyor.NewMux()

mux.HandleFunc("email:welcome", func(ctx context.Context, task *conveyor.Task) error {
	var payload struct {
		UserID int `json:"user_id"`
	}
	if err := task.Bind(&payload); err != nil {
		return conveyor.SkipRetry(err) // permanent failure → dead-letter
	}

	return sendEmail(ctx, payload.UserID) // any other error → retried with backoff
})

// Run blocks until ctx is cancelled, then drains in-flight tasks gracefully.
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

if err := worker.Run(ctx, mux); err != nil {
	log.Fatal(err)
}
```

`WithQueues` maps each served queue to its dispatch weight, and `WithConcurrency`
bounds how many tasks run at once. The worker implements the full session
protocol: concurrency-bounded dispatch, heartbeats that extend leases on long
tasks, best-effort cancellation through the handler's `context.Context`,
full-jitter reconnect on transient failures, and a graceful drain that lets
in-flight tasks finish before the stream closes.

## Outcome mapping

| Handler returns           | Outcome      | Server result                        |
|---------------------------|--------------|--------------------------------------|
| `nil`                     | `SUCCESS`    | completed                            |
| `conveyor.SkipRetry(err)` | `SKIP_RETRY` | archived (dead-lettered) immediately |
| any other non-nil error   | `RETRY`      | retried with backoff, then archived  |

A wrapped `SkipRetry` is still detected, and `conveyor.IsSkipRetry(err)` reports
whether an error carries the marker.

## Payload codecs

A payload is opaque bytes plus a content type. Build one with `conveyor.JSON(v)`
(the default), `conveyor.Bytes(b)` for raw bytes, or `conveyor.Proto(m)` for a
protobuf message. Decode a received task with `task.Bind(&v)`, or read
`task.Payload()` and `task.ContentType()` for the raw bytes. Producer and
consumer must agree on the JSON shape of a given task type; the bytes are
interoperable across the Go, TypeScript, and Python SDKs.

## End-to-end encryption

Set the same `Encryptor` on the client and the worker via `WithEncryption`; the
server only ever stores and relays ciphertext. The built-in AES-256-GCM scheme
is byte-compatible with the other SDKs, so an encrypted task enqueued from Go
decrypts in TypeScript or Python and vice versa:

```go
import "github.com/conveyorq/conveyor/encryption"

codec, err := encryption.NewAESGCM("2026-q3", encryption.Key{ID: "2026-q3", Secret: secret}) // secret: 32 bytes
if err != nil {
	log.Fatal(err)
}

client, _ := conveyor.NewClient(addr, conveyor.WithEncryption(codec))
worker, _ := conveyor.NewWorker(addr,
	conveyor.WithQueues(map[string]int{"email": 1}),
	conveyor.WithConcurrency(10),
	conveyor.WithEncryption(codec),
)
```

Pass several `Key` values to `NewAESGCM` to rotate: the active key seals new data
while retired keys still open existing data.

## Examples

Runnable programs live in
[`examples/standalone`](../../examples/standalone) (a separate producer and
worker against a `conveyord` node) and
[`examples/embedded`](../../examples/embedded) (server and worker in one
process).

## License

Apache-2.0.
