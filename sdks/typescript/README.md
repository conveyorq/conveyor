# Conveyor TypeScript SDK

`@conveyorq/conveyor` is the TypeScript/Node SDK for [Conveyor](../../README.md),
a distributed, push-based task queue. It speaks the same wire protocol as the Go
SDK — a producer `Client` for enqueuing tasks and a `Worker` for processing
them — so Go and TypeScript producers and workers interoperate on the same
queues.

Requires Node.js ≥ 20. The worker opens a bidirectional gRPC stream, so it runs
on Node (not the browser).

## Install

```sh
npm install @conveyorq/conveyor
```

## Enqueue (producer)

```ts
import { Client, newTask, json } from "@conveyorq/conveyor";

const client = new Client("http://localhost:8080", { token: process.env.CONVEYOR_TOKEN });

const info = await client.enqueue(newTask("email:welcome", json({ userId: 42 })), {
  queue: "email",
  maxRetry: 5,
  unique: 24 * 60 * 60 * 1000, // dedup identical welcomes for 24h
});

console.log(info.id, info.state);
```

## Process (worker)

```ts
import { Worker, Mux, skipRetry } from "@conveyorq/conveyor";

const mux = new Mux().handle("email:welcome", async (task, { signal }) => {
  const { userId } = task.json<{ userId: number }>();
  if (!Number.isInteger(userId)) {
    throw skipRetry("invalid userId"); // permanent failure → dead-letter
  }
  await sendEmail(userId, { signal }); // any other throw → retried with backoff
});

const worker = new Worker("http://localhost:8080", {
  queues: { email: 1 },
  concurrency: 20,
  token: process.env.CONVEYOR_TOKEN,
});

const stop = new AbortController();
process.on("SIGTERM", () => stop.abort()); // graceful drain
await worker.run(mux, stop.signal);
```

The worker implements the full session protocol: concurrency-bounded dispatch,
heartbeats that extend leases on long tasks, best-effort cancellation through
the handler's `AbortSignal`, full-jitter reconnect on transient failures, and a
graceful drain that lets in-flight tasks finish before the stream closes.

## Outcome mapping

| Handler does                                  | Outcome      | Server result                          |
|-----------------------------------------------|--------------|----------------------------------------|
| returns / resolves                            | `SUCCESS`    | completed                              |
| throws `SkipRetry`                            | `SKIP_RETRY` | archived (dead-lettered) immediately   |
| throws anything else (or an uncaught reject)  | `RETRY`      | retried with backoff, then archived    |

## End-to-end encryption

Set the same `Encryptor` on the client and the worker; the server only ever
stores and relays ciphertext. The built-in AES-256-GCM scheme is byte-compatible
with the Go SDK, so an encrypted task enqueued from Go decrypts in TypeScript and
vice versa.

```ts
import { newAESGCM } from "@conveyorq/conveyor";

const codec = newAESGCM("2026-q3", { id: "2026-q3", secret }); // secret: 32-byte Uint8Array
const client = new Client(addr, { encryptor: codec });
const worker = new Worker(addr, { queues: { email: 1 }, concurrency: 10, encryptor: codec });
```

## Parity with the Go SDK

| Capability                         | Go SDK                         | TypeScript SDK                                |
|------------------------------------|--------------------------------|-----------------------------------------------|
| Enqueue                            | `Client.Enqueue`               | `Client.enqueue`                              |
| Get task                           | `Client.GetTask`               | `Client.getTask`                              |
| Enqueue options                    | functional options             | `EnqueueOptions` object                       |
| Queue / priority / max-retry       | `Queue`, `Priority`, `MaxRetry`| `queue`, `priority`, `maxRetry`               |
| Delay                              | `ProcessAt` / `ProcessIn`      | `processAt` / `processIn`                     |
| Deadline / timeout                 | `Deadline` / `Timeout`         | `deadline` / `timeout`                        |
| Expiry (pre-dispatch TTL)          | `ExpiresAt` / `ExpiresIn`      | `expiresAt` / `expiresIn`                     |
| Uniqueness                         | `Unique` / `UniqueKey`         | `unique` / `uniqueKey`                        |
| Retention                          | `Retention`                    | `retention`                                   |
| Group aggregation                  | `Group` / `Mux.HandleBatch`    | `group` / `Mux.handleBatch`                   |
| Enqueue middleware                 | `WithEnqueueMiddleware`        | `enqueueMiddleware`                           |
| Handler middleware                 | `Mux.Use` / `Mux.UseBatch`     | `Mux.use` / `Mux.useBatch`                    |
| Permanent failure                  | `SkipRetry(err)`               | `throw skipRetry(...)` / `SkipRetry`          |
| Cancellation                       | `context.Context`              | `HandlerContext.signal` (`AbortSignal`)       |
| Codecs                             | `JSON` / `Bytes`               | `json` / `bytes` / `text`                     |
| End-to-end encryption              | `WithEncryption` + AES-256-GCM | `encryptor` + `newAESGCM` (byte-compatible)   |
| Min server version                 | `WithMinServerVersion`         | `minServerVersion`                            |

The cross-SDK conformance suite is the executable form of this table.

## Develop

```sh
npm install        # install deps
npm run build      # tsc → dist
npm test           # vitest: unit + an integration test against a live conveyord (needs Go)
```

The protobuf in `src/gen` is generated from `protos/` with
`make sdk-ts-gen` (Connect-ES / `protoc-gen-es`). A full runnable application is
in [`examples/typescript`](../../examples/typescript).
