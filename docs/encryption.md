# End-to-end encryption

Encryption protects task **payloads** so the server stores ciphertext only and
holds no keys. Your client seals a payload before it is enqueued and your worker
opens it on dispatch. The encryption happens entirely in your processes, at the
edges of the queue. A compromised database, a compromised server, or an
untrusted operator sees nothing but ciphertext.

This works because a payload is already opaque to Conveyor: the server only ever
stores and relays the bytes, it never inspects them. Turning on encryption
changes what those bytes are; it changes nothing on the server.

Encryption is a **seam**, not a fixed scheme. A built-in AES-256-GCM
implementation ships for the common case, and any `encryption.Encryptor`
(backed by a KMS, an HSM, or your own codec) drops into the same option.

## Turn it on

Set the **same** encryptor on every client and worker that share a queue, with
`WithEncryption`. Build the built-in scheme with `encryption.NewAESGCM`:

```go
secret := make([]byte, 32) // a 32-byte AES-256 key, from your secret store
// ... load secret ...

enc, err := encryption.NewAESGCM("2026-q3", encryption.Key{ID: "2026-q3", Secret: secret})
if err != nil {
    log.Fatal(err)
}

client, _ := conveyor.NewClient(addr, conveyor.WithEncryption(enc))
worker, _ := conveyor.NewWorker(addr,
    conveyor.WithQueues(map[string]int{"default": 1}),
    conveyor.WithEncryption(enc))
```

That is all. The client seals each payload before enqueue; the worker opens it
before your handler runs, so `task.Bind(&v)` and `task.Payload()` see the
original plaintext.

A `nil` encryptor is ignored, so encryption stays off and you can pass an
optionally-configured encryptor without a `nil` check.

## From the CLI

`conveyor enqueue` takes `--encryption-key` (or `$CONVEYOR_ENCRYPTION_KEY`),
formatted as `<id>:<base64-secret>`, a key id and a standard-base64-encoded
32-byte secret:

```sh
conveyor enqueue email:welcome --json '{"user_id":42}' \
  --encryption-key "2026-q3:$(head -c 32 /dev/urandom | base64)"
```

The task is sealed before it leaves the CLI. A worker configured with the same
id and secret decrypts it; the id must match so the worker can find the right
secret.

## Coexistence

Encrypted and plaintext tasks can share a queue. A worker decrypts **only**
tasks that an encrypting client sealed, so a plaintext task from a
non-encrypting client is processed unchanged. You can roll encryption out
gradually, one client at a time.

If an **encrypted** task reaches a worker that has no encryptor, or one whose
key cannot open it, the task **fails without running the handler** and follows
the normal retry/dead-letter path. The worker never hands ciphertext to your
code.

## Key rotation

Keep the old key alongside the new one and make the new one active. New tasks
seal under the active key; tasks still pending under the old key decrypt because
the worker's keyring still holds it:

```go
enc, _ := encryption.NewAESGCM("2026-q3", // active: seals new data
    encryption.Key{ID: "2026-q3", Secret: newSecret},
    encryption.Key{ID: "2026-q2", Secret: oldSecret}) // retained: opens old data
```

Each ciphertext records the id of the key that sealed it, so rotation needs no
coordination beyond shipping the new keyring. Drop a retired key only once no
pending task still references its id.

## What is and isn't covered

- **Payloads are encrypted.** Task metadata, type, queue, and timing are not,
  because the server needs them to route and schedule. Keep secrets in the payload.
- **The dashboard shows ciphertext** for encrypted tasks. The server cannot
  decrypt for display because it has no key, which is the point.
- **Results** are not encrypted today because the Go SDK emits no result bytes.
- **Raw-protocol clients** (hand-rolling the wire protocol without an SDK) do
  not get encryption from the stock binary; use an SDK or run embedded.
- **Unique tasks still work.** The uniqueness key is derived from the plaintext
  before the payload is sealed, so `Unique(ttl)` still rejects duplicate work
  even though every ciphertext is different.

## See also

- The [`encryption`](../encryption) package provides the `Encryptor` seam and the
  built-in AES-256-GCM implementation.
- The [Operations guide](operations.md) covers deployment and configuration.
