// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Conformance worker for the cross-SDK protocol suite (see conformance/ in the
// repo root). It is not a usage example: the Go harness spawns it, drives it
// through one scenario chosen by env vars, and asserts protocol behavior by
// observing the server. Kept deliberately minimal.
//
//   SCENARIO   "sleep" (default): run handlers that sleep SLEEP_MS then succeed,
//              plus large-payload handlers that check the payload digest.
//              "bad-hello": construct a worker with an invalid queue weight,
//              which must be rejected locally (exit 1), never a reconnect loop.
//   SLEEP_MS   handler sleep in milliseconds (sleep scenario).
//   EXPECTED_SHA256  hex SHA-256 digest a large payload must match; a mismatch
//              fails the task without retry (sleep scenario).
//   ENCRYPTION_KEY_HEX  hex 32-byte AES-256 key; when set, the worker opens
//              end-to-end encrypted payloads with it.
//   ENCRYPTION_KEY_ID  the id of that key.
//   CONVEYOR_ADDR  server base URL.

import { createHash } from "node:crypto";

import { type Encryptor, Mux, newAESGCM, skipRetry, Worker } from "@conveyorq/conveyor";

const addr = process.env.CONVEYOR_ADDR ?? "http://localhost:8080";
const scenario = process.env.SCENARIO ?? "sleep";
const sleepMs = Number(process.env.SLEEP_MS ?? "0");
const queue = process.env.QUEUE ?? "default";
const expectedDigest = process.env.EXPECTED_SHA256 ?? "";

/** sleep resolves after ms milliseconds, ignoring cancellation on purpose: the
 * suite checks that a handler outliving the lease keeps its lease via
 * heartbeats, and that a drain waits for it rather than losing it. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** checkDigest fails, without retry, unless payload hashes to the expected SHA-256 hex digest. */
function checkDigest(digest: string): void {
  if (digest !== expectedDigest) {
    throw skipRetry(`payload digest ${digest}, want ${expectedDigest}`);
  }
}

/** encryptor builds the worker's AES-256-GCM encryptor from the env, or none when no key is set. */
function encryptor(): Encryptor | undefined {
  const keyHex = process.env.ENCRYPTION_KEY_HEX ?? "";
  if (keyHex === "") {
    return undefined;
  }

  const keyId = process.env.ENCRYPTION_KEY_ID ?? "";

  return newAESGCM(keyId, { id: keyId, secret: Buffer.from(keyHex, "hex") });
}

async function main(): Promise<void> {
  if (scenario === "bad-hello") {
    try {
      // A zero weight is invalid; the constructor must reject it locally.
      new Worker(addr, { queues: { default: 0 }, concurrency: 1 });
    } catch {
      process.exit(1);
    }

    console.error("bad-hello: an invalid queue weight was accepted");
    process.exit(2);
  }

  const mux = new Mux()
    .handle("conformance:task", async () => {
      await sleep(sleepMs);
    })
    .handleBatch("conformance:batch", async () => {
      await sleep(sleepMs);
    })
    // A large payload must arrive intact, reassembled for a buffered handler
    // and as a stream for a streaming one; a wrong digest archives the task.
    .handle("conformance:large", (task) => {
      checkDigest(createHash("sha256").update(task.payload()).digest("hex"));
    })
    .handleStream("conformance:large-stream", async (task) => {
      const hash = createHash("sha256");
      for await (const chunk of task.payloadStream()) {
        hash.update(chunk);
      }

      checkDigest(hash.digest("hex"));
    });

  const worker = new Worker(addr, { queues: { [queue]: 1 }, concurrency: 8, encryptor: encryptor() });

  const stop = new AbortController();
  process.on("SIGTERM", () => stop.abort());
  process.on("SIGINT", () => stop.abort());

  await worker.run(mux, stop.signal);
}

main().catch((error) => {
  console.error(`conformance worker: ${String(error)}`);
  process.exit(1);
});
