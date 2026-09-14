// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Conformance worker for the cross-SDK protocol suite (see conformance/ in the
// repo root). It is not a usage example: the Go harness spawns it, drives it
// through one scenario chosen by env vars, and asserts protocol behavior by
// observing the server. Kept deliberately minimal.
//
//   SCENARIO   "sleep" (default): run handlers that sleep SLEEP_MS then succeed.
//              "bad-hello": construct a worker with an invalid queue weight,
//              which must be rejected locally (exit 1), never a reconnect loop.
//   SLEEP_MS   handler sleep in milliseconds (sleep scenario).
//   CONVEYOR_ADDR  server base URL.

import { Mux, Worker } from "@conveyorq/conveyor";

const addr = process.env.CONVEYOR_ADDR ?? "http://localhost:8080";
const scenario = process.env.SCENARIO ?? "sleep";
const sleepMs = Number(process.env.SLEEP_MS ?? "0");
const queue = process.env.QUEUE ?? "default";

/** sleep resolves after ms milliseconds, ignoring cancellation on purpose: the
 * suite checks that a handler outliving the lease keeps its lease via
 * heartbeats, and that a drain waits for it rather than losing it. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    });

  const worker = new Worker(addr, { queues: { [queue]: 1 }, concurrency: 8 });

  const stop = new AbortController();
  process.on("SIGTERM", () => stop.abort());
  process.on("SIGINT", () => stop.abort());

  await worker.run(mux, stop.signal);
}

main().catch((error) => {
  console.error(`conformance worker: ${String(error)}`);
  process.exit(1);
});
