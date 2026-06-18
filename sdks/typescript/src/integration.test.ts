// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { execFileSync, type ChildProcess, spawn } from "node:child_process";
import { createServer } from "node:net";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Client } from "./client.js";
import { json } from "./codec.js";
import { newAESGCM } from "./encryption.js";
import { Mux } from "./mux.js";
import { newTask } from "./task.js";
import { Worker } from "./worker.js";

// The repo root, four levels up from sdks/typescript/src.
const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));

let server: ChildProcess;
let baseUrl: string;

/** freePort asks the OS for an unused TCP port. */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      probe.close(() => {
        if (address && typeof address === "object") {
          resolve(address.port);
        } else {
          reject(new Error("could not acquire a free port"));
        }
      });
    });
    probe.on("error", reject);
  });
}

/** waitUntil polls predicate until it resolves true or the deadline passes. */
async function waitUntil(predicate: () => Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate().catch(() => false)) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new Error("waitUntil timed out");
}

beforeAll(async () => {
  const binary = join(mkdtempSync(join(tmpdir(), "conveyord-")), "conveyord");
  execFileSync("go", ["build", "-o", binary, "./cmd/conveyord"], { cwd: repoRoot, stdio: "inherit" });

  const apiPort = await freePort();
  const metricsPort = await freePort();
  baseUrl = `http://127.0.0.1:${apiPort}`;

  server = spawn(binary, ["--dev"], {
    cwd: repoRoot,
    env: {
      ...process.env,
      CONVEYOR_API__LISTEN: `127.0.0.1:${apiPort}`,
      CONVEYOR_METRICS__LISTEN: `127.0.0.1:${metricsPort}`,
    },
    stdio: "ignore",
  });

  // The server is ready once a probe RPC reaches it (a not-found is a healthy
  // reply; a connection refused is not).
  const probe = new Client(baseUrl);
  await waitUntil(async () => {
    try {
      await probe.getTask("readiness-probe");
    } catch (error) {
      return String(error).includes("not found") || String(error).includes("NotFound");
    }
    return true;
  }, 30_000);
}, 120_000);

afterAll(() => {
  server?.kill("SIGKILL");
});

describe("integration against a live conveyord", () => {
  it("round-trips a task from producer to worker to completion", async () => {
    const client = new Client(baseUrl);
    const received: Array<{ userId: number }> = [];

    const mux = new Mux().handle("email:welcome", (task) => {
      received.push(task.json<{ userId: number }>());
    });

    const worker = new Worker(baseUrl, { queues: { default: 1 }, concurrency: 4 });
    const stop = new AbortController();
    const running = worker.run(mux, stop.signal);

    const info = await client.enqueue(newTask("email:welcome", json({ userId: 42 })), { retention: 3_600_000 });

    await waitUntil(async () => (await client.getTask(info.id)).state === "completed", 15_000);
    expect(received).toContainEqual({ userId: 42 });

    stop.abort();
    await running;
  }, 30_000);

  it("round-trips an end-to-end encrypted task", async () => {
    const secret = new Uint8Array(32).fill(7);
    const codec = newAESGCM("k1", { id: "k1", secret });

    const client = new Client(baseUrl, { encryptor: codec });
    let delivered = "";

    const mux = new Mux().handle("secret:task", (task) => {
      delivered = task.json<{ secret: string }>().secret;
    });

    const worker = new Worker(baseUrl, { queues: { default: 1 }, concurrency: 2, encryptor: codec });
    const stop = new AbortController();
    const running = worker.run(mux, stop.signal);

    const info = await client.enqueue(newTask("secret:task", json({ secret: "launch-code" })), {
      retention: 3_600_000,
    });

    await waitUntil(async () => (await client.getTask(info.id)).state === "completed", 15_000);
    expect(delivered).toBe("launch-code");

    stop.abort();
    await running;
  }, 30_000);
});
