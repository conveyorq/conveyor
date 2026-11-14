// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { execFileSync, type ChildProcess, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer } from "node:net";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Code, ConnectError } from "@connectrpc/connect";

import { Client } from "./client.js";
import { bytes, json, stream } from "./codec.js";
import { newAESGCM } from "./encryption.js";
import { Mux } from "./mux.js";
import { DEFAULT_PAYLOAD_CHUNK_WINDOW, PAYLOAD_CHUNK_BYTES } from "./stream.js";
import { newTask, type Task } from "./task.js";
import { Worker } from "./worker.js";

/** The payload cap the test server runs with, room for a payload larger than the chunk window. */
const MAX_PAYLOAD_BYTES = 32 << 20;

/** A payload size above the inline limit, spanning several chunks with a partial last one. */
const LARGE_PAYLOAD_BYTES = 3 * PAYLOAD_CHUNK_BYTES + 12345;

/** How long a test server waits after a group's last member before firing it. */
const GROUP_GRACE_PERIOD = "200ms";

/** How long the slow streaming handler pauses on each chunk it reads. */
const SLOW_READ_MS = 20;

/** How long a completed task stays visible to getTask. */
const RETENTION_MS = 3_600_000;

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
  // Cluster ports are picked too, so the server never collides with another
  // conveyord already holding the defaults.
  const [remotingPort, discoveryPort, peersPort] = [await freePort(), await freePort(), await freePort()];
  baseUrl = `http://127.0.0.1:${apiPort}`;

  server = spawn(binary, ["--dev"], {
    cwd: repoRoot,
    env: {
      ...process.env,
      CONVEYOR_API__LISTEN: `127.0.0.1:${apiPort}`,
      CONVEYOR_METRICS__LISTEN: `127.0.0.1:${metricsPort}`,
      CONVEYOR_CLUSTER__REMOTING_PORT: String(remotingPort),
      CONVEYOR_CLUSTER__DISCOVERY_PORT: String(discoveryPort),
      CONVEYOR_CLUSTER__PEERS_PORT: String(peersPort),
      CONVEYOR_BROKER__MAX_PAYLOAD_BYTES: String(MAX_PAYLOAD_BYTES),
      CONVEYOR_ENGINE__GROUP_GRACE_PERIOD: GROUP_GRACE_PERIOD,
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

/** largePayload returns a deterministic payload of the given size. */
function largePayload(size = LARGE_PAYLOAD_BYTES): Uint8Array {
  const payload = new Uint8Array(size);
  for (let index = 0; index < payload.length; index++) {
    payload[index] = index % 251;
  }

  return payload;
}

/** sha256 returns the hex SHA-256 digest of data. */
function sha256(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

/** streamDigest reads a task's payload stream to its end and returns its digest. */
async function streamDigest(task: Task): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of task.payloadStream()) {
    hash.update(chunk);
  }

  return hash.digest("hex");
}

/** pieces yields data in pieces of the given size, like a Node stream. */
async function* pieces(data: Uint8Array, size: number): AsyncGenerator<Uint8Array> {
  for (let offset = 0; offset < data.length; offset += size) {
    yield data.subarray(offset, offset + size);
  }
}

/** startWorker runs a worker over the mux until the returned stop is called. */
function startWorker(mux: Mux, options: { encryptor?: ReturnType<typeof newAESGCM> } = {}): () => Promise<void> {
  const worker = new Worker(baseUrl, { queues: { default: 1 }, concurrency: 4, ...options });
  const stop = new AbortController();
  const running = worker.run(mux, stop.signal);

  return async () => {
    stop.abort();
    await running;
  };
}

/** awaitState waits until a task reaches the given state. */
async function awaitState(client: Client, id: string, state: string): Promise<void> {
  await waitUntil(async () => (await client.getTask(id)).state === state, 20_000);
}

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

  it("reports task progress from a handler", async () => {
    const client = new Client(baseUrl);

    const mux = new Mux().handle("report:progress", (_task, ctx) => {
      ctx.reportProgress(50, "halfway");
    });

    const worker = new Worker(baseUrl, { queues: { default: 1 }, concurrency: 2 });
    const stop = new AbortController();
    const running = worker.run(mux, stop.signal);

    const info = await client.enqueue(newTask("report:progress", json({})), { retention: 3_600_000 });

    await waitUntil(async () => {
      const task = await client.getTask(info.id);
      return task.progress === 50 && task.progressMessage === "halfway";
    }, 15_000);

    stop.abort();
    await running;
  }, 30_000);

  it("never runs more handlers at once than the declared concurrency", async () => {
    const client = new Client(baseUrl);
    // One slot across two queues is the case the gate has to catch. The server
    // splits a worker's concurrency across its queues by weight but floors every
    // served queue at one slot, so two queues are granted one credit each: the
    // server can have two dispatches outstanding while the worker declared one,
    // and only the local gate keeps the second handler from starting. A
    // concurrency that divides evenly across the queues would leave the server
    // unable to over-grant, and the assertion would hold with no gate at all.
    const concurrency = 1;
    const perQueue = 4;

    let active = 0;
    let peak = 0;
    let done = 0;

    const handler = async (): Promise<void> => {
      active += 1;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 150));
      active -= 1;
      done += 1;
    };

    const mux = new Mux().handle("gate:a", handler).handle("gate:b", handler);
    const worker = new Worker(baseUrl, { queues: { gatea: 1, gateb: 1 }, concurrency });
    const stop = new AbortController();
    const running = worker.run(mux, stop.signal);

    for (let i = 0; i < perQueue; i += 1) {
      await client.enqueue(newTask("gate:a", json({})), { queue: "gatea", retention: 3_600_000 });
      await client.enqueue(newTask("gate:b", json({})), { queue: "gateb", retention: 3_600_000 });
    }

    await waitUntil(async () => done >= perQueue * 2, 20_000);
    expect(peak).toBeLessThanOrEqual(concurrency);

    stop.abort();
    await running;
  }, 40_000);

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

  it("delivers a payload larger than the inline limit reassembled", async () => {
    const client = new Client(baseUrl);
    const payload = largePayload();
    const digests = new Map<string, string>();

    const stop = startWorker(
      new Mux().handle("blob:process", (task) => {
        digests.set(task.id, sha256(task.payload()));
      }),
    );

    const info = await client.enqueue(newTask("blob:process", bytes(payload)), { retention: RETENTION_MS });
    expect(info.payloadChunked).toBe(true);
    expect(info.payloadSize).toBe(payload.length);

    await awaitState(client, info.id, "completed");
    expect(digests.get(info.id)).toBe(sha256(payload));

    await stop();
  }, 40_000);

  it("streams a stream payload to a streaming handler", async () => {
    const client = new Client(baseUrl);
    const payload = largePayload();
    const digests = new Map<string, string>();

    const stop = startWorker(
      new Mux().handleStream("blob:stream", async (task) => {
        digests.set(task.id, await streamDigest(task));
      }),
    );

    const info = await client.enqueue(newTask("blob:stream", stream(pieces(payload, 64 * 1024))), { retention: RETENTION_MS });

    await awaitState(client, info.id, "completed");
    expect(digests.get(info.id)).toBe(sha256(payload));

    await stop();
  }, 40_000);

  it("streams a payload larger than the chunk window to a slow streaming handler", async () => {
    const client = new Client(baseUrl);
    const payload = largePayload((DEFAULT_PAYLOAD_CHUNK_WINDOW + 4) * PAYLOAD_CHUNK_BYTES + 12345);
    const digests = new Map<string, string>();

    const stop = startWorker(
      new Mux().handleStream("blob:slow-stream", async (task) => {
        const hash = createHash("sha256");
        for await (const chunk of task.payloadStream()) {
          await new Promise((resolve) => setTimeout(resolve, SLOW_READ_MS));
          hash.update(chunk);
        }

        digests.set(task.id, hash.digest("hex"));
      }),
    );

    const info = await client.enqueue(newTask("blob:slow-stream", bytes(payload)), { retention: RETENTION_MS });

    await awaitState(client, info.id, "completed");
    expect(digests.get(info.id)).toBe(sha256(payload));

    await stop();
  }, 60_000);

  it("round-trips a large payload encrypted end to end", async () => {
    const codec = newAESGCM("k1", { id: "k1", secret: new Uint8Array(32).fill(1) });
    const client = new Client(baseUrl, { encryptor: codec });
    const payload = largePayload();
    const digests = new Map<string, string>();

    const stop = startWorker(
      new Mux()
        .handle("blob:secret", (task) => {
          digests.set(task.id, sha256(task.payload()));
        })
        .handleStream("blob:secret-stream", async (task) => {
          digests.set(task.id, await streamDigest(task));
        }),
      { encryptor: codec },
    );

    for (const type of ["blob:secret", "blob:secret-stream"]) {
      const info = await client.enqueue(newTask(type, bytes(payload)), { retention: RETENTION_MS });

      await awaitState(client, info.id, "completed");
      expect(digests.get(info.id), type).toBe(sha256(payload));
    }

    await stop();
  }, 40_000);

  it("retries an encrypted chunked task on a worker without the key", async () => {
    const client = new Client(baseUrl, { encryptor: newAESGCM("k1", { id: "k1", secret: new Uint8Array(32).fill(1) }) });
    const stop = startWorker(new Mux().handle("blob:keyless", () => {}));

    const info = await client.enqueue(newTask("blob:keyless", bytes(largePayload())), { retention: RETENTION_MS });

    await awaitState(client, info.id, "retry");
    expect((await client.getTask(info.id)).lastError).toContain("no encryptor");

    await stop();
  }, 40_000);

  it("delivers chunked batch members reassembled", async () => {
    const client = new Client(baseUrl);
    const payload = largePayload();
    const digests = new Map<string, string>();

    const stop = startWorker(
      new Mux().handleBatch("blob:batch", (tasks) => {
        for (const task of tasks) {
          digests.set(task.id, sha256(task.payload()));
        }
      }),
    );

    const ids: string[] = [];
    for (let member = 0; member < 2; member++) {
      ids.push((await client.enqueue(newTask("blob:batch", bytes(payload)), { group: "blobs", retention: RETENTION_MS })).id);
    }

    for (const id of ids) {
      await awaitState(client, id, "completed");
      expect(digests.get(id)).toBe(sha256(payload));
    }

    await stop();
  }, 40_000);

  it("commits nothing when the payload source breaks", async () => {
    const client = new Client(baseUrl);
    const broken = new Error("source broken");
    const source = (async function* () {
      yield new Uint8Array(PAYLOAD_CHUNK_BYTES);
      throw broken;
    })();

    await expect(client.enqueue(newTask("blob:broken", stream(source)), { taskId: "ts-broken-upload" })).rejects.toBe(broken);
    await expect(client.getTask("ts-broken-upload")).rejects.toThrow();
  }, 30_000);

  it("answers a re-sent streamed task id with the committed task", async () => {
    const client = new Client(baseUrl);
    const first = await client.enqueue(newTask("blob:idempotent", bytes(largePayload())), {
      taskId: "ts-idempotent-upload",
      retention: RETENTION_MS,
    });

    const again = await client.enqueue(newTask("blob:idempotent", bytes(largePayload())), {
      taskId: "ts-idempotent-upload",
      retention: RETENTION_MS,
    });

    expect(again.id).toBe(first.id);
  }, 30_000);

  it("rejects a streamed payload above the server's cap", async () => {
    const client = new Client(baseUrl);

    const rejection = await client.enqueue(newTask("blob:huge", bytes(largePayload(MAX_PAYLOAD_BYTES + 1)))).catch((error: unknown) => error);

    expect(rejection).toBeInstanceOf(ConnectError);
    expect((rejection as ConnectError).code).toBe(Code.InvalidArgument);
  }, 30_000);
});
