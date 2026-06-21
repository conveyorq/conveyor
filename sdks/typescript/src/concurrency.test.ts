// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";

import { Client } from "./client.js";
import { json } from "./codec.js";
import { newTask } from "./task.js";

const taskInfo = { id: "t1", queue: "default", type: "t", state: 2, priority: 4, retried: 0, maxRetry: 25, lastError: "" };

describe("concurrencyKey mapping", () => {
  it("sets concurrencyKey on the request", async () => {
    const client = new Client("http://localhost:9");
    const enqueue = vi.fn().mockResolvedValue({ task: taskInfo });
    (client as unknown as { rpc: { enqueue: typeof enqueue } }).rpc = { enqueue };

    await client.enqueue(newTask("t", json(1)), { concurrencyKey: "customer:42" });

    const request = enqueue.mock.calls[0]![0];
    expect(request.concurrencyKey).toBe("customer:42");
  });

  it("defaults concurrencyKey to empty", async () => {
    const client = new Client("http://localhost:9");
    const enqueue = vi.fn().mockResolvedValue({ task: taskInfo });
    (client as unknown as { rpc: { enqueue: typeof enqueue } }).rpc = { enqueue };

    await client.enqueue(newTask("t", json(1)));

    const request = enqueue.mock.calls[0]![0];
    expect(request.concurrencyKey).toBe("");
  });
});
