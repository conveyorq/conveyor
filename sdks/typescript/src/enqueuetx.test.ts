// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";

import { Client } from "./client.js";
import { json } from "./codec.js";
import { ConveyorError } from "./errors.js";
import { newTask } from "./task.js";

const info = (id: string) => ({
  id,
  queue: "default",
  type: "t",
  state: 2,
  priority: 4,
  retried: 0,
  maxRetry: 25,
  lastError: "",
});

describe("enqueueTx", () => {
  it("rejects an empty task list", async () => {
    const client = new Client("http://localhost:9");

    await expect(client.enqueueTx([])).rejects.toBeInstanceOf(ConveyorError);
  });

  it("sends one request per item with its own options and returns tasks in order", async () => {
    const client = new Client("http://localhost:9");
    const enqueueTx = vi.fn().mockResolvedValue({ tasks: [info("tx-1"), info("tx-2")] });
    (client as unknown as { rpc: { enqueueTx: typeof enqueueTx } }).rpc = { enqueueTx };

    const tasks = await client.enqueueTx([
      { task: newTask("test:a", json(1)), options: { taskId: "tx-1", queue: "billing" } },
      { task: newTask("test:b", json(2)), options: { taskId: "tx-2", queue: "mail" } },
    ]);

    expect(tasks.map((t) => t.id)).toEqual(["tx-1", "tx-2"]);

    const request = enqueueTx.mock.calls[0]![0];
    expect(request.tasks).toHaveLength(2);
    expect(request.tasks[0].queue).toBe("billing");
    expect(request.tasks[1].queue).toBe("mail");
    expect(request.tasks[0].type).toBe("test:a");
  });

  it("propagates a per-task validation error and sends nothing", async () => {
    const client = new Client("http://localhost:9");
    const enqueueTx = vi.fn();
    (client as unknown as { rpc: { enqueueTx: typeof enqueueTx } }).rpc = { enqueueTx };

    await expect(
      client.enqueueTx([{ task: newTask("t", json(1)), options: { processAt: new Date(), processIn: 1000 } }]),
    ).rejects.toBeInstanceOf(ConveyorError);

    expect(enqueueTx).not.toHaveBeenCalled();
  });
});
