// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from "vitest";

import { Client } from "./client.js";
import { json } from "./codec.js";
import { DependencyFailurePolicy } from "./gen/conveyor/v1/task_pb.js";
import { newTask } from "./task.js";

// taskInfo is a minimal Enqueue response the mocked rpc returns.
const taskInfo = { id: "t1", queue: "default", type: "t", state: 9, priority: 4, retried: 0, maxRetry: 25, lastError: "" };

describe("dependsOn mapping", () => {
  it("maps task ids and per-dependency failure policies to the request", async () => {
    const client = new Client("http://localhost:9");
    const enqueue = vi.fn().mockResolvedValue({ task: taskInfo });
    // Override the private rpc with a capturing mock.
    (client as unknown as { rpc: { enqueue: typeof enqueue } }).rpc = { enqueue };

    await client.enqueue(newTask("t", json(1)), {
      dependsOn: ["upstream", { taskId: "sibling", onFailure: "cascade-cancel" }, { taskId: "optional", onFailure: "continue" }],
    });

    // Assert field-by-field off the captured request, never on the whole
    // protobuf message (deep-equal on a ConnectRPC message OOMs).
    const request = enqueue.mock.calls[0]![0];
    expect(request.dependsOn).toHaveLength(3);

    expect(request.dependsOn[0].taskId).toBe("upstream");
    expect(request.dependsOn[0].onFailure).toBe(DependencyFailurePolicy.BLOCK);

    expect(request.dependsOn[1].taskId).toBe("sibling");
    expect(request.dependsOn[1].onFailure).toBe(DependencyFailurePolicy.CASCADE_CANCEL);

    expect(request.dependsOn[2].taskId).toBe("optional");
    expect(request.dependsOn[2].onFailure).toBe(DependencyFailurePolicy.CONTINUE);
  });

  it("omits dependsOn when none are declared", async () => {
    const client = new Client("http://localhost:9");
    const enqueue = vi.fn().mockResolvedValue({ task: taskInfo });
    (client as unknown as { rpc: { enqueue: typeof enqueue } }).rpc = { enqueue };

    await client.enqueue(newTask("t", json(1)));

    const request = enqueue.mock.calls[0]![0];
    expect(request.dependsOn).toHaveLength(0);
  });
});
