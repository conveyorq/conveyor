// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { getEventListeners } from "node:events";

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import { ConveyorError } from "./errors.js";
import { BatchDispatchSchema, DispatchSchema, ServerMessageSchema } from "./gen/conveyor/v1/service_pb.js";
import { TaskEnvelopeSchema } from "./gen/conveyor/v1/task_pb.js";
import { Mux } from "./mux.js";
import { Session, Worker, sleep } from "./worker.js";

// The worker validates its session contract locally at construction, so a Hello
// the server would reject fails fast instead of becoming an endless reconnect
// loop against a permanent invalid_argument.
describe("Worker Hello validation", () => {
  it("rejects a worker with no queues", () => {
    expect(() => new Worker("http://localhost:8080", { queues: {}, concurrency: 1 })).toThrow(ConveyorError);
  });

  it("rejects an empty queue name", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { "": 1 }, concurrency: 1 })).toThrow(ConveyorError);
  });

  it("rejects a non-positive queue weight", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { default: 0 }, concurrency: 1 })).toThrow(ConveyorError);
    expect(() => new Worker("http://localhost:8080", { queues: { default: -1 }, concurrency: 1 })).toThrow(ConveyorError);
  });

  // The server restricts queue names to ^[a-zA-Z0-9][a-zA-Z0-9-_.]*$, so a name
  // outside it is refused on every reconnect; the worker must not get that far.
  it("rejects a queue name the server's pattern refuses", () => {
    for (const name of ["my queue", "-leading", ".leading", "has/slash", "héllo"]) {
      expect(
        () => new Worker("http://localhost:8080", { queues: { [name]: 1 }, concurrency: 1 }),
        `queue name ${JSON.stringify(name)} must be rejected`,
      ).toThrow(ConveyorError);
    }
  });

  it("accepts the queue names the server's pattern allows", () => {
    for (const name of ["default", "high-priority", "billing_v2", "a.b.c", "0queue"]) {
      expect(
        () => new Worker("http://localhost:8080", { queues: { [name]: 1 }, concurrency: 1 }),
        `queue name ${JSON.stringify(name)} must be accepted`,
      ).not.toThrow();
    }
  });

  it("rejects a non-positive concurrency", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { default: 1 }, concurrency: 0 })).toThrow(ConveyorError);
  });

  it("accepts a valid contract", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { default: 1, high: 2 }, concurrency: 4 })).not.toThrow();
  });
});

// The reconnect loop sleeps against the worker's own abort signal on every
// attempt, so a listener left behind per sleep would accumulate for as long as
// the server stays down.
describe("sleep", () => {
  it("removes its abort listener once the timer fires", async () => {
    const controller = new AbortController();

    for (let attempt = 0; attempt < 5; attempt++) {
      await sleep(1, controller.signal);
    }

    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  it("resolves early when the signal aborts", async () => {
    const controller = new AbortController();
    const started = Date.now();
    const sleeping = sleep(10_000, controller.signal);

    controller.abort();
    await sleeping;

    expect(Date.now() - started).toBeLessThan(1_000);
  });
});

// A task dispatched after the drain began is never started, but its lease is
// kept alive by the heartbeat until the stream closes, so the server releases
// it without a retry penalty instead of reaping it.
describe("Session drain", () => {
  it("parks a dispatch received while draining and keeps heartbeating it", () => {
    const mux = new Mux();
    mux.handle("demo", async () => {});

    const session = new Session(undefined as never, { queues: { default: 1 }, concurrency: 2 }, undefined, mux, new AbortController().signal);
    session["draining"] = true;

    const envelope = (id: string) => create(TaskEnvelopeSchema, { id, type: "demo", queue: "default" });

    session["handleServerMessage"](
      create(ServerMessageSchema, { frame: { case: "dispatch", value: create(DispatchSchema, { task: envelope("t1") }) } }),
    );
    session["handleServerMessage"](
      create(ServerMessageSchema, {
        frame: { case: "batchDispatch", value: create(BatchDispatchSchema, { tasks: [envelope("t2"), envelope("t3")], group: "g" }) },
      }),
    );

    expect([...session["parked"]]).toEqual(["t1", "t2", "t3"]);
    expect(session["inflight"].size).toBe(0);
    expect(session["activeTaskIds"]().sort()).toEqual(["t1", "t2", "t3"]);
  });
});
