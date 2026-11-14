// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { getEventListeners } from "node:events";

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import { ChunkSealer } from "./chunk.js";
import { ENCRYPTION_MARKER_KEY, ENCRYPTION_MARKER_VALUE, type Encryptor, newAESGCM } from "./encryption.js";
import { ConveyorError } from "./errors.js";
import {
  BatchDispatchSchema,
  DispatchSchema,
  PayloadChunkSchema,
  type Result,
  type ServerMessage,
  ServerMessageSchema,
  TaskOutcome,
  type WorkerMessage,
  WelcomeSchema,
} from "./gen/conveyor/v1/service_pb.js";
import { type TaskEnvelope, TaskEnvelopeSchema } from "./gen/conveyor/v1/task_pb.js";
import { Mux } from "./mux.js";
import {
  CHUNK_WINDOW_EXCEEDED_MESSAGE,
  DEFAULT_PAYLOAD_CHUNK_WINDOW,
  SERVER_PAYLOAD_READ_MESSAGE,
  SESSION_ENDED_MESSAGE,
} from "./stream.js";
import type { Task } from "./task.js";
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

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** How long a test waits for a session to report its results. */
const RESULT_TIMEOUT_MS = 2_000;

/** codec returns an AES-256-GCM encryptor keyed "k1". */
const codec = () => newAESGCM("k1", { id: "k1", secret: new Uint8Array(32).fill(1) });

/** newSession builds a session over the mux that is never connected to a server. */
function newSession(mux: Mux, encryptor?: Encryptor): Session {
  return new Session(undefined as never, { queues: { default: 1 }, concurrency: 2 }, encryptor, mux, new AbortController().signal);
}

/** chunkedEnvelope builds the envelope of a task whose payload arrives in chunks. */
function chunkedEnvelope(id: string, type: string, metadata: Record<string, string> = {}): TaskEnvelope {
  return create(TaskEnvelopeSchema, { id, type, queue: "default", payloadChunked: true, metadata });
}

/** dispatchOf wraps an envelope in a Dispatch frame. */
const dispatchOf = (task: TaskEnvelope): ServerMessage =>
  create(ServerMessageSchema, { frame: { case: "dispatch", value: create(DispatchSchema, { task }) } });

/** chunkOf builds a PayloadChunk frame for a task. */
const chunkOf = (taskId: string, fields: { data?: Uint8Array; last?: boolean; error?: string }): ServerMessage =>
  create(ServerMessageSchema, { frame: { case: "payloadChunk", value: create(PayloadChunkSchema, { taskId, ...fields }) } });

/** deliver applies frames to a session in order, as its receive loop does. */
async function deliver(session: Session, ...messages: ServerMessage[]): Promise<void> {
  for (const message of messages) {
    await session["handleServerMessage"](message);
  }
}

/** results returns the Result frames a session has queued for the server. */
function results(session: Session): Result[] {
  const queue = (session["outbound"] as unknown as { queue: WorkerMessage[] }).queue;

  return queue.flatMap((message) => (message.frame.case === "result" ? [message.frame.value] : []));
}

/** acks returns how many chunks a session has acknowledged for a task. */
function acks(session: Session, taskId: string): number {
  const queue = (session["outbound"] as unknown as { queue: WorkerMessage[] }).queue;

  return queue.reduce(
    (total, message) => total + (message.frame.case === "payloadAck" && message.frame.value.taskId === taskId ? message.frame.value.chunks : 0),
    0,
  );
}

/** welcome applies a Welcome advertising the chunk window, stopping the heartbeat it starts. */
async function welcome(session: Session, payloadChunkWindow: number): Promise<void> {
  await deliver(session, create(ServerMessageSchema, { frame: { case: "welcome", value: create(WelcomeSchema, { payloadChunkWindow }) } }));
  session["stopHeartbeat"]();
}

/** awaitResults waits until a session has queued at least count results. */
async function awaitResults(session: Session, count: number): Promise<Result[]> {
  const deadline = Date.now() + RESULT_TIMEOUT_MS;

  while (results(session).length < count && Date.now() < deadline) {
    await sleep(5, undefined);
  }

  return results(session);
}

describe("Session chunked payloads", () => {
  it("declares payload streaming in Hello", () => {
    const hello = newSession(new Mux())["hello"]();

    expect(hello.frame.case === "hello" && hello.frame.value.payloadStreaming).toBe(true);
  });

  it("runs a handler once its chunked payload is reassembled, heartbeating it meanwhile", async () => {
    const received: string[] = [];
    const session = newSession(
      new Mux().handle("blob", (task) => {
        received.push(task.text());
      }),
    );

    await deliver(session, dispatchOf(chunkedEnvelope("t1", "blob")), chunkOf("t1", { data: encoder.encode("hello ") }));
    expect(session["activeTaskIds"]()).toEqual(["t1"]);
    expect(received).toEqual([]);

    await deliver(session, chunkOf("t1", { data: encoder.encode("world") }), chunkOf("t1", { last: true }));

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.SUCCESS);
    expect(received).toEqual(["hello world"]);
    expect(session["payloads"].size).toBe(0);
  });

  it("starts a streaming handler at once and feeds it the chunks", async () => {
    let started = false;
    let streamed = "";
    let inline = 0;
    const session = newSession(
      new Mux().handleStream("blob", async (task) => {
        started = true;
        inline = task.payload().length;

        for await (const chunk of task.payloadStream()) {
          streamed += decoder.decode(chunk);
        }
      }),
    );

    await deliver(session, dispatchOf(chunkedEnvelope("t1", "blob")), chunkOf("t1", { data: encoder.encode("part one, ") }));
    await sleep(5, undefined);
    expect(started).toBe(true);

    await deliver(session, chunkOf("t1", { data: encoder.encode("part two") }), chunkOf("t1", { last: true }));

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.SUCCESS);
    expect(streamed).toBe("part one, part two");
    expect(inline).toBe(0);
  });

  it("does not stall the session on a streaming handler that stops reading", async () => {
    const session = newSession(new Mux().handleStream("blob", () => {}));

    await deliver(session, dispatchOf(chunkedEnvelope("t1", "blob")));
    await awaitResults(session, 1);

    for (let index = 0; index <= DEFAULT_PAYLOAD_CHUNK_WINDOW; index++) {
      await deliver(session, chunkOf("t1", { data: encoder.encode("unread") }));
    }

    await deliver(session, chunkOf("t1", { last: true }));
    expect(results(session)[0]?.outcome).toBe(TaskOutcome.SUCCESS);
  });

  it("decrypts an end-to-end encrypted chunked payload and strips the marker", async () => {
    const encryptor = codec();
    const sealer = new ChunkSealer(encryptor);
    let seen: Task | undefined;
    const session = newSession(
      new Mux().handle("secret", (task) => {
        seen = task;
      }),
      encryptor,
    );

    await deliver(
      session,
      dispatchOf(chunkedEnvelope("t1", "secret", { [ENCRYPTION_MARKER_KEY]: ENCRYPTION_MARKER_VALUE, tenant: "acme" })),
      chunkOf("t1", { data: await sealer.seal(encoder.encode("launch "), false) }),
      chunkOf("t1", { data: await sealer.seal(encoder.encode("code"), true) }),
      chunkOf("t1", { last: true }),
    );

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.SUCCESS);
    expect(seen?.text()).toBe("launch code");
    expect(seen?.metadata).toEqual({ tenant: "acme" });
  });

  it("retries an encrypted chunked task when the worker has no encryptor", async () => {
    const session = newSession(new Mux().handle("secret", () => {}));

    await deliver(
      session,
      dispatchOf(chunkedEnvelope("t1", "secret", { [ENCRYPTION_MARKER_KEY]: ENCRYPTION_MARKER_VALUE })),
      chunkOf("t1", { data: encoder.encode("sealed") }),
      chunkOf("t1", { last: true }),
    );

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.RETRY);
    expect(result?.errorMsg).toContain("no encryptor");
    expect(session["payloads"].size).toBe(0);
  });

  it("retries a task whose payload the server could not read", async () => {
    const session = newSession(new Mux().handle("blob", () => {}));

    await deliver(session, dispatchOf(chunkedEnvelope("t1", "blob")), chunkOf("t1", { last: true, error: "store down" }));

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.RETRY);
    expect(result?.errorMsg).toBe(`${SERVER_PAYLOAD_READ_MESSAGE}: store down`);
  });

  it("drops chunks for a task with no payload in progress", async () => {
    const session = newSession(new Mux());

    await deliver(session, chunkOf("unknown", { data: encoder.encode("stray") }));

    expect(results(session)).toEqual([]);
  });

  it("runs a batch once every chunked member is reassembled", async () => {
    const received = new Map<string, string>();
    const session = newSession(
      new Mux().handleBatch("blob:batch", (tasks) => {
        for (const task of tasks) {
          received.set(task.id, task.text());
        }
      }),
    );

    const inline = create(TaskEnvelopeSchema, { id: "t0", type: "blob:batch", payload: encoder.encode("inline") });
    const batch = create(ServerMessageSchema, {
      frame: {
        case: "batchDispatch",
        value: create(BatchDispatchSchema, { tasks: [inline, chunkedEnvelope("t1", "blob:batch"), chunkedEnvelope("t2", "blob:batch")] }),
      },
    });

    await deliver(session, batch, chunkOf("t1", { data: encoder.encode("one") }), chunkOf("t2", { data: encoder.encode("two") }));
    await deliver(session, chunkOf("t1", { last: true }));
    await sleep(5, undefined);
    expect(received.size).toBe(0);

    await deliver(session, chunkOf("t2", { last: true }));

    const reported = await awaitResults(session, 3);
    expect(reported.map((result) => result.outcome)).toEqual([TaskOutcome.SUCCESS, TaskOutcome.SUCCESS, TaskOutcome.SUCCESS]);
    expect(Object.fromEntries(received)).toEqual({ t0: "inline", t1: "one", t2: "two" });
  });

  it("retries the whole batch when a member's payload fails", async () => {
    let ran = false;
    const session = newSession(
      new Mux().handleBatch("blob:batch", () => {
        ran = true;
      }),
    );

    const batch = create(ServerMessageSchema, {
      frame: {
        case: "batchDispatch",
        value: create(BatchDispatchSchema, { tasks: [chunkedEnvelope("t1", "blob:batch"), chunkedEnvelope("t2", "blob:batch")] }),
      },
    });

    await deliver(session, batch, chunkOf("t1", { last: true, error: "store down" }));

    const reported = await awaitResults(session, 2);
    expect(reported.map((result) => result.outcome)).toEqual([TaskOutcome.RETRY, TaskOutcome.RETRY]);
    expect(session["payloads"].size).toBe(0);
    expect(ran).toBe(false);
  });

  it("ends every payload still arriving when the session ends", async () => {
    let streamFailure: unknown;
    const session = newSession(
      new Mux()
        .handle("blob", () => {})
        .handleStream("blob:stream", async (task) => {
          try {
            for await (const _chunk of task.payloadStream()) {
              // Drain the stream.
            }
          } catch (error) {
            streamFailure = error;
            throw error;
          }
        }),
    );

    await deliver(session, dispatchOf(chunkedEnvelope("t1", "blob")), dispatchOf(chunkedEnvelope("t2", "blob:stream")));
    await sleep(5, undefined);

    session["abortPayloads"]();

    const reported = await awaitResults(session, 2);
    expect(reported.map((result) => result.errorMsg)).toEqual([SESSION_ENDED_MESSAGE, SESSION_ENDED_MESSAGE]);
    expect(streamFailure).toBeInstanceOf(ConveyorError);
    expect(session["payloads"].size).toBe(0);
  });

  it("takes the chunk window from Welcome, falling back when none is advertised", async () => {
    const session = newSession(new Mux());

    await welcome(session, 4);
    expect(session["chunkWindow"]).toBe(4);

    await welcome(session, 0);
    expect(session["chunkWindow"]).toBe(DEFAULT_PAYLOAD_CHUNK_WINDOW);
  });

  it("acknowledges a buffered chunk on receipt", async () => {
    const session = newSession(new Mux().handle("blob", () => {}));

    await deliver(
      session,
      dispatchOf(chunkedEnvelope("t1", "blob")),
      chunkOf("t1", { data: encoder.encode("one") }),
      chunkOf("t1", { data: encoder.encode("two") }),
    );

    expect(acks(session, "t1")).toBe(2);

    await deliver(session, chunkOf("t1", { last: true }));
    expect(acks(session, "t1")).toBe(2);
  });

  it("acknowledges chunked batch members on receipt", async () => {
    const session = newSession(new Mux().handleBatch("blob:batch", () => {}));
    const batch = create(ServerMessageSchema, {
      frame: { case: "batchDispatch", value: create(BatchDispatchSchema, { tasks: [chunkedEnvelope("t1", "blob:batch")] }) },
    });

    await deliver(session, batch, chunkOf("t1", { data: encoder.encode("one") }));

    expect(acks(session, "t1")).toBe(1);
  });

  it("streams a payload larger than the window as the handler's acknowledgements return credit", async () => {
    const window = 2;
    const parts = ["a", "b", "c", "d", "e", "f", "g"];
    let streamed = "";
    const session = newSession(
      new Mux().handleStream("blob", async (task) => {
        for await (const chunk of task.payloadStream()) {
          await sleep(1, undefined);
          streamed += decoder.decode(chunk);
        }
      }),
    );

    await welcome(session, window);
    await deliver(session, dispatchOf(chunkedEnvelope("t1", "blob")));

    // The fake server keeps at most a window of unacknowledged chunks in flight.
    let sent = 0;
    const deadline = Date.now() + RESULT_TIMEOUT_MS;

    while (sent < parts.length && Date.now() < deadline) {
      if (sent - acks(session, "t1") < window) {
        await deliver(session, chunkOf("t1", { data: encoder.encode(parts[sent]!) }));
        sent += 1;
        continue;
      }

      await sleep(1, undefined);
    }

    await deliver(session, chunkOf("t1", { last: true }));

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.SUCCESS);
    expect(streamed).toBe(parts.join(""));
    expect(acks(session, "t1")).toBe(parts.length);
  });

  it("fails a streamed payload whose server overruns the window", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const session = newSession(
      new Mux().handleStream("blob", async (task) => {
        await gate;
        for await (const _chunk of task.payloadStream()) {
          // Drain the stream.
        }
      }),
    );

    await welcome(session, 2);
    await deliver(
      session,
      dispatchOf(chunkedEnvelope("t1", "blob")),
      chunkOf("t1", { data: encoder.encode("one") }),
      chunkOf("t1", { data: encoder.encode("two") }),
      chunkOf("t1", { data: encoder.encode("over the window") }),
    );

    expect(session["payloads"].size).toBe(0);
    release();

    const [result] = await awaitResults(session, 1);
    expect(result?.outcome).toBe(TaskOutcome.RETRY);
    expect(result?.errorMsg).toBe(CHUNK_WINDOW_EXCEEDED_MESSAGE);
  });
});
