// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { Code, ConnectError } from "@connectrpc/connect";
import { describe, expect, it, vi } from "vitest";

import { ChunkOpener } from "./chunk.js";
import { Client } from "./client.js";
import { bytes, json, stream } from "./codec.js";
import { ENCRYPTION_MARKER_KEY, ENCRYPTION_MARKER_VALUE, newAESGCM } from "./encryption.js";
import { ConveyorError, DuplicateTaskError } from "./errors.js";
import type { EnqueueRequest, EnqueueStreamRequest } from "./gen/conveyor/v1/service_pb.js";
import { INLINE_PAYLOAD_BYTES, PAYLOAD_CHUNK_BYTES } from "./stream.js";
import { newTask } from "./task.js";

/** An hour in milliseconds, the uniqueness window used by these tests. */
const HOUR_MS = 3_600_000;

/** The failure of a payload source that breaks mid-upload. */
const brokenSource = new Error("source broken");

const encoder = new TextEncoder();

/** info builds the wire TaskInfo the mocked server answers with. */
const info = (id: string) => ({
  id,
  queue: "default",
  type: "blob",
  state: 2,
  priority: 4,
  retried: 0,
  maxRetry: 25,
  lastError: "",
  progress: 0,
  progressMessage: "",
  payloadChunked: true,
  payloadSize: 5n,
});

/** codec returns an AES-256-GCM encryptor keyed "k1". */
const codec = () => newAESGCM("k1", { id: "k1", secret: new Uint8Array(32).fill(1) });

/** An upload as the mocked server received it. */
interface Upload {
  /** header is the task header the upload opened with. */
  header: EnqueueRequest;
  /** chunks are the payload chunks read before the mocked server answered. */
  chunks: Uint8Array[];
  /** committed is the size the upload's commit stated; undefined without a commit. */
  committed: bigint | undefined;
  /** signal is the upload's cancellation signal. */
  signal: AbortSignal | undefined;
}

/**
 * mockRpc replaces the client's task service with one that records each
 * streamed upload, reading at most maxChunks chunks before it answers.
 */
function mockRpc(client: Client, maxChunks = Number.POSITIVE_INFINITY) {
  const uploads: Upload[] = [];

  const enqueueStream = vi.fn(async (requests: AsyncIterable<EnqueueStreamRequest>, options?: { signal?: AbortSignal }) => {
    const upload: Upload = { header: undefined as never, chunks: [], committed: undefined, signal: options?.signal };
    uploads.push(upload);

    for await (const request of requests) {
      if (request.frame.case === "header") {
        upload.header = request.frame.value;
        continue;
      }

      if (request.frame.case === "commit") {
        upload.committed = request.frame.value.size;
        continue;
      }

      upload.chunks.push(request.frame.value as Uint8Array);
      if (upload.chunks.length >= maxChunks) {
        break;
      }
    }

    return { task: info("t1") };
  });
  const enqueue = vi.fn().mockResolvedValue({ task: info("t1") });

  (client as unknown as { rpc: unknown }).rpc = { enqueue, enqueueStream };

  return { enqueue, enqueueStream, uploads };
}

/** pieces yields each piece in turn. */
async function* pieces(...items: Uint8Array[]): AsyncGenerator<Uint8Array> {
  yield* items;
}

describe("Client streamed enqueue", () => {
  it("sends a small in-memory payload inline", async () => {
    const client = new Client("http://localhost:9");
    const { enqueue, enqueueStream } = mockRpc(client);

    await client.enqueue(newTask("blob", bytes(new Uint8Array(INLINE_PAYLOAD_BYTES))));

    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueueStream).not.toHaveBeenCalled();
  });

  it("uploads a payload larger than the inline limit in chunks", async () => {
    const client = new Client("http://localhost:9");
    const { enqueue, uploads } = mockRpc(client);
    const payload = new Uint8Array(INLINE_PAYLOAD_BYTES + 1).fill(7);

    const result = await client.enqueue(newTask("blob", bytes(payload), { metadata: { tenant: "acme" } }), { queue: "blobs" });

    expect(enqueue).not.toHaveBeenCalled();
    expect(uploads).toHaveLength(1);

    const [upload] = uploads;
    expect(upload!.header.payload).toHaveLength(0);
    expect(upload!.header.queue).toBe("blobs");
    expect(upload!.header.type).toBe("blob");
    expect(upload!.header.metadata).toEqual({ tenant: "acme" });
    expect(upload!.chunks.map((chunk) => chunk.length)).toEqual([PAYLOAD_CHUNK_BYTES, 1]);
    expect(Buffer.concat(upload!.chunks).equals(payload)).toBe(true);
    expect(upload!.committed).toBe(BigInt(payload.length));
    expect(result.payloadChunked).toBe(true);
    expect(result.payloadSize).toBe(5);
  });

  it("uploads a stream payload through the streaming enqueue, however small", async () => {
    const client = new Client("http://localhost:9");
    const { uploads } = mockRpc(client);

    await client.enqueue(newTask("blob", stream(pieces(encoder.encode("ab"), encoder.encode("cd")))));

    expect(uploads[0]!.header.contentType).toBe("application/octet-stream");
    expect(Buffer.concat(uploads[0]!.chunks).toString()).toBe("abcd");
  });

  it("seals each chunk and marks the header when encryption is on", async () => {
    const encryptor = codec();
    const client = new Client("http://localhost:9", { encryptor });
    const { uploads } = mockRpc(client);

    await client.enqueue(newTask("blob", stream(pieces(encoder.encode("secret")))));

    const [upload] = uploads;
    expect(upload!.header.metadata[ENCRYPTION_MARKER_KEY]).toBe(ENCRYPTION_MARKER_VALUE);
    expect(upload!.chunks).toHaveLength(1);

    expect(upload!.committed).toBe(BigInt(upload!.chunks[0]!.length));

    const opener = new ChunkOpener(encryptor);
    expect(Buffer.from(await opener.open(upload!.chunks[0]!)).toString()).toBe("secret");
    expect(() => opener.finish()).not.toThrow();
  });

  it("cancels the upload without a commit and rethrows a payload source failure", async () => {
    const client = new Client("http://localhost:9");
    const { uploads } = mockRpc(client);

    const source = (async function* () {
      yield new Uint8Array(PAYLOAD_CHUNK_BYTES);
      throw brokenSource;
    })();

    await expect(client.enqueue(newTask("blob", stream(source)))).rejects.toBe(brokenSource);
    expect(uploads[0]!.chunks).toHaveLength(1);
    expect(uploads[0]!.committed).toBeUndefined();
    expect(uploads[0]!.signal?.aborted).toBe(true);
  });

  it("rethrows a payload source failure over the server's rejection", async () => {
    const client = new Client("http://localhost:9");
    const { enqueueStream } = mockRpc(client);
    enqueueStream.mockImplementation(async (requests) => {
      for await (const _request of requests) {
        // Read every frame, as the server does before rejecting the upload.
      }

      throw new ConnectError("the task header must be sent once, first", Code.InvalidArgument);
    });

    const source = (async function* () {
      yield* [];
      throw brokenSource;
    })();

    await expect(client.enqueue(newTask("blob", stream(source)))).rejects.toBe(brokenSource);
  });

  it("returns the server's answer when it ends the upload early and closes the source", async () => {
    const client = new Client("http://localhost:9");
    const { uploads } = mockRpc(client, 1);
    let closed = false;

    const source = (async function* () {
      try {
        for (;;) {
          yield new Uint8Array(PAYLOAD_CHUNK_BYTES);
        }
      } finally {
        closed = true;
      }
    })();

    const result = await client.enqueue(newTask("blob", stream(source)), { taskId: "t1" });

    expect(result.id).toBe("t1");
    expect(uploads[0]!.chunks).toHaveLength(1);
    expect(uploads[0]!.committed).toBeUndefined();
    await vi.waitFor(() => expect(closed).toBe(true));
  });

  it("maps a duplicate rejection to DuplicateTaskError", async () => {
    const client = new Client("http://localhost:9");
    const { enqueueStream } = mockRpc(client);
    enqueueStream.mockRejectedValue(new ConnectError("duplicate", Code.AlreadyExists));

    await expect(client.enqueue(newTask("blob", stream(pieces())), { unique: HOUR_MS, uniqueKey: "k" })).rejects.toBeInstanceOf(
      DuplicateTaskError,
    );
  });

  it("requires an explicit unique key for a stream payload", async () => {
    const client = new Client("http://localhost:9");
    const { uploads } = mockRpc(client);

    await expect(client.enqueue(newTask("blob", stream(pieces())), { unique: HOUR_MS })).rejects.toBeInstanceOf(ConveyorError);

    await client.enqueue(newTask("blob", stream(pieces())), { unique: HOUR_MS, uniqueKey: "k" });
    expect(uploads[0]!.header.uniqueKey).toBe("k");
  });

  it("derives the unique key of a large in-memory payload", async () => {
    const client = new Client("http://localhost:9");
    const { uploads } = mockRpc(client);

    await client.enqueue(newTask("blob", bytes(new Uint8Array(INLINE_PAYLOAD_BYTES + 1))), { unique: HOUR_MS });

    expect(uploads[0]!.header.uniqueKey).toMatch(/^[0-9a-f]{64}$/);
  });

  it("reports the stored payload size and layout of an inline task", async () => {
    const client = new Client("http://localhost:9");
    const { enqueue } = mockRpc(client);
    enqueue.mockResolvedValue({ task: { ...info("t2"), payloadChunked: false, payloadSize: 2n } });

    const result = await client.enqueue(newTask("blob", json(1)));

    expect(result.payloadChunked).toBe(false);
    expect(result.payloadSize).toBe(2);
  });
});
