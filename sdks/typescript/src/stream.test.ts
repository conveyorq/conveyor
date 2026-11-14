// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import { ChunkOpener, ChunkSealer, ChunkTruncatedError } from "./chunk.js";
import { type Encryptor, newAESGCM } from "./encryption.js";
import { ConveyorError } from "./errors.js";
import { EnqueueRequestSchema, type EnqueueStreamRequest, PayloadChunkSchema } from "./gen/conveyor/v1/service_pb.js";
import {
  ChunkStream,
  PAYLOAD_CHUNK_BYTES,
  PayloadAssembly,
  type PayloadResult,
  CHUNK_WINDOW_EXCEEDED_MESSAGE,
  SERVER_PAYLOAD_READ_MESSAGE,
  uploadChunks,
  uploadRequests,
} from "./stream.js";

/** A payload size above the inline limit, spanning several chunks with a partial last one. */
const LARGE_PAYLOAD_BYTES = 3 * PAYLOAD_CHUNK_BYTES + 12345;

/** The chunk window the stream tests run with. */
const TEST_CHUNK_WINDOW = 4;

/** The piece size a Node stream typically yields. */
const SOURCE_PIECE_BYTES = 64 * 1024;

/** The failure of a source that breaks mid-payload. */
const brokenSource = new Error("source broken");

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** largePayload returns a deterministic payload of LARGE_PAYLOAD_BYTES. */
function largePayload(): Uint8Array {
  const payload = new Uint8Array(LARGE_PAYLOAD_BYTES);
  for (let index = 0; index < payload.length; index++) {
    payload[index] = index % 251;
  }

  return payload;
}

/** codec returns an AES-256-GCM encryptor keyed "k1". */
const codec = () => newAESGCM("k1", { id: "k1", secret: new Uint8Array(32).fill(1) });

/** failingEncryptor fails every call. */
const failingEncryptor: Encryptor = {
  encrypt: () => {
    throw brokenSource;
  },
  decrypt: () => {
    throw brokenSource;
  },
};

/** pieces yields data in pieces of the given size. */
async function* pieces(data: Uint8Array, size: number): AsyncGenerator<Uint8Array> {
  for (let offset = 0; offset < data.length; offset += size) {
    yield data.subarray(offset, offset + size);
  }
}

/** brokenAfter yields data, then fails. */
async function* brokenAfter(data: Uint8Array): AsyncGenerator<Uint8Array> {
  yield data;
  throw brokenSource;
}

/** collect drains an async iterable into an array. */
async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const items: T[] = [];
  for await (const item of iterable) {
    items.push(item);
  }

  return items;
}

/** chunkFrame builds a PayloadChunk frame. */
const chunkFrame = (fields: { data?: Uint8Array; last?: boolean; error?: string }) =>
  create(PayloadChunkSchema, { taskId: "t1", ...fields });

describe("uploadChunks", () => {
  it("splits an in-memory payload into chunks", async () => {
    const payload = largePayload();

    const chunks = await collect(uploadChunks(pieces(payload, payload.length), undefined));

    expect(chunks.map((chunk) => chunk.length)).toEqual([PAYLOAD_CHUNK_BYTES, PAYLOAD_CHUNK_BYTES, PAYLOAD_CHUNK_BYTES, 12345]);
    expect(Buffer.concat(chunks).equals(payload)).toBe(true);
  });

  it("regroups small source pieces into full chunks", async () => {
    const payload = largePayload();

    const chunks = await collect(uploadChunks(pieces(payload, SOURCE_PIECE_BYTES), undefined));

    expect(chunks).toHaveLength(4);
    expect(Buffer.concat(chunks).equals(payload)).toBe(true);
  });

  it("passes a piece that fills a chunk through without copying", async () => {
    const piece = new Uint8Array(PAYLOAD_CHUNK_BYTES);

    const [chunk] = await collect(uploadChunks(pieces(piece, piece.length), undefined));

    expect(chunk!.buffer).toBe(piece.buffer);
  });

  it("yields nothing for an empty plaintext payload", async () => {
    expect(await collect(uploadChunks(pieces(new Uint8Array(0), 1), undefined))).toHaveLength(0);
  });

  it("reports a source failure", async () => {
    await expect(collect(uploadChunks(brokenAfter(encoder.encode("partial")), undefined))).rejects.toBe(brokenSource);
  });

  it("rejects a source yielding something other than bytes", async () => {
    const source = (async function* () {
      yield "text" as unknown as Uint8Array;
    })();

    await expect(collect(uploadChunks(source, undefined))).rejects.toThrow(ConveyorError);
  });

  it("seals each chunk on its own", async () => {
    const encryptor = codec();
    const payload = largePayload();

    const sealed = await collect(uploadChunks(pieces(payload, payload.length), encryptor));
    expect(sealed).toHaveLength(4);

    const opener = new ChunkOpener(encryptor);
    const opened: Uint8Array[] = [];

    for (const chunk of sealed) {
      opened.push(await opener.open(chunk));
    }

    expect(() => opener.finish()).not.toThrow();
    expect(Buffer.concat(opened).equals(payload)).toBe(true);
  });

  it("seals an empty payload as one chunk marked last", async () => {
    const encryptor = codec();

    const sealed = await collect(uploadChunks(pieces(new Uint8Array(0), 1), encryptor));
    expect(sealed).toHaveLength(1);

    const opener = new ChunkOpener(encryptor);
    expect(await opener.open(sealed[0]!)).toHaveLength(0);
    expect(() => opener.finish()).not.toThrow();
  });

  it("reports a seal failure", async () => {
    await expect(collect(uploadChunks(pieces(encoder.encode("data"), 4), failingEncryptor))).rejects.toBe(brokenSource);
  });

  it("reports a source failure when sealing", async () => {
    await expect(collect(uploadChunks(brokenAfter(encoder.encode("partial")), codec()))).rejects.toBe(brokenSource);
  });
});

describe("uploadRequests", () => {
  const header = create(EnqueueRequestSchema, { type: "blob:process" });

  it("sends the header, one message per chunk, then the commit stating the size", async () => {
    const requests = await collect(uploadRequests(header, pieces(encoder.encode("abcde"), 2), () => {}));

    expect(requests.map((request) => request.frame.case)).toEqual(["header", "chunk", "chunk", "chunk", "commit"]);
    expect((requests[0]!.frame.value as { type: string }).type).toBe("blob:process");
    expect(decoder.decode(requests[2]!.frame.value as Uint8Array)).toBe("cd");
    expect((requests[4]!.frame.value as { size: bigint }).size).toBe(5n);
  });

  it("commits an empty payload with size zero", async () => {
    const requests = await collect(uploadRequests(header, pieces(new Uint8Array(0), 1), () => {}));

    expect(requests.map((request) => request.frame.case)).toEqual(["header", "commit"]);
    expect((requests[1]!.frame.value as { size: bigint }).size).toBe(0n);
  });

  it("hands a source failure to the callback and rethrows it without a commit", async () => {
    const failures: unknown[] = [];
    const cases: (string | undefined)[] = [];

    await expect(
      (async () => {
        for await (const request of uploadRequests(header, brokenAfter(encoder.encode("x")), (error) => failures.push(error))) {
          cases.push(request.frame.case);
        }
      })(),
    ).rejects.toBe(brokenSource);

    expect(failures).toEqual([brokenSource]);
    expect(cases).toEqual(["header", "chunk"]);
  });

  it("closes the source and sends no commit when returned early", async () => {
    let closed = false;
    const source = (async function* () {
      try {
        yield encoder.encode("first");
        yield encoder.encode("second");
      } finally {
        closed = true;
      }
    })();

    const requests: AsyncGenerator<EnqueueStreamRequest> = uploadRequests(header, source, () => {});
    await requests.next();
    await requests.next();

    expect((await requests.return(undefined)).done).toBe(true);
    expect(closed).toBe(true);
  });
});

/** countAcks returns an acknowledgement callback and a reader of how many times it ran. */
function countAcks(): { ack: () => void; acks: () => number } {
  let count = 0;

  return { ack: () => (count += 1), acks: () => count };
}

describe("ChunkStream", () => {
  it("reads chunks, acknowledging each, then ends", async () => {
    const { ack, acks } = countAcks();
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, ack);
    expect(stream.push(encoder.encode("hello "))).toBe(true);
    expect(stream.push(encoder.encode("world"))).toBe(true);
    stream.end();

    expect(decoder.decode(Buffer.concat(await collect(stream)))).toBe("hello world");
    expect(acks()).toBe(2);
  });

  it("reports the end failure after the last chunk, once", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});
    stream.push(encoder.encode("partial"));
    stream.end(brokenSource);

    const iterator = stream[Symbol.asyncIterator]();
    expect((await iterator.next()).done).toBe(false);
    await expect(iterator.next()).rejects.toBe(brokenSource);
    expect((await iterator.next()).done).toBe(true);
  });

  it("hands a chunk to a read already waiting, acknowledging it", async () => {
    const { ack, acks } = countAcks();
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, ack);
    const waiting = stream[Symbol.asyncIterator]().next();

    expect(stream.push(encoder.encode("late"))).toBe(true);

    expect(decoder.decode((await waiting).value as Uint8Array)).toBe("late");
    expect(acks()).toBe(1);
  });

  it("wakes a read already waiting when it ends", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});
    const waiting = stream[Symbol.asyncIterator]().next();

    stream.end(brokenSource);

    await expect(waiting).rejects.toBe(brokenSource);
  });

  it("never waits on a push, refusing a chunk beyond the window", () => {
    const { ack, acks } = countAcks();
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, ack);

    for (let index = 0; index < TEST_CHUNK_WINDOW; index++) {
      expect(stream.push(encoder.encode("x"))).toBe(true);
    }

    expect(stream.push(encoder.encode("over the window"))).toBe(false);

    stream.abandon();
    stream.abandon();

    expect(stream.push(encoder.encode("dropped"))).toBe(true);
    expect(acks()).toBe(0);
  });

  it("makes room for another chunk as the handler reads", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});

    for (let index = 0; index < TEST_CHUNK_WINDOW; index++) {
      stream.push(encoder.encode("x"));
    }

    await stream[Symbol.asyncIterator]().next();

    expect(stream.push(encoder.encode("next"))).toBe(true);
  });

  it("is abandoned when the reader breaks out of its loop", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});
    stream.push(encoder.encode("first"));

    for await (const chunk of stream) {
      expect(decoder.decode(chunk)).toBe("first");
      break;
    }

    for (let index = 0; index <= TEST_CHUNK_WINDOW; index++) {
      expect(stream.push(encoder.encode("dropped"))).toBe(true);
    }
  });

  it("is abandoned when the reader throws into it", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});

    expect((await stream[Symbol.asyncIterator]().throw!(brokenSource)).done).toBe(true);
    expect((await stream[Symbol.asyncIterator]().next()).done).toBe(true);
  });
});

describe("PayloadAssembly", () => {
  /** buffered returns an assembly that records its result and counts its acknowledgements. */
  function buffered(opener?: ChunkOpener): { assembly: PayloadAssembly; result: () => PayloadResult | undefined; acks: () => number } {
    let result: PayloadResult | undefined;
    const { ack, acks } = countAcks();
    const assembly = new PayloadAssembly(
      opener,
      (outcome) => {
        result = outcome;
      },
      ack,
    );

    return { assembly, result: () => result, acks };
  }

  it("buffers chunks until the last frame, acknowledging each on receipt", async () => {
    const { assembly, result, acks } = buffered();

    expect(await assembly.add(chunkFrame({ data: encoder.encode("ab") }))).toBe(false);
    expect(await assembly.add(chunkFrame({ data: encoder.encode("cd") }))).toBe(false);
    expect(result()).toBeUndefined();
    expect(acks()).toBe(2);
    expect(await assembly.add(chunkFrame({ last: true }))).toBe(true);

    expect(decoder.decode((result() as { payload: Uint8Array }).payload)).toBe("abcd");
    expect(acks()).toBe(2);
  });

  it("reports the server's read failure", async () => {
    const { assembly, result } = buffered();

    expect(await assembly.add(chunkFrame({ last: true, error: "store down" }))).toBe(true);

    const { error } = result() as { error: Error };
    expect(error).toBeInstanceOf(ConveyorError);
    expect(error.message).toBe(`${SERVER_PAYLOAD_READ_MESSAGE}: store down`);
  });

  it("decrypts each chunk and verifies the payload's end", async () => {
    const encryptor = codec();
    const sealer = new ChunkSealer(encryptor);
    const { assembly, result } = buffered(new ChunkOpener(encryptor));

    expect(await assembly.add(chunkFrame({ data: await sealer.seal(encoder.encode("first"), false) }))).toBe(false);
    expect(await assembly.add(chunkFrame({ last: true }))).toBe(true);

    expect((result() as { error: unknown }).error).toBeInstanceOf(ChunkTruncatedError);
  });

  it("reassembles an encrypted payload", async () => {
    const encryptor = codec();
    const sealer = new ChunkSealer(encryptor);
    const { assembly, result } = buffered(new ChunkOpener(encryptor));

    await assembly.add(chunkFrame({ data: await sealer.seal(encoder.encode("sealed "), false) }));
    await assembly.add(chunkFrame({ data: await sealer.seal(encoder.encode("payload"), true) }));
    await assembly.add(chunkFrame({ last: true }));

    expect(decoder.decode((result() as { payload: Uint8Array }).payload)).toBe("sealed payload");
  });

  it("reports an undecryptable chunk without acknowledging it", async () => {
    const { assembly, result, acks } = buffered(new ChunkOpener(codec()));

    expect(await assembly.add(chunkFrame({ data: encoder.encode("not ciphertext") }))).toBe(true);
    expect(result()).toHaveProperty("error");
    expect(acks()).toBe(0);
  });

  it("feeds a streaming handler's stream, which acknowledges what it reads", async () => {
    const { ack, acks } = countAcks();
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, ack);
    const assembly = new PayloadAssembly(undefined, stream);

    expect(await assembly.add(chunkFrame({ data: encoder.encode("streamed") }))).toBe(false);
    expect(acks()).toBe(0);
    expect(await assembly.add(chunkFrame({ last: true }))).toBe(true);

    expect(decoder.decode(Buffer.concat(await collect(stream)))).toBe("streamed");
    expect(acks()).toBe(1);
  });

  it("fails a stream the server overfills beyond the window", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});
    const assembly = new PayloadAssembly(undefined, stream);

    for (let index = 0; index < TEST_CHUNK_WINDOW; index++) {
      expect(await assembly.add(chunkFrame({ data: encoder.encode("x") }))).toBe(false);
    }

    expect(await assembly.add(chunkFrame({ data: encoder.encode("over the window") }))).toBe(true);
    await expect(collect(stream)).rejects.toThrow(CHUNK_WINDOW_EXCEEDED_MESSAGE);
  });

  it("ends a streaming handler's stream with a failure", async () => {
    const stream = new ChunkStream(TEST_CHUNK_WINDOW, () => {});
    const assembly = new PayloadAssembly(undefined, stream);

    assembly.finish(brokenSource);

    await expect(collect(stream)).rejects.toBe(brokenSource);
  });
});
