// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { create } from "@bufbuild/protobuf";

import { type ChunkOpener, ChunkSealer } from "./chunk.js";
import type { Encryptor } from "./encryption.js";
import { ConveyorError } from "./errors.js";
import {
  type EnqueueRequest,
  type EnqueueStreamRequest,
  EnqueueStreamRequestSchema,
  type PayloadChunk,
  PayloadCommitSchema,
} from "./gen/conveyor/v1/service_pb.js";

/**
 * The largest payload the server accepts inline on Enqueue. A larger payload,
 * or one supplied as a stream, is uploaded in chunks through EnqueueStream
 * instead. It mirrors the server's fixed inline limit.
 *
 * @internal
 */
export const INLINE_PAYLOAD_BYTES = 1 << 20;

/**
 * The plaintext size of one uploaded chunk. The server admits chunks up to
 * 2 MiB, leaving room for encryption overhead.
 *
 * @internal
 */
export const PAYLOAD_CHUNK_BYTES = 1 << 20;

/**
 * The per-task chunk window assumed when the server's Welcome advertises none.
 *
 * @internal
 */
export const DEFAULT_PAYLOAD_CHUNK_WINDOW = 16;

/** The failure a payload still arriving reports when its session ends. */
export const SESSION_ENDED_MESSAGE = "conveyor: session ended before the payload arrived";

/** The failure that ends a streamed payload whose server sent more chunks than its window allows. */
export const CHUNK_WINDOW_EXCEEDED_MESSAGE = "conveyor: server exceeded the payload chunk window";

/** The prefix of the failure reported when the server ends a payload early. */
export const SERVER_PAYLOAD_READ_MESSAGE = "conveyor: server could not read the payload";

/** An empty payload, sealed as one chunk so its end is authenticated. */
const EMPTY_CHUNK = new Uint8Array(0);

/**
 * uploadChunks yields a payload as upload chunks of at most
 * {@link PAYLOAD_CHUNK_BYTES}. With an encryptor, each chunk is sealed on its
 * own (see {@link ChunkSealer}), and an empty payload is still sent as one
 * sealed chunk so its end is authenticated.
 *
 * @internal
 */
export function uploadChunks(source: AsyncIterable<Uint8Array>, encryptor: Encryptor | undefined): AsyncIterable<Uint8Array> {
  const plain = sizedChunks(source);
  if (encryptor === undefined) {
    return plain;
  }

  return sealedChunks(encryptor, plain);
}

/**
 * sizedChunks regroups the source's pieces into chunks of exactly
 * {@link PAYLOAD_CHUNK_BYTES}, except a shorter final one. A piece that already
 * fills a chunk is passed through without copying.
 */
async function* sizedChunks(source: AsyncIterable<Uint8Array>): AsyncGenerator<Uint8Array> {
  let parts: Uint8Array[] = [];
  let size = 0;

  for await (const piece of source) {
    if (!(piece instanceof Uint8Array)) {
      throw new ConveyorError("conveyor: a stream payload must yield Uint8Array chunks");
    }

    let offset = 0;

    while (offset < piece.length) {
      const take = Math.min(PAYLOAD_CHUNK_BYTES - size, piece.length - offset);
      parts.push(piece.subarray(offset, offset + take));
      size += take;
      offset += take;

      if (size === PAYLOAD_CHUNK_BYTES) {
        yield joinParts(parts);
        parts = [];
        size = 0;
      }
    }
  }

  if (size > 0) {
    yield joinParts(parts);
  }
}

/** joinParts concatenates chunk parts, returning a lone part as is. */
function joinParts(parts: Uint8Array[]): Uint8Array {
  return parts.length === 1 ? parts[0]! : Buffer.concat(parts);
}

/**
 * sealedChunks seals each plaintext chunk, looking one chunk ahead so the
 * final chunk carries the last flag.
 */
async function* sealedChunks(encryptor: Encryptor, plain: AsyncIterable<Uint8Array>): AsyncGenerator<Uint8Array> {
  const sealer = new ChunkSealer(encryptor);
  let pending: Uint8Array | undefined;

  for await (const data of plain) {
    if (pending !== undefined) {
      yield await sealer.seal(pending, false);
    }

    pending = data;
  }

  yield await sealer.seal(pending ?? EMPTY_CHUNK, true);
}

/**
 * uploadRequests yields the EnqueueStream messages: the task header, one
 * message per chunk, and, once the source has been read to its end, the commit
 * stating the total size sent. The server commits the task only on a matching
 * commit, so an upload cut short commits nothing. A failure pulling the next
 * chunk is handed to `onSourceError` before it is rethrown, so the caller can
 * cancel the upload. Returning the generator early, as the transport does once
 * the server has answered, closes the chunk source.
 *
 * @internal
 */
export async function* uploadRequests(
  header: EnqueueRequest,
  chunks: AsyncIterable<Uint8Array>,
  onSourceError: (error: unknown) => void,
): AsyncGenerator<EnqueueStreamRequest> {
  yield create(EnqueueStreamRequestSchema, { frame: { case: "header", value: header } });

  const iterator = chunks[Symbol.asyncIterator]();
  let size = 0n;

  try {
    for (;;) {
      const next = await pullChunk(iterator, onSourceError);
      if (next.done === true) {
        break;
      }

      yield create(EnqueueStreamRequestSchema, { frame: { case: "chunk", value: next.value } });
      size += BigInt(next.value.length);
    }
  } finally {
    await iterator.return?.();
  }

  yield create(EnqueueStreamRequestSchema, { frame: { case: "commit", value: create(PayloadCommitSchema, { size }) } });
}

/** pullChunk reads the next chunk, reporting a source failure before rethrowing it. */
async function pullChunk(
  iterator: AsyncIterator<Uint8Array>,
  onSourceError: (error: unknown) => void,
): Promise<IteratorResult<Uint8Array>> {
  try {
    return await iterator.next();
  } catch (error) {
    onSourceError(error);
    throw error;
  }
}

/** A pending read of a {@link ChunkStream}. */
interface ChunkReader {
  /** resolve hands the reader its next result. */
  resolve: (result: IteratorResult<Uint8Array>) => void;
  /** reject hands the reader the failure that ended the stream. */
  reject: (error: unknown) => void;
}

/**
 * ChunkStream is the async iterable a streaming handler reads a chunked
 * payload from. The session's receive loop pushes chunks as they arrive, never
 * waiting: the server sends at most a window of unacknowledged chunks per task,
 * and each chunk the handler takes is acknowledged so the server sends the
 * next. A slow handler therefore only slows its own payload. A handler that
 * stops reading abandons the stream (breaking out of its loop, or returning),
 * and later pushes are dropped.
 *
 * @internal
 */
export class ChunkStream implements AsyncIterable<Uint8Array> {
  /** Chunks pushed but not yet read. */
  private readonly chunks: Uint8Array[] = [];
  /** The read waiting for the next chunk, if any. */
  private reader: ChunkReader | undefined;
  /** Whether the receive loop ended the stream. */
  private ended = false;
  /** The failure the reader sees after the last chunk, until it is reported. */
  private failure: { error: unknown } | undefined;
  /** Whether the handler is done with the stream. */
  private abandoned = false;

  /**
   * @param window how many chunks the stream buffers, the server's per-task
   *   chunk window.
   * @param ack acknowledges one chunk the handler took, granting the server
   *   credit for the next.
   */
  constructor(
    private readonly window: number,
    private readonly ack: () => void,
  ) {}

  /**
   * push hands one chunk to the reader without waiting. It returns false when
   * the buffer is full and the reader still wants chunks: the server sent more
   * than its window allows. A chunk for an abandoned stream is dropped.
   */
  push(data: Uint8Array): boolean {
    if (this.abandoned) {
      return true;
    }

    const reader = this.reader;
    this.reader = undefined;

    if (reader !== undefined) {
      this.ack();
      reader.resolve({ value: data, done: false });
      return true;
    }

    if (this.chunks.length >= this.window) {
      return false;
    }

    this.chunks.push(data);

    return true;
  }

  /**
   * end closes the stream: the reader sees `error` after the last chunk, or the
   * end of the iteration when `error` is undefined. The receive loop calls it
   * once.
   */
  end(error?: unknown): void {
    this.ended = true;
    this.failure = error === undefined ? undefined : { error };

    const reader = this.reader;
    this.reader = undefined;

    if (reader !== undefined) {
      this.settle(reader);
    }
  }

  /** abandon marks the stream as no longer read, so later pushes are dropped. */
  abandon(): void {
    this.abandoned = true;
    this.chunks.length = 0;
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    const stop = (): Promise<IteratorResult<Uint8Array>> => {
      this.abandon();

      return Promise.resolve({ value: undefined, done: true });
    };

    return {
      next: () => this.next(),
      return: stop,
      throw: stop,
    };
  }

  /** next resolves with the next chunk, acknowledging it, or with the end of the stream or its failure. */
  private next(): Promise<IteratorResult<Uint8Array>> {
    const data = this.chunks.shift();
    if (data !== undefined) {
      this.ack();

      return Promise.resolve({ value: data, done: false });
    }

    return new Promise((resolve, reject) => {
      const reader = { resolve, reject };
      if (this.ended || this.abandoned) {
        this.settle(reader);
        return;
      }

      this.reader = reader;
    });
  }

  /** settle ends a read at the end of the stream, reporting a failure once. */
  private settle(reader: ChunkReader): void {
    const failure = this.failure;
    this.failure = undefined;

    if (failure !== undefined) {
      reader.reject(failure.error);
      return;
    }

    reader.resolve({ value: undefined, done: true });
  }
}

/** The outcome of a buffered payload: its bytes, or the failure that ended it. */
export type PayloadResult = { payload: Uint8Array } | { error: unknown };

/**
 * PayloadAssembly collects one chunked payload as its PayloadChunk frames
 * arrive, either into a buffer handed to a completion callback or into a
 * streaming handler's {@link ChunkStream}. Only the session's receive loop
 * touches it, and it never waits on the handler.
 *
 * @internal
 */
export class PayloadAssembly {
  /** The opened chunks of a buffered payload. */
  private readonly parts: Uint8Array[] = [];

  /**
   * @param opener decrypts each chunk of an end-to-end encrypted payload;
   *   undefined for plaintext.
   * @param target the streaming handler's stream, or the callback receiving a
   *   buffered payload.
   * @param ack acknowledges one buffered chunk so the server sends the next; a
   *   streamed chunk is acknowledged by its reader instead.
   */
  constructor(
    private readonly opener: ChunkOpener | undefined,
    private readonly target: ChunkStream | ((result: PayloadResult) => void),
    private readonly ack: () => void = () => {},
  ) {}

  /** add applies one chunk frame and resolves true once the payload is finished, successfully or not. */
  async add(chunk: PayloadChunk): Promise<boolean> {
    if (chunk.error !== "") {
      this.finish(new ConveyorError(`${SERVER_PAYLOAD_READ_MESSAGE}: ${chunk.error}`));
      return true;
    }

    if (chunk.last) {
      this.finish(this.verifyEnd());
      return true;
    }

    let data = chunk.data;

    if (this.opener !== undefined) {
      try {
        data = await this.opener.open(data);
      } catch (error) {
        this.finish(error);
        return true;
      }
    }

    if (this.target instanceof ChunkStream) {
      if (!this.target.push(data)) {
        this.finish(new ConveyorError(CHUNK_WINDOW_EXCEEDED_MESSAGE));
        return true;
      }

      return false;
    }

    this.parts.push(data);
    this.ack();

    return false;
  }

  /**
   * finish ends the payload: a streamed one through its stream, a buffered one
   * through its callback. `error` is undefined on success.
   */
  finish(error: unknown): void {
    if (this.target instanceof ChunkStream) {
      this.target.end(error);
      return;
    }

    if (error !== undefined) {
      this.target({ error });
      return;
    }

    this.target({ payload: Buffer.concat(this.parts) });
  }

  /** verifyEnd returns the failure of an encrypted payload that ended early, if any. */
  private verifyEnd(): unknown {
    try {
      this.opener?.finish();
    } catch (error) {
      return error;
    }

    return undefined;
  }
}
