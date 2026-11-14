// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { type Encryptor, MalformedCiphertextError } from "./encryption.js";

/**
 * The plaintext header sealed into every chunk: an 8-byte big-endian sequence
 * number followed by a 1-byte last-chunk flag. Sealing the header with the data
 * lets the opener detect reordered, dropped, or appended chunks, which
 * per-chunk authentication alone would not catch.
 */
const CHUNK_HEADER_BYTES = 9;

/** The offset of the last-chunk flag within the chunk header. */
const CHUNK_FLAG_OFFSET = CHUNK_HEADER_BYTES - 1;

/** The flag value marking the final chunk of a payload. */
const CHUNK_LAST_FLAG = 1;

/**
 * Thrown when a chunk's sequence number is not the next one expected, or a
 * chunk arrives after the chunk marked last.
 */
export class ChunkOrderError extends Error {
  constructor(detail: string) {
    super(`encryption: chunk out of order: ${detail}`);
    this.name = "ChunkOrderError";
  }
}

/** Thrown when a chunked payload ends before the chunk marked last. */
export class ChunkTruncatedError extends Error {
  constructor() {
    super("encryption: chunked payload truncated");
    this.name = "ChunkTruncatedError";
  }
}

/**
 * ChunkSealer seals a chunked payload one chunk at a time, numbering chunks
 * from zero. Use one per payload.
 *
 * The framing is shared by every SDK and the server: each chunk is
 * `encrypt(sequence || last || data)`, so any of them can open what another
 * sealed.
 *
 * @internal
 */
export class ChunkSealer {
  /** The sequence number of the next chunk to seal. */
  private next = 0n;

  constructor(private readonly encryptor: Encryptor) {}

  /**
   * seal frames and encrypts the next chunk. `last` must be true on the final
   * chunk, and only on it; an empty payload is sealed as one empty chunk
   * marked last.
   */
  async seal(data: Uint8Array, last: boolean): Promise<Uint8Array> {
    const framed = new Uint8Array(CHUNK_HEADER_BYTES + data.length);
    new DataView(framed.buffer).setBigUint64(0, this.next);
    framed[CHUNK_FLAG_OFFSET] = last ? CHUNK_LAST_FLAG : 0;
    framed.set(data, CHUNK_HEADER_BYTES);

    const sealed = await this.encryptor.encrypt(framed);
    this.next += 1n;

    return sealed;
  }
}

/**
 * ChunkOpener opens a chunked payload sealed by {@link ChunkSealer}, enforcing
 * that chunks arrive in order and end with the chunk marked last. Use one per
 * payload.
 *
 * @internal
 */
export class ChunkOpener {
  /** The sequence number the next chunk must carry. */
  private next = 0n;
  /** Whether the chunk marked last was opened. */
  private done = false;

  constructor(private readonly encryptor: Encryptor) {}

  /**
   * open decrypts the next chunk and returns its data. It throws
   * {@link ChunkOrderError} when the chunk is out of sequence or follows the
   * chunk marked last, and {@link MalformedCiphertextError} when the decrypted
   * chunk is too short to carry the framing header.
   */
  async open(sealed: Uint8Array): Promise<Uint8Array> {
    if (this.done) {
      throw new ChunkOrderError("chunk after the last chunk");
    }

    const framed = await this.encryptor.decrypt(sealed);
    if (framed.length < CHUNK_HEADER_BYTES) {
      throw new MalformedCiphertextError("chunk shorter than its header");
    }

    const sequence = new DataView(framed.buffer, framed.byteOffset, framed.byteLength).getBigUint64(0);
    if (sequence !== this.next) {
      throw new ChunkOrderError(`got chunk ${sequence}, want ${this.next}`);
    }

    this.next += 1n;
    this.done = framed[CHUNK_FLAG_OFFSET] === CHUNK_LAST_FLAG;

    return framed.subarray(CHUNK_HEADER_BYTES);
  }

  /**
   * finish throws {@link ChunkTruncatedError} unless the payload ended after
   * the chunk marked last. A payload of no chunks at all is not chunked and
   * passes.
   */
  finish(): void {
    if (this.next > 0n && !this.done) {
      throw new ChunkTruncatedError();
    }
  }
}
