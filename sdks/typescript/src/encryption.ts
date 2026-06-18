// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

/**
 * Encryptor seals and opens opaque byte slices end to end: a client encrypts a
 * payload before enqueue and a worker decrypts it on dispatch, so the server
 * stores and relays ciphertext only and holds no keys. Implementations may be
 * async (e.g. a KMS-backed codec); the built-in {@link AESGCM} is local.
 *
 * `decrypt` must accept any ciphertext a prior `encrypt` produced, including
 * ciphertext sealed under a now-retired key the implementation still holds, so
 * key rotation never strands stored data.
 */
export interface Encryptor {
  /** Seal plaintext into ciphertext that {@link Encryptor.decrypt} can open. */
  encrypt(plaintext: Uint8Array): Uint8Array | Promise<Uint8Array>;
  /** Open ciphertext produced by {@link Encryptor.encrypt}. */
  decrypt(ciphertext: Uint8Array): Uint8Array | Promise<Uint8Array>;
}

/**
 * Key is a named AES-256 secret. The id labels ciphertext so the matching
 * secret can be found again after the active key rotates; it travels in the
 * clear and must not itself be sensitive.
 */
export interface Key {
  /** Non-empty, at most 255 bytes. */
  id: string;
  /** 32-byte AES-256 key material. */
  secret: Uint8Array;
}

/** Thrown when a key cannot be used to build an Encryptor. */
export class InvalidKeyError extends Error {
  constructor(message: string) {
    super(`encryption: invalid key: ${message}`);
    this.name = "InvalidKeyError";
  }
}

/** Thrown when decrypting ciphertext sealed under a key the Encryptor does not hold. */
export class UnknownKeyIdError extends Error {
  constructor(keyId: string) {
    super(`encryption: unknown key id ${JSON.stringify(keyId)}`);
    this.name = "UnknownKeyIdError";
  }
}

/** Thrown when ciphertext is truncated or does not match the framing. */
export class MalformedCiphertextError extends Error {
  constructor(message: string) {
    super(`encryption: malformed ciphertext: ${message}`);
    this.name = "MalformedCiphertextError";
  }
}

/** Thrown when ciphertext fails its authentication tag (tampered or wrong key). */
export class AuthenticationError extends Error {
  constructor() {
    super("encryption: authentication failed");
    this.name = "AuthenticationError";
  }
}

// AES-256-GCM framing parameters — byte-compatible with the Go SDK so a task
// sealed by one SDK opens in the other.
const SCHEME_VERSION = 1;
const KEY_BYTES = 32;
const NONCE_BYTES = 12;
const TAG_BYTES = 16;
const MAX_KEY_ID_BYTES = 255;
const HEADER_FIXED_BYTES = 2;

const idEncoder = new TextEncoder();
const idDecoder = new TextDecoder();

/**
 * AESGCM is the built-in {@link Encryptor}: AES-256-GCM with a fresh random
 * nonce per call and a key id framed into each ciphertext. It holds a keyring
 * so a retired key still opens data sealed under it, and seals new data under
 * the active key. Use {@link newAESGCM} to construct one.
 *
 * Ciphertext layout (matching the Go SDK): version byte, key-id length byte,
 * key id, 12-byte nonce, ciphertext, 16-byte GCM tag. The version+id header is
 * authenticated as additional data, binding the key id to the result.
 */
export class AESGCM implements Encryptor {
  private readonly ring: Map<string, Uint8Array>;
  private readonly activeId: string;
  private readonly activeSecret: Uint8Array;

  private constructor(ring: Map<string, Uint8Array>, activeId: string) {
    this.ring = ring;
    this.activeId = activeId;
    this.activeSecret = ring.get(activeId)!;
  }

  /** @internal use {@link newAESGCM}. */
  static build(activeKeyId: string, keys: Key[]): AESGCM {
    if (activeKeyId === "") {
      throw new InvalidKeyError("active key id is empty");
    }

    if (keys.length === 0) {
      throw new InvalidKeyError("no keys provided");
    }

    const ring = new Map<string, Uint8Array>();

    for (const key of keys) {
      if (key.id === "") {
        throw new InvalidKeyError("empty key id");
      }

      if (idEncoder.encode(key.id).length > MAX_KEY_ID_BYTES) {
        throw new InvalidKeyError(`key id ${JSON.stringify(key.id)} exceeds ${MAX_KEY_ID_BYTES} bytes`);
      }

      if (key.secret.length !== KEY_BYTES) {
        throw new InvalidKeyError(`key ${JSON.stringify(key.id)} secret is ${key.secret.length} bytes, want ${KEY_BYTES}`);
      }

      if (ring.has(key.id)) {
        throw new InvalidKeyError(`duplicate key id ${JSON.stringify(key.id)}`);
      }

      ring.set(key.id, key.secret);
    }

    if (!ring.has(activeKeyId)) {
      throw new InvalidKeyError(`active key id ${JSON.stringify(activeKeyId)} not among keys`);
    }

    return new AESGCM(ring, activeKeyId);
  }

  encrypt(plaintext: Uint8Array): Uint8Array {
    const header = encodeHeader(this.activeId);
    const nonce = randomBytes(NONCE_BYTES);

    const cipher = createCipheriv("aes-256-gcm", this.activeSecret, nonce);
    cipher.setAAD(header);
    const body = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    const tag = cipher.getAuthTag();

    return Buffer.concat([header, nonce, body, tag]);
  }

  decrypt(ciphertext: Uint8Array): Uint8Array {
    const { keyId, headerLength } = decodeHeader(ciphertext);

    const secret = this.ring.get(keyId);
    if (secret === undefined) {
      throw new UnknownKeyIdError(keyId);
    }

    const rest = ciphertext.subarray(headerLength);
    if (rest.length < NONCE_BYTES + TAG_BYTES) {
      throw new MalformedCiphertextError("missing nonce or tag");
    }

    const nonce = rest.subarray(0, NONCE_BYTES);
    const body = rest.subarray(NONCE_BYTES, rest.length - TAG_BYTES);
    const tag = rest.subarray(rest.length - TAG_BYTES);
    const header = ciphertext.subarray(0, headerLength);

    const decipher = createDecipheriv("aes-256-gcm", secret, nonce);
    decipher.setAAD(header);
    decipher.setAuthTag(tag);

    try {
      return Buffer.concat([decipher.update(body), decipher.final()]);
    } catch {
      throw new AuthenticationError();
    }
  }
}

/**
 * newAESGCM builds an {@link AESGCM} that seals under the key identified by
 * `activeKeyId` and can open data sealed under any key in `keys`. Pass several
 * keys to support rotation: keep the old key to decrypt existing data while a
 * new active key seals fresh data.
 */
export function newAESGCM(activeKeyId: string, ...keys: Key[]): AESGCM {
  return AESGCM.build(activeKeyId, keys);
}

function encodeHeader(keyId: string): Uint8Array {
  const idBytes = idEncoder.encode(keyId);
  const header = new Uint8Array(HEADER_FIXED_BYTES + idBytes.length);
  header[0] = SCHEME_VERSION;
  header[1] = idBytes.length;
  header.set(idBytes, HEADER_FIXED_BYTES);

  return header;
}

function decodeHeader(ciphertext: Uint8Array): { keyId: string; headerLength: number } {
  if (ciphertext.length < HEADER_FIXED_BYTES) {
    throw new MalformedCiphertextError("shorter than header");
  }

  if (ciphertext[0] !== SCHEME_VERSION) {
    throw new MalformedCiphertextError(`unsupported version ${ciphertext[0]}`);
  }

  const idLength = ciphertext[1]!;
  const headerLength = HEADER_FIXED_BYTES + idLength;

  if (ciphertext.length < headerLength) {
    throw new MalformedCiphertextError("truncated key id");
  }

  const keyId = idDecoder.decode(ciphertext.subarray(HEADER_FIXED_BYTES, headerLength));

  return { keyId, headerLength };
}

/** Metadata key set on an encrypted payload so a worker knows to decrypt it. */
export const ENCRYPTION_MARKER_KEY = "conveyor.encryption";
/** Metadata marker value identifying the framing version. */
export const ENCRYPTION_MARKER_VALUE = "1";
