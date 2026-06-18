// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

/**
 * SkipRetry wraps an error a handler wants the server to treat as permanent:
 * the task is archived (dead-lettered) immediately instead of retried. Throw it
 * from a handler for a failure that retrying cannot fix — a malformed payload,
 * a rejected business rule. Any other thrown error is retried.
 */
export class SkipRetry extends Error {
  /** The underlying cause, if any. */
  override readonly cause?: unknown;

  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = "SkipRetry";
    if (options?.cause !== undefined) {
      this.cause = options.cause;
    }
  }
}

/**
 * skipRetry is a convenience constructor for {@link SkipRetry}. Throw
 * `skipRetry("bad payload")` from a handler to dead-letter the task.
 */
export function skipRetry(message: string, cause?: unknown): SkipRetry {
  return new SkipRetry(message, cause === undefined ? undefined : { cause });
}

/**
 * DuplicateTaskError is thrown by {@link Client.enqueue} when a unique task
 * with the same uniqueness key already exists and has not yet completed.
 */
export class DuplicateTaskError extends Error {
  constructor(message = "conveyor: duplicate task") {
    super(message);
    this.name = "DuplicateTaskError";
  }
}

/**
 * ConveyorError is the base class for SDK-level failures that are not a thrown
 * server error (configuration, codec, and protocol violations).
 */
export class ConveyorError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ConveyorError";
  }
}
