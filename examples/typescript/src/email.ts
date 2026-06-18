// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { randomUUID } from "node:crypto";

/** An email ready to hand to a provider. */
export interface EmailMessage {
  to: string;
  subject: string;
  body: string;
}

/**
 * InvalidRecipientError is a **permanent** failure — a malformed address that
 * retrying cannot fix. The worker maps it to SkipRetry (dead-letter).
 */
export class InvalidRecipientError extends Error {
  constructor(to: string) {
    super(`invalid recipient address: ${JSON.stringify(to)}`);
    this.name = "InvalidRecipientError";
  }
}

/**
 * TransientProviderError is a **temporary** failure (a 5xx, a throttle) that a
 * retry may clear. The worker lets it propagate so Conveyor retries with
 * backoff.
 */
export class TransientProviderError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TransientProviderError";
  }
}

const emailPattern = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

/**
 * EmailProvider stands in for a real provider (SES, SendGrid, Postmark). It
 * validates the recipient, simulates network latency that honors the abort
 * signal, and fails transiently at a configurable rate so retries and
 * dead-lettering are observable end to end.
 */
export class EmailProvider {
  constructor(private readonly failureRate: number) {}

  /** send delivers one message, or throws to signal a permanent/transient failure. */
  async send(message: EmailMessage, signal: AbortSignal): Promise<string> {
    if (!emailPattern.test(message.to)) {
      throw new InvalidRecipientError(message.to);
    }

    await abortableDelay(50 + Math.random() * 250, signal);

    if (Math.random() < this.failureRate) {
      throw new TransientProviderError("provider returned 503 Service Unavailable");
    }

    return `msg_${randomUUID()}`;
  }
}

/** abortableDelay resolves after `ms`, or rejects if the signal aborts first. */
function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("send aborted"));
      return;
    }

    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new Error("send aborted"));
      },
      { once: true },
    );
  });
}
