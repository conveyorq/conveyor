// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { type Handler, Mux, skipRetry, type Task, Worker } from "@conveyorq/conveyor";

import { loadConfig } from "./config.js";
import { type EmailMessage, EmailProvider, InvalidRecipientError } from "./email.js";
import { type Logger, newLogger } from "./logger.js";
import { EMAIL_QUEUE, REMINDER_EMAIL, type ReminderEmail, WELCOME_EMAIL, type WelcomeEmail } from "./tasks.js";

const log = newLogger("email-worker");
const config = loadConfig();
const provider = new EmailProvider(config.failureRate);

/**
 * sender builds a handler that decodes a payload, renders it to an email, and
 * hands it to the provider. It encodes the outcome contract: an invalid
 * recipient is permanent (dead-letter via SkipRetry); any other provider error
 * is transient (re-thrown, so Conveyor retries with backoff); the handler's
 * abort signal cancels a send that overruns its deadline.
 */
function sender<T extends { to: string }>(render: (payload: T) => EmailMessage): Handler {
  return async (task: Task, { signal }): Promise<void> => {
    const entry: Logger = log.with({ taskId: task.id, type: task.type, attempt: task.retried + 1, of: task.maxRetry });
    const payload = task.json<T>();

    try {
      const providerId = await provider.send(render(payload), signal);
      entry.info("email sent", { to: payload.to, providerId });
    } catch (error) {
      if (error instanceof InvalidRecipientError) {
        entry.warn("permanent failure; dead-lettering", { to: payload.to, error: error.message });
        throw skipRetry(error.message);
      }

      entry.warn("transient failure; will retry", { to: payload.to, error: messageOf(error) });
      throw error;
    }
  };
}

const mux = new Mux()
  .handle(
    WELCOME_EMAIL,
    sender<WelcomeEmail>((payload) => ({
      to: payload.to,
      subject: `Welcome to Conveyor, ${payload.name}!`,
      body: `Hi ${payload.name}, thanks for signing up.`,
    })),
  )
  .handle(
    REMINDER_EMAIL,
    sender<ReminderEmail>((payload) => ({ to: payload.to, subject: payload.subject, body: payload.body })),
  );

async function main(): Promise<void> {
  const stop = new AbortController();

  for (const signal of ["SIGTERM", "SIGINT"] as const) {
    process.on(signal, () => {
      log.info("shutdown signal received; draining in-flight tasks", { signal });
      stop.abort();
    });
  }

  const worker = new Worker(config.addr, {
    queues: { [EMAIL_QUEUE]: 1 },
    concurrency: config.concurrency,
    token: config.token,
  });

  log.info("worker starting", { addr: config.addr, queue: EMAIL_QUEUE, concurrency: config.concurrency });
  await worker.run(mux, stop.signal);
  log.info("worker stopped cleanly");
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

main().catch((error: unknown) => {
  log.error("worker crashed", { error: messageOf(error) });
  process.exit(1);
});
