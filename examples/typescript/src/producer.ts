// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { Client, DuplicateTaskError, type EnqueueOptions, json, newTask } from "@conveyorq/conveyor";

import { loadConfig } from "./config.js";
import { newLogger } from "./logger.js";
import { EMAIL_QUEUE, REMINDER_EMAIL, type ReminderEmail, WELCOME_EMAIL, type WelcomeEmail } from "./tasks.js";

const log = newLogger("producer");
const config = loadConfig();
const client = new Client(config.addr, { token: config.token });

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * The producer is a small CLI that enqueues email work:
 *
 *   produce welcome <to> <name>
 *   produce reminder <to> <subject> [delayMinutes]
 */
async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);

  switch (command) {
    case "welcome":
      await enqueueWelcome(args);
      break;

    case "reminder":
      await enqueueReminder(args);
      break;

    default:
      usage();
  }
}

async function enqueueWelcome(args: string[]): Promise<void> {
  const [to, name] = args;
  if (to === undefined || name === undefined) {
    usage();
    return;
  }

  const payload: WelcomeEmail = { userId: to, to, name };

  // A unique key plus a 24h TTL makes re-running this a no-op: at most one
  // pending welcome per recipient, so a retry or a double click never sends two.
  const options: EnqueueOptions = {
    queue: EMAIL_QUEUE,
    maxRetry: config.maxRetry,
    timeout: config.attemptTimeoutMs,
    uniqueKey: `welcome:${to}`,
    unique: DAY_MS,
  };

  try {
    const info = await client.enqueue(newTask(WELCOME_EMAIL, json(payload)), options);
    log.info("welcome email enqueued", { id: info.id, to, state: info.state });
  } catch (error) {
    if (error instanceof DuplicateTaskError) {
      log.warn("welcome already queued for recipient; skipped (deduped)", { to });
      return;
    }

    throw error;
  }
}

async function enqueueReminder(args: string[]): Promise<void> {
  const [to, subject, delayMinutes] = args;
  if (to === undefined || subject === undefined) {
    usage();
    return;
  }

  const payload: ReminderEmail = { userId: to, to, subject, body: `This is your reminder: ${subject}` };

  const options: EnqueueOptions = {
    queue: EMAIL_QUEUE,
    maxRetry: config.maxRetry,
    timeout: config.attemptTimeoutMs,
    ...(delayMinutes !== undefined ? { processIn: Number(delayMinutes) * 60_000 } : {}),
  };

  const info = await client.enqueue(newTask(REMINDER_EMAIL, json(payload)), options);
  log.info("reminder email enqueued", { id: info.id, to, state: info.state });
}

function usage(): void {
  process.stderr.write(
    [
      "usage:",
      "  produce welcome <to> <name>",
      "  produce reminder <to> <subject> [delayMinutes]",
      "",
    ].join("\n"),
  );
  process.exit(2);
}

main().catch((error: unknown) => {
  log.error("enqueue failed", { error: String(error) });
  process.exit(1);
});
