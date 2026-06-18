// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

/**
 * The task contract shared by the producer and the worker. Both sides import
 * these names and payload shapes, so they agree on the JSON encoding of each
 * task type (the only thing the queue requires of two peers).
 */

/** The queue all email work flows through. */
export const EMAIL_QUEUE = "email";

/** Task type: a one-time welcome email. */
export const WELCOME_EMAIL = "email:welcome";

/** Task type: a scheduled reminder email. */
export const REMINDER_EMAIL = "email:reminder";

/** Payload of a {@link WELCOME_EMAIL} task. */
export interface WelcomeEmail {
  userId: string;
  to: string;
  name: string;
}

/** Payload of a {@link REMINDER_EMAIL} task. */
export interface ReminderEmail {
  userId: string;
  to: string;
  subject: string;
  body: string;
}
