/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Expiring tasks: a pre-dispatch TTL. expires_at is the absolute time after
-- which a task that has not yet been dispatched must not run; it is archived
-- instead. NULL means the task never expires. This is distinct from deadline
-- (which cancels a running task) and retention (which purges a completed one).
ALTER TABLE conveyor_tasks ADD COLUMN expires_at TIMESTAMPTZ;

-- The partial index serves the lease predicate (which skips expired tasks) and
-- the expiry sweep (which archives still-waiting expired tasks); both touch
-- only the few rows that carry an expiry. states 1, 2, 4 are scheduled,
-- pending, and retry (conveyor.v1.TaskState) — the waiting states a task can
-- expire from before it is ever dispatched.
CREATE INDEX conveyor_tasks_expiry_idx
  ON conveyor_tasks (expires_at)
  WHERE expires_at IS NOT NULL AND state IN (1, 2, 4);
