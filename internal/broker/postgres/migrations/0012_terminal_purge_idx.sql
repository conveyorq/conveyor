/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Terminal-row purge: the reaper deletes completed rows once their retention
-- lapses and archived or canceled rows once the server-wide archive retention
-- lapses. Both scans filter on state and order by completed_at, so this partial
-- index over the terminal states (5 completed, 6 archived, 7 canceled; see
-- conveyor.v1.TaskState) keeps each purge pass proportional to the rows it
-- removes rather than to the whole task log.
CREATE INDEX conveyor_tasks_terminal_idx
  ON conveyor_tasks (state, completed_at)
  WHERE state IN (5, 6, 7);

-- The expiry sweep archives still-waiting tasks in the scheduled, pending,
-- retry, AND blocked states (9), but the index created in 0006 covered only the
-- first three, so an expiring blocked task fell back to a sequential scan.
-- Recreate it over every state the sweep reads.
DROP INDEX IF EXISTS conveyor_tasks_expiry_idx;
CREATE INDEX conveyor_tasks_expiry_idx
  ON conveyor_tasks (expires_at)
  WHERE expires_at IS NOT NULL AND state IN (1, 2, 4, 9);
