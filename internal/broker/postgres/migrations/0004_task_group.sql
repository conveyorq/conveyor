/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Group aggregation: a task may belong to a named aggregation group within its
-- queue. Members accumulate in the aggregating state and are leased together as
-- one batch when the group fires. group_key is empty for ungrouped tasks. The
-- partial index serves the queue's group sweep (GroupStats) and the batch lease
-- (LeaseGroup), which scan only aggregating members of a (queue, group_key).
ALTER TABLE conveyor_tasks ADD COLUMN group_key TEXT NOT NULL DEFAULT '';

-- state = 8 is TASK_STATE_AGGREGATING (conveyor.v1.TaskState).
CREATE INDEX conveyor_tasks_group_idx
  ON conveyor_tasks (queue, group_key, enqueued_at, id)
  WHERE state = 8;
