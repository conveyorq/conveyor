/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Task dependencies (workflows): a task may declare that it waits for other
-- tasks. Each unresolved dependency is one edge from the dependent to the task
-- it waits on, carrying the policy applied when that dependency fails terminally
-- (1 block, 2 cascade-cancel, 3 continue; see conveyor.v1.DependencyFailurePolicy).
-- A dependent with any edge sits in state 9 (TASK_STATE_BLOCKED) and is not
-- eligible to lease; edges drain as dependencies succeed (or fail under the
-- continue policy), and the dependent is promoted once none remain.
CREATE TABLE conveyor_task_deps (
  dependent_id  TEXT     NOT NULL,
  dependency_id TEXT     NOT NULL,
  on_failure    SMALLINT NOT NULL,
  PRIMARY KEY (dependent_id, dependency_id)
);

-- Reverse lookup: given a task that just went terminal, find the edges that wait
-- on it. This is the hot path of dependency resolution.
CREATE INDEX conveyor_task_deps_dependency_idx
  ON conveyor_task_deps (dependency_id);
