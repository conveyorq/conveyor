/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Record a running task's latest reported progress so the dashboard and admin
-- API can show how far a long task has advanced. progress is a 0..100 percent
-- and progress_message an optional human-readable status. Both are mutable
-- execution fields written under the active lease and overlaid onto the
-- envelope on read, never persisted inside the payload; they default to the
-- "no progress reported" values.
ALTER TABLE conveyor_tasks ADD COLUMN progress SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE conveyor_tasks ADD COLUMN progress_message TEXT NOT NULL DEFAULT '';
