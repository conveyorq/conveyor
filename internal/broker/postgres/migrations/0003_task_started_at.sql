/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Record when the most recent execution attempt began so the dashboard can
-- report execution duration. The lease statement stamps it from the injected
-- clock; it is reset on each re-lease so the value reflects the current
-- attempt, and pairs with completed_at to bound a finished task's runtime.
ALTER TABLE conveyor_tasks ADD COLUMN started_at TIMESTAMPTZ;
