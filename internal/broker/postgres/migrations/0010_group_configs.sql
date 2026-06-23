/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Per-group aggregation overrides. A row replaces the server's global group
-- defaults (max size, max delay, grace period) for one (queue, group_key); an
-- empty group_key is the queue-wide default applied to every group on the queue
-- without its own override. Absence means the group uses the global defaults.
-- This table is config only: the firing decision lives in the group sweeper,
-- which reads these once per sweep, never on the dispatch hot path.
CREATE TABLE conveyor_group_configs (
  queue           TEXT NOT NULL,
  group_key       TEXT NOT NULL,
  max_size        INTEGER NOT NULL,
  max_delay_ms    BIGINT NOT NULL,
  grace_period_ms BIGINT NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue, group_key)
);
