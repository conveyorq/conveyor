/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Per-queue per-key concurrency limits. A row caps how many tasks sharing a
-- concurrency key the queue dispatches at once; absence leaves the queue's keys
-- unbounded. This table is config only: the live in-flight count lives in the
-- dispatching queue grain, not here, so it is read at grain activation and on
-- limit changes, never on the dispatch hot path.
CREATE TABLE conveyor_concurrency_limits (
  queue      TEXT PRIMARY KEY,
  max_active INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
