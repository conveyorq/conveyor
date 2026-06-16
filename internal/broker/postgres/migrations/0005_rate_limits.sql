/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Per-queue dispatch-rate overrides. A row replaces the server's global default
-- rate limit for that queue; absence means the queue uses the default (or is
-- unlimited when no default is set). This table is config only: the live token
-- bucket lives in the dispatching queue grain, not here, so it is read at grain
-- activation and on override changes, never on the dispatch hot path.
CREATE TABLE conveyor_rate_limits (
  queue        TEXT PRIMARY KEY,
  rate_per_sec DOUBLE PRECISION NOT NULL,
  burst        INTEGER NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
