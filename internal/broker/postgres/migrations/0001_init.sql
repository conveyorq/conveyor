-- Initial schema for the Postgres broker.
--
-- State values mirror conveyor.v1.TaskState:
--   1 scheduled, 2 pending, 3 active, 4 retry,
--   5 completed, 6 archived, 7 canceled.
--
-- The payload column holds the serialized TaskEnvelope committed at
-- enqueue time; mutable execution fields (state, retried, last_error,
-- lease, timestamps) are authoritative in their own columns and overlaid
-- onto the envelope on reads.
--
-- Time-dependent statements take "now" as a bind parameter from the
-- injected clock; the column defaults below are fallbacks for ad-hoc
-- inserts only.

CREATE TABLE conveyor_tasks (
  id TEXT PRIMARY KEY,
  queue TEXT NOT NULL DEFAULT 'default',
  type TEXT NOT NULL,
  state SMALLINT NOT NULL,
  priority SMALLINT NOT NULL DEFAULT 4,
  payload BYTEA NOT NULL,
  unique_key TEXT,
  unique_expires_at TIMESTAMPTZ,
  process_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deadline TIMESTAMPTZ,
  max_retry INT NOT NULL DEFAULT 25,
  retried INT NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  lease_id TEXT,
  lease_expires_at TIMESTAMPTZ,
  result BYTEA,
  retention INTERVAL NOT NULL DEFAULT '0',
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX conveyor_tasks_dispatch_idx ON conveyor_tasks (queue, priority DESC, process_at, id) WHERE state IN (2, 4);
CREATE INDEX conveyor_tasks_lease_idx ON conveyor_tasks (lease_expires_at) WHERE state = 3;
CREATE INDEX conveyor_tasks_scheduled_idx ON conveyor_tasks (process_at) WHERE state = 1;
CREATE UNIQUE INDEX conveyor_tasks_unique_idx ON conveyor_tasks (unique_key)
  WHERE unique_key IS NOT NULL AND state IN (1, 2, 3, 4);

CREATE TABLE conveyor_queue_state (
  queue TEXT PRIMARY KEY,
  paused BOOLEAN NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE conveyor_cron_entries (
  id TEXT PRIMARY KEY,
  spec TEXT NOT NULL,
  task_type TEXT NOT NULL,
  queue TEXT NOT NULL,
  payload BYTEA NOT NULL,
  content_type TEXT NOT NULL DEFAULT '',
  options BYTEA NOT NULL,
  paused BOOLEAN NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
