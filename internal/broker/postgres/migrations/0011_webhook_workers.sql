-- Webhook worker registrations: HTTP endpoints the server pushes tasks to.
-- One row per registration, keyed by name. Living in the broker rather than
-- on a node is what lets a registration survive node failover.
CREATE TABLE conveyor_webhook_workers (
  name TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  queues JSONB NOT NULL,
  concurrency INT NOT NULL,
  secrets TEXT[] NOT NULL,
  batch_types TEXT[] NOT NULL DEFAULT '{}',
  request_timeout INTERVAL NOT NULL DEFAULT '0',
  paused BOOLEAN NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
