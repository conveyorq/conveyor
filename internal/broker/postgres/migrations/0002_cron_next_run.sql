-- Persist each cron entry's next fire time so the cluster-singleton scheduler
-- materializes tasks correctly across failover. NULL means "not yet armed":
-- the scheduler computes the first fire time from the spec.
ALTER TABLE conveyor_cron_entries ADD COLUMN next_run_at TIMESTAMPTZ;
