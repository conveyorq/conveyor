-- Supports the QueueStats aggregation (GROUP BY queue, state): without it
-- every admin ListQueues call sequentially scans conveyor_tasks.
CREATE INDEX conveyor_tasks_queue_state_idx ON conveyor_tasks (queue, state);
