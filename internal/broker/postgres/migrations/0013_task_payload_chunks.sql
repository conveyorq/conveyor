/*
 * Copyright 2026 ConveyorQ
 *
 * SPDX-License-Identifier: Apache-2.0
 */

-- Chunked payloads: a payload too large to store inline in the task envelope
-- is stored here, one row per uploaded chunk, in upload order. The envelope in
-- conveyor_tasks.payload then carries payload_chunked and an empty payload.
--
-- The foreign key is deferred so a streamed upload can write its chunks before
-- the task row exists; both commit in one transaction. Deleting or purging a
-- task drops its chunks through the cascade.
--
-- Chunk data is opaque and usually already compressed or encrypted, so it is
-- stored EXTERNAL: out of line in TOAST, without a compression attempt.
CREATE TABLE conveyor_task_payload_chunks (
  task_id TEXT NOT NULL REFERENCES conveyor_tasks (id)
    ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  seq INT NOT NULL,
  data BYTEA NOT NULL,
  CONSTRAINT conveyor_task_payload_chunks_pkey PRIMARY KEY (task_id, seq)
);

ALTER TABLE conveyor_task_payload_chunks ALTER COLUMN data SET STORAGE EXTERNAL;
