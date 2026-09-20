# Use cases

Conveyor fits any work you want to run outside the request path, durably and at
scale. A few shapes it suits especially well, each leaning on the features noted:

- **Transactional email and notifications.** Send the welcome email, receipt, or
  push notification after the request returns. Push dispatch keeps latency low,
  retries with backoff and the dead-letter queue absorb provider outages, and
  per-key concurrency caps in-flight sends per customer. This is exactly the
  [Postmark example](../examples/postmark).
- **Webhook and API fan-out.** Deliver outbound webhooks or call rate-limited
  third-party APIs. Per-queue rate limiting respects a vendor's quota, the
  per-task-type circuit breaker sheds load when an endpoint starts failing, and
  lifecycle events give you a delivery audit trail.
- **Media and document pipelines.** Transcode video, resize images, or render
  PDFs as multi-step jobs. Task dependencies model "extract, then transform, then
  publish" with fan-out/fan-in, weighted queues keep heavy jobs off the fast
  lane, and progress reporting tells a slow job from a stuck one.
- **AI model calls and inference jobs.** Run LLM completions, embeddings,
  transcription, or image generation as background tasks against a paid,
  rate-limited provider. Per-queue rate limiting holds dispatch under the
  provider's per-second quota (over-rate tasks wait, no retry spent), per-key
  concurrency caps simultaneous calls per API key or tenant, retries with backoff
  ride out `429` and transient `5xx` responses, per-task timeouts/deadlines bound
  a slow generation, and progress reporting surfaces how far a long job has run.
- **RAG ingestion and embedding pipelines.** Index a corpus as a dependency
  graph: fan out per document, chain "chunk, then embed, then upsert", and
  coalesce many chunks into one batched embeddings call with group aggregation.
  End-to-end encryption keeps source documents as ciphertext in the queue.
- **Scheduled and recurring work.** Nightly reports, billing runs, retention
  sweeps, and cache warming. Server-persisted cron survives restarts and
  failover, and delayed/scheduled tasks handle one-off future work.
- **Batch and digest processing.** Coalesce a burst of events into one unit of
  work: hourly digest emails, debounced search reindexing, or bulk writes. Group
  aggregation fires a single batch handler on size, delay, or grace period, tuned
  per group.
- **Polyglot background jobs.** Enqueue from a Go API and process on Python or
  TypeScript workers (or any mix). One wire protocol means a task enqueued in one
  language runs on a worker written in another, which suits ML inference,
  scraping, or data sync split across teams and runtimes.
- **Privacy-sensitive workloads.** Process PII, health, or financial payloads
  where the queue must not see plaintext. End-to-end encryption seals payloads in
  the SDK or CLI so the server stores ciphertext only and holds no keys.
