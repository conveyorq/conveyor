# Email delivery — Conveyor TypeScript example

A production-shaped **email-delivery service** built on the
[Conveyor TypeScript SDK](../../sdks/typescript) — the canonical task-queue use
case (the one asynq and River lead with). A **producer** enqueues welcome and
reminder emails; a **worker** sends them through a provider with retries,
dead-lettering, per-attempt timeouts, idempotent dedup, structured logging, and
graceful shutdown.

## What it demonstrates

- **Two task types** sharing a contract (`src/tasks.ts`) so producer and worker
  agree on the JSON payloads.
- **Retries vs. dead-lettering** — a transient provider error (simulated 503) is
  re-thrown so Conveyor retries with backoff; an invalid recipient throws
  `skipRetry(...)` and is dead-lettered immediately, no wasted retries.
- **Idempotent enqueue** — a welcome uses a unique key (`welcome:<to>`) with a
  24h TTL, so a double-submit never sends two emails (`DuplicateTaskError` is
  handled).
- **Per-attempt timeout** — each task carries a `timeout`; the handler's
  `AbortSignal` cancels a provider call that overruns it.
- **Scheduled work** — reminders can be delayed with `processIn`.
- **Graceful drain** — on `SIGTERM`/`SIGINT` the worker stops accepting new work
  and lets in-flight sends finish before the stream closes.
- **Structured JSON logging** (`src/logger.ts`) ready for a log pipeline.

## Run it (Docker)

Brings up Postgres, the Conveyor server, and the worker:

```sh
docker compose -f examples/typescript/docker-compose.yml up --build
```

In another terminal, enqueue some work (reusing the worker image):

```sh
# a welcome email
docker compose -f examples/typescript/docker-compose.yml run --rm worker \
  npm run produce -- welcome ada@example.com Ada

# re-running is a no-op (deduped for 24h)
docker compose -f examples/typescript/docker-compose.yml run --rm worker \
  npm run produce -- welcome ada@example.com Ada

# a reminder, delayed 1 minute
docker compose -f examples/typescript/docker-compose.yml run --rm worker \
  npm run produce -- reminder ada@example.com "Finish your setup" 1

# an invalid address → dead-lettered, no retries
docker compose -f examples/typescript/docker-compose.yml run --rm worker \
  npm run produce -- welcome not-an-email Ada
```

Watch the worker logs: most sends succeed, ~15% fail transiently and retry, and
the invalid address is dead-lettered.

## Run it (local Node)

Start a server (`conveyord --dev` gives you a standalone in-memory one with auth
off), then:

```sh
cd examples/typescript
npm install

# worker
CONVEYOR_ADDR=http://localhost:8080 npm run worker

# producer (another terminal)
CONVEYOR_ADDR=http://localhost:8080 npm run produce -- welcome ada@example.com Ada
```

## Configuration (env)

| Variable                  | Default                  | Meaning                                            |
|---------------------------|--------------------------|----------------------------------------------------|
| `CONVEYOR_ADDR`           | `http://localhost:8080`  | Conveyor server base URL                           |
| `CONVEYOR_TOKEN`          | _(unset)_                | Bearer token (when the server has auth enabled)    |
| `WORKER_CONCURRENCY`      | `20`                     | Simultaneous sends                                 |
| `EMAIL_MAX_RETRY`         | `5`                      | Retry budget before dead-lettering                 |
| `EMAIL_ATTEMPT_TIMEOUT_MS`| `10000`                  | Per-attempt timeout                                |
| `EMAIL_FAILURE_RATE`      | `0.15`                   | Simulated transient-failure rate (demo)            |
| `LOG_LEVEL`               | `info`                   | `debug` / `info` / `warn` / `error`                |

## Operate it

Inspect and manage tasks with the `conveyor` CLI or the dashboard at
`http://localhost:8080`. For example, cap the send rate to protect the provider:

```sh
conveyor ratelimit set email --rate 50 --burst 100
```

Files: `src/tasks.ts` (contract), `src/producer.ts` (enqueue CLI),
`src/worker.ts` (handlers + lifecycle), `src/email.ts` (provider), plus
`config.ts` and `logger.ts`.
