---
layout: home

hero:
  name: Conveyor
  text: A distributed, push-based task queue
  tagline: "Persistent tasks with at-least-once execution, retries with backoff, scheduling, and priorities, backed by Postgres or an in-memory broker, with no Redis and no polling."
  actions:
    - theme: brand
      text: Guide
      link: /install
    - theme: alt
      text: GitHub
      link: https://github.com/conveyorq/conveyor

intro:
  kicker: Overview
  title: How it works
  details: "Conveyor has three moving parts: your client enqueues tasks, the server owns them, and your workers process them. A durable broker is the source of truth, and tasks are persisted before they're dispatched, so they survive any crash."
  alt: "Conveyor architecture: the SDK and CLI enqueue into named, prioritized queues held in a multi-node conveyord cluster backed by a durable Postgres or in-memory broker; the cluster pushes queued tasks to a worker's free concurrency slots, which acknowledge back."
  parts:
    - term: Client
      icon: webhook
      details: "Your code that enqueues tasks."
    - term: Server (conveyord)
      icon: concurrency
      details: "Owns the queues and decides who runs what, when."
    - term: Worker
      icon: worker
      details: "Your code that receives tasks from the server and executes them."
    - term: Broker
      icon: broker
      details: "The durable store (Postgres, or in-memory for dev) that is the single source of truth."

bands:
  - kicker: Getting started
    title: Produce and consume
    details: "The two halves of using Conveyor, a worker that registers a handler per task type and a client that enqueues tasks, have the same shape in every SDK."
    items:
      - title: Concepts
        icon: task
        details: "The core vocabulary in plain terms: task, queue, client, server, worker, and broker, and how they fit together. Start here."
        link: /concepts
      - title: Use cases
        icon: list
        details: "The shapes of work Conveyor suits, from transactional email to AI inference jobs, and the features each leans on."
        link: /use-cases
      - title: Usage guide
        icon: worker
        details: "The full worker and enqueue snippets for Go, TypeScript, Python, and the CLI."
        link: /usage
      - title: CLI reference
        icon: cli
        details: "Every conveyor command, its flags, and the global address, token, and encryption settings, for producing and operating."
        link: /cli
      - title: Dashboard
        icon: dashboard
        details: "The embedded operations console: inspect queues, tasks, cron, and workers, and act on them from a browser."
        link: /dashboard
      - title: HTTP API
        icon: http
        details: "Enqueue and inspect tasks from any language over plain HTTP/JSON, no SDK required."
        link: /http-api
      - title: Webhook workers
        icon: webhook
        details: "Process pushed tasks over a signed JSON-RPC endpoint, with no SDK, while staying push-based."
        link: /webhook-workers
      - title: Embedded mode
        icon: embedded
        details: "Run broker, server, and dispatch inside your own Go process, and what that means for durability and availability."
        link: /embedded
      - title: Go, TypeScript, and Python SDKs
        icon: sdk
        details: "One wire protocol, three SDKs: a task enqueued from any of them runs on a worker written in any other."
        link: https://github.com/conveyorq/conveyor/tree/main/sdks

  - kicker: Features
    title: Shape the work
    details: "Named queues with weights, bounded worker concurrency, retries with configurable backoff, per-task priorities, delayed and scheduled tasks, cron, unique tasks, retention and archival, and a read-only admin and inspection API, all enforced server-side. Your code only writes handlers and enqueues tasks."
    items:
      - title: Task dependencies
        icon: workflow
        details: "Order work with chains and fan-out/fan-in, and choose what happens when a dependency fails."
        link: /workflows
      - title: Group aggregation
        icon: group
        details: "How to enqueue grouped tasks, write batch handlers, and tune the firing policy."
        link: /grouping
      - title: Rate limiting
        icon: ratelimit
        details: "Cap per-queue dispatch rate with a global default and live per-queue overrides."
        link: /rate-limiting
      - title: Concurrency limits
        icon: concurrency
        details: "Cap how many tasks run at once per concurrency key, set per queue and tunable live."
        link: /concurrency
      - title: End-to-end encryption
        icon: encryption
        details: "Seal task payloads in the SDK or CLI so the server stores ciphertext only and holds no keys."
        link: /encryption
      - title: Expiring tasks
        icon: expiring
        details: "A pre-dispatch TTL, and how it differs from a deadline and from retention."
        link: /expiring-jobs
      - title: Lifecycle events
        icon: events
        details: "The push event stream and webhook sink, covering event types, filtering, delivery semantics, and configuration."
        link: /events

  - kicker: Operations
    title: Run it in production
    details: "The server coordinates queues, scheduling, and lease recovery across a cluster of conveyord nodes: they rebalance automatically when a node is lost, and no task is dropped. Scale by adding nodes; the broker is the only stateful dependency."
    items:
      - title: Operations guide
        icon: operations
        details: "Deployment modes, configuration, scaling, broker sizing, security, observability, and upgrades."
        link: /operations
      - title: High availability
        icon: ha
        details: "A complete clustered deployment that ties the server, Postgres, and worker tiers together."
        link: /high-availability
      - title: Architecture
        icon: architecture
        details: "How the system works inside: the actor runtime, queue grains, broker, dispatch, and clustering, for contributors."
        link: /architecture
      - title: Wire protocol
        icon: protocol
        details: "The normative protocol spec for SDK authors building a Conveyor client or worker in another language."
        link: /protocol
      - title: How it compares
        icon: compare
        details: "Conveyor next to asynq and River, capability by capability, and when a workflow engine is the better fit."
        link: /comparison
      - title: Migrating from asynq
        icon: migrate
        details: "Side-by-side API mapping."
        link: /migrate-from-asynq
      - title: Migrating from River
        icon: migrate
        details: "Side-by-side API mapping, and the one trade-off to decide first (transactional enqueue)."
        link: /migrate-from-river

note:
  title: Push, not poll
  icon: push
  details: "The server pushes tasks to workers the instant work exists and a worker has free capacity, so there's no poll interval to tune and no Redis. Each worker opens one persistent connection, tells the server which queues it serves and how much it can handle, and receives work over that stream. When a worker is saturated it simply stops accepting more, and the extra work waits safely in the broker."
---
