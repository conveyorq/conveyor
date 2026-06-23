// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package actors implements the coordination layer of conveyord: the
// queue grains that dispatch work, the scheduler and reaper maintenance
// actors, and the engine that assembles them on a GoAkt actor system.
// All durable state lives in the broker; every actor rebuilds its state
// from the broker on (re)activation.
package actors

import (
	"context"
	"crypto/rand"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/extension"

	"github.com/conveyorq/conveyor/internal/backoff"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/events"
	"github.com/conveyorq/conveyor/internal/metrics"
)

// BrokerExtensionID names the actor-system extension carrying the broker and its
// companions. Grains resolve it on activation, which is what lets a grain
// relocate to another node and rebuild from scratch.
const BrokerExtensionID = "broker"

// Settings tunes the engine actors.
type Settings struct {
	// LeaseTTL is how long a task lease lives before the reaper may
	// reclaim it.
	LeaseTTL time.Duration
	// LeaseBatchMax caps how many tasks one lease cycle may claim.
	LeaseBatchMax int
	// ResolverPoolSize is the number of dependency-resolver routees behind the
	// per-node resolver router. It bounds how many dependency resolutions run
	// concurrently, so completion-time resolution never overwhelms the broker.
	ResolverPoolSize int
	// ReapInterval is the cadence of the reaper maintenance pass.
	ReapInterval time.Duration
	// PromoteInterval is the cadence of scheduled-task promotion.
	PromoteInterval time.Duration
	// PassivateAfter is the idle time before a queue grain deactivates.
	PassivateAfter time.Duration
	// GroupMaxSize fires an aggregation group once this many members
	// accumulate.
	GroupMaxSize int
	// GroupMaxDelay fires a group this long after its first member, capping
	// aggregation latency.
	GroupMaxDelay time.Duration
	// GroupGracePeriod fires a group this long after its most recent member,
	// coalescing a burst once it goes quiet.
	GroupGracePeriod time.Duration
	// GroupSweepInterval is the cadence of the group-aggregation sweep that
	// fires groups whose delay or grace threshold has elapsed.
	GroupSweepInterval time.Duration
	// RateLimitEnabled gates dispatch rate limiting. When false, no queue
	// enforces a limit even if one is configured.
	RateLimitEnabled bool
	// RateLimitRatePerSec is the global default dispatch rate, in tasks per
	// second, applied to every queue without its own override. Zero means no
	// default (queues are unlimited unless overridden).
	RateLimitRatePerSec float64
	// RateLimitBurst is the global default token-bucket depth, paired with
	// RateLimitRatePerSec.
	RateLimitBurst int
	// EventsEnabled gates the task lifecycle event stream. When false, no events
	// are propagated and WatchEvents is unavailable.
	EventsEnabled bool
	// EventBufferSize is the per-watcher event-stream buffer depth; zero selects
	// the events package default.
	EventBufferSize int
	// RetryBackoff is the default retry backoff a gateway applies to a failed
	// task that carries no per-task retry policy.
	RetryBackoff backoff.Strategy
}

// Counters are the core engine counters, safe for concurrent use. OTel
// export wiring arrives with the observability phase; these are the
// authoritative in-process values it will read from.
type Counters struct {
	// Enqueued counts tasks durably committed through the engine.
	Enqueued atomic.Int64
	// Dispatched counts ExecuteTask messages handed to gateways.
	Dispatched atomic.Int64
	// Completed counts successful executions reported back.
	Completed atomic.Int64
	// Failed counts failed executions reported back.
	Failed atomic.Int64
	// Active is the number of executions currently in flight.
	Active atomic.Int64
	// Retried counts executions returned for a later retry.
	Retried atomic.Int64
	// Archived counts executions dead-lettered (retries exhausted, skip-retry,
	// or admin cancel).
	Archived atomic.Int64
	// Released counts deliveries returned to the queue for redelivery without
	// a retry penalty.
	Released atomic.Int64
}

// Runtime is the actor-system extension giving every actor and grain
// access to the broker, clock, settings, logger, and counters.
type Runtime struct {
	// broker is the durable task log.
	broker broker.Broker
	// clock supplies the current time for every component.
	clock clock.Clock
	// settings tunes the engine actors.
	settings Settings
	// logger is the process logger shared by all actors.
	logger *slog.Logger
	// counters are the core engine counters.
	counters *Counters
	// metrics are the synchronous timing/canary instruments.
	metrics *metrics.Engine
	// eventBus fans task lifecycle events out to WatchEvents streams and the
	// webhook sink on this node. The relay actor publishes into it; it is always
	// present but only fed when events are enabled.
	eventBus *events.EventBus

	// idMutex guards idEntropy: lease and task ids are generated from any
	// goroutine (API handlers, grains).
	idMutex sync.Mutex
	// idEntropy is the monotonic ULID entropy source.
	idEntropy *ulid.MonotonicEntropy

	// resolver is the per-node dependency-resolver router, set once after the
	// actor system starts. Nil until then; a nil resolver means completion-time
	// resolution is skipped and the reaper sweep is the sole path.
	resolver atomic.Pointer[goakt.PID]
}

var _ extension.Extension = (*Runtime)(nil)

// NewRuntime assembles the engine runtime extension.
func NewRuntime(taskLog broker.Broker, timeSource clock.Clock, settings Settings, logger *slog.Logger) *Runtime {
	instruments, err := metrics.NewEngine()
	if err != nil {
		logger.Warn("registering engine metrics instruments failed", "error", err)
	}

	eventBus := events.NewEventBus(settings.EventBufferSize, func() {
		instruments.EventDropped(context.Background())
	})

	return &Runtime{
		broker:    taskLog,
		clock:     timeSource,
		settings:  settings,
		logger:    logger,
		counters:  &Counters{},
		metrics:   instruments,
		eventBus:  eventBus,
		idEntropy: ulid.Monotonic(rand.Reader, 0),
	}
}

// ID implements extension.Extension.
func (r *Runtime) ID() string {
	return BrokerExtensionID
}

// Broker returns the durable task log.
func (r *Runtime) Broker() broker.Broker {
	return r.broker
}

// Resolver returns the node's dependency-resolver router, or nil before the
// engine has spawned it.
func (r *Runtime) Resolver() *goakt.PID {
	return r.resolver.Load()
}

// SetResolver records the node's dependency-resolver router. The engine calls
// it once after spawning the router, before any worker session is served.
func (r *Runtime) SetResolver(pid *goakt.PID) {
	r.resolver.Store(pid)
}

// Clock returns the injected time source.
func (r *Runtime) Clock() clock.Clock {
	return r.clock
}

// Settings returns the engine settings.
func (r *Runtime) Settings() Settings {
	return r.settings
}

// Logger returns the process logger.
func (r *Runtime) Logger() *slog.Logger {
	return r.logger
}

// Counters returns the core engine counters.
func (r *Runtime) Counters() *Counters {
	return r.counters
}

// Metrics returns the synchronous timing and canary instruments.
func (r *Runtime) Metrics() *metrics.Engine {
	return r.metrics
}

// EventBus returns the node-local lifecycle-event fan-out.
func (r *Runtime) EventBus() *events.EventBus {
	return r.eventBus
}

// NewID returns a fresh ULID derived from the injected clock.
func (r *Runtime) NewID() string {
	r.idMutex.Lock()
	defer r.idMutex.Unlock()

	return ulid.MustNew(ulid.Timestamp(r.clock.Now()), r.idEntropy).String()
}
