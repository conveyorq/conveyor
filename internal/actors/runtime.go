// Package actors implements the coordination layer of conveyord: the
// queue grains that dispatch work, the scheduler and reaper maintenance
// actors, and the engine that assembles them on a GoAkt actor system.
// All durable state lives in the broker; every actor rebuilds its state
// from the broker on (re)activation.
package actors

import (
	"crypto/rand"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/tochemey/goakt/v4/extension"

	"github.com/tochemey/conveyor/internal/broker"
	"github.com/tochemey/conveyor/internal/clock"
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
	// ReapInterval is the cadence of the reaper maintenance pass.
	ReapInterval time.Duration
	// PromoteInterval is the cadence of scheduled-task promotion.
	PromoteInterval time.Duration
	// PassivateAfter is the idle time before a queue grain deactivates.
	PassivateAfter time.Duration
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

	// idMutex guards idEntropy: lease and task ids are generated from any
	// goroutine (API handlers, grains).
	idMutex sync.Mutex
	// idEntropy is the monotonic ULID entropy source.
	idEntropy *ulid.MonotonicEntropy
}

var _ extension.Extension = (*Runtime)(nil)

// NewRuntime assembles the engine runtime extension.
func NewRuntime(taskLog broker.Broker, timeSource clock.Clock, settings Settings, logger *slog.Logger) *Runtime {
	return &Runtime{
		broker:    taskLog,
		clock:     timeSource,
		settings:  settings,
		logger:    logger,
		counters:  &Counters{},
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

// NewID returns a fresh ULID derived from the injected clock.
func (r *Runtime) NewID() string {
	r.idMutex.Lock()
	defer r.idMutex.Unlock()

	return ulid.MustNew(ulid.Timestamp(r.clock.Now()), r.idEntropy).String()
}
