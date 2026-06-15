// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package actors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery"
	gerrors "github.com/tochemey/goakt/v4/errors"
	goaktlog "github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
	gtls "github.com/tochemey/goakt/v4/tls"
	"google.golang.org/protobuf/proto"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Names of the maintenance singletons. Each is spawned as a cluster
// singleton: exactly one instance runs cluster-wide, placed on the leader
// and relocated to a survivor on node loss. Every node calls SpawnSingleton
// with the same name, which is idempotent — the leader hosts it and the
// rest no-op. Each singleton schedules its own recurring tick on start, so
// the tick stream follows the singleton across failover.
const (
	schedulerActorName = "conveyor-scheduler"
	reaperActorName    = "conveyor-reaper"
)

// Singleton placement consults every peer's load before choosing a host, so a
// node booting into a cluster that is still reconciling membership (a peer just
// left or joined, as during a rolling restart) can get a transient error from a
// peer that is not yet serving. These bound a short retry so such a node still
// starts instead of crashing.
const (
	singletonSpawnAttempts   = 5
	singletonSpawnRetryDelay = 500 * time.Millisecond
)

// Config wires one engine node. Clustering is always on: a node with no
// peers is a cluster of one running the identical code path.
type Config struct {
	// Name is the actor system name; all nodes of one cluster share it.
	Name string
	// BindAddr is the host remoting and discovery bind to.
	BindAddr string
	// RemotingPort is the GoAkt remoting port.
	RemotingPort int
	// DiscoveryPort is the gossip bootstrap port.
	DiscoveryPort int
	// PeersPort is the cluster peers port.
	PeersPort int
	// Provider is the cluster discovery provider. The caller builds it
	// from configuration (static, NATS, Consul, etcd, mDNS, DNS-SD, or
	// Kubernetes); the engine never chooses one itself.
	Provider discovery.Provider
	// TLS optionally secures cluster remoting with mutual TLS. The caller
	// builds it from configuration; the engine passes it through to GoAkt
	// unchanged. A nil value leaves remoting in cleartext.
	TLS *gtls.Info
	// Settings tunes the engine actors.
	Settings Settings
}

// Engine is the coordination layer of one conveyord node: the actor
// system hosting queue grains, the scheduler, and the reaper, plus the
// enqueue entry point that the API layer calls.
type Engine struct {
	// runtime is the engine runtime shared with every actor.
	runtime *Runtime
	// config is the node configuration.
	config Config
	// system is the GoAkt actor system; nil until Start succeeds.
	system goakt.ActorSystem
	// wakers coalesces enqueue wake-ups per queue (queue -> *queueWaker).
	wakers sync.Map
}

// queueWaker coalesces wake-up hints for one queue. A burst of enqueues
// produces at most one in-flight wake because the grain drains the broker on
// each wake; redundant hints would only flood its mailbox. dirty records that
// a wake is owed; running records that the drain goroutine is live.
type queueWaker struct {
	// mu guards dirty and running.
	mu sync.Mutex
	// dirty is true when an enqueue happened since the last wake was sent.
	dirty bool
	// running is true while a drain goroutine is sending wakes.
	running bool
}

// NewEngine assembles an engine node around a broker.
func NewEngine(taskLog broker.Broker, timeSource clock.Clock, logger *slog.Logger, config Config) *Engine {
	return &Engine{
		runtime: NewRuntime(taskLog, timeSource, config.Settings, logger),
		config:  config,
	}
}

// Start boots the clustered actor system, spawns the maintenance actors,
// and starts their ticks.
func (e *Engine) Start(ctx context.Context) error {
	if e.config.Provider == nil {
		return fmt.Errorf("engine config: discovery provider is required")
	}

	options := []goakt.Option{
		goakt.WithLogger(goaktlog.NewSlogFrom(e.runtime.Logger(), goaktlog.InfoLevel)),
		goakt.WithExtensions(e.runtime),
		// Record actor and cluster runtime metrics into the process-global
		// meter provider; the server installs a Prometheus-backed provider,
		// and without one this is a no-op.
		goakt.WithMetrics(),
		goakt.WithRemote(remote.NewConfig(e.config.BindAddr, e.config.RemotingPort)),
		goakt.WithCluster(goakt.NewClusterConfig().
			WithDiscovery(e.config.Provider).
			WithDiscoveryPort(e.config.DiscoveryPort).
			WithPeersPort(e.config.PeersPort).
			WithMinimumPeersQuorum(1).
			WithReplicaCount(1).
			WithKinds(NewScheduler(), NewReaper()).
			WithGrains(new(QueueGrain))),
	}

	if e.config.TLS != nil {
		options = append(options, goakt.WithTLS(e.config.TLS))
	}

	system, err := goakt.NewActorSystem(e.config.Name, options...)
	if err != nil {
		return fmt.Errorf("building actor system: %w", err)
	}

	// GoAkt captures the start context as the base context of its remoting
	// server: a boot- or signal-scoped context would silently kill all
	// grain messaging the moment it ends. The engine's lifetime is
	// controlled by Stop alone, so the system starts detached.
	if err = system.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("starting actor system: %w", err)
	}

	e.system = system

	if err = spawnSingleton(ctx, system, schedulerActorName, NewScheduler()); err != nil {
		return fmt.Errorf("spawning scheduler singleton: %w", err)
	}

	if err = spawnSingleton(ctx, system, reaperActorName, NewReaper()); err != nil {
		return fmt.Errorf("spawning reaper singleton: %w", err)
	}

	e.runtime.Logger().Info("engine started", "system", e.config.Name, "bind", e.config.BindAddr, "discovery", e.config.Provider.ID())

	return nil
}

// spawnSingleton spawns a cluster singleton, tolerating the already-exists
// outcome. Every node calls this with the same name on start; the leader
// hosts the singleton and the rest see ErrSingletonAlreadyExists, which is
// the desired state — exactly one instance runs cluster-wide. GoAkt's
// relocator re-spawns it on a survivor when the host node is lost.
func spawnSingleton(ctx context.Context, system goakt.ActorSystem, name string, actor goakt.Actor) error {
	var err error

	for attempt := range singletonSpawnAttempts {
		_, err = system.SpawnSingleton(ctx, name, actor)
		if err == nil || errors.Is(err, gerrors.ErrSingletonAlreadyExists) {
			return nil
		}

		if attempt == singletonSpawnAttempts-1 {
			break
		}

		// The error is most likely a peer mid-reconciliation; wait for the
		// cluster to settle, honoring a caller cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(singletonSpawnRetryDelay):
		}
	}

	return err
}

// Stop shuts the actor system down. Worker sessions must be drained
// before this call: GoAkt rejects every user message the moment its stop
// sequence begins, so a gateway can no longer process a drain request —
// or any durable transition — once the system is stopping.
func (e *Engine) Stop(ctx context.Context) error {
	return e.system.Stop(ctx)
}

// System exposes the actor system for components that attach to it
// (worker gateways, tests).
func (e *Engine) System() goakt.ActorSystem {
	return e.system
}

// Counters returns the core engine counters.
func (e *Engine) Counters() *Counters {
	return e.runtime.Counters()
}

// Settings returns the engine settings.
func (e *Engine) Settings() Settings {
	return e.runtime.Settings()
}

// NewID returns a fresh ULID for tasks and sessions.
func (e *Engine) NewID() string {
	return e.runtime.NewID()
}

// Enqueue durably commits a task and wakes its queue grain. The task is
// assigned a fresh ULID when it carries none. The wake-up is a best-effort
// hint: a lost one is recovered by the reaper sweep.
func (e *Engine) Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) error {
	if task.GetId() == "" {
		task.Id = e.runtime.NewID()
	}

	if err := e.runtime.Broker().Enqueue(ctx, task); err != nil {
		return err
	}

	e.runtime.Counters().Enqueued.Add(1)
	e.wake(task.GetQueue())

	return nil
}

// wake schedules a coalesced, asynchronous wake-up for a queue's grain. It
// never blocks the caller: GoAkt's TellGrain waits for the grain to process
// the hint, so waking per enqueue would serialize a burst on the grain's
// turn rate. Coalescing keeps at most one wake in flight per queue, which is
// enough because the grain drains the broker on each one.
func (e *Engine) wake(queue string) {
	value, _ := e.wakers.LoadOrStore(queue, &queueWaker{})
	waker := value.(*queueWaker)

	waker.mu.Lock()
	waker.dirty = true

	if waker.running {
		waker.mu.Unlock()

		return
	}

	waker.running = true
	waker.mu.Unlock()

	go e.drainWaker(queue, waker)
}

// drainWaker sends wake-ups for a queue until no enqueue is owed, then exits.
// The dirty flag is checked and cleared under the lock, so an enqueue that
// arrives during a send is never lost: it either re-arms this loop or starts
// a fresh goroutine. A failed send is best-effort; the reaper sweep recovers.
func (e *Engine) drainWaker(queue string, waker *queueWaker) {
	for {
		waker.mu.Lock()

		if !waker.dirty {
			waker.running = false
			waker.mu.Unlock()

			return
		}

		waker.dirty = false
		waker.mu.Unlock()

		if err := e.TellQueue(context.Background(), queue, &conveyorv1.TasksAvailable{Queue: queue}); err != nil {
			e.runtime.Logger().Warn("queue wake-up failed; reaper sweep will recover", "queue", queue, "error", err)
		}
	}
}

// TellQueue sends a message to a queue's grain, activating it if needed.
func (e *Engine) TellQueue(ctx context.Context, queue string, message proto.Message) error {
	identity, err := e.system.GrainIdentity(ctx, QueueGrainName(queue), queueGrainFactory,
		goakt.WithGrainDeactivateAfter(e.config.Settings.PassivateAfter))
	if err != nil {
		return fmt.Errorf("resolving queue grain %s: %w", queue, err)
	}

	return e.system.TellGrain(ctx, identity, message)
}
