package actors

import (
	"context"
	"fmt"
	"log/slog"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery"
	goaktlog "github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
	"google.golang.org/protobuf/proto"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Name prefixes of the per-node maintenance actors. Actor names are
// cluster-wide unique, so each node suffixes its remoting port until the
// clustering phase converts these to cluster singletons. Concurrent
// maintenance actors are safe: every broker maintenance operation is
// atomic, extra passes are no-ops.
const (
	schedulerActorName = "conveyor-scheduler"
	reaperActorName    = "conveyor-reaper"
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

	system, err := goakt.NewActorSystem(e.config.Name,
		goakt.WithLogger(goaktlog.NewSlogFrom(e.runtime.Logger(), goaktlog.InfoLevel)),
		goakt.WithExtensions(e.runtime),
		goakt.WithRemote(remote.NewConfig(e.config.BindAddr, e.config.RemotingPort)),
		goakt.WithCluster(goakt.NewClusterConfig().
			WithDiscovery(e.config.Provider).
			WithDiscoveryPort(e.config.DiscoveryPort).
			WithPeersPort(e.config.PeersPort).
			WithMinimumPeersQuorum(1).
			WithReplicaCount(1).
			WithGrains(new(QueueGrain))),
	)
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

	scheduler, err := system.Spawn(ctx,
		fmt.Sprintf("%s-%d", schedulerActorName, e.config.RemotingPort),
		NewScheduler(),
		goakt.WithLongLived(),
		goakt.WithRelocationDisabled())
	if err != nil {
		return fmt.Errorf("spawning scheduler: %w", err)
	}

	reaper, err := system.Spawn(ctx,
		fmt.Sprintf("%s-%d", reaperActorName, e.config.RemotingPort),
		NewReaper(),
		goakt.WithLongLived(),
		goakt.WithRelocationDisabled())
	if err != nil {
		return fmt.Errorf("spawning reaper: %w", err)
	}

	if err = system.Schedule(ctx, new(conveyorv1.PromoteTick), scheduler, e.config.Settings.PromoteInterval); err != nil {
		return fmt.Errorf("scheduling promotion ticks: %w", err)
	}

	if err = system.Schedule(ctx, new(conveyorv1.ReapTick), reaper, e.config.Settings.ReapInterval); err != nil {
		return fmt.Errorf("scheduling reaper ticks: %w", err)
	}

	e.runtime.Logger().Info("engine started", "system", e.config.Name, "bind", e.config.BindAddr, "discovery", e.config.Provider.ID())

	return nil
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

	if err := e.TellQueue(ctx, task.GetQueue(), &conveyorv1.TaskEnqueued{Queue: task.GetQueue()}); err != nil {
		e.runtime.Logger().Warn("enqueue wake-up failed; reaper sweep will recover", "queue", task.GetQueue(), "error", err)
	}

	return nil
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
