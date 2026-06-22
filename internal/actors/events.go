// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"log/slog"

	goakt "github.com/tochemey/goakt/v4/actor"

	"github.com/conveyorq/conveyor/internal/events"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// eventsTopic is the cluster pub/sub topic task lifecycle events are published
// to. Every node's relay subscribes to it, so an event published on any node
// fans out to every node's local event bus.
const eventsTopic = "conveyor.task.events"

// eventRelayName prefixes the per-node relay actor name. The relay is node-local
// but GoAkt registers actor names cluster-wide, so the engine qualifies it with
// the node's remoting address to stay unique.
const eventRelayName = "conveyor-event-relay"

// topicSink is an events.Sink that publishes each event to the cluster topic.
// The broker emits transitions into it; the topic fans them out cluster-wide to
// the relays, which feed each node's local event bus. Publishing is a
// fire-and-forget Tell, so it never blocks the broker's transition path.
type topicSink struct {
	// system is the actor system whose topic actor receives the publishes.
	system goakt.ActorSystem
	// newID mints a unique message id per publish, which the topic actor's
	// dedup window keys on.
	newID func() string
	// logger reports a dropped publish.
	logger *slog.Logger
}

// enforce interface compliance at compile time.
var _ events.Sink = (*topicSink)(nil)

// Emit publishes one event to the cluster topic. A missing topic actor (cluster
// not started) drops the event, the best-effort contract.
func (s *topicSink) Emit(event *conveyorv1.TaskEvent) {
	if event == nil {
		return
	}

	topicActor := s.system.TopicActor()
	if topicActor == nil {
		return
	}

	if err := goakt.Tell(context.Background(), topicActor, goakt.NewPublish(s.newID(), eventsTopic, event)); err != nil {
		// Best-effort: a dropped publish is recovered by neither retry nor
		// durability by design. The reaper and admin reads remain the source of
		// truth; events are an observability channel.
		s.logger.Debug("event publish dropped", "task_id", event.GetId(), "error", err)

		return
	}
}

// eventRelay is the per-node bridge from the cluster topic to the node-local
// event bus. It subscribes to the topic on start and republishes every received
// event into the bus, where WatchEvents streams and the webhook sink read it.
type eventRelay struct {
	// runtime is resolved from the system extension on start.
	runtime *Runtime
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*eventRelay)(nil)

// PreStart resolves the engine runtime from the system extension.
func (r *eventRelay) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("event relay: extension %q is not registered", BrokerExtensionID)
	}

	r.runtime = runtime

	return nil
}

// Receive subscribes to the topic on start and feeds received events into the
// node-local bus.
func (r *eventRelay) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		topicActor := ctx.ActorSystem().TopicActor()
		if topicActor == nil {
			r.runtime.Logger().Warn("event relay: no topic actor; lifecycle events will not stream")

			return
		}

		ctx.Tell(topicActor, goakt.NewSubscribe(eventsTopic))

	case *goakt.SubscribeAck:
		r.runtime.Logger().Debug("event relay subscribed", "topic", eventsTopic)

	case *conveyorv1.TaskEvent:
		r.runtime.EventBus().Emit(message)

	default:
		ctx.Unhandled()
	}
}

// PostStop is a no-op: the relay holds no durable state and the topic actor
// drops a terminated subscriber automatically.
func (r *eventRelay) PostStop(*goakt.Context) error {
	return nil
}
