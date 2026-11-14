// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"

	goakt "github.com/tochemey/goakt/v4/actor"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// resolverRouterName is the node-local name of the dependency-resolver router.
const resolverRouterName = "conveyor-resolver"

// DependencyResolver reconciles the dependents of a finished task off the
// gateway's turn. A pool of these sits behind the per-node resolver router: the
// gateway hands off a ResolveDependents message with a non-blocking tell, the
// router spreads it across the pool, and the routee runs the resolving broker
// transaction. Each routee keeps one resolution in flight, in a PipeTo task,
// and queues the rest in its own state: the pool still caps how many
// resolutions run at once, and no turn waits on the broker, so a slow store
// never holds a worker of the dispatcher pool the node's actors and grains
// share.
type DependencyResolver struct {
	// runtime is the engine runtime.
	runtime *Runtime
	// pending are the finished tasks waiting for this routee's resolution, in
	// arrival order.
	pending []string
	// resolving records that a resolution is in flight.
	resolving bool
}

// dependentsResolved reports that a routee's resolution finished.
type dependentsResolved struct{}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*DependencyResolver)(nil)

// NewDependencyResolver returns a resolver routee shell; it resolves the
// runtime from the actor-system extension in PreStart, like the other actors.
func NewDependencyResolver() *DependencyResolver {
	return &DependencyResolver{}
}

// PreStart implements goakt.Actor.
func (d *DependencyResolver) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("dependency resolver %s: extension %q is not registered", ctx.ActorName(), BrokerExtensionID)
	}

	d.runtime = runtime
	// A restart reuses this instance; a resolution that ended while it was
	// stopped never reported, and queued requests died with the mailbox.
	d.pending = nil
	d.resolving = false

	return nil
}

// Receive resolves the dependents of finished tasks. The router unwraps the
// broadcast envelope, so each routee sees a bare ResolveDependents message.
func (d *DependencyResolver) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		// Nothing to arm: routees are stateless and react only to requests.

	case *conveyorv1.ResolveDependents:
		d.pending = append(d.pending, message.GetTaskId())
		d.resolveNext(ctx)

	case dependentsResolved:
		d.resolving = false
		d.resolveNext(ctx)

	default:
		ctx.Unhandled()
	}
}

// PostStop implements goakt.Actor.
func (d *DependencyResolver) PostStop(_ *goakt.Context) error {
	return nil
}

// resolveNext starts the next queued resolution unless one is in flight.
func (d *DependencyResolver) resolveNext(ctx *goakt.ReceiveContext) {
	if d.resolving || len(d.pending) == 0 {
		return
	}

	taskID := d.pending[0]
	d.pending = d.pending[1:]
	d.resolving = true

	system, runtime := ctx.ActorSystem(), d.runtime

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		resolveDependents(context.Background(), system, runtime, taskID)

		return dependentsResolved{}, nil
	})
}

// resolveDependents reconciles one finished task's dependents and wakes any
// queue whose work became eligible. Resolution is best-effort (the reaper
// sweep backstops any failure), so a broker error is logged, never escalated.
// It is a PipeTo task body: it uses only the actor system and runtime it is
// given.
func resolveDependents(ctx context.Context, system goakt.ActorSystem, runtime *Runtime, taskID string) {
	queues, err := runtime.Broker().ResolveDependents(ctx, taskID)
	if err != nil {
		runtime.Logger().Warn("resolving dependents failed", "task_id", taskID, "error", err)

		return
	}

	for _, queue := range queues {
		wakeQueue(ctx, system, runtime, queue, 0)
	}
}
