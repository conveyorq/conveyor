// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
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
// transaction. Blocking a routee is harmless — its mailbox carries only
// resolution requests — so the bounded pool caps how many resolutions run at
// once without ever stalling task delivery.
type DependencyResolver struct {
	// runtime is the engine runtime.
	runtime *Runtime
}

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

	return nil
}

// Receive resolves the dependents of finished tasks. The router unwraps the
// broadcast envelope, so each routee sees a bare ResolveDependents message.
func (d *DependencyResolver) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		// Nothing to arm: routees are stateless and react only to requests.

	case *conveyorv1.ResolveDependents:
		d.resolve(ctx, message)

	default:
		ctx.Unhandled()
	}
}

// PostStop implements goakt.Actor.
func (d *DependencyResolver) PostStop(_ *goakt.Context) error {
	return nil
}

// resolve reconciles one finished task's dependents and wakes any queue whose
// work became eligible. The broker call is the long-running task this pool
// exists to offload. Resolution is best-effort — the reaper sweep backstops any
// failure — so a broker error is logged, never escalated.
func (d *DependencyResolver) resolve(ctx *goakt.ReceiveContext, message *conveyorv1.ResolveDependents) {
	goCtx := ctx.Context()

	queues, err := d.runtime.Broker().ResolveDependents(goCtx, message.GetTaskId())
	if err != nil {
		d.runtime.Logger().Warn("resolving dependents failed", "task_id", message.GetTaskId(), "error", err)

		return
	}

	for _, queue := range queues {
		wakeQueue(goCtx, ctx.ActorSystem(), d.runtime, queue, 0)
	}
}
