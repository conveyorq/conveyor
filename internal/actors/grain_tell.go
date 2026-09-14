// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"
)

// tellQueueGrain resolves the grain of queue and delivers message to it from
// inside an actor turn without holding the turn's dispatcher worker. GoAkt's
// TellGrain returns only once the grain has run the message, and grains share
// the actors' dispatcher pool, which is sized to the CPU count: a turn that
// waits inside it leaves one worker fewer for the grain it waits on, and as
// many such turns as there are workers stall every grain on the node until
// their waits time out. Resolution and the wait therefore run on their own
// goroutine. Resolving by name on every call keeps no identity on the actor,
// so a report can never be dropped for arriving before a cached identity did;
// for an active grain the resolution is a registry lookup. A failure is logged
// under what with the given fields, since every message sent this way is a
// hint the maintenance sweeps recover from, and onFailure, when set, runs for
// either failure so a caller can count it.
func tellQueueGrain(ctx context.Context, system goakt.ActorSystem, runtime *Runtime, queue string, message proto.Message, what string, onFailure func(), fields ...any) {
	goCtx := context.WithoutCancel(ctx)
	passivateAfter := runtime.Settings().PassivateAfter
	logger := runtime.Logger()

	go func() {
		identity, err := goakt.GrainOf[*QueueGrain](goCtx, system, QueueGrainName(queue),
			goakt.WithGrainDeactivateAfter(passivateAfter))
		if err != nil {
			logger.Warn("resolving queue grain failed", append(fields, "queue", queue, "error", err)...)

			if onFailure != nil {
				onFailure()
			}

			return
		}

		if err := system.TellGrain(goCtx, identity, message); err != nil {
			logger.Warn(what, append(fields, "queue", queue, "error", err)...)

			if onFailure != nil {
				onFailure()
			}
		}
	}()
}
