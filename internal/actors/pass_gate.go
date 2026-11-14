// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"

	goakt "github.com/tochemey/goakt/v4/actor"
)

// passFinished reports that a maintenance pass started by a passGate ended.
type passFinished struct{}

// passGate runs a tick-driven maintenance actor's broker work off its mailbox
// turn, one pass at a time. A pass is a run of broker round trips (reaping,
// promoting, sweeping) that a degraded store can stretch arbitrarily; run in a
// turn it would hold a worker of the dispatcher pool every actor and grain on
// the node shares. The pass runs in a PipeTo task instead, over values the
// caller captured in the turn, and a tick that arrives while one is running is
// absorbed: the next tick after it ends starts the next pass. The actor owns
// its gate and must hand passFinished back to finish.
type passGate struct {
	// running records that a pass is in flight.
	running bool
}

// start begins a pass unless one is already running. pass must only use
// values captured by the caller, never the actor's state.
func (p *passGate) start(ctx *goakt.ReceiveContext, pass func(ctx context.Context)) {
	if p.running {
		return
	}

	p.running = true

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		pass(context.Background())

		return passFinished{}, nil
	})
}

// finish marks the running pass as ended, so the next tick starts another.
func (p *passGate) finish() {
	p.running = false
}
