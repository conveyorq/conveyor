// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
)

// gatedTick asks the gated test actor to start a pass.
type gatedTick struct{}

// gatedActor runs a pass per tick through a passGate; each pass blocks until
// the test releases it, so overlapping ticks can be observed.
type gatedActor struct {
	// passes is the gate under test.
	passes passGate
	// started counts passes that began.
	started *atomic.Int32
	// release unblocks the running pass.
	release chan struct{}
}

// PreStart implements goakt.Actor.
func (a *gatedActor) PreStart(*goakt.Context) error { return nil }

// Receive implements goakt.Actor.
func (a *gatedActor) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case gatedTick:
		started, release := a.started, a.release
		a.passes.start(ctx, func(context.Context) {
			started.Add(1)
			<-release
		})

	case passFinished:
		a.passes.finish()

	default:
		ctx.Unhandled()
	}
}

// PostStop implements goakt.Actor.
func (a *gatedActor) PostStop(*goakt.Context) error { return nil }

func TestPassGateRunsOnePassAtATime(t *testing.T) {
	ctx := context.Background()
	engine := startEngine(t, memory.New(clock.System()))

	actor := &gatedActor{started: new(atomic.Int32), release: make(chan struct{})}
	pid, err := engine.System().Spawn(ctx, "gated-test-actor", actor)
	require.NoError(t, err)

	require.NoError(t, goakt.Tell(ctx, pid, gatedTick{}))
	require.Eventually(t, func() bool { return actor.started.Load() == 1 }, 5*time.Second, 10*time.Millisecond)

	// Ticks that arrive while the pass runs are absorbed, not queued.
	require.NoError(t, goakt.Tell(ctx, pid, gatedTick{}))
	require.NoError(t, goakt.Tell(ctx, pid, gatedTick{}))
	require.Never(t, func() bool { return actor.started.Load() > 1 }, 200*time.Millisecond, 20*time.Millisecond)

	actor.release <- struct{}{}

	// Once the pass ends, the next tick starts the next one.
	require.Eventually(t, func() bool {
		_ = goakt.Tell(ctx, pid, gatedTick{})

		return actor.started.Load() == 2
	}, 5*time.Second, 20*time.Millisecond)

	close(actor.release)
}

func TestMaintenanceActorsClearTheirPassGateOnStart(t *testing.T) {
	// Each actor starts with its gate marked running, as a restart that lost a
	// pass's completion would leave it; PreStart must clear the gate so the
	// first tick still runs a pass.
	cases := []struct {
		name   string
		actor  func() goakt.Actor
		passes func(*callCountingBroker) int32
	}{
		{"reaper", func() goakt.Actor { return &Reaper{passes: passGate{running: true}} },
			func(c *callCountingBroker) int32 { return c.pendingCounts.Load() }},
		{"scheduler", func() goakt.Actor { return &Scheduler{passes: passGate{running: true}} },
			func(c *callCountingBroker) int32 { return c.promotions.Load() }},
		{"group-sweeper", func() goakt.Actor { return &GroupSweeper{passes: passGate{running: true}} },
			func(c *callCountingBroker) int32 { return c.groupStats.Load() }},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			counting := &callCountingBroker{Broker: memory.New(clock.System())}
			system := startBareSystem(t, counting)

			_, err := system.Spawn(context.Background(), "gate-"+test.name, test.actor())
			require.NoError(t, err)

			require.Eventually(t, func() bool { return test.passes(counting) > 0 },
				5*time.Second, 20*time.Millisecond, "a pass runs after start")
		})
	}
}
