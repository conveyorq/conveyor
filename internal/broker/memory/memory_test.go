// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"testing"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/brokertest"
	"github.com/conveyorq/conveyor/internal/clock"
)

func TestConformance(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, timeSource clock.Clock) broker.Broker {
		instance := New(timeSource)
		t.Cleanup(func() { _ = instance.Close() })

		return instance
	})
}
