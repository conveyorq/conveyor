package memory

import (
	"testing"

	"github.com/tochemey/conveyor/internal/broker"
	"github.com/tochemey/conveyor/internal/broker/brokertest"
	"github.com/tochemey/conveyor/internal/clock"
)

func TestConformance(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, timeSource clock.Clock) broker.Broker {
		instance := New(timeSource)
		t.Cleanup(func() { _ = instance.Close() })

		return instance
	})
}
