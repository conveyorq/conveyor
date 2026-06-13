package actors

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
)

// newTestRuntime assembles a runtime over a fresh memory broker.
func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()

	taskLog := memory.New(clock.System())
	t.Cleanup(func() { _ = taskLog.Close() })

	return NewRuntime(taskLog, clock.System(), testSettings, quietLogger())
}

func TestRuntimeAccessors(t *testing.T) {
	taskLog := memory.New(clock.System())
	timeSource := clock.NewFake(time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC))
	logger := quietLogger()

	runtime := NewRuntime(taskLog, timeSource, testSettings, logger)

	require.Equal(t, BrokerExtensionID, runtime.ID())
	require.Same(t, taskLog, runtime.Broker())
	require.Equal(t, timeSource.Now(), runtime.Clock().Now())
	require.Equal(t, testSettings, runtime.Settings())
	require.Same(t, logger, runtime.Logger())
	require.NotNil(t, runtime.Counters())
}

func TestRuntimeNewIDIsUniqueAndMonotonic(t *testing.T) {
	runtime := newTestRuntime(t)

	previous := ""

	for range 1000 {
		id := runtime.NewID()
		require.Len(t, id, 26, "ULIDs are 26 characters")
		require.Greater(t, id, previous, "ids must be monotonically increasing")

		previous = id
	}
}

func TestRuntimeNewIDIsConcurrencySafe(t *testing.T) {
	runtime := newTestRuntime(t)

	const goroutines = 8
	const perGoroutine = 200

	var mutex sync.Mutex
	seen := make(map[string]bool, goroutines*perGoroutine)

	var group sync.WaitGroup

	for range goroutines {
		group.Go(func() {
			for range perGoroutine {
				id := runtime.NewID()

				mutex.Lock()
				seen[id] = true
				mutex.Unlock()
			}
		})
	}

	group.Wait()
	require.Len(t, seen, goroutines*perGoroutine, "all generated ids must be distinct")
}
