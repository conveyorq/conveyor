// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerQueuesWeightsTransactionalHighest(t *testing.T) {
	queues := WorkerQueues()

	require.Greater(t, queues[QueueTransactional], queues[QueueDefault])
	require.Greater(t, queues[QueueDefault], queues[QueueMarketing])

	for name, weight := range queues {
		require.Positive(t, weight, "queue %q must have a positive weight", name)
	}
}

func TestDeliverableAddressIsNotAHardBounce(t *testing.T) {
	addr := DeliverableAddress(42)

	require.Equal(t, "user42@example.com", addr)
	require.False(t, IsHardBounce(addr))
}

func TestBounceAddressIsAHardBounce(t *testing.T) {
	addr := BounceAddress(42)

	require.Equal(t, "user42@bounce.invalid", addr)
	require.True(t, IsHardBounce(addr))
}
