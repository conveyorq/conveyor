// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// noLatency configures a provider that never fails and returns immediately, for
// deterministic fast tests.
var noLatency = ProviderConfig{Latency: time.Nanosecond, FailureRate: 0}

func TestProviderHealthySendSucceeds(t *testing.T) {
	provider := NewProvider(noLatency)

	err := provider.Send(context.Background(), Email{To: DeliverableAddress(1)})

	require.NoError(t, err)
	require.Equal(t, int64(1), provider.Stats().Sent)
}

func TestProviderHardBounceIsPermanent(t *testing.T) {
	provider := NewProvider(noLatency)

	err := provider.Send(context.Background(), Email{To: BounceAddress(1)})

	require.ErrorIs(t, err, ErrHardBounce)
	require.Equal(t, int64(1), provider.Stats().Bounced)
}

func TestProviderOutageRejectsEverySend(t *testing.T) {
	provider := NewProvider(noLatency)
	provider.SetDown(true)

	require.True(t, provider.Down())

	err := provider.Send(context.Background(), Email{To: DeliverableAddress(1)})

	require.ErrorIs(t, err, ErrProviderDown)
	require.Equal(t, int64(0), provider.Stats().Sent)

	provider.SetDown(false)
	require.NoError(t, provider.Send(context.Background(), Email{To: DeliverableAddress(1)}))
}

func TestProviderAlwaysTransientWhenRateIsOne(t *testing.T) {
	provider := NewProvider(ProviderConfig{Latency: time.Nanosecond, FailureRate: 1})

	err := provider.Send(context.Background(), Email{To: DeliverableAddress(1)})

	require.ErrorIs(t, err, ErrTransient)
	require.Equal(t, int64(1), provider.Stats().Transient)
}

func TestProviderHonorsConnectionLimit(t *testing.T) {
	const maxConnections = 2

	// A long latency holds each acquired connection open until released.
	provider := NewProvider(ProviderConfig{MaxConnections: maxConnections, Latency: time.Hour, FailureRate: 0})

	holderCtx, releaseHolders := context.WithCancel(context.Background())
	defer releaseHolders()

	var holders sync.WaitGroup

	for range maxConnections {
		holders.Go(func() {
			_ = provider.Send(holderCtx, Email{To: DeliverableAddress(1)})
		})
	}

	require.Eventually(t, func() bool {
		return provider.InFlight() == maxConnections
	}, time.Second, time.Millisecond, "holders never claimed every connection")

	// With every connection busy, a further send cannot proceed: a canceled
	// context aborts it at the connection wait instead of letting it through.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, provider.Send(canceled, Email{To: DeliverableAddress(1)}), context.Canceled)
	require.Equal(t, int64(0), provider.Stats().Sent, "no holder should have completed while held open")

	releaseHolders()
	holders.Wait()
}

func TestProviderHonorsContextCancellation(t *testing.T) {
	provider := NewProvider(ProviderConfig{Latency: time.Hour, FailureRate: 0})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := provider.Send(ctx, Email{To: DeliverableAddress(1)})

	require.ErrorIs(t, err, context.Canceled)
}
