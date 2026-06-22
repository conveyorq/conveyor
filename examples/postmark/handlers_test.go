// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// discardLogger is a logger that drops output, for handlers under test.
var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// fastProvider is a never-failing provider with negligible latency.
func fastProvider() *Provider {
	return NewProvider(ProviderConfig{Latency: time.Nanosecond, FailureRate: 0})
}

func TestSendEmailDeliversDeliverableMail(t *testing.T) {
	provider := fastProvider()

	task := conveyor.NewTask(TaskWelcome, conveyor.JSON(Email{UserID: 1, To: DeliverableAddress(1)}))

	require.NoError(t, sendEmail(provider)(context.Background(), task))
	require.Equal(t, int64(1), provider.Stats().Sent)
}

func TestSendEmailArchivesHardBounce(t *testing.T) {
	provider := fastProvider()

	task := conveyor.NewTask(TaskCampaign, conveyor.JSON(Email{UserID: 1, To: BounceAddress(1)}))

	err := sendEmail(provider)(context.Background(), task)

	require.True(t, conveyor.IsSkipRetry(err), "a hard bounce must be archived, not retried")
	require.ErrorIs(t, err, ErrHardBounce)
}

func TestSendEmailRetriesTransientFailure(t *testing.T) {
	provider := fastProvider()
	provider.SetDown(true)

	task := conveyor.NewTask(TaskWelcome, conveyor.JSON(Email{UserID: 1, To: DeliverableAddress(1)}))

	err := sendEmail(provider)(context.Background(), task)

	require.Error(t, err)
	require.False(t, conveyor.IsSkipRetry(err), "an outage is transient and must be retried")
	require.ErrorIs(t, err, ErrProviderDown)
}

func TestSendEmailArchivesUndecodablePayload(t *testing.T) {
	provider := fastProvider()

	// A non-JSON payload bound into the Email struct can never decode.
	task := conveyor.NewTask(TaskWelcome, conveyor.Bytes([]byte("not json")))

	err := sendEmail(provider)(context.Background(), task)

	require.True(t, conveyor.IsSkipRetry(err), "an undecodable payload must be archived")
}

func TestSendDigestDeliversEveryRecipient(t *testing.T) {
	provider := fastProvider()

	require.NoError(t, sendDigest(provider, discardLogger)(context.Background(), nil))
	require.Equal(t, int64(digestUserCount), provider.Stats().Sent)
}

func TestSendDigestRetriesOnOutage(t *testing.T) {
	provider := fastProvider()
	provider.SetDown(true)

	err := sendDigest(provider, discardLogger)(context.Background(), nil)

	require.Error(t, err)
	require.False(t, conveyor.IsSkipRetry(err))
}
