// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// The asynq-user porting test: the SDK sample promised in the public docs
// must compile and run as written. This file is an external test package
// on purpose — it consumes the SDK exactly like user code does, aliased
// import and all. Only the server address, token plumbing, and durations
// differ from the published sample; every API call is verbatim.
package conveyor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/embedded"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// portingTestTimeout bounds the whole round trip.
const portingTestTimeout = 60 * time.Second

// WelcomeEmail is the sample's task payload.
type WelcomeEmail struct {
	// UserID identifies the recipient.
	UserID int `json:"user_id"`
}

func TestPortingSampleCompilesAndRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), portingTestTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), portingTestTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	serverURL := "http://" + system.Addr()
	token := ""

	sent := make(chan WelcomeEmail, 1)
	sendEmail := func(_ context.Context, email WelcomeEmail) error {
		sent <- email

		return nil
	}

	middlewareRan := make(chan string, 1)
	loggingMiddleware := func(next conveyor.HandlerFunc) conveyor.HandlerFunc {
		return func(ctx context.Context, task *conveyor.Task) error {
			select {
			case middlewareRan <- task.Type():
			default:
			}

			return next(ctx, task)
		}
	}

	// ---- enqueue side ----
	client, err := conveyor.NewClient(serverURL,
		conveyor.WithToken(token),
	)
	require.NoError(t, err)

	info, err := client.Enqueue(ctx,
		conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: 42})),
		conveyor.Queue("critical"),
		conveyor.MaxRetry(10),
		conveyor.ProcessIn(5*time.Millisecond),
		conveyor.Unique(24*time.Hour),
		conveyor.Priority(7),
		conveyor.Retention(48*time.Hour),
	)
	require.NoError(t, err)
	require.NotEmpty(t, info.ID)

	// ---- worker side ----
	w, err := conveyor.NewWorker(serverURL,
		conveyor.WithToken(token),
		conveyor.WithQueues(map[string]int{"critical": 6, "default": 3, "low": 1}),
		conveyor.WithConcurrency(20),
	)
	require.NoError(t, err)

	mux := conveyor.NewMux()
	mux.Use(loggingMiddleware)
	mux.HandleFunc("email:welcome", func(ctx context.Context, t *conveyor.Task) error {
		var p WelcomeEmail
		if err := t.Bind(&p); err != nil {
			return conveyor.SkipRetry(err)
		}

		return sendEmail(ctx, p)
	})

	runCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()

	runDone := make(chan error, 1)

	go func() { runDone <- w.Run(runCtx, mux) }()

	select {
	case email := <-sent:
		require.Equal(t, 42, email.UserID)

	case <-ctx.Done():
		t.Fatal("the sample task was never processed")
	}

	require.Equal(t, "email:welcome", <-middlewareRan)

	stopWorker()
	require.NoError(t, <-runDone)
}
