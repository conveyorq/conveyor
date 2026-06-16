// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command embedded runs a complete Conveyor system inside one process —
// server, worker, and client — with zero external infrastructure. The
// handler and enqueue code is identical to the standalone example; only
// the constructors differ, which is the whole migration path between the
// two modes.
//
// Usage:
//
//	go run ./examples/embedded
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/conveyorq/conveyor/embedded"
	conveyor "github.com/conveyorq/conveyor/sdk"
)

// taskCount is how many welcome emails this run enqueues and awaits.
const taskCount = 10

// runTimeout bounds the whole demonstration.
const runTimeout = 30 * time.Second

// pollInterval is the cadence of completion polling.
const pollInterval = 50 * time.Millisecond

// WelcomeEmail is the payload of an email:welcome task.
type WelcomeEmail struct {
	// UserID identifies the recipient.
	UserID int `json:"user_id"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{Broker: embedded.Memory()})
	if err != nil {
		log.Fatalf("embedded: %v", err)
	}

	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), runTimeout)
		defer stopCancel()

		if err := system.Stop(stopCtx); err != nil {
			log.Printf("embedded: stopping: %v", err)
		}
	}()

	var processed atomic.Int64

	mux := conveyor.NewMux()

	mux.HandleFunc("email:welcome", func(_ context.Context, task *conveyor.Task) error {
		var email WelcomeEmail
		if err := task.Bind(&email); err != nil {
			return conveyor.SkipRetry(err)
		}

		fmt.Printf("sent welcome email to user %d (task %s)\n", email.UserID, task.ID())
		processed.Add(1)

		return nil
	})

	worker := system.Worker(
		conveyor.WithQueues(map[string]int{"default": 1}),
		conveyor.WithConcurrency(8),
	)

	go func() {
		if err := worker.Run(ctx, mux); err != nil {
			log.Printf("worker: %v", err)
		}
	}()

	client := system.Client()

	for userID := range taskCount {
		info, err := client.Enqueue(ctx,
			conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: userID})),
			conveyor.Retention(time.Hour),
		)
		if err != nil {
			log.Fatalf("enqueue user %d: %v", userID, err)
		}

		fmt.Printf("enqueued %s for user %d (%s)\n", info.ID, userID, info.State)
	}

	for processed.Load() < taskCount {
		select {
		case <-ctx.Done():
			log.Fatalf("timed out: processed %d of %d tasks", processed.Load(), taskCount)

		case <-time.After(pollInterval):
		}
	}

	fmt.Printf("processed all %d tasks in-process — no broker, no server, no network\n", taskCount)
}
