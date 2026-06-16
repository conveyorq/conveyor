// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command worker is a minimal Conveyor worker process: it connects to a
// conveyord node, declares the queues it serves, and processes welcome
// emails until interrupted.
//
// Usage:
//
//	conveyord --dev                     # terminal 1: the server
//	go run ./examples/standalone/worker # terminal 2: this worker
//	go run ./examples/standalone/client # terminal 3: enqueue some work
//
// Configuration comes from CONVEYOR_ADDR (default http://localhost:8080)
// and CONVEYOR_TOKEN (empty for --dev servers).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	conveyor "github.com/conveyorq/conveyor/sdk"
)

// defaultAddr is the conveyord API address used when CONVEYOR_ADDR is unset.
const defaultAddr = "http://localhost:8080"

// sendDuration simulates the latency of an SMTP round trip.
const sendDuration = 200 * time.Millisecond

// WelcomeEmail is the payload of an email:welcome task.
type WelcomeEmail struct {
	// UserID identifies the recipient.
	UserID int `json:"user_id"`
}

func main() {
	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	worker, err := conveyor.NewWorker(addr,
		conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")),
		conveyor.WithQueues(map[string]int{"default": 1}),
		conveyor.WithConcurrency(10),
	)
	if err != nil {
		log.Fatalf("worker: %v", err)
	}

	mux := conveyor.NewMux()

	mux.HandleFunc("email:welcome", func(ctx context.Context, task *conveyor.Task) error {
		var email WelcomeEmail
		if err := task.Bind(&email); err != nil {
			// A payload that cannot decode now never will: archive it.
			return conveyor.SkipRetry(err)
		}

		// Simulate the SMTP round trip, honoring cancellation.
		select {
		case <-time.After(sendDuration):
		case <-ctx.Done():
			return ctx.Err()
		}

		fmt.Printf("sent welcome email to user %d (task %s)\n", email.UserID, task.ID())

		return nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker connected to %q; processing queue %q", addr, "default") //nolint:gosec // addr is operator-supplied config, quoted for log safety

	if err := worker.Run(ctx, mux); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
