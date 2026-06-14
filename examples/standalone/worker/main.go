// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
