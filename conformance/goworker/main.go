// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command goworker is the Go conformance worker for the cross-SDK protocol
// suite (see the conformance package). It is not a usage example: the harness
// spawns it, drives it through one scenario chosen by env vars, and asserts
// protocol behavior by observing the server. Kept deliberately minimal and
// parallel to the TypeScript and Python conformance workers.
//
//	SCENARIO       "sleep" (default): run handlers that sleep SLEEP_MS then succeed.
//	               "bad-hello": construct a worker with an invalid queue weight,
//	               which must be rejected locally (exit 1), never a reconnect loop.
//	SLEEP_MS       handler sleep in milliseconds (sleep scenario).
//	QUEUE          queue to serve (default "default").
//	CONVEYOR_ADDR  server base URL.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

func main() {
	addr := envOr("CONVEYOR_ADDR", "http://localhost:8080")
	scenario := envOr("SCENARIO", "sleep")
	queue := envOr("QUEUE", "default")

	sleepMS, _ := strconv.Atoi(os.Getenv("SLEEP_MS"))
	sleepFor := time.Duration(sleepMS) * time.Millisecond

	if scenario == "bad-hello" {
		// A zero weight is invalid; NewWorker must reject it locally.
		_, err := conveyor.NewWorker(addr,
			conveyor.WithQueues(map[string]int{queue: 0}), conveyor.WithConcurrency(1))
		if err != nil {
			os.Exit(1)
		}

		log.Println("bad-hello: an invalid queue weight was accepted")
		os.Exit(2)
	}

	worker, err := conveyor.NewWorker(addr,
		conveyor.WithQueues(map[string]int{queue: 1}), conveyor.WithConcurrency(8))
	if err != nil {
		log.Fatalf("worker: %v", err)
	}

	mux := conveyor.NewMux()

	// Sleep the full duration, ignoring cancellation on purpose: the suite checks
	// that a handler outliving the lease keeps its lease via heartbeats, and that
	// a drain waits for it rather than losing it.
	mux.HandleFunc("conformance:task", func(_ context.Context, _ *conveyor.Task) error {
		time.Sleep(sleepFor)

		return nil
	})

	mux.HandleBatch("conformance:batch", func(_ context.Context, _ []*conveyor.Task) error {
		time.Sleep(sleepFor)

		return nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := worker.Run(ctx, mux); err != nil {
		log.Fatalf("worker run: %v", err)
	}
}

// envOr returns the environment variable value, or fallback when it is unset.
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
