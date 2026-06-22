// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command producer simulates the customer apps hitting the platform's API: it
// enqueues a continuous, transactional-heavy mix of notification tasks against
// a conveyord node until interrupted. Pair it with one or more worker processes
// and watch the queues flow in the dashboard.
//
// Usage:
//
//	go run ./examples/postmark/cmd/producer [--interval 400ms]
//
// Configuration comes from CONVEYOR_ADDR (default http://localhost:8080) and
// CONVEYOR_TOKEN (empty for --dev servers).
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/conveyorq/conveyor/examples/postmark"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// defaultAddr is the conveyord API address used when CONVEYOR_ADDR is unset.
const defaultAddr = "http://localhost:8080"

func main() {
	interval := flag.Duration("interval", 400*time.Millisecond, "delay between enqueued product actions")
	flag.Parse()

	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	client, err := conveyor.NewClient(addr, conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")))
	if err != nil {
		log.Fatalf("postmark: client: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("postmark producer started", "addr", addr, "interval", interval.String())

	if err := postmark.NewProducer(client, logger).Run(ctx, *interval); err != nil && ctx.Err() == nil {
		log.Fatalf("postmark: producer: %v", err)
	}
}
