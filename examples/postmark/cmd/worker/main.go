// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command worker is a Postmark worker process: it connects to a conveyord node,
// serves the platform's three queues, and delivers mail through the simulated
// provider until interrupted. Run several against a cluster to see the queue
// weights split work across worker sessions.
//
// There are two ways to drive the simulated provider into an outage so the
// per-task-type circuit breaker trips and recovers:
//
//   - Send SIGUSR1 to toggle an outage on or off (handy when running locally).
//   - Set --outage-every and --outage-for to cycle outages on a schedule, which
//     is how the in-cluster pods demonstrate the breaker without a shell.
//
// Usage:
//
//	go run ./examples/postmark/cmd/worker [--outage-every 4m --outage-for 40s]
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

// workerConcurrency is the worker's total execution slots, set above the
// provider's connection limit so back-pressure is visible.
const workerConcurrency = 20

func main() {
	outageEvery := flag.Duration("outage-every", 0, "cycle a simulated provider outage this often (0 disables)")
	outageFor := flag.Duration("outage-for", 0, "how long each scheduled outage lasts")
	flag.Parse()

	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	worker, err := conveyor.NewWorker(addr,
		conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")),
		conveyor.WithQueues(postmark.WorkerQueues()),
		conveyor.WithConcurrency(workerConcurrency),
	)
	if err != nil {
		log.Fatalf("postmark: worker: %v", err)
	}

	provider := postmark.NewProvider(postmark.DefaultProviderConfig())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go toggleOutageOnSignal(ctx, provider, logger)

	if *outageEvery > 0 && *outageFor > 0 {
		go cycleOutages(ctx, provider, *outageEvery, *outageFor, logger)
	}

	logger.Info("postmark worker connected", "addr", addr, "queues", postmark.WorkerQueues())

	if err := worker.Run(ctx, postmark.NewMux(provider, logger)); err != nil {
		log.Fatalf("postmark: worker: %v", err)
	}
}

// toggleOutageOnSignal flips the simulated provider between healthy and down
// each time the process receives SIGUSR1, until ctx is canceled.
func toggleOutageOnSignal(ctx context.Context, provider *postmark.Provider, logger *slog.Logger) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1)

	defer signal.Stop(signals)

	for {
		select {
		case <-ctx.Done():
			return

		case <-signals:
			down := !provider.Down()
			provider.SetDown(down)
			logger.Warn("provider outage toggled", "down", down)
		}
	}
}

// cycleOutages drives the provider down for outageFor, healthy until the next
// period, and so on, until ctx is canceled. It lets the in-cluster deployment
// demonstrate the circuit breaker tripping and recovering on a schedule.
func cycleOutages(ctx context.Context, provider *postmark.Provider, every, duration time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			provider.SetDown(true)
			logger.Warn("simulated provider outage started", "duration", duration.String())

			select {
			case <-ctx.Done():
				return

			case <-time.After(duration):
				provider.SetDown(false)
				logger.Info("simulated provider outage ended")
			}
		}
	}
}
