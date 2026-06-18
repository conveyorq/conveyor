// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command benchmark drives a fixed workload of no-op tasks through an embedded
// Conveyor engine and reports end-to-end throughput and enqueue→complete
// latency. It is the reproducible harness for §6.4; the numbers are workload-
// and hardware-specific. This measures the full client→worker pipeline (enqueue
// RPC, dispatch stream, completion report), which is heavier than the engine's
// internal grain dispatch (~10k tasks/s on this hardware). See README.md.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conveyorq/conveyor/embedded"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

const taskType = "bench:noop"

func main() {
	var (
		tasks       = flag.Int("tasks", 20_000, "number of tasks to drive through the engine")
		concurrency = flag.Int("concurrency", 50, "worker concurrency")
		producers   = flag.Int("producers", 8, "concurrent enqueue goroutines")
		brokerKind  = flag.String("broker", "memory", "broker: memory | postgres")
		dsn         = flag.String("dsn", "", "postgres DSN (required when broker=postgres)")
	)

	flag.Parse()

	if err := run(*tasks, *concurrency, *producers, *brokerKind, *dsn); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark:", err)
		os.Exit(1)
	}
}

func run(tasks, concurrency, producers int, brokerKind, dsn string) error {
	ctx := context.Background()

	broker, err := selectBroker(brokerKind, dsn)
	if err != nil {
		return err
	}

	system, err := embedded.Start(ctx, embedded.Config{Broker: broker})
	if err != nil {
		return fmt.Errorf("starting embedded engine: %w", err)
	}

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = system.Stop(stopCtx)
	}()

	// latencies records each task's enqueue→complete duration; done releases
	// once every task has been processed.
	latencies := make([]time.Duration, 0, tasks)

	var (
		mu        sync.Mutex
		processed atomic.Int64
		done      = make(chan struct{})
	)

	mux := conveyor.NewMux()
	mux.HandleFunc(taskType, func(_ context.Context, task *conveyor.Task) error {
		enqueuedAt := int64(binary.BigEndian.Uint64(task.Payload()))

		mu.Lock()
		latencies = append(latencies, time.Since(time.Unix(0, enqueuedAt)))
		mu.Unlock()

		if processed.Add(1) == int64(tasks) {
			close(done)
		}

		return nil
	})

	worker := system.Worker(
		conveyor.WithConcurrency(concurrency),
		conveyor.WithQueues(map[string]int{"default": 1}),
	)

	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()

	go func() { _ = worker.Run(workerCtx, mux) }()

	// Drive the load from several producers, timing the whole run from the
	// first enqueue to the last completion. A benchmark measures real elapsed
	// time, so it reads the wall clock directly.
	start := time.Now() //nolint:forbidigo // benchmark measures real wall-clock time

	if err := enqueueAll(ctx, system.Client(), tasks, producers); err != nil {
		return err
	}

	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		return fmt.Errorf("timed out after %d/%d tasks", processed.Load(), tasks)
	}

	elapsed := time.Since(start)

	report(brokerKind, tasks, concurrency, producers, elapsed, latencies)

	return nil
}

// selectBroker resolves the broker flag to an embedded broker.
func selectBroker(kind, dsn string) (embedded.Broker, error) {
	switch kind {
	case "memory":
		return embedded.Memory(), nil

	case "postgres":
		if dsn == "" {
			return embedded.Broker{}, fmt.Errorf("broker=postgres requires --dsn")
		}

		return embedded.Postgres(dsn), nil

	default:
		return embedded.Broker{}, fmt.Errorf("unknown broker %q (use memory or postgres)", kind)
	}
}

// enqueueAll commits tasks tasks across producers goroutines; each payload
// carries the enqueue timestamp so the handler can measure latency.
func enqueueAll(ctx context.Context, client *conveyor.Client, tasks, producers int) error {
	var (
		next   atomic.Int64
		group  sync.WaitGroup
		failed atomic.Pointer[error]
	)

	for range producers {
		group.Go(func() {
			for next.Add(1) <= int64(tasks) {
				payload := make([]byte, 8)
				enqueuedAt := time.Now().UnixNano() //nolint:forbidigo // benchmark measures real wall-clock time
				binary.BigEndian.PutUint64(payload, uint64(enqueuedAt))

				if _, err := client.Enqueue(ctx, conveyor.NewTask(taskType, conveyor.Bytes(payload))); err != nil {
					failed.Store(&err)

					return
				}
			}
		})
	}

	group.Wait()

	if err := failed.Load(); err != nil {
		return fmt.Errorf("enqueue: %w", *err)
	}

	return nil
}

// report prints the throughput and latency summary.
func report(brokerKind string, tasks, concurrency, producers int, elapsed time.Duration, latencies []time.Duration) {
	slices.Sort(latencies)

	throughput := float64(tasks) / elapsed.Seconds()

	fmt.Printf("conveyor (broker=%s, concurrency=%d, producers=%d)\n", brokerKind, concurrency, producers)
	fmt.Printf("  tasks       %d\n", tasks)
	fmt.Printf("  wall clock  %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  throughput  %.0f tasks/s\n", throughput)
	fmt.Printf("  latency p50 %s\n", percentile(latencies, 0.50).Round(time.Microsecond))
	fmt.Printf("  latency p95 %s\n", percentile(latencies, 0.95).Round(time.Microsecond))
	fmt.Printf("  latency p99 %s\n", percentile(latencies, 0.99).Round(time.Microsecond))
}

// percentile returns the p-quantile of a sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int(p * float64(len(sorted)))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}
