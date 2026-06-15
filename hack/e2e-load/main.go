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

// Command e2e-load is the workload driver for the kind-based rolling-restart
// test. It is both producer and worker against a Conveyor cluster reached
// through its load-balanced API Service: it enqueues a fixed number of tasks
// spread over time while its own worker processes them, then waits for every
// task to complete. Run concurrently with a `kubectl rollout restart` of the
// server StatefulSet, it proves a client keeps making progress and loses
// nothing while the servers are replaced one at a time. It exits non-zero if
// any produced task fails to complete.
//
// Configuration comes from CONVEYOR_ADDR (the API Service URL) and
// CONVEYOR_TOKEN, plus flags for the workload shape.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"sync/atomic"
	"time"

	conveyor "github.com/conveyorq/conveyor/sdk"
)

// defaultAddr is the API address used when CONVEYOR_ADDR is unset.
const defaultAddr = "http://localhost:8080"

// loadQueue and loadTaskType name the queue served and the task enqueued.
const (
	loadQueue    = "default"
	loadTaskType = "loadtest"
)

// workerConcurrency is the number of tasks the driver processes at once.
const workerConcurrency = 20

// Enqueue retry bounds: a pod restart can briefly refuse a connection, so each
// enqueue retries rather than dropping load.
const (
	enqueueAttempts   = 30
	enqueueRetryDelay = 500 * time.Millisecond
)

// completionPollInterval is how often the drain wait re-checks progress.
const completionPollInterval = time.Second

func main() {
	total := flag.Int("total", 300, "number of tasks to produce")
	produceInterval := flag.Duration("interval", 400*time.Millisecond, "delay between enqueues, spreading load across the rollout")
	handlerDelay := flag.Duration("handler-delay", 50*time.Millisecond, "simulated work performed per task")
	drainTimeout := flag.Duration("drain-timeout", 3*time.Minute, "how long to wait for every produced task to complete")

	flag.Parse()

	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	token := os.Getenv("CONVEYOR_TOKEN")

	var completed atomic.Int64

	worker, err := conveyor.NewWorker(addr,
		conveyor.WithToken(token),
		conveyor.WithQueues(map[string]int{loadQueue: 1}),
		conveyor.WithConcurrency(workerConcurrency),
	)
	if err != nil {
		log.Fatalf("load: worker: %v", err)
	}

	mux := conveyor.NewMux()
	mux.HandleFunc(loadTaskType, func(ctx context.Context, _ *conveyor.Task) error {
		select {
		case <-time.After(*handlerDelay):
		case <-ctx.Done():
			return ctx.Err()
		}

		completed.Add(1)

		return nil
	})

	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	go func() {
		if runErr := worker.Run(workerCtx, mux); runErr != nil && workerCtx.Err() == nil {
			log.Printf("load: worker stopped early: %v", runErr)
		}
	}()

	client, err := conveyor.NewClient(addr, conveyor.WithToken(token))
	if err != nil {
		log.Fatalf("load: client: %v", err)
	}

	produced := produce(client, *total, *produceInterval)
	log.Printf("load: produced %d tasks; waiting for completion", produced)

	done := waitForCompletion(&completed, produced, *drainTimeout)
	stopWorker()

	log.Printf("load: produced=%d completed=%d", produced, completed.Load())

	if !done {
		log.Printf("load: FAIL: only %d of %d tasks completed across the rolling restart", completed.Load(), produced)
		os.Exit(1)
	}

	log.Printf("load: PASS: every task completed across the rolling restart")
}

// produce enqueues total tasks, one per interval, and returns how many were
// durably accepted. Spreading enqueues over time keeps load flowing for the
// whole rollout rather than draining before it starts.
func produce(client *conveyor.Client, total int, interval time.Duration) int {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	produced := 0

	for range total {
		<-ticker.C

		if enqueue(client) {
			produced++
		}
	}

	return produced
}

// enqueue commits one task, retrying transient failures while a server pod is
// restarting. It reports whether the task was durably accepted.
func enqueue(client *conveyor.Client) bool {
	for attempt := range enqueueAttempts {
		_, err := client.Enqueue(context.Background(),
			conveyor.NewTask(loadTaskType, conveyor.Bytes(nil)),
			conveyor.Queue(loadQueue), conveyor.Retention(time.Hour))
		if err == nil {
			return true
		}

		if attempt == enqueueAttempts-1 {
			log.Printf("load: enqueue gave up after %d attempts: %v", enqueueAttempts, err)

			return false
		}

		time.Sleep(enqueueRetryDelay)
	}

	return false
}

// waitForCompletion blocks until completed reaches produced or the timeout
// elapses, returning whether every produced task finished.
func waitForCompletion(completed *atomic.Int64, produced int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(completionPollInterval)
	defer ticker.Stop()

	for {
		if int(completed.Load()) >= produced {
			return true
		}

		select {
		case <-deadline.C:
			return int(completed.Load()) >= produced

		case <-ticker.C:
		}
	}
}
