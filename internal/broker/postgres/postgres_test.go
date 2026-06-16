// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/brokertest"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// postgresImage pins the database version under test.
const postgresImage = "postgres:16-alpine"

// containerDSN points at the testcontainers-managed Postgres started in
// TestMain.
var containerDSN string

// TestMain starts one throwaway Postgres container shared by the whole
// package run.
func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("conveyor"),
		tcpostgres.WithUsername("conveyor"),
		tcpostgres.WithPassword("conveyor"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}

	containerDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("postgres connection string: %v", err)
	}

	code := m.Run()

	if err = container.Terminate(ctx); err != nil {
		log.Printf("terminate postgres container: %v", err)
	}

	os.Exit(code)
}

// testDSN returns the database under test.
func testDSN() string {
	return containerDSN
}

// truncateAll empties every broker table so a test starts from scratch.
// The tables exist because New ran the migrations beforehand.
func truncateAll(tb testing.TB) {
	tb.Helper()

	ctx := context.Background()

	connection, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		tb.Fatalf("connect for truncate: %v", err)
	}

	defer func() { _ = connection.Close(ctx) }()

	if _, err = connection.Exec(ctx, "TRUNCATE conveyor_tasks, conveyor_queue_state, conveyor_cron_entries"); err != nil {
		tb.Fatalf("truncate: %v", err)
	}
}

// newBroker connects a fresh broker and truncates all tables so every test
// starts empty.
func newBroker(t *testing.T, timeSource clock.Clock) broker.Broker {
	t.Helper()

	instance, err := New(context.Background(), testDSN(), timeSource)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() { _ = instance.Close() })
	truncateAll(t)

	return instance
}

func TestConformance(t *testing.T) {
	brokertest.Run(t, newBroker)
}

// BenchmarkLease100 measures the broker performance floor: leasing a
// 100-task batch must stay at or under 2ms against local Postgres.
func BenchmarkLease100(b *testing.B) {
	const batchSize = 100

	timeSource := clock.System()

	instance, err := New(context.Background(), testDSN(), timeSource)
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	defer func() { _ = instance.Close() }()
	truncateAll(b)

	ctx := context.Background()
	now := timeSource.Now()

	for sequence := range (b.N + 1) * batchSize {
		task := &conveyorv1.TaskEnvelope{
			Id:          fmt.Sprintf("bench-%010d", sequence),
			Queue:       "bench",
			Type:        "bench:task",
			Payload:     []byte(`{"n":1}`),
			ContentType: "application/json",
			Options:     &conveyorv1.TaskOptions{MaxRetry: 25},
			EnqueuedAt:  timestamppb.New(now),
		}

		if err = instance.Enqueue(ctx, task); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}

	b.ResetTimer()

	for iteration := range b.N {
		leased, err := instance.Lease(ctx, "bench", batchSize, time.Minute, fmt.Sprintf("bench-lease-%d", iteration))
		if err != nil {
			b.Fatalf("Lease: %v", err)
		}

		if len(leased) != batchSize {
			b.Fatalf("leased %d, want %d", len(leased), batchSize)
		}
	}
}
