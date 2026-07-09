// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/brokertest"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// errBoom is the database failure injected into mocked query paths.
var errBoom = errors.New("boom")

// mockStart is the fixed wall clock the mocked broker reads from.
var mockStart = time.Unix(1_700_000_000, 0).UTC()

// newMockBroker returns a broker backed by a pgxmock pool, letting tests drive
// the query and error paths the live-database conformance suite cannot reach.
func newMockBroker(t *testing.T) (*Broker, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)

	t.Cleanup(mock.Close)

	return &Broker{pool: mock, clock: clock.NewFake(mockStart)}, mock
}

// anyArgs returns n argument matchers, since pgxmock validates the bind-argument
// count of every expected statement.
func anyArgs(n int) []any {
	args := make([]any, n)

	for i := range args {
		args[i] = pgxmock.AnyArg()
	}

	return args
}

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

	if _, err = connection.Exec(ctx, "TRUNCATE conveyor_tasks, conveyor_task_deps, conveyor_queue_state, conveyor_cron_entries, conveyor_rate_limits, conveyor_concurrency_limits, conveyor_group_configs"); err != nil {
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

// TestPoolErrorPropagation drives a database failure into the first statement
// of each method and asserts it surfaces, covering the post-call error guards
// the live conformance suite never trips.
func TestPoolErrorPropagation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		exec bool
		args int
		call func(*Broker) error
	}{
		{"Enqueue", true, 18, func(b *Broker) error {
			return b.Enqueue(ctx, &conveyorv1.TaskEnvelope{Id: "t", Queue: "q", Type: "demo", Options: &conveyorv1.TaskOptions{}})
		}},
		{"Lease", false, 5, func(b *Broker) error { _, err := b.Lease(ctx, "q", 1, time.Second, "L"); return err }},
		{"LeaseGroup", false, 6, func(b *Broker) error { _, err := b.LeaseGroup(ctx, "q", "g", 1, time.Second, "L"); return err }},
		{"GroupStats", false, 0, func(b *Broker) error { _, err := b.GroupStats(ctx); return err }},
		{"ExtendLease", true, 4, func(b *Broker) error { return b.ExtendLease(ctx, "t", "L", time.Second) }},
		{"Ack", false, 4, func(b *Broker) error { return b.Ack(ctx, "t", "L", nil) }},
		{"Fail", false, 5, func(b *Broker) error { return b.Fail(ctx, "t", "L", "boom", mockStart) }},
		{"Release", false, 3, func(b *Broker) error { return b.Release(ctx, "t", "L") }},
		{"ArchiveLeased", false, 4, func(b *Broker) error { return b.Archive(ctx, "t", "L", "boom") }},
		{"ArchiveAny", false, 3, func(b *Broker) error { return b.Archive(ctx, "t", "", "boom") }},
		{"ReapExpiredLeases", false, 3, func(b *Broker) error { _, err := b.ReapExpiredLeases(ctx, 1); return err }},
		{"PromoteScheduled", false, 2, func(b *Broker) error { _, err := b.PromoteScheduled(ctx, 1); return err }},
		{"ResolveDependents", false, 1, func(b *Broker) error { _, err := b.ResolveDependents(ctx, "t"); return err }},
		{"PendingCount", false, 1, func(b *Broker) error { _, err := b.PendingCount(ctx); return err }},
		{"QueueStats", false, 0, func(b *Broker) error { _, err := b.QueueStats(ctx); return err }},
		{"SetQueuePaused", true, 3, func(b *Broker) error { return b.SetQueuePaused(ctx, "q", true) }},
		{"QueuePaused", false, 1, func(b *Broker) error { _, err := b.QueuePaused(ctx, "q"); return err }},
		{"SetQueueRateLimit", true, 4, func(b *Broker) error { return b.SetQueueRateLimit(ctx, "q", 1, 1) }},
		{"DeleteQueueRateLimit", true, 1, func(b *Broker) error { return b.DeleteQueueRateLimit(ctx, "q") }},
		{"QueueRateLimit", false, 1, func(b *Broker) error { _, _, err := b.QueueRateLimit(ctx, "q"); return err }},
		{"QueueRateLimits", false, 0, func(b *Broker) error { _, err := b.QueueRateLimits(ctx); return err }},
		{"SetQueueConcurrencyLimit", true, 3, func(b *Broker) error { return b.SetQueueConcurrencyLimit(ctx, "q", 1) }},
		{"DeleteQueueConcurrencyLimit", true, 1, func(b *Broker) error { return b.DeleteQueueConcurrencyLimit(ctx, "q") }},
		{"QueueConcurrencyLimit", false, 1, func(b *Broker) error { _, _, err := b.QueueConcurrencyLimit(ctx, "q"); return err }},
		{"QueueConcurrencyLimits", false, 0, func(b *Broker) error { _, err := b.QueueConcurrencyLimits(ctx); return err }},
		{"SetGroupConfig", true, 6, func(b *Broker) error { return b.SetGroupConfig(ctx, "q", "g", 1, time.Second, time.Second) }},
		{"DeleteGroupConfig", true, 2, func(b *Broker) error { return b.DeleteGroupConfig(ctx, "q", "g") }},
		{"GroupConfigs", false, 0, func(b *Broker) error { _, err := b.GroupConfigs(ctx); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, mock := newMockBroker(t)

			if tc.exec {
				mock.ExpectExec("").WithArgs(anyArgs(tc.args)...).WillReturnError(errBoom)
			} else {
				mock.ExpectQuery("").WithArgs(anyArgs(tc.args)...).WillReturnError(errBoom)
			}

			require.ErrorIs(t, tc.call(b), errBoom)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestLeaseLostOnMissingRow covers the not-found branch of the lease-scoped
// mutations: a vanished or re-leased row makes the RETURNING query match
// nothing, which maps to ErrLeaseLost.
func TestLeaseLostOnMissingRow(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		args int
		call func(*Broker) error
	}{
		{"Ack", 4, func(b *Broker) error { return b.Ack(ctx, "t", "L", nil) }},
		{"Fail", 5, func(b *Broker) error { return b.Fail(ctx, "t", "L", "boom", mockStart) }},
		{"Release", 3, func(b *Broker) error { return b.Release(ctx, "t", "L") }},
		{"ArchiveLeased", 4, func(b *Broker) error { return b.Archive(ctx, "t", "L", "boom") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, mock := newMockBroker(t)
			mock.ExpectQuery("").WithArgs(anyArgs(tc.args)...).WillReturnError(pgx.ErrNoRows)

			require.ErrorIs(t, tc.call(b), broker.ErrLeaseLost)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestRowIterationErrors covers the per-row scan failure and the post-loop
// rows.Err() guard of every method that streams a result set.
func TestRowIterationErrors(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		args int
		call func(*Broker) error
	}{
		{"GroupStats", 0, func(b *Broker) error { _, err := b.GroupStats(ctx); return err }},
		{"ReapExpiredLeases", 3, func(b *Broker) error { _, err := b.ReapExpiredLeases(ctx, 1); return err }},
		{"PromoteScheduled", 2, func(b *Broker) error { _, err := b.PromoteScheduled(ctx, 1); return err }},
		{"PendingCount", 1, func(b *Broker) error { _, err := b.PendingCount(ctx); return err }},
		{"QueueStats", 0, func(b *Broker) error { _, err := b.QueueStats(ctx); return err }},
		{"QueueRateLimits", 0, func(b *Broker) error { _, err := b.QueueRateLimits(ctx); return err }},
		{"QueueConcurrencyLimits", 0, func(b *Broker) error { _, err := b.QueueConcurrencyLimits(ctx); return err }},
		{"GroupConfigs", 0, func(b *Broker) error { _, err := b.GroupConfigs(ctx); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name+"/scan", func(t *testing.T) {
			b, mock := newMockBroker(t)
			// A single-column row cannot satisfy the multi-column Scan.
			mock.ExpectQuery("").WithArgs(anyArgs(tc.args)...).WillReturnRows(pgxmock.NewRows([]string{"only"}).AddRow("x"))

			require.Error(t, tc.call(b))
			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run(tc.name+"/rows", func(t *testing.T) {
			b, mock := newMockBroker(t)
			// An empty result whose close surfaces an error reaches the
			// post-loop rows.Err() guard without first tripping a scan.
			mock.ExpectQuery("").WithArgs(anyArgs(tc.args)...).WillReturnRows(pgxmock.NewRows([]string{"only"}).CloseError(errBoom))

			require.ErrorIs(t, tc.call(b), errBoom)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// badPayload is a byte slice that is not a valid TaskEnvelope, forcing the
// envelope-unmarshal guard on the read paths.
var badPayload = []byte{0xff, 0xff}

// taskRowColumns are the columns the task read paths scan.
var taskRowColumns = []string{"payload", "state", "retried", "last_error", "started_at", "completed_at"}

func TestExtendLeaseLostWhenNoRowUpdated(t *testing.T) {
	b, mock := newMockBroker(t)
	mock.ExpectExec("").WithArgs(anyArgs(4)...).WillReturnResult(pgxmock.NewResult("UPDATE", 0))

	require.ErrorIs(t, b.ExtendLease(context.Background(), "t", "L", time.Second), broker.ErrLeaseLost)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExplainMissOutcomes exercises every branch of the shared explainMiss
// helper through CancelTask: a guarded mutation that matches no row asks the
// database whether the task is missing or merely in an ineligible state.
func TestExplainMissOutcomes(t *testing.T) {
	ctx := context.Background()

	t.Run("primary query error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.CancelTask(ctx, "t"), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("task not found", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnError(pgx.ErrNoRows)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(pgx.ErrNoRows)

		require.ErrorIs(t, b.CancelTask(ctx, "t"), broker.ErrTaskNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("lookup error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnError(pgx.ErrNoRows)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.CancelTask(ctx, "t"), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("invalid state", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnError(pgx.ErrNoRows)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(pgxmock.NewRows([]string{"state"}).AddRow(int16(5)))

		require.ErrorIs(t, b.CancelTask(ctx, "t"), broker.ErrInvalidState)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestWaitingMutationsExplainMiss covers the not-found routing of the remaining
// single-row waiting-state mutations.
func TestWaitingMutationsExplainMiss(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		args int
		call func(*Broker) error
	}{
		{"ArchiveTask", 2, func(b *Broker) error { return b.ArchiveTask(ctx, "t") }},
		{"RunTaskNow", 2, func(b *Broker) error { return b.RunTaskNow(ctx, "t") }},
		{"ArchiveAny", 3, func(b *Broker) error { return b.Archive(ctx, "t", "", "boom") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, mock := newMockBroker(t)
			mock.ExpectQuery("").WithArgs(anyArgs(tc.args)...).WillReturnError(pgx.ErrNoRows)
			mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(pgx.ErrNoRows)

			require.ErrorIs(t, tc.call(b), broker.ErrTaskNotFound)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeleteTaskPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("exec error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.DeleteTask(ctx, "t"), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("missing task", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnResult(pgxmock.NewResult("DELETE", 0))
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(pgx.ErrNoRows)

		require.ErrorIs(t, b.DeleteTask(ctx, "t"), broker.ErrTaskNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("drop edges error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.DeleteTask(ctx, "t"), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestGetTaskPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("query error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		_, _, err := b.GetTask(ctx, "t")
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("not found", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(pgx.ErrNoRows)

		_, _, err := b.GetTask(ctx, "t")
		require.ErrorIs(t, err, broker.ErrTaskNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("unmarshal error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(
			pgxmock.NewRows(taskRowColumns).AddRow(badPayload, int16(1), int32(0), "", nil, nil))

		_, _, err := b.GetTask(ctx, "t")
		require.Error(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestListTasksPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("query error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		_, err := b.ListTasks(ctx, broker.TaskQuery{})
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("scan error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(pgxmock.NewRows([]string{"only"}).AddRow("x"))

		_, err := b.ListTasks(ctx, broker.TaskQuery{})
		require.Error(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("unmarshal error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(
			pgxmock.NewRows([]string{"payload", "retried", "last_error", "state", "started_at", "completed_at"}).
				AddRow(badPayload, int32(0), "", int16(1), nil, nil))

		_, err := b.ListTasks(ctx, broker.TaskQuery{})
		require.Error(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("rows error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(pgxmock.NewRows([]string{"only"}).CloseError(errBoom))

		_, err := b.ListTasks(ctx, broker.TaskQuery{})
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestCronExecPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("upsert error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(10)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.UpsertCronEntry(ctx, &broker.CronEntry{ID: "c"}), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("update next run error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.UpdateCronNextRun(ctx, "c", time.Time{}, mockStart), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("set paused error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.SetCronPaused(ctx, "c", true), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("set paused missing", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		require.ErrorIs(t, b.SetCronPaused(ctx, "c", true), broker.ErrTaskNotFound)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("delete error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		require.ErrorIs(t, b.DeleteCronEntry(ctx, "c"), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestCronListPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("list query error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WillReturnError(errBoom)

		_, err := b.ListCronEntries(ctx)
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("list scan error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WillReturnRows(pgxmock.NewRows([]string{"only"}).AddRow("x"))

		_, err := b.ListCronEntries(ctx)
		require.Error(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("list rows error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WillReturnRows(pgxmock.NewRows([]string{"only"}).CloseError(errBoom))

		_, err := b.ListCronEntries(ctx)
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("due query error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnError(errBoom)

		_, err := b.ListDueCronEntries(ctx, mockStart)
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestTransactionBeginErrors covers the begin-transaction guard of the
// transactional paths.
func TestTransactionBeginErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("enqueue with edges", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin().WillReturnError(errBoom)

		task := &conveyorv1.TaskEnvelope{Id: "t", Queue: "q", Type: "demo", Options: &conveyorv1.TaskOptions{
			DependsOn: []*conveyorv1.TaskDependency{{TaskId: "dep"}},
		}}
		require.ErrorIs(t, b.Enqueue(ctx, task), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("promote ready dependents", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin().WillReturnError(errBoom)

		_, err := b.PromoteReadyDependents(ctx, 1)
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("resolve dependents", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectBegin().WillReturnError(errBoom)

		_, err := b.ResolveDependents(ctx, "t")
		require.ErrorIs(t, err, errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestEnqueueCommitUniqueViolation covers the single-task transactional path's
// commit-time unique-violation branch, which maps to ErrDuplicateTask.
func TestEnqueueCommitUniqueViolation(t *testing.T) {
	b, mock := newMockBroker(t)

	task := &conveyorv1.TaskEnvelope{Id: "t", Queue: "q", Type: "demo", Options: &conveyorv1.TaskOptions{UniqueKey: "k"}}

	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("UPDATE", 0))
	mock.ExpectExec("").WithArgs(anyArgs(18)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit().WillReturnError(&pgconn.PgError{Code: uniqueViolationCode, ConstraintName: uniqueIndexName})
	mock.ExpectRollback()

	require.ErrorIs(t, b.Enqueue(context.Background(), task), broker.ErrDuplicateTask)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestEnqueueBatchMock covers the error and rollback branches of the atomic
// EnqueueBatch path that the live-database conformance suite does not reach.
func TestEnqueueBatchMock(t *testing.T) {
	ctx := context.Background()

	simple := func(id string) *conveyorv1.TaskEnvelope {
		return &conveyorv1.TaskEnvelope{Id: id, Queue: "q", Type: "demo", Options: &conveyorv1.TaskOptions{}}
	}

	t.Run("empty batch is a no-op", func(t *testing.T) {
		b, mock := newMockBroker(t)

		require.NoError(t, b.EnqueueBatch(ctx, nil))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("grouped schedule rejected before any write", func(t *testing.T) {
		b, mock := newMockBroker(t)

		task := simple("t")
		task.Options.Group = "g"
		task.Options.ProcessAt = timestamppb.New(mockStart.Add(time.Hour))

		err := b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{task})
		require.ErrorIs(t, err, broker.ErrGroupedSchedule)

		var batchErr *broker.BatchError
		require.ErrorAs(t, err, &batchErr)
		require.Equal(t, 0, batchErr.Index)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("begin error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin().WillReturnError(errBoom)

		require.ErrorIs(t, b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{simple("t")}), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("insert error rolls back the whole batch", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin()
		mock.ExpectExec("").WithArgs(anyArgs(18)...).WillReturnError(errBoom)
		mock.ExpectRollback()

		err := b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{simple("a"), simple("b")})
		require.ErrorIs(t, err, errBoom)

		var batchErr *broker.BatchError
		require.ErrorAs(t, err, &batchErr)
		require.Equal(t, 0, batchErr.Index)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("commit error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin()
		mock.ExpectExec("").WithArgs(anyArgs(18)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		mock.ExpectCommit().WillReturnError(errBoom)
		mock.ExpectRollback()

		require.ErrorIs(t, b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{simple("a")}), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	unique := func(id string) *conveyorv1.TaskEnvelope {
		task := simple(id)
		task.Options.UniqueKey = "claimed"

		return task
	}

	t.Run("release lapsed unique claim error", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin()
		mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnError(errBoom)
		mock.ExpectRollback()

		require.ErrorIs(t, b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{unique("a")}), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("unique violation maps to duplicate", func(t *testing.T) {
		b, mock := newMockBroker(t)
		mock.ExpectBegin()
		mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("UPDATE", 0))
		mock.ExpectExec("").WithArgs(anyArgs(18)...).WillReturnError(
			&pgconn.PgError{Code: uniqueViolationCode, ConstraintName: uniqueIndexName})
		mock.ExpectRollback()

		err := b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{unique("a")})
		require.ErrorIs(t, err, broker.ErrDuplicateTask)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("dependency edge insert error rolls back", func(t *testing.T) {
		b, mock := newMockBroker(t)

		task := simple("a")
		task.Options.DependsOn = []*conveyorv1.TaskDependency{{TaskId: "dep"}}

		mock.ExpectBegin()
		// Unknown dependency: the states query returns no rows, so the task is
		// committed blocked and an edge is recorded.
		mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(pgxmock.NewRows([]string{"id", "state"}))
		mock.ExpectExec("").WithArgs(anyArgs(18)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnError(errBoom)
		mock.ExpectRollback()

		require.ErrorIs(t, b.EnqueueBatch(ctx, []*conveyorv1.TaskEnvelope{task}), errBoom)
		require.NoError(t, mock.ExpectationsWereMet())
	})
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
