// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"sync"
	"time"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Broker method names the fault broker can be armed against. They are the
// branching selectors callers pass to fault and clear, so they live as named
// constants rather than bare strings.
const (
	methodEnqueue            = "Enqueue"
	methodRelease            = "Release"
	methodSetQueuePaused     = "SetQueuePaused"
	methodReapExpiredLeases  = "ReapExpiredLeases"
	methodPurgeCompleted     = "PurgeCompleted"
	methodArchiveExpired     = "ArchiveExpired"
	methodPendingCount       = "PendingCount"
	methodPromoteScheduled   = "PromoteScheduled"
	methodListDueCronEntries = "ListDueCronEntries"
	methodGroupStats         = "GroupStats"
	methodUpdateCronNextRun  = "UpdateCronNextRun"
)

// faultBroker wraps a real broker and returns a configured error from selected
// methods, so tests can drive the error-handling branches a healthy broker
// never reaches. Every method not armed delegates to the embedded broker, and
// the inner broker stays reachable so a test can assert real state through it.
type faultBroker struct {
	broker.Broker

	// mutex guards faults.
	mutex sync.Mutex
	// faults maps an armed method name to the error it returns.
	faults map[string]error
}

// newFaultBroker wraps inner with no faults armed.
func newFaultBroker(inner broker.Broker) *faultBroker {
	return &faultBroker{Broker: inner, faults: make(map[string]error)}
}

// fault arms a method to return err until it is cleared.
func (f *faultBroker) fault(method string, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.faults[method] = err
}

// clear disarms a method, restoring delegation to the inner broker.
func (f *faultBroker) clear(method string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	delete(f.faults, method)
}

// armed returns the error a method is armed with, or nil.
func (f *faultBroker) armed(method string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.faults[method]
}

// Enqueue fails when armed, otherwise delegates.
func (f *faultBroker) Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) error {
	if err := f.armed(methodEnqueue); err != nil {
		return err
	}

	return f.Broker.Enqueue(ctx, task)
}

// Release fails when armed, otherwise delegates.
func (f *faultBroker) Release(ctx context.Context, taskID, leaseID string) error {
	if err := f.armed(methodRelease); err != nil {
		return err
	}

	return f.Broker.Release(ctx, taskID, leaseID)
}

// SetQueuePaused fails when armed, otherwise delegates.
func (f *faultBroker) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	if err := f.armed(methodSetQueuePaused); err != nil {
		return err
	}

	return f.Broker.SetQueuePaused(ctx, queue, paused)
}

// ReapExpiredLeases fails when armed, otherwise delegates.
func (f *faultBroker) ReapExpiredLeases(ctx context.Context, limit int) ([]string, error) {
	if err := f.armed(methodReapExpiredLeases); err != nil {
		return nil, err
	}

	return f.Broker.ReapExpiredLeases(ctx, limit)
}

// PurgeCompleted fails when armed, otherwise delegates.
func (f *faultBroker) PurgeCompleted(ctx context.Context, limit int) (int, error) {
	if err := f.armed(methodPurgeCompleted); err != nil {
		return 0, err
	}

	return f.Broker.PurgeCompleted(ctx, limit)
}

// ArchiveExpired fails when armed, otherwise delegates.
func (f *faultBroker) ArchiveExpired(ctx context.Context, limit int) (int, error) {
	if err := f.armed(methodArchiveExpired); err != nil {
		return 0, err
	}

	return f.Broker.ArchiveExpired(ctx, limit)
}

// PendingCount fails when armed, otherwise delegates.
func (f *faultBroker) PendingCount(ctx context.Context) (map[string]int64, error) {
	if err := f.armed(methodPendingCount); err != nil {
		return nil, err
	}

	return f.Broker.PendingCount(ctx)
}

// PromoteScheduled fails when armed, otherwise delegates.
func (f *faultBroker) PromoteScheduled(ctx context.Context, limit int) ([]string, error) {
	if err := f.armed(methodPromoteScheduled); err != nil {
		return nil, err
	}

	return f.Broker.PromoteScheduled(ctx, limit)
}

// ListDueCronEntries fails when armed, otherwise delegates.
func (f *faultBroker) ListDueCronEntries(ctx context.Context, now time.Time) ([]*broker.CronEntry, error) {
	if err := f.armed(methodListDueCronEntries); err != nil {
		return nil, err
	}

	return f.Broker.ListDueCronEntries(ctx, now)
}

// GroupStats fails when armed, otherwise delegates.
func (f *faultBroker) GroupStats(ctx context.Context) ([]broker.GroupStat, error) {
	if err := f.armed(methodGroupStats); err != nil {
		return nil, err
	}

	return f.Broker.GroupStats(ctx)
}

// UpdateCronNextRun fails when armed, otherwise delegates.
func (f *faultBroker) UpdateCronNextRun(ctx context.Context, id string, expected, next time.Time) error {
	if err := f.armed(methodUpdateCronNextRun); err != nil {
		return err
	}

	return f.Broker.UpdateCronNextRun(ctx, id, expected, next)
}
