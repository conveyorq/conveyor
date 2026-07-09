// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// errFault is the error a faultBroker injects on its selected method.
var errFault = errors.New("injected broker fault")

// faultBroker wraps a Broker and forces one named method to return errFault,
// leaving every other method delegating to the wrapped broker unchanged. It
// exercises an admin handler's broker-failure branch (the CodeInternal return)
// that the happy-path tests never reach. Embedding the Broker interface keeps
// the harness small and forward-compatible: a method added to Broker later
// still compiles and delegates here until a test opts it in to faulting.
type faultBroker struct {
	broker.Broker
	failOn string
}

// SetQueueRateLimit faults when selected; otherwise delegates.
func (f *faultBroker) SetQueueRateLimit(ctx context.Context, queue string, ratePerSec float64, burst int) error {
	if f.failOn == "SetQueueRateLimit" {
		return errFault
	}

	return f.Broker.SetQueueRateLimit(ctx, queue, ratePerSec, burst)
}

// DeleteQueueRateLimit faults when selected; otherwise delegates.
func (f *faultBroker) DeleteQueueRateLimit(ctx context.Context, queue string) error {
	if f.failOn == "DeleteQueueRateLimit" {
		return errFault
	}

	return f.Broker.DeleteQueueRateLimit(ctx, queue)
}

// QueueRateLimits faults when selected; otherwise delegates.
func (f *faultBroker) QueueRateLimits(ctx context.Context) ([]broker.RateLimit, error) {
	if f.failOn == "QueueRateLimits" {
		return nil, errFault
	}

	return f.Broker.QueueRateLimits(ctx)
}

// SetQueueConcurrencyLimit faults when selected; otherwise delegates.
func (f *faultBroker) SetQueueConcurrencyLimit(ctx context.Context, queue string, maxActive int) error {
	if f.failOn == "SetQueueConcurrencyLimit" {
		return errFault
	}

	return f.Broker.SetQueueConcurrencyLimit(ctx, queue, maxActive)
}

// DeleteQueueConcurrencyLimit faults when selected; otherwise delegates.
func (f *faultBroker) DeleteQueueConcurrencyLimit(ctx context.Context, queue string) error {
	if f.failOn == "DeleteQueueConcurrencyLimit" {
		return errFault
	}

	return f.Broker.DeleteQueueConcurrencyLimit(ctx, queue)
}

// QueueConcurrencyLimits faults when selected; otherwise delegates.
func (f *faultBroker) QueueConcurrencyLimits(ctx context.Context) ([]broker.ConcurrencyLimit, error) {
	if f.failOn == "QueueConcurrencyLimits" {
		return nil, errFault
	}

	return f.Broker.QueueConcurrencyLimits(ctx)
}

// SetGroupConfig faults when selected; otherwise delegates.
func (f *faultBroker) SetGroupConfig(ctx context.Context, queue, group string, maxSize int, maxDelay, gracePeriod time.Duration) error {
	if f.failOn == "SetGroupConfig" {
		return errFault
	}

	return f.Broker.SetGroupConfig(ctx, queue, group, maxSize, maxDelay, gracePeriod)
}

// DeleteGroupConfig faults when selected; otherwise delegates.
func (f *faultBroker) DeleteGroupConfig(ctx context.Context, queue, group string) error {
	if f.failOn == "DeleteGroupConfig" {
		return errFault
	}

	return f.Broker.DeleteGroupConfig(ctx, queue, group)
}

// GroupConfigs faults when selected; otherwise delegates.
func (f *faultBroker) GroupConfigs(ctx context.Context) ([]broker.GroupConfig, error) {
	if f.failOn == "GroupConfigs" {
		return nil, errFault
	}

	return f.Broker.GroupConfigs(ctx)
}

// newFaultAdminService builds an AdminService whose broker faults on failOn. The
// engine is the real test engine; the config handlers under test fail at the
// broker before any engine call, so the injected fault is what the handler maps
// to a connect error.
func newFaultAdminService(t *testing.T, failOn string) *AdminService {
	t.Helper()

	engine, taskLog := startTestEngine(t)

	return NewAdminService(engine, &faultBroker{Broker: taskLog, failOn: failOn}, clock.System(), stubSessions(nil), true)
}

// TestAdminConfigHandlersReturnInternalOnBrokerFault asserts every per-queue
// rate-limit, concurrency, and per-group config handler maps a broker failure to
// connect's Internal code instead of leaking it or panicking.
func TestAdminConfigHandlersReturnInternalOnBrokerFault(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		failOn string
		call   func(*AdminService) error
	}{
		{"ListRateLimits", "QueueRateLimits", func(a *AdminService) error {
			_, err := a.ListRateLimits(ctx, connect.NewRequest(&conveyorv1.ListRateLimitsRequest{}))
			return err
		}},
		{"SetQueueRateLimit", "SetQueueRateLimit", func(a *AdminService) error {
			_, err := a.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{Queue: defaultQueueName, RatePerSec: 1, Burst: 1}))
			return err
		}},
		{"DeleteQueueRateLimit", "DeleteQueueRateLimit", func(a *AdminService) error {
			_, err := a.DeleteQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.DeleteQueueRateLimitRequest{Queue: defaultQueueName}))
			return err
		}},
		{"ListConcurrencyLimits", "QueueConcurrencyLimits", func(a *AdminService) error {
			_, err := a.ListConcurrencyLimits(ctx, connect.NewRequest(&conveyorv1.ListConcurrencyLimitsRequest{}))
			return err
		}},
		{"SetQueueConcurrencyLimit", "SetQueueConcurrencyLimit", func(a *AdminService) error {
			_, err := a.SetQueueConcurrencyLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueConcurrencyLimitRequest{Queue: defaultQueueName, MaxActive: 1}))
			return err
		}},
		{"DeleteQueueConcurrencyLimit", "DeleteQueueConcurrencyLimit", func(a *AdminService) error {
			_, err := a.DeleteQueueConcurrencyLimit(ctx, connect.NewRequest(&conveyorv1.DeleteQueueConcurrencyLimitRequest{Queue: defaultQueueName}))
			return err
		}},
		{"ListGroupConfigs", "GroupConfigs", func(a *AdminService) error {
			_, err := a.ListGroupConfigs(ctx, connect.NewRequest(&conveyorv1.ListGroupConfigsRequest{}))
			return err
		}},
		{"SetGroupConfig", "SetGroupConfig", func(a *AdminService) error {
			_, err := a.SetGroupConfig(ctx, connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
				Queue: defaultQueueName, Group: "emails", MaxSize: 1, MaxDelay: durationpb.New(time.Minute), GracePeriod: durationpb.New(time.Second),
			}))
			return err
		}},
		{"DeleteGroupConfig", "DeleteGroupConfig", func(a *AdminService) error {
			_, err := a.DeleteGroupConfig(ctx, connect.NewRequest(&conveyorv1.DeleteGroupConfigRequest{Queue: defaultQueueName, Group: "emails"}))
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			admin := newFaultAdminService(t, tc.failOn)

			err := tc.call(admin)
			require.Equal(t, connect.CodeInternal, connect.CodeOf(err), "broker fault must map to Internal")
		})
	}
}
