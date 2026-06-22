// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestDeriveEventTypes(t *testing.T) {
	const (
		unspecified = conveyorv1.TaskState_TASK_STATE_UNSPECIFIED
		scheduled   = conveyorv1.TaskState_TASK_STATE_SCHEDULED
		pending     = conveyorv1.TaskState_TASK_STATE_PENDING
		active      = conveyorv1.TaskState_TASK_STATE_ACTIVE
		retry       = conveyorv1.TaskState_TASK_STATE_RETRY
		completed   = conveyorv1.TaskState_TASK_STATE_COMPLETED
		archived    = conveyorv1.TaskState_TASK_STATE_ARCHIVED
		canceled    = conveyorv1.TaskState_TASK_STATE_CANCELED
		aggregating = conveyorv1.TaskState_TASK_STATE_AGGREGATING
		blocked     = conveyorv1.TaskState_TASK_STATE_BLOCKED
	)

	cases := []struct {
		name     string
		old, new conveyorv1.TaskState
		want     conveyorv1.TaskEventType
	}{
		{"enqueue pending", unspecified, pending, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
		{"enqueue scheduled", unspecified, scheduled, conveyorv1.TaskEventType_TASK_EVENT_TYPE_SCHEDULED},
		{"enqueue blocked", unspecified, blocked, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
		{"enqueue aggregating", unspecified, aggregating, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
		{"enqueue cascade canceled", unspecified, canceled, conveyorv1.TaskEventType_TASK_EVENT_TYPE_CANCELED},
		{"lease", pending, active, conveyorv1.TaskEventType_TASK_EVENT_TYPE_LEASED},
		{"complete", active, completed, conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED},
		{"retry", active, retry, conveyorv1.TaskEventType_TASK_EVENT_TYPE_RETRIED},
		{"archive from active", active, archived, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED},
		{"archive waiting", pending, archived, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED},
		{"cancel waiting", pending, canceled, conveyorv1.TaskEventType_TASK_EVENT_TYPE_CANCELED},
		{"release", active, pending, conveyorv1.TaskEventType_TASK_EVENT_TYPE_RELEASED},
		{"promote scheduled", scheduled, pending, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
		{"promote blocked", blocked, pending, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
		{"run archived again", archived, pending, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Derive(tc.old, tc.new, "id", "queue", "type", "", 0, time.Unix(1, 0))
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.GetEventType())
			assert.Equal(t, tc.new, got.GetState())
		})
	}
}

func TestDeriveCarriesFields(t *testing.T) {
	occurredAt := time.Unix(1700000000, 0).UTC()

	got := Derive(conveyorv1.TaskState_TASK_STATE_ACTIVE, conveyorv1.TaskState_TASK_STATE_RETRY,
		"task-1", "emails", "email:welcome", "smtp timeout", 3, occurredAt)

	require.NotNil(t, got)
	assert.Equal(t, "task-1", got.GetId())
	assert.Equal(t, "emails", got.GetQueue())
	assert.Equal(t, "email:welcome", got.GetType())
	assert.Equal(t, int32(3), got.GetAttempt())
	assert.Equal(t, "smtp timeout", got.GetLastError())
	assert.Equal(t, occurredAt, got.GetOccurredAt().AsTime())
}

func TestDeriveReturnsNilForNoopTransition(t *testing.T) {
	// pending to pending (e.g. RunTaskNow on an already-pending task) and any
	// unrecognized target carry no event.
	assert.Nil(t, Derive(conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED,
		"id", "queue", "type", "", 0, time.Unix(1, 0)))
}
