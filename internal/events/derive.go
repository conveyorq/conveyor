// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Derive builds the lifecycle event for a transition from oldState to newState,
// or nil when the pair carries no meaningful event (an unrecognized or no-op
// transition). oldState is TASK_STATE_UNSPECIFIED for a freshly committed task.
// It is the single source of truth for the event-type mapping, shared by every
// broker so the stream means the same thing regardless of storage engine. The
// resulting state rides on the event beside the derived type, so a watcher can
// filter on either.
func Derive(oldState, newState conveyorv1.TaskState, id, queue, taskType, lastError string, attempt int32, occurredAt time.Time) *conveyorv1.TaskEvent {
	// A transition that does not change state carries no event (e.g. making an
	// already-pending task due again).
	if oldState == newState {
		return nil
	}

	eventType := classify(oldState, newState)
	if eventType == conveyorv1.TaskEventType_TASK_EVENT_TYPE_UNSPECIFIED {
		return nil
	}

	return &conveyorv1.TaskEvent{
		Id:         id,
		Queue:      queue,
		Type:       taskType,
		State:      newState,
		EventType:  eventType,
		OccurredAt: timestamppb.New(occurredAt),
		Attempt:    attempt,
		LastError:  lastError,
	}
}

// classify maps a (oldState, newState) transition to its event type. The new
// state is the primary signal; the old state only distinguishes a release
// (active to pending) from a fresh enqueue or a promotion (both also land in
// pending). A task first committed while blocked or aggregating reports
// ENQUEUED — it has entered the system — with the state field carrying the
// precise state.
func classify(oldState, newState conveyorv1.TaskState) conveyorv1.TaskEventType {
	switch newState {
	case conveyorv1.TaskState_TASK_STATE_ACTIVE:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_LEASED

	case conveyorv1.TaskState_TASK_STATE_COMPLETED:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED

	case conveyorv1.TaskState_TASK_STATE_RETRY:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_RETRIED

	case conveyorv1.TaskState_TASK_STATE_ARCHIVED:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED

	case conveyorv1.TaskState_TASK_STATE_CANCELED:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_CANCELED

	case conveyorv1.TaskState_TASK_STATE_SCHEDULED:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_SCHEDULED

	case conveyorv1.TaskState_TASK_STATE_PENDING:
		if oldState == conveyorv1.TaskState_TASK_STATE_ACTIVE {
			return conveyorv1.TaskEventType_TASK_EVENT_TYPE_RELEASED
		}

		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED

	case conveyorv1.TaskState_TASK_STATE_AGGREGATING, conveyorv1.TaskState_TASK_STATE_BLOCKED:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED

	default:
		return conveyorv1.TaskEventType_TASK_EVENT_TYPE_UNSPECIFIED
	}
}
