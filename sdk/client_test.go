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

package conveyor

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestNewClientRequiresBaseURL(t *testing.T) {
	_, err := NewClient("")
	require.ErrorContains(t, err, "base URL is required")
}

func TestClientEnqueueValidation(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1")
	require.NoError(t, err)

	ctx := context.Background()

	_, err = client.Enqueue(ctx, nil)
	require.ErrorContains(t, err, "task is required")

	_, err = client.Enqueue(ctx, NewTask("", JSON("x")))
	require.ErrorContains(t, err, "task type is required")

	_, err = client.Enqueue(ctx, NewTask("test:bad", JSON(make(chan int))))
	require.ErrorContains(t, err, "encoding JSON payload")

	_, err = client.Enqueue(ctx, NewTask("test:ok", JSON("x")),
		ProcessAt(clock.System().Now().Add(time.Hour)), ProcessIn(time.Hour))
	require.ErrorContains(t, err, "mutually exclusive")

	_, err = client.GetTask(ctx, "")
	require.ErrorContains(t, err, "task id is required")
}

func TestClientEnqueueAndGetTask(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	ctx := context.Background()

	info, err := client.Enqueue(ctx, NewTask("email:welcome", JSON(map[string]int{"user_id": 42})),
		Queue("critical"), MaxRetry(10), Priority(7), Retention(time.Hour))
	require.NoError(t, err)
	require.NotEmpty(t, info.ID)
	require.Equal(t, "critical", info.Queue)
	require.Equal(t, "email:welcome", info.Type)
	require.Equal(t, TaskStatePending, info.State)
	require.Equal(t, 7, info.Priority)
	require.Equal(t, 10, info.MaxRetry)
	require.False(t, info.EnqueuedAt.IsZero())

	fetched, err := client.GetTask(ctx, info.ID)
	require.NoError(t, err)
	require.Equal(t, info.ID, fetched.ID)
	require.Equal(t, TaskStatePending, fetched.State)
}

func TestClientEnqueueAppliesServerDefaults(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("test:defaults", JSON("x")))
	require.NoError(t, err)
	require.Equal(t, "default", info.Queue)
	require.Equal(t, 4, info.Priority)
	require.Equal(t, 25, info.MaxRetry)
}

func TestClientEnqueueScheduled(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("test:later", JSON("x")),
		ProcessIn(time.Hour))
	require.NoError(t, err)
	require.Equal(t, TaskStateScheduled, info.State)
	require.False(t, info.ProcessAt.IsZero())
}

func TestClientEnqueueWithTaskIDIsIdempotent(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	ctx := context.Background()

	first, err := client.Enqueue(ctx, NewTask("test:idem", JSON("x")), TaskID("01TESTIDEMPOTENT0000000000"))
	require.NoError(t, err)

	second, err := client.Enqueue(ctx, NewTask("test:idem", JSON("x")), TaskID("01TESTIDEMPOTENT0000000000"))
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
}

func TestClientGetTaskNotFound(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.GetTask(context.Background(), "01UNKNOWNTASK0000000000000")
	require.ErrorIs(t, err, ErrTaskNotFound)
}

func TestClientAuthentication(t *testing.T) {
	baseURL := startTestServer(t, []string{"top-secret"})

	denied, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = denied.Enqueue(context.Background(), NewTask("test:auth", JSON("x")))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(errors.Unwrap(err)))

	allowed, err := NewClient(baseURL, WithToken("top-secret"))
	require.NoError(t, err)

	_, err = allowed.Enqueue(context.Background(), NewTask("test:auth", JSON("x")))
	require.NoError(t, err)
}

func TestClientEnqueueUniqueIsAccepted(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("test:unique", JSON("x")),
		Unique(24*time.Hour), Retention(time.Hour))
	require.NoError(t, err)
	require.NotEmpty(t, info.ID)
}

func TestDerivedUniqueKeyIsDeterministic(t *testing.T) {
	key := derivedUniqueKey("email:welcome", []byte(`{"user_id":42}`))

	require.Equal(t, key, derivedUniqueKey("email:welcome", []byte(`{"user_id":42}`)))
	require.NotEqual(t, key, derivedUniqueKey("email:welcome", []byte(`{"user_id":43}`)))
	require.NotEqual(t, key, derivedUniqueKey("email:goodbye", []byte(`{"user_id":42}`)))
	require.Len(t, key, 64, "the key is hex-encoded SHA-256")
}

func TestWireErrorMapsSentinels(t *testing.T) {
	duplicate := wireError(connect.NewError(connect.CodeAlreadyExists, errors.New("dup")))
	require.ErrorIs(t, duplicate, ErrDuplicateTask)

	missing := wireError(connect.NewError(connect.CodeNotFound, errors.New("missing")))
	require.ErrorIs(t, missing, ErrTaskNotFound)

	other := wireError(connect.NewError(connect.CodeInternal, errors.New("boom")))
	require.NotErrorIs(t, other, ErrDuplicateTask)
	require.NotErrorIs(t, other, ErrTaskNotFound)
}

func TestTaskStateFromProto(t *testing.T) {
	cases := map[conveyorv1.TaskState]TaskState{
		conveyorv1.TaskState_TASK_STATE_SCHEDULED:   TaskStateScheduled,
		conveyorv1.TaskState_TASK_STATE_PENDING:     TaskStatePending,
		conveyorv1.TaskState_TASK_STATE_ACTIVE:      TaskStateActive,
		conveyorv1.TaskState_TASK_STATE_RETRY:       TaskStateRetry,
		conveyorv1.TaskState_TASK_STATE_COMPLETED:   TaskStateCompleted,
		conveyorv1.TaskState_TASK_STATE_ARCHIVED:    TaskStateArchived,
		conveyorv1.TaskState_TASK_STATE_CANCELED:    TaskStateCanceled,
		conveyorv1.TaskState_TASK_STATE_UNSPECIFIED: TaskStateUnknown,
	}

	for wire, want := range cases {
		require.Equal(t, want, taskStateFromProto(wire))
	}
}
