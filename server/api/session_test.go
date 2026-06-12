package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
)

// Frame constructors keep the tests terse.

func helloFrame(queues map[string]int32, concurrency int32) *conveyorv1.WorkerMessage {
	return &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Hello{
			Hello: &conveyorv1.Hello{Queues: queues, Concurrency: concurrency},
		},
	}
}

func creditFrame(n int32) *conveyorv1.WorkerMessage {
	return &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Credit{Credit: &conveyorv1.Credit{N: n}},
	}
}

func resultFrame(taskID string, outcome conveyorv1.TaskOutcome) *conveyorv1.WorkerMessage {
	return &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Result{Result: &conveyorv1.Result{TaskId: taskID, Outcome: outcome}},
	}
}

func heartbeatFrame(ids ...string) *conveyorv1.WorkerMessage {
	return &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Heartbeat{Heartbeat: &conveyorv1.Heartbeat{ActiveTaskIds: ids}},
	}
}

// validOpen returns a session state advanced past a valid Hello.
func validOpen(t *testing.T) *sessionState {
	t.Helper()

	state := &sessionState{}
	require.NoError(t, state.check(helloFrame(map[string]int32{"default": 1}, 4)))

	return state
}

func TestSessionRejectsFramesBeforeHello(t *testing.T) {
	frames := map[string]*conveyorv1.WorkerMessage{
		"credit":    creditFrame(1),
		"result":    resultFrame("task-1", conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS),
		"heartbeat": heartbeatFrame("task-1"),
		"empty":     {},
	}

	for name, frame := range frames {
		state := &sessionState{}
		require.ErrorIs(t, state.check(frame), errFrameBeforeHello, "frame %s", name)
	}
}

func TestSessionRejectsDuplicateHello(t *testing.T) {
	state := validOpen(t)

	require.ErrorIs(t, state.check(helloFrame(map[string]int32{"default": 1}, 4)), errDuplicateHello)
}

func TestSessionHelloValidation(t *testing.T) {
	cases := map[string]*conveyorv1.WorkerMessage{
		"zero concurrency":     helloFrame(map[string]int32{"default": 1}, 0),
		"negative concurrency": helloFrame(map[string]int32{"default": 1}, -1),
		"no queues":            helloFrame(nil, 4),
		"invalid queue name":   helloFrame(map[string]int32{"bad queue!": 1}, 4),
		"leading dot":          helloFrame(map[string]int32{".hidden": 1}, 4),
		"zero weight":          helloFrame(map[string]int32{"default": 0}, 4),
	}

	for name, frame := range cases {
		state := &sessionState{}
		require.Error(t, state.check(frame), "case %s", name)
		require.False(t, state.helloSeen, "case %s must not open the session", name)
	}
}

func TestSessionAcceptsValidFrames(t *testing.T) {
	state := validOpen(t)

	require.NoError(t, state.check(creditFrame(1)))
	require.NoError(t, state.check(creditFrame(4)))
	require.NoError(t, state.check(resultFrame("task-1", conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS)))
	require.NoError(t, state.check(resultFrame("task-2", conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY)))
	require.NoError(t, state.check(resultFrame("task-3", conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY)))
	require.NoError(t, state.check(resultFrame("task-4", conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED)))
	require.NoError(t, state.check(heartbeatFrame()))
	require.NoError(t, state.check(heartbeatFrame("task-5")))
}

func TestSessionCreditValidation(t *testing.T) {
	state := validOpen(t)

	require.ErrorContains(t, state.check(creditFrame(0)), "credit must be positive")
	require.ErrorContains(t, state.check(creditFrame(-3)), "credit must be positive")
	require.ErrorContains(t, state.check(creditFrame(5)), "exceeds declared concurrency")
}

func TestSessionResultValidation(t *testing.T) {
	state := validOpen(t)

	require.ErrorContains(t, state.check(resultFrame("", conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS)), "without task_id")
	require.ErrorContains(t, state.check(resultFrame("task-1", conveyorv1.TaskOutcome_TASK_OUTCOME_UNSPECIFIED)), "undefined task outcome")
	require.ErrorContains(t, state.check(resultFrame("task-1", conveyorv1.TaskOutcome(99))), "undefined task outcome")
}

func TestSessionRejectsEmptyFrameAfterHello(t *testing.T) {
	state := validOpen(t)

	require.ErrorIs(t, state.check(&conveyorv1.WorkerMessage{}), errEmptyFrame)
}

// FuzzSessionFrameStateMachine throws arbitrary frame sequences at the
// state machine: it must never panic, never open a session without a valid
// Hello, and never admit an out-of-bounds credit.
func FuzzSessionFrameStateMachine(f *testing.F) {
	// Seed corpus: frame kind selectors with raw field material.
	f.Add([]byte{0, 1, 2, 3}, "default", int32(4), int32(1), "task-1", int32(1))
	f.Add([]byte{1, 1, 1}, "q", int32(0), int32(-1), "", int32(0))
	f.Add([]byte{2, 0, 2}, "bad name!", int32(1), int32(100), "task", int32(99))
	f.Add([]byte{3, 4, 0, 0}, "", int32(-5), int32(7), "x", int32(2))

	f.Fuzz(func(t *testing.T, kinds []byte, queue string, concurrency, credit int32, taskID string, outcome int32) {
		state := &sessionState{}

		for _, kind := range kinds {
			var frame *conveyorv1.WorkerMessage

			switch kind % 5 {
			case 0:
				frame = helloFrame(map[string]int32{queue: int32(kind % 3)}, concurrency)
			case 1:
				frame = creditFrame(credit)
			case 2:
				frame = resultFrame(taskID, conveyorv1.TaskOutcome(outcome))
			case 3:
				frame = heartbeatFrame(taskID)
			default:
				frame = &conveyorv1.WorkerMessage{}
			}

			err := state.check(frame)

			if state.helloSeen && state.concurrency <= 0 {
				t.Fatalf("session open with non-positive concurrency %d", state.concurrency)
			}

			if err == nil && !state.helloSeen {
				t.Fatal("frame accepted before Hello")
			}

			if err == nil && frame.GetCredit() != nil {
				if frame.GetCredit().GetN() <= 0 || frame.GetCredit().GetN() > state.concurrency {
					t.Fatalf("out-of-bounds credit %d accepted", frame.GetCredit().GetN())
				}
			}
		}
	})
}
