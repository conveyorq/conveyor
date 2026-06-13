package api

import (
	"errors"
	"fmt"
	"regexp"

	"golang.org/x/mod/semver"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// queueNamePattern restricts queue names to what grain identity names
// accept; a queue name becomes part of its grain's cluster-wide identity.
var queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-_.]*$`)

// minSDKVersion is the oldest worker SDK version admitted to a session.
// Versions that do not parse as semver — dev builds, "unknown", custom
// clients — are admitted: the gate only rejects releases known to predate
// the wire protocol. The current value admits every build, including the
// v0.0.0 pseudo-versions Go stamps on untagged checkouts; bump it when a
// wire change leaves older SDKs behind. A variable, not a constant, so
// tests can exercise the gate.
var minSDKVersion = "v0.0.0-0"

// Protocol violation errors. Each one ends the offending session.
var (
	// errFrameBeforeHello is returned when any frame precedes Hello.
	errFrameBeforeHello = errors.New("protocol violation: first frame must be Hello")
	// errDuplicateHello is returned when a session sends Hello twice.
	errDuplicateHello = errors.New("protocol violation: duplicate Hello frame")
	// errEmptyFrame is returned for a frame carrying no recognized payload.
	errEmptyFrame = errors.New("protocol violation: empty or unknown frame")
)

// sessionState is the frame state machine of one worker session. It is
// pure — no I/O, no clock — so it can be driven exhaustively by tests and
// the fuzzer. check validates one incoming frame and advances the state;
// any error is a protocol violation that must end the session.
type sessionState struct {
	// helloSeen records whether the opening Hello arrived.
	helloSeen bool
	// concurrency is the worker's declared execution slots.
	concurrency int32
}

// check validates one worker frame against the session state and advances
// the state.
func (s *sessionState) check(message *conveyorv1.WorkerMessage) error {
	switch frame := message.GetFrame().(type) {
	case *conveyorv1.WorkerMessage_Hello:
		if s.helloSeen {
			return errDuplicateHello
		}

		if err := validateHello(frame.Hello); err != nil {
			return err
		}

		s.helloSeen = true
		s.concurrency = frame.Hello.GetConcurrency()

		return nil

	case *conveyorv1.WorkerMessage_Credit:
		if !s.helloSeen {
			return errFrameBeforeHello
		}

		return s.validateCredit(frame.Credit)

	case *conveyorv1.WorkerMessage_Result:
		if !s.helloSeen {
			return errFrameBeforeHello
		}

		return validateResult(frame.Result)

	case *conveyorv1.WorkerMessage_Heartbeat:
		if !s.helloSeen {
			return errFrameBeforeHello
		}

		return nil

	default:
		if !s.helloSeen {
			return errFrameBeforeHello
		}

		return errEmptyFrame
	}
}

// validateHello checks the session-opening frame: a supported SDK
// version, at least one validly named queue with a positive weight, and
// positive concurrency.
func validateHello(hello *conveyorv1.Hello) error {
	version := hello.GetSdkVersion()
	if semver.IsValid(version) && semver.Compare(version, minSDKVersion) < 0 {
		return fmt.Errorf("sdk version %s is no longer supported, the minimum is %s; upgrade the worker SDK", version, minSDKVersion)
	}

	if hello.GetConcurrency() <= 0 {
		return fmt.Errorf("protocol violation: concurrency must be positive, got %d", hello.GetConcurrency())
	}

	if len(hello.GetQueues()) == 0 {
		return errors.New("protocol violation: Hello must declare at least one queue")
	}

	for name, weight := range hello.GetQueues() {
		if !queueNamePattern.MatchString(name) {
			return fmt.Errorf("protocol violation: invalid queue name %q", name)
		}

		if weight <= 0 {
			return fmt.Errorf("protocol violation: queue %q weight must be positive, got %d", name, weight)
		}
	}

	return nil
}

// validateCredit bounds a credit grant: positive, and never more slots
// than the worker declared at Hello.
func (s *sessionState) validateCredit(credit *conveyorv1.Credit) error {
	if credit.GetN() <= 0 {
		return fmt.Errorf("protocol violation: credit must be positive, got %d", credit.GetN())
	}

	if credit.GetN() > s.concurrency {
		return fmt.Errorf("protocol violation: credit %d exceeds declared concurrency %d", credit.GetN(), s.concurrency)
	}

	return nil
}

// validateResult checks a result frame: a task id and a defined outcome.
func validateResult(result *conveyorv1.Result) error {
	if result.GetTaskId() == "" {
		return errors.New("protocol violation: Result without task_id")
	}

	switch result.GetOutcome() {
	case conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
		conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
		conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY,
		conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED:
		return nil

	default:
		return fmt.Errorf("protocol violation: undefined task outcome %d", result.GetOutcome())
	}
}
