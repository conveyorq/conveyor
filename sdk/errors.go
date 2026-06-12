// Package conveyor is the public Go SDK for the Conveyor task processing
// system.
//
// A Client enqueues tasks; a Worker executes them through a Mux that
// routes by task type, with middleware, codecs (JSON, Bytes, Proto), and
// per-task options (queue, priority, scheduling, retries, uniqueness).
// Worker.Run reconnects with jittered backoff and recovers handler
// panics, reporting them as retryable failures. No exported identifier in
// this package references protobuf or GoAkt types.
package conveyor

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the SDK. Match with errors.Is.
var (
	// ErrDuplicateTask is returned by Enqueue when a unique task with the
	// same key already exists and is not yet complete.
	ErrDuplicateTask = errors.New("conveyor: task already exists")

	// ErrTaskNotFound is returned when the referenced task id is unknown.
	ErrTaskNotFound = errors.New("conveyor: task not found")
)

// skipRetry wraps a handler error to signal that the task must be archived
// immediately instead of retried.
type skipRetry struct {
	// err is the handler error that caused the task to be archived.
	err error
}

// Error implements the error interface.
func (s *skipRetry) Error() string {
	return fmt.Sprintf("skip retry: %v", s.err)
}

// Unwrap exposes the wrapped error to errors.Is and errors.As.
func (s *skipRetry) Unwrap() error {
	return s.err
}

// SkipRetry wraps err so that the worker reports the task as non-retryable:
// the server archives it immediately, regardless of remaining retries.
// Returning a nil err still skips retries with an unspecified cause.
func SkipRetry(err error) error {
	return &skipRetry{err: err}
}

// IsSkipRetry reports whether err (or any error it wraps) was marked with
// SkipRetry. The worker runtime uses it to map handler errors to outcomes.
func IsSkipRetry(err error) bool {
	var target *skipRetry

	return errors.As(err, &target)
}
