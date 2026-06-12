package conveyor

import (
	"context"
)

// contextKey is the private type of the SDK's context keys, so they cannot
// collide with keys from other packages.
type contextKey int

// Context keys for the task values injected into handler contexts.
const (
	// taskIDContextKey carries the executing task's id.
	taskIDContextKey contextKey = iota
	// retryCountContextKey carries the executing task's retry count.
	retryCountContextKey
	// maxRetryContextKey carries the executing task's retry budget.
	maxRetryContextKey
)

// withTaskValues injects the task's identity and retry counters into the
// context a handler runs under.
func withTaskValues(ctx context.Context, task *Task) context.Context {
	ctx = context.WithValue(ctx, taskIDContextKey, task.id)
	ctx = context.WithValue(ctx, retryCountContextKey, task.retried)
	ctx = context.WithValue(ctx, maxRetryContextKey, task.maxRetry)

	return ctx
}

// GetTaskID returns the id of the task the handler is executing. The bool
// reports whether ctx is a handler context carrying task values.
func GetTaskID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(taskIDContextKey).(string)

	return id, ok
}

// GetRetryCount returns how many times the executing task has been retried.
// The bool reports whether ctx is a handler context carrying task values.
func GetRetryCount(ctx context.Context) (int, bool) {
	count, ok := ctx.Value(retryCountContextKey).(int)

	return count, ok
}

// GetMaxRetry returns the executing task's retry budget. The bool reports
// whether ctx is a handler context carrying task values.
func GetMaxRetry(ctx context.Context) (int, bool) {
	budget, ok := ctx.Value(maxRetryContextKey).(int)

	return budget, ok
}
