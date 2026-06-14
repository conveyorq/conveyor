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
