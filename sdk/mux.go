package conveyor

import (
	"context"
	"fmt"
)

// HandlerFunc processes one task. Returning nil acknowledges the task;
// returning an error retries it with server-computed backoff, unless the
// error is wrapped with SkipRetry, which archives the task immediately.
// Handlers must be idempotent and should honor ctx cancellation.
type HandlerFunc func(ctx context.Context, task *Task) error

// Mux routes dispatched tasks to handlers by task type.
type Mux struct {
	// handlers maps task type to its handler.
	handlers map[string]HandlerFunc
}

// NewMux builds an empty task router.
func NewMux() *Mux {
	return &Mux{handlers: make(map[string]HandlerFunc)}
}

// HandleFunc registers the handler of one task type. Registering a nil
// handler, an empty type, or the same type twice panics, mirroring
// net/http: routing tables are wired at startup, where failing fast beats
// failing on first dispatch.
func (m *Mux) HandleFunc(taskType string, handler HandlerFunc) {
	if taskType == "" {
		panic("conveyor: HandleFunc with empty task type")
	}

	if handler == nil {
		panic("conveyor: HandleFunc with nil handler")
	}

	if _, exists := m.handlers[taskType]; exists {
		panic(fmt.Sprintf("conveyor: duplicate handler for task type %q", taskType))
	}

	m.handlers[taskType] = handler
}

// handler returns the handler registered for a task type.
func (m *Mux) handler(taskType string) (HandlerFunc, bool) {
	handler, ok := m.handlers[taskType]

	return handler, ok
}
