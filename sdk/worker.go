// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/conveyorq/conveyor/internal/backoff"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/sdk/internal/transport"
)

// modulePath identifies this module in build info for the SDK version
// reported at session open.
const modulePath = "github.com/conveyorq/conveyor"

// unknownVersion is reported when build info carries no module version.
const unknownVersion = "devel"

// Reconnect backoff bounds: full-jitter exponential delays between session
// attempts, so a worker fleet does not stampede a restarting server.
const (
	// reconnectBaseDelay is the delay ceiling after the first failure.
	reconnectBaseDelay = 500 * time.Millisecond
	// reconnectMaxDelay bounds the delay regardless of failure count.
	reconnectMaxDelay = 30 * time.Second
)

// Cancellation causes distinguish why a task's execution context was canceled,
// which decides the outcome reported to the server. They are attached as the
// context cause, not surfaced to handlers, which always observe context.Canceled.
var (
	// errDraining marks a task canceled because the worker is shutting down.
	// Such a task is reported RELEASED: it returns to the queue with no retry
	// penalty and no backoff, so a deploy or scale-down never costs a task an
	// attempt.
	errDraining = errors.New("conveyor: worker draining")
	// errServerCanceled marks a task canceled by a server Cancel frame (an
	// admin cancel or a lost lease). It is reported RETRY so the server applies
	// its own policy — archiving an admin cancel, redelivering a lost lease.
	errServerCanceled = errors.New("conveyor: canceled by server")
)

// Worker holds one session to a Conveyor server and executes dispatched
// tasks through a Mux.
type Worker struct {
	// wire is the ConnectRPC transport.
	wire *transport.Client
	// queues maps queue name to dispatch weight.
	queues map[string]int
	// concurrency is the total concurrent execution slots.
	concurrency int
	// minServerVersion is the minimum server version the worker requires,
	// sent in Hello; empty imposes no requirement.
	minServerVersion string
}

// NewWorker builds a Worker for the Conveyor server at baseURL. WithQueues
// and WithConcurrency are required.
func NewWorker(baseURL string, opts ...Option) (*Worker, error) {
	if baseURL == "" {
		return nil, errors.New("conveyor: base URL is required")
	}

	settings := &options{}

	for _, opt := range opts {
		opt(settings)
	}

	if len(settings.queues) == 0 {
		return nil, errors.New("conveyor: at least one queue is required (WithQueues)")
	}

	for name, weight := range settings.queues {
		if name == "" {
			return nil, errors.New("conveyor: queue names must not be empty")
		}

		if weight <= 0 {
			return nil, fmt.Errorf("conveyor: queue %q weight must be positive, got %d", name, weight)
		}
	}

	if settings.concurrency <= 0 {
		return nil, fmt.Errorf("conveyor: concurrency must be positive, got %d (WithConcurrency)", settings.concurrency)
	}

	return &Worker{
		wire:             transport.New(baseURL, settings.token),
		queues:           settings.queues,
		concurrency:      settings.concurrency,
		minServerVersion: settings.minServerVersion,
	}, nil
}

// Run processes dispatched tasks until ctx is canceled, reconnecting with
// jittered exponential backoff whenever the session fails or the stream
// drops. Permanent rejections return immediately instead of retrying: a
// rejected token never heals, and neither does a session contract the
// server refuses (an outdated SDK version, an invalid queue declaration).
// Cancellation returns nil: the server releases everything still in
// flight for immediate redelivery elsewhere.
func (w *Worker) Run(ctx context.Context, mux *Mux) error {
	if mux == nil {
		return errors.New("conveyor: mux is required")
	}

	strategy := backoff.New(reconnectBaseDelay, reconnectMaxDelay)

	var failures int32

	for {
		established, err := w.runSession(ctx, mux)

		if ctx.Err() != nil {
			return nil
		}

		switch connect.CodeOf(err) {
		case connect.CodeUnauthenticated, connect.CodePermissionDenied:
			return err

		case connect.CodeInvalidArgument:
			// The server rejected the session contract itself — an
			// outdated SDK version or a malformed Hello. The same binary
			// can never succeed by retrying. The wire check matters: a
			// connection severed mid-frame synthesizes the same code
			// locally ("protocol error: incomplete envelope"), and that
			// case must keep reconnecting.
			if connect.IsWireError(err) {
				return err
			}
		}

		failures++

		if established {
			failures = 0
		}

		timer := time.NewTimer(strategy.Delay(failures - 1))

		select {
		case <-ctx.Done():
			timer.Stop()

			return nil

		case <-timer.C:
		}
	}
}

// runSession opens one worker session and drives it until it ends. The
// bool reports whether the session was established (Welcome received), so
// Run can reset its reconnect backoff.
func (w *Worker) runSession(ctx context.Context, mux *Mux) (bool, error) {
	stream := w.wire.Session(ctx)

	defer func() {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	}()

	queues := make(map[string]int32, len(w.queues))
	for name, weight := range w.queues {
		queues[name] = int32(weight)
	}

	hello := &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Hello{
			Hello: &conveyorv1.Hello{
				Queues:           queues,
				Concurrency:      int32(w.concurrency),
				SdkVersion:       sdkVersion(),
				MinServerVersion: w.minServerVersion,
			},
		},
	}

	if err := stream.Send(hello); err != nil {
		return false, fmt.Errorf("conveyor: opening session: %w", err)
	}

	first, err := stream.Receive()
	if err != nil {
		return false, fmt.Errorf("conveyor: awaiting Welcome: %w", err)
	}

	welcome := first.GetWelcome()
	if welcome == nil {
		return false, errors.New("conveyor: protocol violation: first server frame is not Welcome")
	}

	session := &workerSession{
		stream:     stream,
		mux:        mux,
		slots:      make(chan struct{}, w.concurrency),
		cancels:    make(map[string]context.CancelCauseFunc),
		sessionID:  welcome.GetSessionId(),
		runContext: ctx,
	}

	return true, session.run(welcome.GetHeartbeatInterval().AsDuration())
}

// workerSession is the state of one live session stream.
type workerSession struct {
	// stream is the session stream.
	stream *connect.BidiStreamForClient[conveyorv1.WorkerMessage, conveyorv1.ServerMessage]
	// mux routes tasks to handlers.
	mux *Mux
	// slots gates handler executions to the declared concurrency.
	slots chan struct{}
	// sendMutex serializes stream sends across executor goroutines.
	sendMutex sync.Mutex
	// stateMutex guards cancels.
	stateMutex sync.Mutex
	// cancels maps every unresolved task id to its execution cancel; its
	// keys are the heartbeat's active id set. The cause passed to the cancel
	// decides the reported outcome (see errDraining, errServerCanceled).
	cancels map[string]context.CancelCauseFunc
	// sessionID is the server-assigned session id.
	sessionID string
	// runContext is the Run context; its cancellation ends the session.
	runContext context.Context
}

// run drives the receive loop and the heartbeat until the stream ends.
// Executions still in flight are canceled on the way out with the draining
// cause, so each is reported RELEASED: a deploy or scale-down hands its tasks
// back with no retry penalty. The server also releases the leases on stream
// close, so an abrupt disconnect reaches the same penalty-free outcome.
func (s *workerSession) run(heartbeatInterval time.Duration) error {
	defer s.cancelAll()

	done := make(chan struct{})
	defer close(done)

	// Context cancellation alone does not abort an established duplex
	// HTTP/2 stream: once response headers are in, no transport goroutine
	// watches the context anymore. Closing the request side signals EOF
	// to the server, which ends the session and unblocks Receive.
	go func() {
		select {
		case <-s.runContext.Done():
			_ = s.stream.CloseRequest()

		case <-done:
		}
	}()

	if heartbeatInterval > 0 {
		go s.heartbeatLoop(heartbeatInterval, done)
	}

	for {
		message, err := s.stream.Receive()
		if err != nil {
			if s.runContext.Err() != nil {
				return nil
			}

			return fmt.Errorf("conveyor: session stream ended: %w", err)
		}

		switch frame := message.GetFrame().(type) {
		case *conveyorv1.ServerMessage_Dispatch:
			s.dispatch(frame.Dispatch)

		case *conveyorv1.ServerMessage_Cancel:
			s.cancel(frame.Cancel.GetTaskId())

		default:
			// Welcome duplicates and Pings need no action.
		}
	}
}

// heartbeatLoop reports the unresolved task ids on the session cadence so
// their leases extend.
func (s *workerSession) heartbeatLoop(interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case <-ticker.C:
			heartbeat := &conveyorv1.WorkerMessage{
				Frame: &conveyorv1.WorkerMessage_Heartbeat{
					Heartbeat: &conveyorv1.Heartbeat{ActiveTaskIds: s.activeTaskIDs()},
				},
			}

			if err := s.send(heartbeat); err != nil {
				return
			}
		}
	}
}

// dispatch registers a delivered task and starts its execution. The task
// is tracked before a slot frees up, so heartbeats extend the lease of
// queued work too.
//
// The execution context is rooted at context.Background, not runContext, so a
// shutdown does not pre-empt it with a generic cause: cancellation is driven
// explicitly (drain via cancelAll, an admin cancel via cancel) and the cause
// it carries decides the reported outcome.
func (s *workerSession) dispatch(dispatch *conveyorv1.Dispatch) {
	task := dispatch.GetTask()

	executionCtx, cancel := context.WithCancelCause(context.Background())
	release := func() { cancel(nil) }

	if deadline := dispatch.GetDeadline(); deadline.IsValid() {
		var stopDeadline context.CancelFunc

		executionCtx, stopDeadline = context.WithDeadline(executionCtx, deadline.AsTime())
		release = func() { stopDeadline(); cancel(nil) }
	}

	s.stateMutex.Lock()
	s.cancels[task.GetId()] = cancel
	s.stateMutex.Unlock()

	go s.execute(executionCtx, release, task)
}

// execute waits for a slot, runs the handler, and reports the result. release
// frees the execution context once the task is done.
func (s *workerSession) execute(ctx context.Context, release func(), envelope *conveyorv1.TaskEnvelope) {
	defer release()

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()

	case <-ctx.Done():
		s.finish(ctx, envelope.GetId(), ctx.Err())

		return
	}

	task := &Task{
		id:          envelope.GetId(),
		queue:       envelope.GetQueue(),
		taskType:    envelope.GetType(),
		payload:     envelope.GetPayload(),
		contentType: envelope.GetContentType(),
		metadata:    envelope.GetMetadata(),
		retried:     int(envelope.GetRetried()),
		maxRetry:    int(envelope.GetOptions().GetMaxRetry()),
	}

	handler, ok := s.mux.handler(task.taskType)
	if !ok {
		s.finish(ctx, task.id, fmt.Errorf("no handler registered for task type %q", task.taskType))

		return
	}

	s.finish(ctx, task.id, traced(withTaskValues(ctx, task), task, func(spanCtx context.Context) error {
		return invoke(spanCtx, handler, task)
	}))
}

// invoke runs one handler, converting a panic into a retryable error
// carrying the stack: a panicking handler never kills the worker process.
func invoke(ctx context.Context, handler HandlerFunc, task *Task) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v\n%s", recovered, debug.Stack())
		}
	}()

	return handler(ctx, task)
}

// finish reports one execution outcome and forgets the task. The execution
// context is consulted for its cancellation cause, which distinguishes a
// drain (RELEASED) from a server cancel or a genuine failure (RETRY).
func (s *workerSession) finish(ctx context.Context, taskID string, handlerErr error) {
	s.stateMutex.Lock()
	delete(s.cancels, taskID)
	s.stateMutex.Unlock()

	result := &conveyorv1.Result{TaskId: taskID, Outcome: outcomeFor(ctx, handlerErr)}

	if handlerErr != nil {
		result.ErrorMsg = handlerErr.Error()
	}

	frame := &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Result{Result: result},
	}

	// A send failure means the stream is gone; the server releases the
	// task on stream close and redelivers it.
	_ = s.send(frame)
}

// cancelAll aborts every execution still in flight because the worker is
// draining; each aborted task is reported RELEASED.
func (s *workerSession) cancelAll() {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	for _, cancelFunc := range s.cancels {
		cancelFunc(errDraining)
	}
}

// cancel aborts the execution of one task in response to a server Cancel
// frame, if it is still running; the task is reported RETRY.
func (s *workerSession) cancel(taskID string) {
	s.stateMutex.Lock()
	cancelFunc, ok := s.cancels[taskID]
	s.stateMutex.Unlock()

	if ok {
		cancelFunc(errServerCanceled)
	}
}

// activeTaskIDs snapshots the unresolved task ids.
func (s *workerSession) activeTaskIDs() []string {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	ids := make([]string, 0, len(s.cancels))

	for id := range s.cancels {
		ids = append(ids, id)
	}

	return ids
}

// send serializes one frame onto the stream.
func (s *workerSession) send(message *conveyorv1.WorkerMessage) error {
	s.sendMutex.Lock()
	defer s.sendMutex.Unlock()

	return s.stream.Send(message)
}

// outcomeFor maps a handler result to its wire outcome. A completed task is
// SUCCESS even mid-drain (never discard finished work); an explicit SkipRetry
// is honored next; a task canceled by the drain is RELEASED (no retry
// penalty); everything else — genuine errors, deadlines, server cancels — is
// RETRY, leaving the server to apply its policy.
func outcomeFor(ctx context.Context, err error) conveyorv1.TaskOutcome {
	switch {
	case err == nil:
		return conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS

	case IsSkipRetry(err):
		return conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY

	case errors.Is(context.Cause(ctx), errDraining):
		return conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED

	default:
		return conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY
	}
}

// sdkVersion reports the conveyor module version baked into the binary.
func sdkVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return unknownVersion
	}

	if info.Main.Path == modulePath && info.Main.Version != "" {
		return info.Main.Version
	}

	for _, dependency := range info.Deps {
		if dependency.Path == modulePath {
			return dependency.Version
		}
	}

	return unknownVersion
}
