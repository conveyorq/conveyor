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

package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// heartbeatDivisor derives the worker heartbeat interval from the lease
// TTL: three heartbeats per lease keeps one lost heartbeat harmless.
const heartbeatDivisor = 3

// gatewayStopTimeout bounds gateway drain and shutdown on session close.
const gatewayStopTimeout = 15 * time.Second

// sessionDrainPoll is how often DrainSessions re-checks that every closed
// session has finished releasing its in-flight tasks.
const sessionDrainPoll = 50 * time.Millisecond

// errSessionSetupFailed is the client-visible error when the gateway
// cannot be spawned; details stay in the server log.
var errSessionSetupFailed = errors.New("session setup failed")

// errNodeDraining is the client-visible error when a session arrives on a
// node that is shutting down; the worker's reconnect loop lands it on
// another node.
var errNodeDraining = errors.New("node is shutting down; reconnect to another node")

// SessionSnapshot is the externally visible description of one connected
// worker session, surfaced by the AdminService worker-topology view.
type SessionSnapshot struct {
	// ID is the server-assigned session id.
	ID string
	// Queues are the queue names the worker serves, sorted.
	Queues []string
	// Concurrency is the worker's declared concurrency.
	Concurrency int32
	// SDKVersion is the worker's reported SDK version.
	SDKVersion string
	// ConnectedAt is when the session's Hello was accepted.
	ConnectedAt time.Time
}

// sessionEntry holds a live session's cancel function and its snapshot.
type sessionEntry struct {
	// cancel ends the session.
	cancel context.CancelFunc
	// snapshot describes the session for the topology view.
	snapshot SessionSnapshot
}

// WorkerService serves the worker session protocol: one bidirectional
// stream per worker process, bridged to a per-session gateway actor.
type WorkerService struct {
	// engine spawns the gateway actor of each accepted session.
	engine *actors.Engine
	// logger reports session lifecycle and failures.
	logger *slog.Logger
	// timeSource stamps session connection times.
	timeSource clock.Clock
	// sessionMutex guards sessions and draining.
	sessionMutex sync.Mutex
	// sessions maps live session ids to their entry.
	sessions map[string]*sessionEntry
	// draining rejects new sessions once shutdown has begun.
	draining bool
}

// enforce interface compliance at compile time.
var _ conveyorv1connect.WorkerServiceHandler = (*WorkerService)(nil)

// NewWorkerService assembles the worker session service.
func NewWorkerService(engine *actors.Engine, logger *slog.Logger, timeSource clock.Clock) *WorkerService {
	return &WorkerService{
		engine:     engine,
		logger:     logger,
		timeSource: timeSource,
		sessions:   make(map[string]*sessionEntry),
	}
}

// ActiveSessions returns the number of live worker sessions.
func (s *WorkerService) ActiveSessions() int64 {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()

	return int64(len(s.sessions))
}

// Sessions returns a snapshot of the live worker sessions on this node,
// sorted by session id for a stable ordering.
func (s *WorkerService) Sessions() []SessionSnapshot {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()

	snapshots := make([]SessionSnapshot, 0, len(s.sessions))
	for _, entry := range s.sessions {
		snapshots = append(snapshots, entry.snapshot)
	}

	slices.SortFunc(snapshots, func(a, b SessionSnapshot) int {
		return strings.Compare(a.ID, b.ID)
	})

	return snapshots
}

// DrainSessions ends every live worker session and rejects new ones, then
// waits until each closed session has stopped its gateway — releasing all
// in-flight tasks for immediate redelivery — or the context lapses. The
// server runs it before stopping the engine: once the actor system begins
// stopping it rejects all user messages, and gateways can no longer
// process drain requests.
func (s *WorkerService) DrainSessions(ctx context.Context) error {
	s.sessionMutex.Lock()
	s.draining = true

	cancels := make([]context.CancelFunc, 0, len(s.sessions))
	for _, entry := range s.sessions {
		cancels = append(cancels, entry.cancel)
	}

	s.sessionMutex.Unlock()

	if len(cancels) == 0 {
		return nil
	}

	s.logger.Info("draining worker sessions for shutdown", "sessions", len(cancels))

	for _, cancel := range cancels {
		cancel()
	}

	ticker := time.NewTicker(sessionDrainPoll)
	defer ticker.Stop()

	for {
		s.sessionMutex.Lock()
		remaining := len(s.sessions)
		s.sessionMutex.Unlock()

		if remaining == 0 {
			s.logger.Info("worker sessions drained")

			return nil
		}

		select {
		case <-ctx.Done():
			s.logger.Warn("session drain incomplete; lease expiry will recover", "remaining", remaining)

			return ctx.Err()

		case <-ticker.C:
		}
	}
}

// registerSession admits one accepted session into the registry, or
// reports that the node is draining.
func (s *WorkerService) registerSession(snapshot SessionSnapshot, cancel context.CancelFunc) error {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()

	if s.draining {
		return errNodeDraining
	}

	s.sessions[snapshot.ID] = &sessionEntry{cancel: cancel, snapshot: snapshot}

	return nil
}

// unregisterSession removes one session from the registry. It runs after
// the session's gateway has stopped, so an empty registry means every
// in-flight task has been released.
func (s *WorkerService) unregisterSession(sessionID string) {
	s.sessionMutex.Lock()
	defer s.sessionMutex.Unlock()

	delete(s.sessions, sessionID)
}

// streamSender adapts the session stream to the gateway's FrameSender.
// Dispatch frames arrive from the gateway actor while protocol frames are
// sent from the session loop, so sends are serialized by a mutex.
type streamSender struct {
	// mutex serializes writes to the stream.
	mutex sync.Mutex
	// stream is the live session stream.
	stream *connect.BidiStream[conveyorv1.WorkerMessage, conveyorv1.ServerMessage]
}

// Send implements actors.FrameSender.
func (s *streamSender) Send(message *conveyorv1.ServerMessage) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.stream.Send(message)
}

// Session implements the worker stream protocol: the first frame must be
// Hello, the server answers Welcome, and every later frame is validated
// and forwarded to the session's gateway actor. Any protocol violation
// ends the stream; the deferred gateway stop releases all in-flight tasks
// for immediate redelivery, whatever the reason the stream ended.
func (s *WorkerService) Session(ctx context.Context, stream *connect.BidiStream[conveyorv1.WorkerMessage, conveyorv1.ServerMessage]) error {
	first, err := stream.Receive()
	if err != nil {
		return err
	}

	state := &sessionState{}
	if err := state.check(first); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	hello := first.GetHello()
	sessionID := s.engine.NewID()
	queues := slices.Sorted(maps.Keys(hello.GetQueues()))
	sender := &streamSender{stream: stream}

	session := actors.GatewaySession{
		SessionID:   sessionID,
		Queues:      queues,
		Concurrency: hello.GetConcurrency(),
	}

	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	snapshot := SessionSnapshot{
		ID:          sessionID,
		Queues:      queues,
		Concurrency: hello.GetConcurrency(),
		SDKVersion:  hello.GetSdkVersion(),
		ConnectedAt: s.timeSource.Now(),
	}

	if err := s.registerSession(snapshot, sessionCancel); err != nil {
		return connect.NewError(connect.CodeUnavailable, err)
	}

	// Unregistration is deferred first so it runs after the gateway stop
	// below: an empty registry guarantees every in-flight task has been
	// released, which is what DrainSessions waits for.
	defer s.unregisterSession(sessionID)

	handle, err := s.engine.SpawnGateway(ctx, session, sender)
	if err != nil {
		s.logger.Error("gateway spawn failed", "session_id", sessionID, "error", err)

		return connect.NewError(connect.CodeInternal, errSessionSetupFailed)
	}

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), gatewayStopTimeout)
		defer cancel()

		if err := handle.Stop(stopCtx); err != nil {
			s.logger.Warn("gateway stop failed", "session_id", sessionID, "error", err)
		}
	}()

	leaseTTL := s.engine.Settings().LeaseTTL

	welcome := &conveyorv1.ServerMessage{
		Frame: &conveyorv1.ServerMessage_Welcome{
			Welcome: &conveyorv1.Welcome{
				SessionId:         sessionID,
				LeaseTtl:          durationpb.New(leaseTTL),
				HeartbeatInterval: durationpb.New(leaseTTL / heartbeatDivisor),
			},
		},
	}

	if err := sender.Send(welcome); err != nil {
		return err
	}

	s.logger.Info("worker session opened", "session_id", sessionID, "queues", queues,
		"concurrency", hello.GetConcurrency(), "sdk_version", hello.GetSdkVersion())

	// Receiving runs in its own goroutine so the session loop can also
	// react to a drain: a blocked Receive only ends when the stream does.
	frames := make(chan *conveyorv1.WorkerMessage)
	receiveErrs := make(chan error, 1)

	go func() {
		for {
			message, err := stream.Receive()
			if err != nil {
				select {
				case receiveErrs <- err:
				case <-sessionCtx.Done():
				}

				return
			}

			select {
			case frames <- message:
			case <-sessionCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-sessionCtx.Done():
			// Drain or request teardown: frames already received are
			// still applied — dropping a buffered Result would re-run a
			// task its worker completed. Then returning closes the
			// stream and the deferred gateway stop releases everything
			// genuinely still in flight.
			s.flushFrames(ctx, sessionID, state, handle, frames)
			s.logger.Info("worker session closed by server", "session_id", sessionID)

			return nil

		case err := <-receiveErrs:
			if errors.Is(err, io.EOF) {
				s.logger.Info("worker session closed", "session_id", sessionID)

				return nil
			}

			s.logger.Info("worker session ended", "session_id", sessionID, "error", err)

			return err

		case message := <-frames:
			if err := state.check(message); err != nil {
				s.logger.Warn("worker session protocol violation", "session_id", sessionID, "error", err)

				return connect.NewError(connect.CodeInvalidArgument, err)
			}

			if err := s.forward(ctx, handle, message); err != nil {
				s.logger.Error("frame forwarding failed", "session_id", sessionID, "error", err)

				return connect.NewError(connect.CodeInternal, errSessionSetupFailed)
			}
		}
	}
}

// flushFrames applies the frames that were already received when the
// session was told to close. Validation failures are only logged: the
// session is ending either way.
func (s *WorkerService) flushFrames(ctx context.Context, sessionID string, state *sessionState, handle *actors.GatewayHandle, frames <-chan *conveyorv1.WorkerMessage) {
	for {
		select {
		case message := <-frames:
			if err := state.check(message); err != nil {
				s.logger.Warn("frame dropped at session close", "session_id", sessionID, "error", err)

				continue
			}

			if err := s.forward(ctx, handle, message); err != nil {
				s.logger.Warn("frame forwarding failed at session close", "session_id", sessionID, "error", err)
			}

		default:
			return
		}
	}
}

// forward hands one validated worker frame to the gateway actor.
func (s *WorkerService) forward(ctx context.Context, handle *actors.GatewayHandle, message *conveyorv1.WorkerMessage) error {
	switch frame := message.GetFrame().(type) {
	case *conveyorv1.WorkerMessage_Credit:
		return handle.Tell(ctx, frame.Credit)

	case *conveyorv1.WorkerMessage_Result:
		return handle.Tell(ctx, frame.Result)

	case *conveyorv1.WorkerMessage_Heartbeat:
		return handle.Tell(ctx, frame.Heartbeat)

	default:
		// The state machine admits no other frame past Hello.
		return nil
	}
}
