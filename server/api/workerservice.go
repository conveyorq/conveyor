package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/tochemey/conveyor/internal/actors"
	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
	"github.com/tochemey/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// heartbeatDivisor derives the worker heartbeat interval from the lease
// TTL: three heartbeats per lease keeps one lost heartbeat harmless.
const heartbeatDivisor = 3

// gatewayStopTimeout bounds gateway drain and shutdown on session close.
const gatewayStopTimeout = 15 * time.Second

// errSessionSetupFailed is the client-visible error when the gateway
// cannot be spawned; details stay in the server log.
var errSessionSetupFailed = errors.New("session setup failed")

// WorkerService serves the worker session protocol: one bidirectional
// stream per worker process, bridged to a per-session gateway actor.
type WorkerService struct {
	// engine spawns the gateway actor of each accepted session.
	engine *actors.Engine
	// logger reports session lifecycle and failures.
	logger *slog.Logger
}

// enforce interface compliance at compile time.
var _ conveyorv1connect.WorkerServiceHandler = (*WorkerService)(nil)

// NewWorkerService assembles the worker session service.
func NewWorkerService(engine *actors.Engine, logger *slog.Logger) *WorkerService {
	return &WorkerService{engine: engine, logger: logger}
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

	for {
		message, err := stream.Receive()

		if errors.Is(err, io.EOF) {
			s.logger.Info("worker session closed", "session_id", sessionID)

			return nil
		}

		if err != nil {
			s.logger.Info("worker session ended", "session_id", sessionID, "error", err)

			return err
		}

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
