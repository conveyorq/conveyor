// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"bytes"
	"context"
	"fmt"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// PayloadChunkWindow is how many PayloadChunk frames of one task a gateway
// sends ahead of the worker's PayloadAck credit. The session advertises it in
// Welcome, so a worker can buffer every chunk it is sent and never has to stop
// reading its stream: a slow handler holds back only its own payload, never
// the session or the gateway.
const PayloadChunkWindow = 16

// payloadReadFailurePrefix prefixes the retryable failure recorded when a
// gateway cannot read a chunked payload from the broker.
const payloadReadFailurePrefix = "reading chunked payload: "

// A chunked payload is streamed by the gateway itself, as mailbox state: each
// payloadStream records how far its payload got and how much credit the
// worker has granted, and every run of chunks is read and sent by a PipeTo
// task whose outcome returns as a chunksSent message. A PayloadAck grants more
// credit and starts the next run. Nothing outside a turn touches the state.

// payloadStream is one chunked payload being streamed to the session.
type payloadStream struct {
	// id identifies this stream among every stream the gateway has run, so a
	// run reported after its stream ended cannot be applied to a later stream
	// of the same task (a released task can come straight back).
	id uint64
	// next is the next chunk to send.
	next int
	// total is the payload's chunk count.
	total int
	// credit is how many more chunks the worker has room for.
	credit int
	// sending records that a run of chunks is in flight.
	sending bool
	// failure builds the outcome a read failure applies: a retryable Result,
	// or a BatchResult failing every member of the task's batch.
	failure func(err error) proto.Message
}

// chunksSent reports one run of chunks a PipeTo task read and sent.
type chunksSent struct {
	// taskID is the task whose chunks were sent.
	taskID string
	// streamID is the stream the run belonged to.
	streamID uint64
	// sent is how many chunks reached the stream.
	sent int
	// readErr is the broker failure that ended the run, if any.
	readErr error
	// broken records that the stream failed; the session is ending.
	broken bool
}

// assembledDispatch carries a dispatch frame whose chunked payloads were
// reassembled inline off the turn, for a worker that does not stream
// payloads. The gateway queues it in its outbox like any other dispatch.
type assembledDispatch struct {
	// frame is the ready-to-queue dispatch frame and its delivery.
	frame outboundFrame
}

// sendDispatch queues the dispatch frame build makes for tasks. Chunked
// payloads follow the frame as a stream for a session that streams payloads;
// for any other session they are reassembled inline off the turn first, and
// the frame is queued once they are read. failure builds the outcome a payload
// read failure applies to the delivery.
func (g *Gateway) sendDispatch(ctx *goakt.ReceiveContext, delivery *outboundDelivery, tasks []*conveyorv1.TaskEnvelope, build func(tasks []*conveyorv1.TaskEnvelope) *conveyorv1.ServerMessage, failure func(err error) proto.Message) {
	if !hasChunkedPayload(tasks...) {
		g.enqueueFrame(ctx, outboundFrame{frame: build(tasks), delivery: delivery})

		return
	}

	if g.session.PayloadStreaming {
		delivery.streamed = tasks
		delivery.failure = failure
		g.enqueueFrame(ctx, outboundFrame{frame: build(tasks), delivery: delivery})

		return
	}

	taskLog := g.runtime.Broker()
	lease := leaseKeeper{taskLog: taskLog, leaseID: delivery.leaseID, ttl: g.runtime.Settings().LeaseTTL}

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		assembled, err := assemblePayloads(context.Background(), taskLog, tasks, lease)
		if err != nil {
			return failure(err), nil
		}

		return assembledDispatch{frame: outboundFrame{frame: build(assembled), delivery: delivery}}, nil
	})
}

// hasChunkedPayload reports whether any task carries a chunked payload.
func hasChunkedPayload(tasks ...*conveyorv1.TaskEnvelope) bool {
	for _, task := range tasks {
		if task.GetPayloadChunked() {
			return true
		}
	}

	return false
}

// startPayloadStreams begins streaming every chunked payload among tasks,
// whose Dispatch or BatchDispatch was just sent. failure builds the outcome
// applied if a payload cannot be read.
func (g *Gateway) startPayloadStreams(ctx *goakt.ReceiveContext, tasks []*conveyorv1.TaskEnvelope, failure func(err error) proto.Message) {
	for _, task := range tasks {
		if !task.GetPayloadChunked() {
			continue
		}

		g.nextStreamID++
		g.payloadStreams[task.GetId()] = &payloadStream{
			id:      g.nextStreamID,
			total:   int(task.GetPayloadChunks()),
			credit:  PayloadChunkWindow,
			failure: failure,
		}

		g.pumpPayload(ctx, task.GetId())
	}
}

// pumpPayload starts the next run of a payload's chunks when none is in flight
// and the worker has credit. The run that reaches the end of the payload also
// sends its closing frame; a payload of no chunks sends only that.
func (g *Gateway) pumpPayload(ctx *goakt.ReceiveContext, taskID string) {
	stream, ok := g.payloadStreams[taskID]
	if !ok || stream.sending {
		return
	}

	count := min(stream.credit, stream.total-stream.next)
	last := stream.next+count == stream.total

	if count == 0 && !last {
		return
	}

	stream.sending = true
	streamID := stream.id
	from := stream.next
	taskLog := g.runtime.Broker()
	sender := g.sender

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		sent := sendChunkRun(context.Background(), taskLog, sender, taskID, from, count, last)
		sent.streamID = streamID

		return sent, nil
	})
}

// payloadSent applies one finished run: a read failure fails the delivery, a
// broken stream ends the stream (the session's drain releases the task), and
// otherwise the payload advances and the next run starts if credit allows.
func (g *Gateway) payloadSent(ctx *goakt.ReceiveContext, message chunksSent) {
	stream, ok := g.payloadStreams[message.taskID]
	if !ok || stream.id != message.streamID {
		return
	}

	stream.sending = false
	stream.next += message.sent
	stream.credit -= message.sent

	if message.readErr != nil {
		g.runtime.Logger().Warn("reading chunked payload failed; delivery retried", "task_id", message.taskID, "error", message.readErr)
		g.endPayloadStreams(message.taskID)
		g.applyPayloadFailure(ctx, stream.failure(message.readErr))

		return
	}

	if message.broken || stream.next == stream.total {
		g.endPayloadStreams(message.taskID)

		return
	}

	g.pumpPayload(ctx, message.taskID)
}

// payloadAck grants a payload the credit the worker returned and resumes it.
// Credit never exceeds the window, so an over-generous ack cannot make the
// gateway outrun the worker's buffer.
func (g *Gateway) payloadAck(ctx *goakt.ReceiveContext, message *conveyorv1.PayloadAck) {
	stream, ok := g.payloadStreams[message.GetTaskId()]
	if !ok {
		return
	}

	stream.credit = min(stream.credit+int(message.GetChunks()), PayloadChunkWindow)
	g.pumpPayload(ctx, message.GetTaskId())
}

// applyPayloadFailure resolves a delivery whose payload could not be read
// through the same path as a worker-reported outcome.
func (g *Gateway) applyPayloadFailure(ctx *goakt.ReceiveContext, outcome proto.Message) {
	switch message := outcome.(type) {
	case *conveyorv1.BatchResult:
		g.batchResult(ctx, message)

	case *conveyorv1.Result:
		g.result(ctx, message)
	}
}

// endPayloadStreams stops streaming the given tasks' payloads. A run already in
// flight finishes, but its outcome is then ignored.
func (g *Gateway) endPayloadStreams(taskIDs ...string) {
	for _, taskID := range taskIDs {
		delete(g.payloadStreams, taskID)
	}
}

// sendChunkRun reads count chunks of a payload from chunk from and sends them
// as PayloadChunk frames, followed by the closing frame when last is set. A read
// failure is sent as an error frame and reported. It is a PipeTo task body:
// sender is safe for concurrent use with the gateway's own sends.
func sendChunkRun(ctx context.Context, taskLog broker.Broker, sender FrameSender, taskID string, from, count int, last bool) chunksSent {
	sent := chunksSent{taskID: taskID}

	for data, err := range taskLog.ReadPayload(ctx, taskID, from, count) {
		if err != nil {
			sendChunk(sender, &conveyorv1.PayloadChunk{TaskId: taskID, Last: true, Error: err.Error()})
			sent.readErr = err

			return sent
		}

		if !sendChunk(sender, &conveyorv1.PayloadChunk{TaskId: taskID, Data: data}) {
			sent.broken = true

			return sent
		}

		sent.sent++
	}

	if last && !sendChunk(sender, &conveyorv1.PayloadChunk{TaskId: taskID, Last: true}) {
		sent.broken = true
	}

	return sent
}

// sendChunk pushes one PayloadChunk frame, reporting whether the stream is
// intact.
func sendChunk(sender FrameSender, chunk *conveyorv1.PayloadChunk) bool {
	return sender.Send(&conveyorv1.ServerMessage{Frame: &conveyorv1.ServerMessage_PayloadChunk{PayloadChunk: chunk}}) == nil
}

// leaseKeeper renews a delivery's lease while its payload is reassembled for a
// worker that does not stream payloads. That worker learns of the task only
// when the reassembled frame arrives, so it cannot heartbeat it meanwhile; a
// reassembly that outlived the lease would otherwise let the reaper hand the
// task to another worker while this one is still about to run it.
type leaseKeeper struct {
	// taskLog is the broker the lease lives in.
	taskLog broker.Broker
	// leaseID is the delivery's lease.
	leaseID string
	// ttl is how far each renewal extends the lease.
	ttl time.Duration
}

// renew extends the lease of one task, failing with broker.ErrLeaseLost when
// another delivery owns it now.
func (k leaseKeeper) renew(ctx context.Context, taskID string) error {
	return k.taskLog.ExtendLease(ctx, taskID, k.leaseID, k.ttl)
}

// assemblePayloads returns tasks with every chunked payload read inline into a
// clone; the originals are never mutated. Each task's lease is renewed every
// PayloadChunkWindow chunks while it is read. It is a PipeTo task body.
func assemblePayloads(ctx context.Context, taskLog broker.Broker, tasks []*conveyorv1.TaskEnvelope, lease leaseKeeper) ([]*conveyorv1.TaskEnvelope, error) {
	assembled := make([]*conveyorv1.TaskEnvelope, len(tasks))

	for index, task := range tasks {
		if !task.GetPayloadChunked() {
			assembled[index] = task

			continue
		}

		taskID := task.GetId()

		payload, err := readChunkedPayload(ctx, taskLog, task, func() error { return lease.renew(ctx, taskID) })
		if err != nil {
			return nil, err
		}

		inline := proto.Clone(task).(*conveyorv1.TaskEnvelope)
		inline.Payload = payload
		inline.PayloadChunked = false
		assembled[index] = inline
	}

	return assembled, nil
}

// readChunkedPayload reads a whole chunked payload into one buffer, for a
// consumer that cannot take it in chunks. renew, when set, runs after every
// PayloadChunkWindow chunks to keep the delivery's lease alive; its failure
// ends the read.
func readChunkedPayload(ctx context.Context, taskLog broker.Broker, task *conveyorv1.TaskEnvelope, renew func() error) ([]byte, error) {
	var payload bytes.Buffer

	read := 0

	for data, err := range taskLog.ReadPayload(ctx, task.GetId(), 0, int(task.GetPayloadChunks())) {
		if err != nil {
			return nil, err
		}

		payload.Write(data)
		read++

		if renew == nil || read%PayloadChunkWindow != 0 {
			continue
		}

		if err = renew(); err != nil {
			return nil, err
		}
	}

	return payload.Bytes(), nil
}

// retryResult is the outcome of a single delivery whose payload could not be
// read: a retryable failure, so the task backs off and retries.
func retryResult(taskID string, err error) *conveyorv1.Result {
	return &conveyorv1.Result{
		TaskId:   taskID,
		Outcome:  conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
		ErrorMsg: fmt.Sprintf("%s%v", payloadReadFailurePrefix, err),
	}
}

// retryBatchResult is the outcome of a batch whose payload could not be read:
// every member fails retryably.
func retryBatchResult(tasks []*conveyorv1.TaskEnvelope, err error) *conveyorv1.BatchResult {
	results := make([]*conveyorv1.Result, 0, len(tasks))

	for _, task := range tasks {
		results = append(results, retryResult(task.GetId(), err))
	}

	return &conveyorv1.BatchResult{Results: results}
}
