// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// A gateway never writes to the worker's stream inside its turn: a send can
// wait on HTTP/2 flow control, and a turn that waits holds a worker of the
// dispatcher pool every actor and grain on the node shares. Frames go into an
// outbox in the gateway's state instead, and one PipeTo task at a time sends a
// snapshot of it in order, so Dispatch, BatchDispatch, and Cancel frames reach
// the worker in the order the gateway decided them. The flush reports back as
// a framesFlushed message, and what each frame was for (start a payload
// stream, release an undeliverable task) happens in the turn that applies it.
// Payload chunks are the one exception: their runs are sent by the payload
// streams themselves, and a stream only starts once its dispatch frame left.

// outboundFrame is one frame queued for the worker.
type outboundFrame struct {
	// frame is the frame to send.
	frame *conveyorv1.ServerMessage
	// delivery is the dispatch the frame carries; nil for a frame that needs
	// no follow-up, such as Cancel.
	delivery *outboundDelivery
}

// outboundDelivery is a dispatch whose send the gateway follows up on.
type outboundDelivery struct {
	// leaseID is the delivery's lease.
	leaseID string
	// taskIDs are the delivered tasks; a batch carries every member.
	taskIDs []string
	// batch records whether the frame is a BatchDispatch.
	batch bool
	// streamed are the delivered tasks whose chunked payloads follow the
	// frame as PayloadChunk frames; empty when no payload is streamed.
	streamed []*conveyorv1.TaskEnvelope
	// failure builds the outcome a payload read failure applies.
	failure func(err error) proto.Message
}

// framesFlushed reports one outbox flush: the first sent frames reached the
// stream, and err, when set, is the send failure that stopped the rest.
type framesFlushed struct {
	// flushID identifies the flush, so a report from before a restart is
	// never taken for the current flush.
	flushID uint64
	// frames is the flushed snapshot, in send order.
	frames []outboundFrame
	// sent is how many frames reached the stream.
	sent int
	// err is the send failure that stopped the flush.
	err error
}

// brokerWritten reports that a PipeTo task finished best-effort broker writes
// (lease releases, progress) whose failures it already logged; nothing
// follows in the turn.
type brokerWritten struct{}

// leaseRef names one task's lease, for broker writes made off the turn.
type leaseRef struct {
	// taskID is the leased task.
	taskID string
	// leaseID is the lease the write is scoped to.
	leaseID string
}

// enqueueFrame queues a frame for the worker and flushes when no flush is in
// flight.
func (g *Gateway) enqueueFrame(ctx *goakt.ReceiveContext, frame outboundFrame) {
	g.outbox = append(g.outbox, frame)
	g.flushOutbox(ctx)
}

// flushOutbox sends the queued frames, in order, in one PipeTo task. A frame
// whose delivery is no longer in flight (resolved, or drained when the session
// closed) is dropped rather than sent, so a released task is never handed to a
// worker that no longer owns it.
func (g *Gateway) flushOutbox(ctx *goakt.ReceiveContext) {
	if g.flushing || len(g.outbox) == 0 {
		return
	}

	frames := make([]outboundFrame, 0, len(g.outbox))

	for _, frame := range g.outbox {
		if frame.delivery != nil && !g.deliveryInFlight(frame.delivery) {
			continue
		}

		frames = append(frames, frame)
	}

	g.outbox = nil

	if len(frames) == 0 {
		return
	}

	g.flushing = true
	g.nextFlushID++
	flushID := g.nextFlushID
	sender := g.sender

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		for index, frame := range frames {
			if err := sender.Send(frame.frame); err != nil {
				return framesFlushed{flushID: flushID, frames: frames, sent: index, err: err}, nil
			}
		}

		return framesFlushed{flushID: flushID, frames: frames, sent: len(frames)}, nil
	})
}

// outboxFlushed applies a finished flush: a sent dispatch starts streaming its
// chunked payloads, an unsent one is released (the stream is broken), and the
// next flush starts with whatever was queued meanwhile.
func (g *Gateway) outboxFlushed(ctx *goakt.ReceiveContext, message framesFlushed) {
	if message.flushID != g.nextFlushID {
		return
	}

	g.flushing = false

	for _, frame := range message.frames[:message.sent] {
		delivery := frame.delivery
		if delivery == nil || len(delivery.streamed) == 0 || !g.deliveryInFlight(delivery) {
			continue
		}

		g.startPayloadStreams(ctx, delivery.streamed, delivery.failure)
	}

	for _, frame := range message.frames[message.sent:] {
		g.frameUndeliverable(ctx, frame, message.err)
	}

	g.flushOutbox(ctx)
}

// frameUndeliverable handles a frame the broken stream refused: a dispatch is
// released for immediate redelivery, anything else is only logged.
func (g *Gateway) frameUndeliverable(ctx *goakt.ReceiveContext, frame outboundFrame, sendErr error) {
	delivery := frame.delivery
	if delivery == nil {
		g.runtime.Logger().Warn("frame send failed", "gateway", g.name, "error", sendErr)

		return
	}

	if delivery.batch {
		g.runtime.Logger().Warn("batch dispatch send failed; releasing group", "gateway", g.name, "error", sendErr)
		g.releaseBatch(ctx, delivery.leaseID, delivery.taskIDs)

		return
	}

	if _, ok := g.inflight[delivery.taskIDs[0]]; ok {
		g.releaseUndeliverable(ctx, delivery.taskIDs[0], delivery.leaseID, sendErr)
	}
}

// deliveryInFlight reports whether every task of a delivery is still tracked.
func (g *Gateway) deliveryInFlight(delivery *outboundDelivery) bool {
	for _, taskID := range delivery.taskIDs {
		if _, ok := g.inflight[taskID]; !ok {
			return false
		}
	}

	return true
}

// releaseUndeliverable drops a single delivery whose dispatch frame could not
// be sent and releases it for immediate redelivery: the stream is broken and
// the session handler is about to stop this gateway.
func (g *Gateway) releaseUndeliverable(ctx *goakt.ReceiveContext, taskID, leaseID string, sendErr error) {
	delete(g.inflight, taskID)
	g.endPayloadStreams(taskID)

	g.runtime.Counters().Active.Add(-1)
	g.runtime.Counters().Released.Add(1)
	g.runtime.Logger().Warn("dispatch send failed; task released", "task_id", taskID, "error", sendErr)

	pipeLeaseReleases(ctx, g.runtime, []leaseRef{{taskID: taskID, leaseID: leaseID}}, "releasing undeliverable task failed")
}

// releaseBatch releases an undeliverable batch's members back to pending and
// drops them from tracking; redelivery follows on resume or lease expiry.
func (g *Gateway) releaseBatch(ctx *goakt.ReceiveContext, leaseID string, ids []string) {
	refs := make([]leaseRef, 0, len(ids))

	for _, id := range ids {
		if _, ok := g.inflight[id]; !ok {
			continue
		}

		delete(g.inflight, id)
		g.endPayloadStreams(id)
		refs = append(refs, leaseRef{taskID: id, leaseID: leaseID})

		g.runtime.Counters().Active.Add(-1)
		g.runtime.Counters().Released.Add(1)
	}

	delete(g.batches, leaseID)

	pipeLeaseReleases(ctx, g.runtime, refs, "releasing undeliverable batch member failed")
}

// pipeLeaseReleases hands each lease back to the broker in a PipeTo task, so
// the tasks redeliver immediately with no retry penalty. A lease already lost
// to another delivery is fine; any other failure is logged under what, and
// lease expiry recovers the task. The actor receives brokerWritten when done.
func pipeLeaseReleases(ctx *goakt.ReceiveContext, runtime *Runtime, refs []leaseRef, what string) {
	if len(refs) == 0 {
		return
	}

	taskLog := runtime.Broker()
	logger := runtime.Logger()

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		for _, ref := range refs {
			err := taskLog.Release(context.Background(), ref.taskID, ref.leaseID)
			if err != nil && !errors.Is(err, broker.ErrLeaseLost) {
				logger.Warn(what, "task_id", ref.taskID, "error", err)
			}
		}

		return brokerWritten{}, nil
	})
}
