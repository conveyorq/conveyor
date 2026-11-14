// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// Task is a unit of work: what clients enqueue and what handlers receive.
// On the enqueue side only the type and payload are set; the remaining
// accessors are filled in for handler-side tasks.
type Task struct {
	// id is the task ULID; empty until the server assigns one.
	id string
	// queue is the queue the task belongs to.
	queue string
	// taskType is the handler routing key, e.g. "email:welcome".
	taskType string
	// payload is the encoded payload.
	payload []byte
	// payloadReader supplies a streamed payload: on the enqueue side, the
	// reader given to Stream; on the handler side, the chunks of a payload
	// delivered to a streaming handler.
	payloadReader io.Reader
	// contentType describes the payload encoding.
	contentType string
	// metadata carries user tags and trace propagation.
	metadata map[string]string
	// retried is how many times the task has been retried.
	retried int
	// maxRetry is the task's retry budget.
	maxRetry int
	// payloadErr is a deferred payload encoding failure.
	payloadErr error
}

// NewTask builds an enqueueable task of the given type. A payload encoding
// failure is carried inside and surfaces from Client.Enqueue.
func NewTask(taskType string, payload Payload) *Task {
	return &Task{
		taskType:      taskType,
		payload:       payload.data,
		payloadReader: payload.reader,
		contentType:   payload.contentType,
		payloadErr:    payload.err,
	}
}

// ID returns the task ULID; empty before the task is enqueued.
func (t *Task) ID() string { return t.id }

// Type returns the handler routing key.
func (t *Task) Type() string { return t.taskType }

// Queue returns the queue the task belongs to; empty before enqueue.
func (t *Task) Queue() string { return t.queue }

// Payload returns the raw encoded payload. It is nil for a payload a streaming
// handler receives (see Mux.HandleStreamFunc); read PayloadReader instead.
func (t *Task) Payload() []byte { return t.payload }

// PayloadReader returns the payload as a reader. A streaming handler reads a
// chunked payload here as it arrives; for any other task it reads Payload.
func (t *Task) PayloadReader() io.Reader {
	if t.payloadReader != nil {
		return t.payloadReader
	}

	return bytes.NewReader(t.payload)
}

// ContentType returns the payload encoding, e.g. "application/json".
func (t *Task) ContentType() string { return t.contentType }

// Retried returns how many times the task has been retried so far.
func (t *Task) Retried() int { return t.retried }

// MaxRetry returns the task's retry budget.
func (t *Task) MaxRetry() int { return t.maxRetry }

// Metadata returns the task's metadata tags; the map must not be mutated.
func (t *Task) Metadata() map[string]string { return t.metadata }

// Bind decodes the payload into v according to the content type: JSON
// payloads unmarshal into any value, binary payloads bind to *[]byte, and
// protobuf payloads bind to a proto.Message. Handlers typically wrap a
// Bind failure in SkipRetry: a payload that cannot decode now never will.
func (t *Task) Bind(v any) error {
	switch t.contentType {
	case ContentTypeJSON:
		if err := json.Unmarshal(t.payload, v); err != nil {
			return fmt.Errorf("conveyor: binding JSON payload: %w", err)
		}

		return nil

	case ContentTypeBytes:
		target, ok := v.(*[]byte)
		if !ok {
			return fmt.Errorf("conveyor: binary payloads bind to *[]byte, got %T", v)
		}

		*target = t.payload

		return nil

	case ContentTypeProto:
		message, ok := v.(proto.Message)
		if !ok {
			return fmt.Errorf("conveyor: protobuf payloads bind to a proto.Message, got %T", v)
		}

		if err := proto.Unmarshal(t.payload, message); err != nil {
			return fmt.Errorf("conveyor: binding protobuf payload: %w", err)
		}

		return nil

	default:
		return fmt.Errorf("conveyor: no codec for content type %q", t.contentType)
	}
}

// streamed reports whether the task is enqueued through the streaming upload:
// its payload is a reader, or too large to send inline.
func (t *Task) streamed() bool {
	return t.payloadReader != nil || len(t.payload) > inlinePayloadBytes
}
