// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"encoding/json"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

// Content types produced by the built-in payload constructors.
const (
	// ContentTypeJSON marks a JSON-encoded payload, the default codec.
	ContentTypeJSON = "application/json"
	// ContentTypeBytes marks an opaque binary payload.
	ContentTypeBytes = "application/octet-stream"
	// ContentTypeProto marks a protobuf-encoded payload.
	ContentTypeProto = "application/x-protobuf"
)

// Payload is an encoded task payload plus its content type. Build one with
// JSON, Bytes, Proto, or Stream; an encoding failure is carried inside and
// surfaces from Client.Enqueue, so call sites stay single-expression.
type Payload struct {
	// data is the encoded payload.
	data []byte
	// reader supplies a streamed payload; nil for an in-memory one.
	reader io.Reader
	// contentType describes the encoding.
	contentType string
	// err is a deferred encoding failure.
	err error
}

// JSON encodes v as the task payload with content type application/json.
func JSON(v any) Payload {
	data, err := json.Marshal(v)
	if err != nil {
		return Payload{err: fmt.Errorf("conveyor: encoding JSON payload: %w", err)}
	}

	return Payload{data: data, contentType: ContentTypeJSON}
}

// Bytes uses b verbatim as the task payload with content type
// application/octet-stream.
func Bytes(b []byte) Payload {
	return Payload{data: b, contentType: ContentTypeBytes}
}

// Proto encodes m as the task payload with content type
// application/x-protobuf. An opt-in convenience: the payload is still
// opaque bytes on the wire, and the handler side binds it back with
// Task.Bind into a value of the same message type.
func Proto(m proto.Message) Payload {
	data, err := proto.Marshal(m)
	if err != nil {
		return Payload{err: fmt.Errorf("conveyor: encoding protobuf payload: %w", err)}
	}

	return Payload{data: data, contentType: ContentTypeProto}
}

// Stream supplies the task payload from r with the given content type, for a
// payload too large to hold in memory. Client.Enqueue uploads it in chunks
// through the server's streaming enqueue, reading r to its end, and the
// worker side receives it chunked (see Task.PayloadReader). A streamed
// payload cannot be enqueued with EnqueueTx, and Unique needs an explicit
// UniqueKey since the payload is never held whole to hash.
func Stream(r io.Reader, contentType string) Payload {
	if r == nil {
		return Payload{err: fmt.Errorf("conveyor: Stream needs a reader")}
	}

	return Payload{reader: r, contentType: contentType}
}
