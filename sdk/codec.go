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
	"encoding/json"
	"fmt"

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
// JSON or Bytes; an encoding failure is carried inside and surfaces from
// Client.Enqueue, so call sites stay single-expression.
type Payload struct {
	// data is the encoded payload.
	data []byte
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
