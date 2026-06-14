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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMuxRoutesByType(t *testing.T) {
	mux := NewMux()

	mux.HandleFunc("email:welcome", func(context.Context, *Task) error { return nil })

	handler, ok := mux.handler("email:welcome")
	require.True(t, ok)
	require.NotNil(t, handler)

	_, ok = mux.handler("email:goodbye")
	require.False(t, ok)
}

func TestMuxHandleFuncPanicsOnEmptyType(t *testing.T) {
	require.PanicsWithValue(t, "conveyor: HandleFunc with empty task type", func() {
		NewMux().HandleFunc("", func(context.Context, *Task) error { return nil })
	})
}

func TestMuxHandleFuncPanicsOnNilHandler(t *testing.T) {
	require.PanicsWithValue(t, "conveyor: HandleFunc with nil handler", func() {
		NewMux().HandleFunc("email:welcome", nil)
	})
}

func TestMuxHandleFuncPanicsOnDuplicate(t *testing.T) {
	mux := NewMux()
	mux.HandleFunc("email:welcome", func(context.Context, *Task) error { return nil })

	require.Panics(t, func() {
		mux.HandleFunc("email:welcome", func(context.Context, *Task) error { return nil })
	})
}

func TestMuxUseWrapsHandlersInOrder(t *testing.T) {
	var calls []string

	record := func(name string) MiddlewareFunc {
		return func(next HandlerFunc) HandlerFunc {
			return func(ctx context.Context, task *Task) error {
				calls = append(calls, name)

				return next(ctx, task)
			}
		}
	}

	mux := NewMux()
	mux.Use(record("first"))

	mux.HandleFunc("email:welcome", func(context.Context, *Task) error {
		calls = append(calls, "handler")

		return nil
	})

	// Use after HandleFunc still applies: middleware wraps at dispatch.
	mux.Use(record("second"))

	handler, ok := mux.handler("email:welcome")
	require.True(t, ok)
	require.NoError(t, handler(context.Background(), &Task{}))
	require.Equal(t, []string{"first", "second", "handler"}, calls)
}

func TestMuxUsePanicsOnNilMiddleware(t *testing.T) {
	require.PanicsWithValue(t, "conveyor: Use with nil middleware", func() {
		NewMux().Use(nil)
	})
}

func TestMuxMiddlewareSeesHandlerError(t *testing.T) {
	handlerErr := errors.New("boom")

	var seen error

	mux := NewMux()

	mux.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, task *Task) error {
			seen = next(ctx, task)

			return seen
		}
	})

	mux.HandleFunc("email:welcome", func(context.Context, *Task) error { return handlerErr })

	handler, ok := mux.handler("email:welcome")
	require.True(t, ok)
	require.ErrorIs(t, handler(context.Background(), &Task{}), handlerErr)
	require.ErrorIs(t, seen, handlerErr)
}
