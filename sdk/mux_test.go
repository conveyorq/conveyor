package conveyor

import (
	"context"
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
