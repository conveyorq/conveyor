package conveyor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptionsApply(t *testing.T) {
	settings := &options{}

	for _, opt := range []Option{
		WithToken("secret"),
		WithQueues(map[string]int{"critical": 6, "default": 3}),
		WithConcurrency(20),
	} {
		opt(settings)
	}

	require.Equal(t, "secret", settings.token)
	require.Equal(t, map[string]int{"critical": 6, "default": 3}, settings.queues)
	require.Equal(t, 20, settings.concurrency)
}

func TestEnqueueOptionsApply(t *testing.T) {
	processAt := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	settings := &enqueueOptions{}

	for _, opt := range []EnqueueOption{
		TaskID("01JXAMPLE"),
		Queue("critical"),
		MaxRetry(10),
		Priority(7),
		ProcessAt(processAt),
		ProcessIn(5 * time.Minute),
		Retention(48 * time.Hour),
	} {
		opt(settings)
	}

	require.Equal(t, "01JXAMPLE", settings.taskID)
	require.Equal(t, "critical", settings.queue)
	require.Equal(t, 10, settings.maxRetry)
	require.Equal(t, 7, settings.priority)
	require.Equal(t, processAt, settings.processAt)
	require.Equal(t, 5*time.Minute, settings.processIn)
	require.Equal(t, 48*time.Hour, settings.retention)
}
