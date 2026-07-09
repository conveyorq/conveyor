// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// validWebhookWorkerMessage builds a wire registration that passes
// validation, optionally mutated to produce one specific violation.
func validWebhookWorkerMessage(mutate func(*conveyorv1.WebhookWorker)) *conveyorv1.WebhookWorker {
	worker := &conveyorv1.WebhookWorker{
		Name:        "hooks",
		Url:         "https://example.com/tasks",
		Queues:      map[string]int32{"default": 1},
		Concurrency: 4,
		Secrets:     []string{"secret"},
	}

	if mutate != nil {
		mutate(worker)
	}

	return worker
}

func TestWebhookWorkerAdminLifecycle(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)
	admin := NewAdminService(engine, taskLog, clock.System(), stubSessions(nil), true)

	worker := validWebhookWorkerMessage(func(w *conveyorv1.WebhookWorker) {
		w.BatchTypes = []string{"report:batch"}
		w.RequestTimeout = durationpb.New(45 * time.Second)
	})

	_, err := admin.UpsertWebhookWorker(ctx, connect.NewRequest(&conveyorv1.UpsertWebhookWorkerRequest{Worker: worker}))
	require.NoError(t, err)

	listed, err := admin.ListWebhookWorkers(ctx, connect.NewRequest(&conveyorv1.ListWebhookWorkersRequest{}))
	require.NoError(t, err)
	require.Len(t, listed.Msg.GetWorkers(), 1)

	got := listed.Msg.GetWorkers()[0]
	require.Equal(t, "hooks", got.GetName())
	require.Equal(t, "https://example.com/tasks", got.GetUrl())
	require.EqualValues(t, 4, got.GetConcurrency())
	require.Equal(t, []string{"report:batch"}, got.GetBatchTypes())
	require.Equal(t, 45*time.Second, got.GetRequestTimeout().AsDuration())
	require.Empty(t, got.GetSecrets(), "list responses must redact the signing secrets")

	// The stored registration keeps its secrets even though listing hides
	// them.
	stored, err := taskLog.GetWebhookWorker(ctx, "hooks")
	require.NoError(t, err)
	require.Equal(t, []string{"secret"}, stored.Secrets)

	_, err = admin.PauseWebhookWorker(ctx, connect.NewRequest(&conveyorv1.PauseWebhookWorkerRequest{Name: "hooks"}))
	require.NoError(t, err)

	stored, err = taskLog.GetWebhookWorker(ctx, "hooks")
	require.NoError(t, err)
	require.True(t, stored.Paused)

	_, err = admin.ResumeWebhookWorker(ctx, connect.NewRequest(&conveyorv1.ResumeWebhookWorkerRequest{Name: "hooks"}))
	require.NoError(t, err)

	stored, err = taskLog.GetWebhookWorker(ctx, "hooks")
	require.NoError(t, err)
	require.False(t, stored.Paused)

	_, err = admin.DeleteWebhookWorker(ctx, connect.NewRequest(&conveyorv1.DeleteWebhookWorkerRequest{Name: "hooks"}))
	require.NoError(t, err)

	listed, err = admin.ListWebhookWorkers(ctx, connect.NewRequest(&conveyorv1.ListWebhookWorkersRequest{}))
	require.NoError(t, err)
	require.Empty(t, listed.Msg.GetWorkers())

	// Deleting an absent registration is a no-op; pausing one is not found.
	_, err = admin.DeleteWebhookWorker(ctx, connect.NewRequest(&conveyorv1.DeleteWebhookWorkerRequest{Name: "hooks"}))
	require.NoError(t, err)

	_, err = admin.PauseWebhookWorker(ctx, connect.NewRequest(&conveyorv1.PauseWebhookWorkerRequest{Name: "hooks"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestWebhookWorkerAdminRejectsEmptyName proves the name-required guard on
// every registration handler that takes a name.
func TestWebhookWorkerAdminRejectsEmptyName(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)
	admin := NewAdminService(engine, taskLog, clock.System(), stubSessions(nil), true)

	_, err := admin.PauseWebhookWorker(ctx, connect.NewRequest(&conveyorv1.PauseWebhookWorkerRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = admin.ResumeWebhookWorker(ctx, connect.NewRequest(&conveyorv1.ResumeWebhookWorkerRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = admin.DeleteWebhookWorker(ctx, connect.NewRequest(&conveyorv1.DeleteWebhookWorkerRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestUpsertWebhookWorkerValidation(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)
	admin := NewAdminService(engine, taskLog, clock.System(), stubSessions(nil), true)

	cases := []struct {
		name   string
		mutate func(*conveyorv1.WebhookWorker)
	}{
		{"bad name", func(w *conveyorv1.WebhookWorker) { w.Name = "-bad" }},
		{"bad url", func(w *conveyorv1.WebhookWorker) { w.Url = "ftp://example.com" }},
		{"no queues", func(w *conveyorv1.WebhookWorker) { w.Queues = nil }},
		{"bad queue name", func(w *conveyorv1.WebhookWorker) { w.Queues = map[string]int32{"-bad": 1} }},
		{"zero weight", func(w *conveyorv1.WebhookWorker) { w.Queues = map[string]int32{"default": 0} }},
		{"zero concurrency", func(w *conveyorv1.WebhookWorker) { w.Concurrency = 0 }},
		{"no secrets", func(w *conveyorv1.WebhookWorker) { w.Secrets = nil }},
		{"three secrets", func(w *conveyorv1.WebhookWorker) { w.Secrets = []string{"a", "b", "c"} }},
		{"empty secret", func(w *conveyorv1.WebhookWorker) { w.Secrets = []string{""} }},
		{"negative timeout", func(w *conveyorv1.WebhookWorker) { w.RequestTimeout = durationpb.New(-time.Second) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := connect.NewRequest(&conveyorv1.UpsertWebhookWorkerRequest{Worker: validWebhookWorkerMessage(tc.mutate)})
			_, err := admin.UpsertWebhookWorker(ctx, request)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}

	// A nil worker is rejected outright.
	_, err := admin.UpsertWebhookWorker(ctx, connect.NewRequest(&conveyorv1.UpsertWebhookWorkerRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestUpsertWebhookWorkerRejectsHTTPOutsideDev(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)

	// A production-posture admin service (bearer auth on) refuses plaintext
	// delivery URLs; a development one admits them.
	strict := NewAdminService(engine, taskLog, clock.System(), stubSessions(nil), false)
	request := connect.NewRequest(&conveyorv1.UpsertWebhookWorkerRequest{
		Worker: validWebhookWorkerMessage(func(w *conveyorv1.WebhookWorker) { w.Url = "http://example.com/tasks" }),
	})

	_, err := strict.UpsertWebhookWorker(ctx, request)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	relaxed := NewAdminService(engine, taskLog, clock.System(), stubSessions(nil), true)
	_, err = relaxed.UpsertWebhookWorker(ctx, request)
	require.NoError(t, err)
}
