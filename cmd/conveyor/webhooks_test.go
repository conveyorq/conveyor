// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebhooksUsageErrors(t *testing.T) {
	err := run([]string{"webhooks"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage")

	err = run([]string{"webhooks", "bogus"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "unknown subcommand")

	err = run([]string{"webhooks", "add", "only-name"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage")

	err = run([]string{"webhooks", "pause"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage")
}

func TestParseQueueWeights(t *testing.T) {
	queues, err := parseQueueWeights([]string{"billing=3", "default"})
	require.NoError(t, err)
	require.Equal(t, map[string]int32{"billing": 3, "default": 1}, queues)

	_, err = parseQueueWeights(nil)
	require.ErrorContains(t, err, "at least one --queue")

	_, err = parseQueueWeights([]string{"billing=zero"})
	require.ErrorContains(t, err, "invalid queue weight")

	_, err = parseQueueWeights([]string{"billing=0"})
	require.ErrorContains(t, err, "invalid queue weight")
}

func TestWebhooksAgainstEmbeddedServer(t *testing.T) {
	addr := startEmbeddedNode(t)

	var addOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "webhooks", "add", "billing-hooks", "https://hooks.example.com/tasks",
		"--queue", "billing=3", "--secret", "s3cret", "--concurrency", "8"}, &addOut))
	require.Contains(t, addOut.String(), "billing-hooks saved")

	var listOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "webhooks", "list"}, &listOut))
	require.Contains(t, listOut.String(), "billing-hooks")
	require.Contains(t, listOut.String(), "billing=3")
	require.NotContains(t, listOut.String(), "s3cret", "secrets never appear in listings")

	var pauseOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "webhooks", "pause", "billing-hooks"}, &pauseOut))
	require.Contains(t, pauseOut.String(), "paused")

	var resumeOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "webhooks", "resume", "billing-hooks"}, &resumeOut))
	require.Contains(t, resumeOut.String(), "resumed")

	err := run([]string{"--addr", addr, "webhooks", "pause", "missing"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "does not exist")

	// Server-side validation rejects an invalid registration.
	err = run([]string{"--addr", addr, "webhooks", "add", "bad", "ftp://nope",
		"--queue", "default", "--secret", "s"}, &bytes.Buffer{})
	require.Error(t, err)

	var deleteOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "webhooks", "delete", "billing-hooks"}, &deleteOut))
	require.Contains(t, deleteOut.String(), "deleted")

	var emptyOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "webhooks", "list"}, &emptyOut))
	require.NotContains(t, emptyOut.String(), "billing-hooks")
}
