// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"testing"

	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestDependsOnOptions(t *testing.T) {
	settings := &enqueueOptions{}

	DependsOn("task-a", "task-b")(settings)
	DependsOnTasks(Dependency{TaskID: "task-c", OnFailure: ContinueOnFailure})(settings)

	require.Len(t, settings.dependsOn, 3)
	require.Equal(t, "task-a", settings.dependsOn[0].TaskID)
	require.Equal(t, BlockOnFailure, settings.dependsOn[0].OnFailure)
	require.Equal(t, "task-c", settings.dependsOn[2].TaskID)
	require.Equal(t, ContinueOnFailure, settings.dependsOn[2].OnFailure)
}

func TestDependenciesToProto(t *testing.T) {
	edges := dependenciesToProto([]Dependency{
		{TaskID: "a"},
		{TaskID: "b", OnFailure: CascadeCancelOnFailure},
		{TaskID: "c", OnFailure: ContinueOnFailure},
	})

	require.Len(t, edges, 3)
	require.Equal(t, "a", edges[0].GetTaskId())
	require.Equal(t, conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK, edges[0].GetOnFailure())
	require.Equal(t, conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL, edges[1].GetOnFailure())
	require.Equal(t, conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE, edges[2].GetOnFailure())
}
