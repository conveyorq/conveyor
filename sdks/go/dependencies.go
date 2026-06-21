// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"

// DependencyFailurePolicy decides a dependent task's fate when a task it
// depends on fails terminally (its retries are exhausted, it is skipped, or it
// is canceled) instead of succeeding.
type DependencyFailurePolicy int

const (
	// BlockOnFailure keeps the dependent blocked indefinitely: the dependency
	// never succeeded, so the dependent never becomes eligible. It is the
	// default for a dependency declared without an explicit policy.
	BlockOnFailure DependencyFailurePolicy = iota
	// CascadeCancelOnFailure cancels the dependent (and, in turn, its own
	// dependents) when the dependency fails.
	CascadeCancelOnFailure
	// ContinueOnFailure treats the failed dependency as satisfied, so the
	// dependent proceeds once its remaining dependencies clear.
	ContinueOnFailure
)

// Dependency is one task a task waits for. The dependent stays blocked until
// the referenced task reaches a terminal success; OnFailure decides what
// happens if it fails terminally instead.
type Dependency struct {
	// TaskID is the id of the task that must finish first.
	TaskID string
	// OnFailure is the policy applied when the dependency fails terminally.
	// The zero value, BlockOnFailure, keeps the dependent blocked.
	OnFailure DependencyFailurePolicy
}

// DependsOn blocks the task until every named task reaches a terminal success,
// building a workflow: a chain ("run B after A") or a fan-in (a continuation
// that depends on every task of a fan-out batch). Each dependency uses the
// block-on-failure policy; for per-dependency failure policies, use
// DependsOnTasks. Dependencies must be acyclic — a cycle leaves its tasks
// blocked forever.
func DependsOn(taskIDs ...string) EnqueueOption {
	return func(o *enqueueOptions) {
		for _, id := range taskIDs {
			o.dependsOn = append(o.dependsOn, Dependency{TaskID: id})
		}
	}
}

// DependsOnTasks blocks the task until every dependency reaches a terminal
// success, applying each dependency's on-failure policy if it fails instead. It
// is the explicit-policy form of DependsOn.
func DependsOnTasks(deps ...Dependency) EnqueueOption {
	return func(o *enqueueOptions) {
		o.dependsOn = append(o.dependsOn, deps...)
	}
}

// dependenciesToProto converts the SDK dependencies to their wire form.
func dependenciesToProto(deps []Dependency) []*conveyorv1.TaskDependency {
	edges := make([]*conveyorv1.TaskDependency, 0, len(deps))

	for _, dependency := range deps {
		edges = append(edges, &conveyorv1.TaskDependency{
			TaskId:    dependency.TaskID,
			OnFailure: dependency.OnFailure.toProto(),
		})
	}

	return edges
}

// toProto maps an SDK failure policy to its wire enum value.
func (p DependencyFailurePolicy) toProto() conveyorv1.DependencyFailurePolicy {
	switch p {
	case CascadeCancelOnFailure:
		return conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL
	case ContinueOnFailure:
		return conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE
	default:
		return conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK
	}
}
