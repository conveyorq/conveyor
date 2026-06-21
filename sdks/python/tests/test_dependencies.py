# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

from conveyorq import Client, Dependency, DependencyFailure, json, new_task
from conveyorq.gen.conveyor.v1 import service_pb2, task_pb2


class _CapturingStub:
    """A stub that records the enqueued request and returns a fixed response."""

    def __init__(self) -> None:
        self.request = None

    async def Enqueue(self, request, metadata=None):  # noqa: N802 - gRPC method name
        self.request = request
        return service_pb2.EnqueueResponse(
            task=service_pb2.TaskInfo(id="t1", state=task_pb2.TASK_STATE_BLOCKED)
        )


async def test_depends_on_maps_ids_and_failure_policies():
    client = Client("http://localhost:9", token="x")
    stub = _CapturingStub()
    client._stub = stub  # inject the capturing stub, bypassing the channel

    try:
        await client.enqueue(
            new_task("t", json(1)),
            depends_on=[
                "upstream",
                Dependency("sibling", DependencyFailure.CASCADE_CANCEL),
                Dependency("optional", DependencyFailure.CONTINUE),
            ],
        )
    finally:
        await client.close()

    deps = stub.request.depends_on
    assert len(deps) == 3

    assert deps[0].task_id == "upstream"
    assert deps[0].on_failure == task_pb2.DEPENDENCY_FAILURE_POLICY_BLOCK

    assert deps[1].task_id == "sibling"
    assert deps[1].on_failure == task_pb2.DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL

    assert deps[2].task_id == "optional"
    assert deps[2].on_failure == task_pb2.DEPENDENCY_FAILURE_POLICY_CONTINUE


async def test_depends_on_defaults_to_empty():
    client = Client("http://localhost:9", token="x")
    stub = _CapturingStub()
    client._stub = stub

    try:
        await client.enqueue(new_task("t", json(1)))
    finally:
        await client.close()

    assert len(stub.request.depends_on) == 0
