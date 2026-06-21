# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

from conveyorq import Client, json, new_task
from conveyorq.gen.conveyor.v1 import service_pb2, task_pb2


class _CapturingStub:
    def __init__(self) -> None:
        self.request = None

    async def Enqueue(self, request, metadata=None):  # noqa: N802 - gRPC method name
        self.request = request
        return service_pb2.EnqueueResponse(
            task=service_pb2.TaskInfo(id="t1", state=task_pb2.TASK_STATE_PENDING)
        )


async def test_concurrency_key_maps_to_request():
    client = Client("http://localhost:9", token="x")
    stub = _CapturingStub()
    client._stub = stub

    try:
        await client.enqueue(new_task("t", json(1)), concurrency_key="customer:42")
    finally:
        await client.close()

    assert stub.request.concurrency_key == "customer:42"


async def test_concurrency_key_defaults_to_empty():
    client = Client("http://localhost:9", token="x")
    stub = _CapturingStub()
    client._stub = stub

    try:
        await client.enqueue(new_task("t", json(1)))
    finally:
        await client.close()

    assert stub.request.concurrency_key == ""
