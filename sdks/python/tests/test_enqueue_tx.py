# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from conveyorq import Client, ConveyorError, TxTask, json, new_task
from conveyorq.gen.conveyor.v1 import service_pb2, task_pb2


class _CapturingTxStub:
    def __init__(self) -> None:
        self.request = None

    async def EnqueueTx(self, request, metadata=None):  # noqa: N802 - gRPC method name
        self.request = request
        return service_pb2.EnqueueTxResponse(
            tasks=[
                service_pb2.TaskInfo(
                    id=task.task_id,
                    type=task.type,
                    queue=task.queue,
                    state=task_pb2.TASK_STATE_PENDING,
                )
                for task in request.tasks
            ]
        )


async def test_enqueue_tx_commits_all_with_per_task_options():
    client = Client("http://localhost:9", token="x")
    stub = _CapturingTxStub()
    client._stub = stub

    try:
        tasks = await client.enqueue_tx(
            [
                TxTask(new_task("test:a", json(1)), {"task_id": "tx-1", "queue": "billing"}),
                TxTask(new_task("test:b", json(2)), {"task_id": "tx-2", "queue": "mail"}),
            ]
        )
    finally:
        await client.close()

    assert [task.id for task in tasks] == ["tx-1", "tx-2"]
    assert stub.request.tasks[0].queue == "billing"
    assert stub.request.tasks[1].queue == "mail"
    assert stub.request.tasks[0].type == "test:a"


async def test_enqueue_tx_rejects_empty_list():
    client = Client("http://localhost:9", token="x")

    try:
        with pytest.raises(ConveyorError):
            await client.enqueue_tx([])
    finally:
        await client.close()


async def test_enqueue_tx_propagates_validation_error():
    client = Client("http://localhost:9", token="x")

    try:
        with pytest.raises(ConveyorError):
            await client.enqueue_tx(
                [TxTask(new_task("t", json(1)), {"process_at": 1, "process_in": 1})]
            )
    finally:
        await client.close()
