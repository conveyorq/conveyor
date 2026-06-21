from conveyor.v1 import task_pb2 as _task_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union
DESCRIPTOR: _descriptor.FileDescriptor

class TaskEnqueued(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class TasksAvailable(_message.Message):
    __slots__ = ('queue', 'hint')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    HINT_FIELD_NUMBER: _ClassVar[int]
    queue: str
    hint: int

    def __init__(self, queue: _Optional[str]=..., hint: _Optional[int]=...) -> None:
        ...

class RegisterGateway(_message.Message):
    __slots__ = ('queue', 'gateway_name', 'capacity', 'batch_types')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    GATEWAY_NAME_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    BATCH_TYPES_FIELD_NUMBER: _ClassVar[int]
    queue: str
    gateway_name: str
    capacity: int
    batch_types: _containers.RepeatedScalarFieldContainer[str]

    def __init__(self, queue: _Optional[str]=..., gateway_name: _Optional[str]=..., capacity: _Optional[int]=..., batch_types: _Optional[_Iterable[str]]=...) -> None:
        ...

class GatewayCredit(_message.Message):
    __slots__ = ('queue', 'gateway_name', 'credits')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    GATEWAY_NAME_FIELD_NUMBER: _ClassVar[int]
    CREDITS_FIELD_NUMBER: _ClassVar[int]
    queue: str
    gateway_name: str
    credits: int

    def __init__(self, queue: _Optional[str]=..., gateway_name: _Optional[str]=..., credits: _Optional[int]=...) -> None:
        ...

class ExecuteTask(_message.Message):
    __slots__ = ('task', 'lease_id', 'lease_expires_at')
    TASK_FIELD_NUMBER: _ClassVar[int]
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    task: _task_pb2.TaskEnvelope
    lease_id: str
    lease_expires_at: _timestamp_pb2.Timestamp

    def __init__(self, task: _Optional[_Union[_task_pb2.TaskEnvelope, _Mapping]]=..., lease_id: _Optional[str]=..., lease_expires_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class ExecuteBatch(_message.Message):
    __slots__ = ('tasks', 'lease_id', 'lease_expires_at', 'group')
    TASKS_FIELD_NUMBER: _ClassVar[int]
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[_task_pb2.TaskEnvelope]
    lease_id: str
    lease_expires_at: _timestamp_pb2.Timestamp
    group: str

    def __init__(self, tasks: _Optional[_Iterable[_Union[_task_pb2.TaskEnvelope, _Mapping]]]=..., lease_id: _Optional[str]=..., lease_expires_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., group: _Optional[str]=...) -> None:
        ...

class FireGroup(_message.Message):
    __slots__ = ('queue', 'group', 'type')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    group: str
    type: str

    def __init__(self, queue: _Optional[str]=..., group: _Optional[str]=..., type: _Optional[str]=...) -> None:
        ...

class GroupLeaseCompleted(_message.Message):
    __slots__ = ('tasks', 'lease_id', 'lease_expires_at', 'group', 'type', 'error')
    TASKS_FIELD_NUMBER: _ClassVar[int]
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[_task_pb2.TaskEnvelope]
    lease_id: str
    lease_expires_at: _timestamp_pb2.Timestamp
    group: str
    type: str
    error: str

    def __init__(self, tasks: _Optional[_Iterable[_Union[_task_pb2.TaskEnvelope, _Mapping]]]=..., lease_id: _Optional[str]=..., lease_expires_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., group: _Optional[str]=..., type: _Optional[str]=..., error: _Optional[str]=...) -> None:
        ...

class TaskCompleted(_message.Message):
    __slots__ = ('task_id', 'queue', 'success', 'gateway_name')
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    GATEWAY_NAME_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    queue: str
    success: bool
    gateway_name: str

    def __init__(self, task_id: _Optional[str]=..., queue: _Optional[str]=..., success: bool=..., gateway_name: _Optional[str]=...) -> None:
        ...

class BatchCompleted(_message.Message):
    __slots__ = ('queue', 'gateway_name', 'total', 'succeeded')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    GATEWAY_NAME_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    queue: str
    gateway_name: str
    total: int
    succeeded: int

    def __init__(self, queue: _Optional[str]=..., gateway_name: _Optional[str]=..., total: _Optional[int]=..., succeeded: _Optional[int]=...) -> None:
        ...

class DrainQueue(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class ResumeQueue(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class CancelActive(_message.Message):
    __slots__ = ('task_id',)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str

    def __init__(self, task_id: _Optional[str]=...) -> None:
        ...

class RateLimitChanged(_message.Message):
    __slots__ = ('queue', 'rate_per_sec', 'burst')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    RATE_PER_SEC_FIELD_NUMBER: _ClassVar[int]
    BURST_FIELD_NUMBER: _ClassVar[int]
    queue: str
    rate_per_sec: float
    burst: int

    def __init__(self, queue: _Optional[str]=..., rate_per_sec: _Optional[float]=..., burst: _Optional[int]=...) -> None:
        ...

class ConcurrencyLimitChanged(_message.Message):
    __slots__ = ('queue', 'max_active')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    max_active: int

    def __init__(self, queue: _Optional[str]=..., max_active: _Optional[int]=...) -> None:
        ...

class FireCron(_message.Message):
    __slots__ = ('entry_id',)
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    entry_id: str

    def __init__(self, entry_id: _Optional[str]=...) -> None:
        ...

class CronEntriesChanged(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class LeaseCycleCompleted(_message.Message):
    __slots__ = ('tasks', 'lease_id', 'lease_expires_at', 'error')
    TASKS_FIELD_NUMBER: _ClassVar[int]
    LEASE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[_task_pb2.TaskEnvelope]
    lease_id: str
    lease_expires_at: _timestamp_pb2.Timestamp
    error: str

    def __init__(self, tasks: _Optional[_Iterable[_Union[_task_pb2.TaskEnvelope, _Mapping]]]=..., lease_id: _Optional[str]=..., lease_expires_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., error: _Optional[str]=...) -> None:
        ...

class LeasedTasksReleased(_message.Message):
    __slots__ = ('released', 'failed')
    RELEASED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    released: int
    failed: int

    def __init__(self, released: _Optional[int]=..., failed: _Optional[int]=...) -> None:
        ...

class PromoteTick(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ReapTick(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class GroupSweepTick(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ResolveDependents(_message.Message):
    __slots__ = ('task_id',)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str

    def __init__(self, task_id: _Optional[str]=...) -> None:
        ...