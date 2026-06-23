from conveyor.v1 import task_pb2 as _task_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union
DESCRIPTOR: _descriptor.FileDescriptor

class TaskOutcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_OUTCOME_UNSPECIFIED: _ClassVar[TaskOutcome]
    TASK_OUTCOME_SUCCESS: _ClassVar[TaskOutcome]
    TASK_OUTCOME_RETRY: _ClassVar[TaskOutcome]
    TASK_OUTCOME_SKIP_RETRY: _ClassVar[TaskOutcome]
    TASK_OUTCOME_RELEASED: _ClassVar[TaskOutcome]
TASK_OUTCOME_UNSPECIFIED: TaskOutcome
TASK_OUTCOME_SUCCESS: TaskOutcome
TASK_OUTCOME_RETRY: TaskOutcome
TASK_OUTCOME_SKIP_RETRY: TaskOutcome
TASK_OUTCOME_RELEASED: TaskOutcome

class EnqueueRequest(_message.Message):
    __slots__ = ('task_id', 'queue', 'type', 'payload', 'content_type', 'metadata', 'max_retry', 'timeout', 'deadline', 'process_at', 'process_in', 'unique_key', 'unique_ttl', 'priority', 'retention', 'group', 'expires_in', 'expires_at', 'depends_on', 'concurrency_key')

    class MetadataEntry(_message.Message):
        __slots__ = ('key', 'value')
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str

        def __init__(self, key: _Optional[str]=..., value: _Optional[str]=...) -> None:
            ...
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    PROCESS_AT_FIELD_NUMBER: _ClassVar[int]
    PROCESS_IN_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_KEY_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_TTL_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    RETENTION_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    queue: str
    type: str
    payload: bytes
    content_type: str
    metadata: _containers.ScalarMap[str, str]
    max_retry: int
    timeout: _duration_pb2.Duration
    deadline: _timestamp_pb2.Timestamp
    process_at: _timestamp_pb2.Timestamp
    process_in: _duration_pb2.Duration
    unique_key: str
    unique_ttl: _duration_pb2.Duration
    priority: int
    retention: _duration_pb2.Duration
    group: str
    expires_in: _duration_pb2.Duration
    expires_at: _timestamp_pb2.Timestamp
    depends_on: _containers.RepeatedCompositeFieldContainer[_task_pb2.TaskDependency]
    concurrency_key: str

    def __init__(self, task_id: _Optional[str]=..., queue: _Optional[str]=..., type: _Optional[str]=..., payload: _Optional[bytes]=..., content_type: _Optional[str]=..., metadata: _Optional[_Mapping[str, str]]=..., max_retry: _Optional[int]=..., timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., deadline: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., process_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., process_in: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., unique_key: _Optional[str]=..., unique_ttl: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., priority: _Optional[int]=..., retention: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., group: _Optional[str]=..., expires_in: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., expires_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., depends_on: _Optional[_Iterable[_Union[_task_pb2.TaskDependency, _Mapping]]]=..., concurrency_key: _Optional[str]=...) -> None:
        ...

class EnqueueResponse(_message.Message):
    __slots__ = ('task',)
    TASK_FIELD_NUMBER: _ClassVar[int]
    task: TaskInfo

    def __init__(self, task: _Optional[_Union[TaskInfo, _Mapping]]=...) -> None:
        ...

class EnqueueBatchRequest(_message.Message):
    __slots__ = ('tasks',)
    TASKS_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[EnqueueRequest]

    def __init__(self, tasks: _Optional[_Iterable[_Union[EnqueueRequest, _Mapping]]]=...) -> None:
        ...

class EnqueueBatchResponse(_message.Message):
    __slots__ = ('results',)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[EnqueueResult]

    def __init__(self, results: _Optional[_Iterable[_Union[EnqueueResult, _Mapping]]]=...) -> None:
        ...

class EnqueueResult(_message.Message):
    __slots__ = ('task', 'error')
    TASK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    task: TaskInfo
    error: str

    def __init__(self, task: _Optional[_Union[TaskInfo, _Mapping]]=..., error: _Optional[str]=...) -> None:
        ...

class GetTaskRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class GetTaskResponse(_message.Message):
    __slots__ = ('task',)
    TASK_FIELD_NUMBER: _ClassVar[int]
    task: TaskInfo

    def __init__(self, task: _Optional[_Union[TaskInfo, _Mapping]]=...) -> None:
        ...

class TaskInfo(_message.Message):
    __slots__ = ('id', 'queue', 'type', 'state', 'priority', 'retried', 'max_retry', 'last_error', 'enqueued_at', 'process_at', 'completed_at', 'payload', 'content_type', 'started_at')
    ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    RETRIED_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRY_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    ENQUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    PROCESS_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    queue: str
    type: str
    state: _task_pb2.TaskState
    priority: int
    retried: int
    max_retry: int
    last_error: str
    enqueued_at: _timestamp_pb2.Timestamp
    process_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    payload: bytes
    content_type: str
    started_at: _timestamp_pb2.Timestamp

    def __init__(self, id: _Optional[str]=..., queue: _Optional[str]=..., type: _Optional[str]=..., state: _Optional[_Union[_task_pb2.TaskState, str]]=..., priority: _Optional[int]=..., retried: _Optional[int]=..., max_retry: _Optional[int]=..., last_error: _Optional[str]=..., enqueued_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., process_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., completed_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., payload: _Optional[bytes]=..., content_type: _Optional[str]=..., started_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class WorkerMessage(_message.Message):
    __slots__ = ('hello', 'credit', 'result', 'heartbeat', 'batch_result')
    HELLO_FIELD_NUMBER: _ClassVar[int]
    CREDIT_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    BATCH_RESULT_FIELD_NUMBER: _ClassVar[int]
    hello: Hello
    credit: Credit
    result: Result
    heartbeat: Heartbeat
    batch_result: BatchResult

    def __init__(self, hello: _Optional[_Union[Hello, _Mapping]]=..., credit: _Optional[_Union[Credit, _Mapping]]=..., result: _Optional[_Union[Result, _Mapping]]=..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]]=..., batch_result: _Optional[_Union[BatchResult, _Mapping]]=...) -> None:
        ...

class ServerMessage(_message.Message):
    __slots__ = ('welcome', 'dispatch', 'cancel', 'ping', 'batch_dispatch')
    WELCOME_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    BATCH_DISPATCH_FIELD_NUMBER: _ClassVar[int]
    welcome: Welcome
    dispatch: Dispatch
    cancel: Cancel
    ping: Ping
    batch_dispatch: BatchDispatch

    def __init__(self, welcome: _Optional[_Union[Welcome, _Mapping]]=..., dispatch: _Optional[_Union[Dispatch, _Mapping]]=..., cancel: _Optional[_Union[Cancel, _Mapping]]=..., ping: _Optional[_Union[Ping, _Mapping]]=..., batch_dispatch: _Optional[_Union[BatchDispatch, _Mapping]]=...) -> None:
        ...

class Hello(_message.Message):
    __slots__ = ('queues', 'concurrency', 'labels', 'sdk_version', 'min_server_version', 'batch_types')

    class QueuesEntry(_message.Message):
        __slots__ = ('key', 'value')
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int

        def __init__(self, key: _Optional[str]=..., value: _Optional[int]=...) -> None:
            ...

    class LabelsEntry(_message.Message):
        __slots__ = ('key', 'value')
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str

        def __init__(self, key: _Optional[str]=..., value: _Optional[str]=...) -> None:
            ...
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    MIN_SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    BATCH_TYPES_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.ScalarMap[str, int]
    concurrency: int
    labels: _containers.ScalarMap[str, str]
    sdk_version: str
    min_server_version: str
    batch_types: _containers.RepeatedScalarFieldContainer[str]

    def __init__(self, queues: _Optional[_Mapping[str, int]]=..., concurrency: _Optional[int]=..., labels: _Optional[_Mapping[str, str]]=..., sdk_version: _Optional[str]=..., min_server_version: _Optional[str]=..., batch_types: _Optional[_Iterable[str]]=...) -> None:
        ...

class Credit(_message.Message):
    __slots__ = ('n',)
    N_FIELD_NUMBER: _ClassVar[int]
    n: int

    def __init__(self, n: _Optional[int]=...) -> None:
        ...

class Result(_message.Message):
    __slots__ = ('task_id', 'outcome', 'error_msg', 'result')
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    ERROR_MSG_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    outcome: TaskOutcome
    error_msg: str
    result: bytes

    def __init__(self, task_id: _Optional[str]=..., outcome: _Optional[_Union[TaskOutcome, str]]=..., error_msg: _Optional[str]=..., result: _Optional[bytes]=...) -> None:
        ...

class Heartbeat(_message.Message):
    __slots__ = ('active_task_ids',)
    ACTIVE_TASK_IDS_FIELD_NUMBER: _ClassVar[int]
    active_task_ids: _containers.RepeatedScalarFieldContainer[str]

    def __init__(self, active_task_ids: _Optional[_Iterable[str]]=...) -> None:
        ...

class Welcome(_message.Message):
    __slots__ = ('session_id', 'lease_ttl', 'heartbeat_interval', 'server_version', 'min_sdk_version')
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    MIN_SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    lease_ttl: _duration_pb2.Duration
    heartbeat_interval: _duration_pb2.Duration
    server_version: str
    min_sdk_version: str

    def __init__(self, session_id: _Optional[str]=..., lease_ttl: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., heartbeat_interval: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., server_version: _Optional[str]=..., min_sdk_version: _Optional[str]=...) -> None:
        ...

class Dispatch(_message.Message):
    __slots__ = ('task', 'deadline')
    TASK_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    task: _task_pb2.TaskEnvelope
    deadline: _timestamp_pb2.Timestamp

    def __init__(self, task: _Optional[_Union[_task_pb2.TaskEnvelope, _Mapping]]=..., deadline: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class BatchDispatch(_message.Message):
    __slots__ = ('tasks', 'deadline', 'group')
    TASKS_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[_task_pb2.TaskEnvelope]
    deadline: _timestamp_pb2.Timestamp
    group: str

    def __init__(self, tasks: _Optional[_Iterable[_Union[_task_pb2.TaskEnvelope, _Mapping]]]=..., deadline: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., group: _Optional[str]=...) -> None:
        ...

class BatchResult(_message.Message):
    __slots__ = ('results',)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[Result]

    def __init__(self, results: _Optional[_Iterable[_Union[Result, _Mapping]]]=...) -> None:
        ...

class Cancel(_message.Message):
    __slots__ = ('task_id',)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str

    def __init__(self, task_id: _Optional[str]=...) -> None:
        ...

class Ping(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListQueuesRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListQueuesResponse(_message.Message):
    __slots__ = ('queues',)
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedCompositeFieldContainer[QueueInfo]

    def __init__(self, queues: _Optional[_Iterable[_Union[QueueInfo, _Mapping]]]=...) -> None:
        ...

class QueueInfo(_message.Message):
    __slots__ = ('name', 'paused', 'scheduled', 'pending', 'active', 'retry', 'completed', 'archived', 'aggregating', 'blocked')
    NAME_FIELD_NUMBER: _ClassVar[int]
    PAUSED_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    ARCHIVED_FIELD_NUMBER: _ClassVar[int]
    AGGREGATING_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_FIELD_NUMBER: _ClassVar[int]
    name: str
    paused: bool
    scheduled: int
    pending: int
    active: int
    retry: int
    completed: int
    archived: int
    aggregating: int
    blocked: int

    def __init__(self, name: _Optional[str]=..., paused: bool=..., scheduled: _Optional[int]=..., pending: _Optional[int]=..., active: _Optional[int]=..., retry: _Optional[int]=..., completed: _Optional[int]=..., archived: _Optional[int]=..., aggregating: _Optional[int]=..., blocked: _Optional[int]=...) -> None:
        ...

class PauseQueueRequest(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class PauseQueueResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ResumeQueueRequest(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class ResumeQueueResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class RateLimitInfo(_message.Message):
    __slots__ = ('queue', 'rate_per_sec', 'burst')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    RATE_PER_SEC_FIELD_NUMBER: _ClassVar[int]
    BURST_FIELD_NUMBER: _ClassVar[int]
    queue: str
    rate_per_sec: float
    burst: int

    def __init__(self, queue: _Optional[str]=..., rate_per_sec: _Optional[float]=..., burst: _Optional[int]=...) -> None:
        ...

class ListRateLimitsRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListRateLimitsResponse(_message.Message):
    __slots__ = ('limits',)
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    limits: _containers.RepeatedCompositeFieldContainer[RateLimitInfo]

    def __init__(self, limits: _Optional[_Iterable[_Union[RateLimitInfo, _Mapping]]]=...) -> None:
        ...

class SetQueueRateLimitRequest(_message.Message):
    __slots__ = ('queue', 'rate_per_sec', 'burst')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    RATE_PER_SEC_FIELD_NUMBER: _ClassVar[int]
    BURST_FIELD_NUMBER: _ClassVar[int]
    queue: str
    rate_per_sec: float
    burst: int

    def __init__(self, queue: _Optional[str]=..., rate_per_sec: _Optional[float]=..., burst: _Optional[int]=...) -> None:
        ...

class SetQueueRateLimitResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class DeleteQueueRateLimitRequest(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class DeleteQueueRateLimitResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ConcurrencyLimitInfo(_message.Message):
    __slots__ = ('queue', 'max_active')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    max_active: int

    def __init__(self, queue: _Optional[str]=..., max_active: _Optional[int]=...) -> None:
        ...

class ListConcurrencyLimitsRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListConcurrencyLimitsResponse(_message.Message):
    __slots__ = ('limits',)
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    limits: _containers.RepeatedCompositeFieldContainer[ConcurrencyLimitInfo]

    def __init__(self, limits: _Optional[_Iterable[_Union[ConcurrencyLimitInfo, _Mapping]]]=...) -> None:
        ...

class SetQueueConcurrencyLimitRequest(_message.Message):
    __slots__ = ('queue', 'max_active')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    max_active: int

    def __init__(self, queue: _Optional[str]=..., max_active: _Optional[int]=...) -> None:
        ...

class SetQueueConcurrencyLimitResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class DeleteQueueConcurrencyLimitRequest(_message.Message):
    __slots__ = ('queue',)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str

    def __init__(self, queue: _Optional[str]=...) -> None:
        ...

class DeleteQueueConcurrencyLimitResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListTasksRequest(_message.Message):
    __slots__ = ('queue', 'state', 'limit', 'page_token')
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    queue: str
    state: _task_pb2.TaskState
    limit: int
    page_token: str

    def __init__(self, queue: _Optional[str]=..., state: _Optional[_Union[_task_pb2.TaskState, str]]=..., limit: _Optional[int]=..., page_token: _Optional[str]=...) -> None:
        ...

class ListTasksResponse(_message.Message):
    __slots__ = ('tasks', 'next_page_token')
    TASKS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[TaskInfo]
    next_page_token: str

    def __init__(self, tasks: _Optional[_Iterable[_Union[TaskInfo, _Mapping]]]=..., next_page_token: _Optional[str]=...) -> None:
        ...

class CancelTaskRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class CancelTaskResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class DeleteTaskRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class DeleteTaskResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class RunTaskRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class RunTaskResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class RescheduleTaskRequest(_message.Message):
    __slots__ = ('id', 'process_at', 'process_in')
    ID_FIELD_NUMBER: _ClassVar[int]
    PROCESS_AT_FIELD_NUMBER: _ClassVar[int]
    PROCESS_IN_FIELD_NUMBER: _ClassVar[int]
    id: str
    process_at: _timestamp_pb2.Timestamp
    process_in: _duration_pb2.Duration

    def __init__(self, id: _Optional[str]=..., process_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., process_in: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=...) -> None:
        ...

class RescheduleTaskResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ArchiveTaskRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class ArchiveTaskResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class BatchTasksRequest(_message.Message):
    __slots__ = ('ids',)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[str]

    def __init__(self, ids: _Optional[_Iterable[str]]=...) -> None:
        ...

class BatchTasksResponse(_message.Message):
    __slots__ = ('results',)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[TaskActionResult]

    def __init__(self, results: _Optional[_Iterable[_Union[TaskActionResult, _Mapping]]]=...) -> None:
        ...

class TaskActionResult(_message.Message):
    __slots__ = ('id', 'error')
    ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    id: str
    error: str

    def __init__(self, id: _Optional[str]=..., error: _Optional[str]=...) -> None:
        ...

class ListCronRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListCronResponse(_message.Message):
    __slots__ = ('entries',)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[CronEntry]

    def __init__(self, entries: _Optional[_Iterable[_Union[CronEntry, _Mapping]]]=...) -> None:
        ...

class CronEntry(_message.Message):
    __slots__ = ('id', 'spec', 'task_type', 'queue', 'payload', 'content_type', 'options', 'paused', 'next_run_at')
    ID_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    PAUSED_FIELD_NUMBER: _ClassVar[int]
    NEXT_RUN_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    spec: str
    task_type: str
    queue: str
    payload: bytes
    content_type: str
    options: _task_pb2.TaskOptions
    paused: bool
    next_run_at: _timestamp_pb2.Timestamp

    def __init__(self, id: _Optional[str]=..., spec: _Optional[str]=..., task_type: _Optional[str]=..., queue: _Optional[str]=..., payload: _Optional[bytes]=..., content_type: _Optional[str]=..., options: _Optional[_Union[_task_pb2.TaskOptions, _Mapping]]=..., paused: bool=..., next_run_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class UpsertCronRequest(_message.Message):
    __slots__ = ('entry',)
    ENTRY_FIELD_NUMBER: _ClassVar[int]
    entry: CronEntry

    def __init__(self, entry: _Optional[_Union[CronEntry, _Mapping]]=...) -> None:
        ...

class UpsertCronResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class PauseCronRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class PauseCronResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ResumeCronRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class ResumeCronResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class DeleteCronRequest(_message.Message):
    __slots__ = ('id',)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str

    def __init__(self, id: _Optional[str]=...) -> None:
        ...

class DeleteCronResponse(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ClusterInfoRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ClusterInfoResponse(_message.Message):
    __slots__ = ('nodes',)
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[NodeInfo]

    def __init__(self, nodes: _Optional[_Iterable[_Union[NodeInfo, _Mapping]]]=...) -> None:
        ...

class NodeInfo(_message.Message):
    __slots__ = ('address', 'started_at')
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    address: str
    started_at: _timestamp_pb2.Timestamp

    def __init__(self, address: _Optional[str]=..., started_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class ListWorkerSessionsRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class ListWorkerSessionsResponse(_message.Message):
    __slots__ = ('sessions',)
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    sessions: _containers.RepeatedCompositeFieldContainer[WorkerSession]

    def __init__(self, sessions: _Optional[_Iterable[_Union[WorkerSession, _Mapping]]]=...) -> None:
        ...

class WorkerSession(_message.Message):
    __slots__ = ('id', 'queues', 'concurrency', 'sdk_version', 'connected_at')
    ID_FIELD_NUMBER: _ClassVar[int]
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    queues: _containers.RepeatedScalarFieldContainer[str]
    concurrency: int
    sdk_version: str
    connected_at: _timestamp_pb2.Timestamp

    def __init__(self, id: _Optional[str]=..., queues: _Optional[_Iterable[str]]=..., concurrency: _Optional[int]=..., sdk_version: _Optional[str]=..., connected_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class BrokerInfoRequest(_message.Message):
    __slots__ = ()

    def __init__(self) -> None:
        ...

class BrokerInfoResponse(_message.Message):
    __slots__ = ('driver', 'metrics')

    class MetricsEntry(_message.Message):
        __slots__ = ('key', 'value')
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str

        def __init__(self, key: _Optional[str]=..., value: _Optional[str]=...) -> None:
            ...
    DRIVER_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    driver: str
    metrics: _containers.ScalarMap[str, str]

    def __init__(self, driver: _Optional[str]=..., metrics: _Optional[_Mapping[str, str]]=...) -> None:
        ...

class WatchEventsRequest(_message.Message):
    __slots__ = ('queues', 'event_types')
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPES_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedScalarFieldContainer[str]
    event_types: _containers.RepeatedScalarFieldContainer[_task_pb2.TaskEventType]

    def __init__(self, queues: _Optional[_Iterable[str]]=..., event_types: _Optional[_Iterable[_Union[_task_pb2.TaskEventType, str]]]=...) -> None:
        ...