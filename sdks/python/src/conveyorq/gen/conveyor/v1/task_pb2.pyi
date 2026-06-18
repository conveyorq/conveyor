from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union
DESCRIPTOR: _descriptor.FileDescriptor

class TaskState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_STATE_UNSPECIFIED: _ClassVar[TaskState]
    TASK_STATE_SCHEDULED: _ClassVar[TaskState]
    TASK_STATE_PENDING: _ClassVar[TaskState]
    TASK_STATE_ACTIVE: _ClassVar[TaskState]
    TASK_STATE_RETRY: _ClassVar[TaskState]
    TASK_STATE_COMPLETED: _ClassVar[TaskState]
    TASK_STATE_ARCHIVED: _ClassVar[TaskState]
    TASK_STATE_CANCELED: _ClassVar[TaskState]
    TASK_STATE_AGGREGATING: _ClassVar[TaskState]
TASK_STATE_UNSPECIFIED: TaskState
TASK_STATE_SCHEDULED: TaskState
TASK_STATE_PENDING: TaskState
TASK_STATE_ACTIVE: TaskState
TASK_STATE_RETRY: TaskState
TASK_STATE_COMPLETED: TaskState
TASK_STATE_ARCHIVED: TaskState
TASK_STATE_CANCELED: TaskState
TASK_STATE_AGGREGATING: TaskState

class TaskEnvelope(_message.Message):
    __slots__ = ('id', 'queue', 'type', 'payload', 'content_type', 'metadata', 'options', 'retried', 'last_error', 'enqueued_at', 'started_at', 'completed_at')

    class MetadataEntry(_message.Message):
        __slots__ = ('key', 'value')
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str

        def __init__(self, key: _Optional[str]=..., value: _Optional[str]=...) -> None:
            ...
    ID_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    RETRIED_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    ENQUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    queue: str
    type: str
    payload: bytes
    content_type: str
    metadata: _containers.ScalarMap[str, str]
    options: TaskOptions
    retried: int
    last_error: str
    enqueued_at: _timestamp_pb2.Timestamp
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp

    def __init__(self, id: _Optional[str]=..., queue: _Optional[str]=..., type: _Optional[str]=..., payload: _Optional[bytes]=..., content_type: _Optional[str]=..., metadata: _Optional[_Mapping[str, str]]=..., options: _Optional[_Union[TaskOptions, _Mapping]]=..., retried: _Optional[int]=..., last_error: _Optional[str]=..., enqueued_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., started_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., completed_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...

class TaskOptions(_message.Message):
    __slots__ = ('max_retry', 'timeout', 'deadline', 'process_at', 'unique_key', 'unique_ttl', 'retention', 'priority', 'group', 'expires_at')
    MAX_RETRY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    PROCESS_AT_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_KEY_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_TTL_FIELD_NUMBER: _ClassVar[int]
    RETENTION_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    max_retry: int
    timeout: _duration_pb2.Duration
    deadline: _timestamp_pb2.Timestamp
    process_at: _timestamp_pb2.Timestamp
    unique_key: str
    unique_ttl: _duration_pb2.Duration
    retention: _duration_pb2.Duration
    priority: int
    group: str
    expires_at: _timestamp_pb2.Timestamp

    def __init__(self, max_retry: _Optional[int]=..., timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., deadline: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., process_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=..., unique_key: _Optional[str]=..., unique_ttl: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., retention: _Optional[_Union[_duration_pb2.Duration, _Mapping]]=..., priority: _Optional[int]=..., group: _Optional[str]=..., expires_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]]=...) -> None:
        ...