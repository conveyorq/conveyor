"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 3, '', 'conveyor/v1/task.proto')
_sym_db = _symbol_database.Default()
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x16conveyor/v1/task.proto\x12\x0bconveyor.v1\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto"\xa4\x02\n\tTaskEvent\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x14\n\x05queue\x18\x02 \x01(\tR\x05queue\x12\x12\n\x04type\x18\x03 \x01(\tR\x04type\x12,\n\x05state\x18\x04 \x01(\x0e2\x16.conveyor.v1.TaskStateR\x05state\x129\n\nevent_type\x18\x05 \x01(\x0e2\x1a.conveyor.v1.TaskEventTypeR\teventType\x12;\n\x0boccurred_at\x18\x06 \x01(\x0b2\x1a.google.protobuf.TimestampR\noccurredAt\x12\x18\n\x07attempt\x18\x07 \x01(\x05R\x07attempt\x12\x1d\n\nlast_error\x18\x08 \x01(\tR\tlastError"n\n\x0eTaskDependency\x12\x17\n\x07task_id\x18\x01 \x01(\tR\x06taskId\x12C\n\non_failure\x18\x02 \x01(\x0e2$.conveyor.v1.DependencyFailurePolicyR\tonFailure"\xf2\x04\n\x0cTaskEnvelope\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x14\n\x05queue\x18\x02 \x01(\tR\x05queue\x12\x12\n\x04type\x18\x03 \x01(\tR\x04type\x12\x18\n\x07payload\x18\x04 \x01(\x0cR\x07payload\x12!\n\x0ccontent_type\x18\x05 \x01(\tR\x0bcontentType\x12C\n\x08metadata\x18\x06 \x03(\x0b2\'.conveyor.v1.TaskEnvelope.MetadataEntryR\x08metadata\x122\n\x07options\x18\x07 \x01(\x0b2\x18.conveyor.v1.TaskOptionsR\x07options\x12\x18\n\x07retried\x18\x08 \x01(\x05R\x07retried\x12\x1d\n\nlast_error\x18\t \x01(\tR\tlastError\x12;\n\x0benqueued_at\x18\n \x01(\x0b2\x1a.google.protobuf.TimestampR\nenqueuedAt\x129\n\nstarted_at\x18\x0b \x01(\x0b2\x1a.google.protobuf.TimestampR\tstartedAt\x12=\n\x0ccompleted_at\x18\x0c \x01(\x0b2\x1a.google.protobuf.TimestampR\x0bcompletedAt\x12\x1a\n\x08progress\x18\r \x01(\rR\x08progress\x12)\n\x10progress_message\x18\x0e \x01(\tR\x0fprogressMessage\x1a;\n\rMetadataEntry\x12\x10\n\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n\x05value\x18\x02 \x01(\tR\x05value:\x028\x01"\xf9\x04\n\x0bTaskOptions\x12\x1b\n\tmax_retry\x18\x01 \x01(\x05R\x08maxRetry\x123\n\x07timeout\x18\x02 \x01(\x0b2\x19.google.protobuf.DurationR\x07timeout\x126\n\x08deadline\x18\x03 \x01(\x0b2\x1a.google.protobuf.TimestampR\x08deadline\x129\n\nprocess_at\x18\x04 \x01(\x0b2\x1a.google.protobuf.TimestampR\tprocessAt\x12\x1d\n\nunique_key\x18\x05 \x01(\tR\tuniqueKey\x128\n\nunique_ttl\x18\x06 \x01(\x0b2\x19.google.protobuf.DurationR\tuniqueTtl\x127\n\tretention\x18\x07 \x01(\x0b2\x19.google.protobuf.DurationR\tretention\x12\x1a\n\x08priority\x18\x08 \x01(\x05R\x08priority\x12\x14\n\x05group\x18\t \x01(\tR\x05group\x129\n\nexpires_at\x18\n \x01(\x0b2\x1a.google.protobuf.TimestampR\texpiresAt\x12:\n\ndepends_on\x18\x0b \x03(\x0b2\x1b.conveyor.v1.TaskDependencyR\tdependsOn\x12\'\n\x0fconcurrency_key\x18\x0c \x01(\tR\x0econcurrencyKey\x12;\n\x0cretry_policy\x18\r \x01(\x0b2\x18.conveyor.v1.RetryPolicyR\x0bretryPolicyJ\x04\x08\x0e\x10\x10"\xa1\x01\n\x0bRetryPolicy\x126\n\x08strategy\x18\x01 \x01(\x0e2\x1a.conveyor.v1.RetryStrategyR\x08strategy\x12-\n\x04base\x18\x02 \x01(\x0b2\x19.google.protobuf.DurationR\x04base\x12+\n\x03max\x18\x03 \x01(\x0b2\x19.google.protobuf.DurationR\x03max*\x86\x02\n\tTaskState\x12\x1a\n\x16TASK_STATE_UNSPECIFIED\x10\x00\x12\x18\n\x14TASK_STATE_SCHEDULED\x10\x01\x12\x16\n\x12TASK_STATE_PENDING\x10\x02\x12\x15\n\x11TASK_STATE_ACTIVE\x10\x03\x12\x14\n\x10TASK_STATE_RETRY\x10\x04\x12\x18\n\x14TASK_STATE_COMPLETED\x10\x05\x12\x17\n\x13TASK_STATE_ARCHIVED\x10\x06\x12\x17\n\x13TASK_STATE_CANCELED\x10\x07\x12\x1a\n\x16TASK_STATE_AGGREGATING\x10\x08\x12\x16\n\x12TASK_STATE_BLOCKED\x10\t*\x9f\x02\n\rTaskEventType\x12\x1f\n\x1bTASK_EVENT_TYPE_UNSPECIFIED\x10\x00\x12\x1c\n\x18TASK_EVENT_TYPE_ENQUEUED\x10\x01\x12\x1d\n\x19TASK_EVENT_TYPE_SCHEDULED\x10\x02\x12\x1a\n\x16TASK_EVENT_TYPE_LEASED\x10\x03\x12\x1d\n\x19TASK_EVENT_TYPE_COMPLETED\x10\x04\x12\x1b\n\x17TASK_EVENT_TYPE_RETRIED\x10\x05\x12\x1c\n\x18TASK_EVENT_TYPE_ARCHIVED\x10\x06\x12\x1c\n\x18TASK_EVENT_TYPE_CANCELED\x10\x07\x12\x1c\n\x18TASK_EVENT_TYPE_RELEASED\x10\x08*\xbf\x01\n\x17DependencyFailurePolicy\x12)\n%DEPENDENCY_FAILURE_POLICY_UNSPECIFIED\x10\x00\x12#\n\x1fDEPENDENCY_FAILURE_POLICY_BLOCK\x10\x01\x12,\n(DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL\x10\x02\x12&\n"DEPENDENCY_FAILURE_POLICY_CONTINUE\x10\x03*\x84\x01\n\rRetryStrategy\x12\x1e\n\x1aRETRY_STRATEGY_UNSPECIFIED\x10\x00\x12\x1e\n\x1aRETRY_STRATEGY_EXPONENTIAL\x10\x01\x12\x19\n\x15RETRY_STRATEGY_LINEAR\x10\x02\x12\x18\n\x14RETRY_STRATEGY_FIXED\x10\x03b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'conveyor.v1.task_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    DESCRIPTOR._loaded_options = None
    _globals['_TASKENVELOPE_METADATAENTRY']._loaded_options = None
    _globals['_TASKENVELOPE_METADATAENTRY']._serialized_options = b'8\x01'
    _globals['_TASKSTATE']._serialized_start = 1941
    _globals['_TASKSTATE']._serialized_end = 2203
    _globals['_TASKEVENTTYPE']._serialized_start = 2206
    _globals['_TASKEVENTTYPE']._serialized_end = 2493
    _globals['_DEPENDENCYFAILUREPOLICY']._serialized_start = 2496
    _globals['_DEPENDENCYFAILUREPOLICY']._serialized_end = 2687
    _globals['_RETRYSTRATEGY']._serialized_start = 2690
    _globals['_RETRYSTRATEGY']._serialized_end = 2822
    _globals['_TASKEVENT']._serialized_start = 105
    _globals['_TASKEVENT']._serialized_end = 397
    _globals['_TASKDEPENDENCY']._serialized_start = 399
    _globals['_TASKDEPENDENCY']._serialized_end = 509
    _globals['_TASKENVELOPE']._serialized_start = 512
    _globals['_TASKENVELOPE']._serialized_end = 1138
    _globals['_TASKENVELOPE_METADATAENTRY']._serialized_start = 1079
    _globals['_TASKENVELOPE_METADATAENTRY']._serialized_end = 1138
    _globals['_TASKOPTIONS']._serialized_start = 1141
    _globals['_TASKOPTIONS']._serialized_end = 1774
    _globals['_RETRYPOLICY']._serialized_start = 1777
    _globals['_RETRYPOLICY']._serialized_end = 1938