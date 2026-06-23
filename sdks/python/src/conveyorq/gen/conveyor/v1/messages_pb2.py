"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 3, '', 'conveyor/v1/messages.proto')
_sym_db = _symbol_database.Default()
from ...conveyor.v1 import task_pb2 as conveyor_dot_v1_dot_task__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1aconveyor/v1/messages.proto\x12\x0bconveyor.v1\x1a\x16conveyor/v1/task.proto\x1a\x1fgoogle/protobuf/timestamp.proto"$\n\x0cTaskEnqueued\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue":\n\x0eTasksAvailable\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12\x12\n\x04hint\x18\x02 \x01(\x03R\x04hint"\x9f\x01\n\x0fRegisterGateway\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12!\n\x0cgateway_name\x18\x02 \x01(\tR\x0bgatewayName\x12\x1a\n\x08capacity\x18\x03 \x01(\x05R\x08capacity\x12\x1f\n\x0bbatch_types\x18\x04 \x03(\tR\nbatchTypes\x12\x16\n\x06weight\x18\x05 \x01(\x05R\x06weight"b\n\rGatewayCredit\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12!\n\x0cgateway_name\x18\x02 \x01(\tR\x0bgatewayName\x12\x18\n\x07credits\x18\x03 \x01(\x05R\x07credits"\x9d\x01\n\x0bExecuteTask\x12-\n\x04task\x18\x01 \x01(\x0b2\x19.conveyor.v1.TaskEnvelopeR\x04task\x12\x19\n\x08lease_id\x18\x02 \x01(\tR\x07leaseId\x12D\n\x10lease_expires_at\x18\x03 \x01(\x0b2\x1a.google.protobuf.TimestampR\x0eleaseExpiresAt"\xb6\x01\n\x0cExecuteBatch\x12/\n\x05tasks\x18\x01 \x03(\x0b2\x19.conveyor.v1.TaskEnvelopeR\x05tasks\x12\x19\n\x08lease_id\x18\x02 \x01(\tR\x07leaseId\x12D\n\x10lease_expires_at\x18\x03 \x01(\x0b2\x1a.google.protobuf.TimestampR\x0eleaseExpiresAt\x12\x14\n\x05group\x18\x04 \x01(\tR\x05group"K\n\tFireGroup\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12\x14\n\x05group\x18\x02 \x01(\tR\x05group\x12\x12\n\x04type\x18\x03 \x01(\tR\x04type"\xe7\x01\n\x13GroupLeaseCompleted\x12/\n\x05tasks\x18\x01 \x03(\x0b2\x19.conveyor.v1.TaskEnvelopeR\x05tasks\x12\x19\n\x08lease_id\x18\x02 \x01(\tR\x07leaseId\x12D\n\x10lease_expires_at\x18\x03 \x01(\x0b2\x1a.google.protobuf.TimestampR\x0eleaseExpiresAt\x12\x14\n\x05group\x18\x04 \x01(\tR\x05group\x12\x12\n\x04type\x18\x05 \x01(\tR\x04type\x12\x14\n\x05error\x18\x06 \x01(\tR\x05error"{\n\rTaskCompleted\x12\x17\n\x07task_id\x18\x01 \x01(\tR\x06taskId\x12\x14\n\x05queue\x18\x02 \x01(\tR\x05queue\x12\x18\n\x07success\x18\x03 \x01(\x08R\x07success\x12!\n\x0cgateway_name\x18\x04 \x01(\tR\x0bgatewayName"}\n\x0eBatchCompleted\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12!\n\x0cgateway_name\x18\x02 \x01(\tR\x0bgatewayName\x12\x14\n\x05total\x18\x03 \x01(\x05R\x05total\x12\x1c\n\tsucceeded\x18\x04 \x01(\x05R\tsucceeded""\n\nDrainQueue\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue"#\n\x0bResumeQueue\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue"\'\n\x0cCancelActive\x12\x17\n\x07task_id\x18\x01 \x01(\tR\x06taskId"`\n\x10RateLimitChanged\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12 \n\x0crate_per_sec\x18\x02 \x01(\x01R\nratePerSec\x12\x14\n\x05burst\x18\x03 \x01(\x05R\x05burst"N\n\x17ConcurrencyLimitChanged\x12\x14\n\x05queue\x18\x01 \x01(\tR\x05queue\x12\x1d\n\nmax_active\x18\x02 \x01(\x05R\tmaxActive"%\n\x08FireCron\x12\x19\n\x08entry_id\x18\x01 \x01(\tR\x07entryId"\x14\n\x12CronEntriesChanged"\xbd\x01\n\x13LeaseCycleCompleted\x12/\n\x05tasks\x18\x01 \x03(\x0b2\x19.conveyor.v1.TaskEnvelopeR\x05tasks\x12\x19\n\x08lease_id\x18\x02 \x01(\tR\x07leaseId\x12D\n\x10lease_expires_at\x18\x03 \x01(\x0b2\x1a.google.protobuf.TimestampR\x0eleaseExpiresAt\x12\x14\n\x05error\x18\x04 \x01(\tR\x05error"I\n\x13LeasedTasksReleased\x12\x1a\n\x08released\x18\x01 \x01(\x05R\x08released\x12\x16\n\x06failed\x18\x02 \x01(\x05R\x06failed"\r\n\x0bPromoteTick"\n\n\x08ReapTick"\x10\n\x0eGroupSweepTick",\n\x11ResolveDependents\x12\x17\n\x07task_id\x18\x01 \x01(\tR\x06taskIdb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'conveyor.v1.messages_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    DESCRIPTOR._loaded_options = None
    _globals['_TASKENQUEUED']._serialized_start = 100
    _globals['_TASKENQUEUED']._serialized_end = 136
    _globals['_TASKSAVAILABLE']._serialized_start = 138
    _globals['_TASKSAVAILABLE']._serialized_end = 196
    _globals['_REGISTERGATEWAY']._serialized_start = 199
    _globals['_REGISTERGATEWAY']._serialized_end = 358
    _globals['_GATEWAYCREDIT']._serialized_start = 360
    _globals['_GATEWAYCREDIT']._serialized_end = 458
    _globals['_EXECUTETASK']._serialized_start = 461
    _globals['_EXECUTETASK']._serialized_end = 618
    _globals['_EXECUTEBATCH']._serialized_start = 621
    _globals['_EXECUTEBATCH']._serialized_end = 803
    _globals['_FIREGROUP']._serialized_start = 805
    _globals['_FIREGROUP']._serialized_end = 880
    _globals['_GROUPLEASECOMPLETED']._serialized_start = 883
    _globals['_GROUPLEASECOMPLETED']._serialized_end = 1114
    _globals['_TASKCOMPLETED']._serialized_start = 1116
    _globals['_TASKCOMPLETED']._serialized_end = 1239
    _globals['_BATCHCOMPLETED']._serialized_start = 1241
    _globals['_BATCHCOMPLETED']._serialized_end = 1366
    _globals['_DRAINQUEUE']._serialized_start = 1368
    _globals['_DRAINQUEUE']._serialized_end = 1402
    _globals['_RESUMEQUEUE']._serialized_start = 1404
    _globals['_RESUMEQUEUE']._serialized_end = 1439
    _globals['_CANCELACTIVE']._serialized_start = 1441
    _globals['_CANCELACTIVE']._serialized_end = 1480
    _globals['_RATELIMITCHANGED']._serialized_start = 1482
    _globals['_RATELIMITCHANGED']._serialized_end = 1578
    _globals['_CONCURRENCYLIMITCHANGED']._serialized_start = 1580
    _globals['_CONCURRENCYLIMITCHANGED']._serialized_end = 1658
    _globals['_FIRECRON']._serialized_start = 1660
    _globals['_FIRECRON']._serialized_end = 1697
    _globals['_CRONENTRIESCHANGED']._serialized_start = 1699
    _globals['_CRONENTRIESCHANGED']._serialized_end = 1719
    _globals['_LEASECYCLECOMPLETED']._serialized_start = 1722
    _globals['_LEASECYCLECOMPLETED']._serialized_end = 1911
    _globals['_LEASEDTASKSRELEASED']._serialized_start = 1913
    _globals['_LEASEDTASKSRELEASED']._serialized_end = 1986
    _globals['_PROMOTETICK']._serialized_start = 1988
    _globals['_PROMOTETICK']._serialized_end = 2001
    _globals['_REAPTICK']._serialized_start = 2003
    _globals['_REAPTICK']._serialized_end = 2013
    _globals['_GROUPSWEEPTICK']._serialized_start = 2015
    _globals['_GROUPSWEEPTICK']._serialized_end = 2031
    _globals['_RESOLVEDEPENDENTS']._serialized_start = 2033
    _globals['_RESOLVEDEPENDENTS']._serialized_end = 2077