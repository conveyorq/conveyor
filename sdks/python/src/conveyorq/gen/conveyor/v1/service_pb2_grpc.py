"""Client and server classes corresponding to protobuf-defined services."""
import grpc
from ...conveyor.v1 import service_pb2 as conveyor_dot_v1_dot_service__pb2
from ...conveyor.v1 import task_pb2 as conveyor_dot_v1_dot_task__pb2

class TaskServiceStub(object):
    """Public wire contract. One ConnectRPC port serves gRPC, gRPC-Web, and
    HTTP/JSON. SDKs, the CLI, and the dashboard all consume these services;
    after v1.0 changes must be additive only.

    TaskService is the enqueue-side API.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Enqueue = channel.unary_unary('/conveyor.v1.TaskService/Enqueue', request_serializer=conveyor_dot_v1_dot_service__pb2.EnqueueRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.EnqueueResponse.FromString, _registered_method=True)
        self.EnqueueBatch = channel.unary_unary('/conveyor.v1.TaskService/EnqueueBatch', request_serializer=conveyor_dot_v1_dot_service__pb2.EnqueueBatchRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.EnqueueBatchResponse.FromString, _registered_method=True)
        self.EnqueueTx = channel.unary_unary('/conveyor.v1.TaskService/EnqueueTx', request_serializer=conveyor_dot_v1_dot_service__pb2.EnqueueTxRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.EnqueueTxResponse.FromString, _registered_method=True)
        self.GetTask = channel.unary_unary('/conveyor.v1.TaskService/GetTask', request_serializer=conveyor_dot_v1_dot_service__pb2.GetTaskRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.GetTaskResponse.FromString, _registered_method=True)

class TaskServiceServicer(object):
    """Public wire contract. One ConnectRPC port serves gRPC, gRPC-Web, and
    HTTP/JSON. SDKs, the CLI, and the dashboard all consume these services;
    after v1.0 changes must be additive only.

    TaskService is the enqueue-side API.
    """

    def Enqueue(self, request, context):
        """Enqueue durably commits one task. Idempotent on a client-supplied id.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def EnqueueBatch(self, request, context):
        """EnqueueBatch commits many tasks in one call; results are per-item.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def EnqueueTx(self, request, context):
        """EnqueueTx commits many tasks atomically: either every task in the request
        is enqueued, or none is. Any failure (a duplicate unique key, a unique-key
        collision between two tasks in the same request, or an invalid task) commits
        nothing and returns an error identifying the offending task. Re-committing an
        existing id is a no-op and does not abort the request. Unlike EnqueueBatch,
        there are no per-item results: success returns the committed tasks in order.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetTask(self, request, context):
        """GetTask returns the current state of one task.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_TaskServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {'Enqueue': grpc.unary_unary_rpc_method_handler(servicer.Enqueue, request_deserializer=conveyor_dot_v1_dot_service__pb2.EnqueueRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.EnqueueResponse.SerializeToString), 'EnqueueBatch': grpc.unary_unary_rpc_method_handler(servicer.EnqueueBatch, request_deserializer=conveyor_dot_v1_dot_service__pb2.EnqueueBatchRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.EnqueueBatchResponse.SerializeToString), 'EnqueueTx': grpc.unary_unary_rpc_method_handler(servicer.EnqueueTx, request_deserializer=conveyor_dot_v1_dot_service__pb2.EnqueueTxRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.EnqueueTxResponse.SerializeToString), 'GetTask': grpc.unary_unary_rpc_method_handler(servicer.GetTask, request_deserializer=conveyor_dot_v1_dot_service__pb2.GetTaskRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.GetTaskResponse.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('conveyor.v1.TaskService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('conveyor.v1.TaskService', rpc_method_handlers)

class TaskService(object):
    """Public wire contract. One ConnectRPC port serves gRPC, gRPC-Web, and
    HTTP/JSON. SDKs, the CLI, and the dashboard all consume these services;
    after v1.0 changes must be additive only.

    TaskService is the enqueue-side API.
    """

    @staticmethod
    def Enqueue(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.TaskService/Enqueue', conveyor_dot_v1_dot_service__pb2.EnqueueRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.EnqueueResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def EnqueueBatch(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.TaskService/EnqueueBatch', conveyor_dot_v1_dot_service__pb2.EnqueueBatchRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.EnqueueBatchResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def EnqueueTx(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.TaskService/EnqueueTx', conveyor_dot_v1_dot_service__pb2.EnqueueTxRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.EnqueueTxResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def GetTask(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.TaskService/GetTask', conveyor_dot_v1_dot_service__pb2.GetTaskRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.GetTaskResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

class WorkerServiceStub(object):
    """WorkerService carries worker sessions.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Session = channel.stream_stream('/conveyor.v1.WorkerService/Session', request_serializer=conveyor_dot_v1_dot_service__pb2.WorkerMessage.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ServerMessage.FromString, _registered_method=True)

class WorkerServiceServicer(object):
    """WorkerService carries worker sessions.
    """

    def Session(self, request_iterator, context):
        """Session is the single bidirectional stream a worker process holds.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_WorkerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {'Session': grpc.stream_stream_rpc_method_handler(servicer.Session, request_deserializer=conveyor_dot_v1_dot_service__pb2.WorkerMessage.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ServerMessage.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('conveyor.v1.WorkerService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('conveyor.v1.WorkerService', rpc_method_handlers)

class WorkerService(object):
    """WorkerService carries worker sessions.
    """

    @staticmethod
    def Session(request_iterator, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/conveyor.v1.WorkerService/Session', conveyor_dot_v1_dot_service__pb2.WorkerMessage.SerializeToString, conveyor_dot_v1_dot_service__pb2.ServerMessage.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

class AdminServiceStub(object):
    """AdminService is the inspection and operations API.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ListQueues = channel.unary_unary('/conveyor.v1.AdminService/ListQueues', request_serializer=conveyor_dot_v1_dot_service__pb2.ListQueuesRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListQueuesResponse.FromString, _registered_method=True)
        self.PauseQueue = channel.unary_unary('/conveyor.v1.AdminService/PauseQueue', request_serializer=conveyor_dot_v1_dot_service__pb2.PauseQueueRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.PauseQueueResponse.FromString, _registered_method=True)
        self.ResumeQueue = channel.unary_unary('/conveyor.v1.AdminService/ResumeQueue', request_serializer=conveyor_dot_v1_dot_service__pb2.ResumeQueueRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ResumeQueueResponse.FromString, _registered_method=True)
        self.ListRateLimits = channel.unary_unary('/conveyor.v1.AdminService/ListRateLimits', request_serializer=conveyor_dot_v1_dot_service__pb2.ListRateLimitsRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListRateLimitsResponse.FromString, _registered_method=True)
        self.SetQueueRateLimit = channel.unary_unary('/conveyor.v1.AdminService/SetQueueRateLimit', request_serializer=conveyor_dot_v1_dot_service__pb2.SetQueueRateLimitRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.SetQueueRateLimitResponse.FromString, _registered_method=True)
        self.DeleteQueueRateLimit = channel.unary_unary('/conveyor.v1.AdminService/DeleteQueueRateLimit', request_serializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueRateLimitRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueRateLimitResponse.FromString, _registered_method=True)
        self.ListConcurrencyLimits = channel.unary_unary('/conveyor.v1.AdminService/ListConcurrencyLimits', request_serializer=conveyor_dot_v1_dot_service__pb2.ListConcurrencyLimitsRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListConcurrencyLimitsResponse.FromString, _registered_method=True)
        self.SetQueueConcurrencyLimit = channel.unary_unary('/conveyor.v1.AdminService/SetQueueConcurrencyLimit', request_serializer=conveyor_dot_v1_dot_service__pb2.SetQueueConcurrencyLimitRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.SetQueueConcurrencyLimitResponse.FromString, _registered_method=True)
        self.DeleteQueueConcurrencyLimit = channel.unary_unary('/conveyor.v1.AdminService/DeleteQueueConcurrencyLimit', request_serializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueConcurrencyLimitRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueConcurrencyLimitResponse.FromString, _registered_method=True)
        self.ListGroupConfigs = channel.unary_unary('/conveyor.v1.AdminService/ListGroupConfigs', request_serializer=conveyor_dot_v1_dot_service__pb2.ListGroupConfigsRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListGroupConfigsResponse.FromString, _registered_method=True)
        self.SetGroupConfig = channel.unary_unary('/conveyor.v1.AdminService/SetGroupConfig', request_serializer=conveyor_dot_v1_dot_service__pb2.SetGroupConfigRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.SetGroupConfigResponse.FromString, _registered_method=True)
        self.DeleteGroupConfig = channel.unary_unary('/conveyor.v1.AdminService/DeleteGroupConfig', request_serializer=conveyor_dot_v1_dot_service__pb2.DeleteGroupConfigRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteGroupConfigResponse.FromString, _registered_method=True)
        self.ListTasks = channel.unary_unary('/conveyor.v1.AdminService/ListTasks', request_serializer=conveyor_dot_v1_dot_service__pb2.ListTasksRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListTasksResponse.FromString, _registered_method=True)
        self.CancelTask = channel.unary_unary('/conveyor.v1.AdminService/CancelTask', request_serializer=conveyor_dot_v1_dot_service__pb2.CancelTaskRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.CancelTaskResponse.FromString, _registered_method=True)
        self.DeleteTask = channel.unary_unary('/conveyor.v1.AdminService/DeleteTask', request_serializer=conveyor_dot_v1_dot_service__pb2.DeleteTaskRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteTaskResponse.FromString, _registered_method=True)
        self.RunTask = channel.unary_unary('/conveyor.v1.AdminService/RunTask', request_serializer=conveyor_dot_v1_dot_service__pb2.RunTaskRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.RunTaskResponse.FromString, _registered_method=True)
        self.RescheduleTask = channel.unary_unary('/conveyor.v1.AdminService/RescheduleTask', request_serializer=conveyor_dot_v1_dot_service__pb2.RescheduleTaskRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.RescheduleTaskResponse.FromString, _registered_method=True)
        self.ArchiveTask = channel.unary_unary('/conveyor.v1.AdminService/ArchiveTask', request_serializer=conveyor_dot_v1_dot_service__pb2.ArchiveTaskRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ArchiveTaskResponse.FromString, _registered_method=True)
        self.BatchDeleteTasks = channel.unary_unary('/conveyor.v1.AdminService/BatchDeleteTasks', request_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, _registered_method=True)
        self.BatchRunTasks = channel.unary_unary('/conveyor.v1.AdminService/BatchRunTasks', request_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, _registered_method=True)
        self.BatchCancelTasks = channel.unary_unary('/conveyor.v1.AdminService/BatchCancelTasks', request_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, _registered_method=True)
        self.BatchArchiveTasks = channel.unary_unary('/conveyor.v1.AdminService/BatchArchiveTasks', request_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, _registered_method=True)
        self.ListCron = channel.unary_unary('/conveyor.v1.AdminService/ListCron', request_serializer=conveyor_dot_v1_dot_service__pb2.ListCronRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListCronResponse.FromString, _registered_method=True)
        self.UpsertCron = channel.unary_unary('/conveyor.v1.AdminService/UpsertCron', request_serializer=conveyor_dot_v1_dot_service__pb2.UpsertCronRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.UpsertCronResponse.FromString, _registered_method=True)
        self.PauseCron = channel.unary_unary('/conveyor.v1.AdminService/PauseCron', request_serializer=conveyor_dot_v1_dot_service__pb2.PauseCronRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.PauseCronResponse.FromString, _registered_method=True)
        self.ResumeCron = channel.unary_unary('/conveyor.v1.AdminService/ResumeCron', request_serializer=conveyor_dot_v1_dot_service__pb2.ResumeCronRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ResumeCronResponse.FromString, _registered_method=True)
        self.DeleteCron = channel.unary_unary('/conveyor.v1.AdminService/DeleteCron', request_serializer=conveyor_dot_v1_dot_service__pb2.DeleteCronRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteCronResponse.FromString, _registered_method=True)
        self.ClusterInfo = channel.unary_unary('/conveyor.v1.AdminService/ClusterInfo', request_serializer=conveyor_dot_v1_dot_service__pb2.ClusterInfoRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ClusterInfoResponse.FromString, _registered_method=True)
        self.ListWorkerSessions = channel.unary_unary('/conveyor.v1.AdminService/ListWorkerSessions', request_serializer=conveyor_dot_v1_dot_service__pb2.ListWorkerSessionsRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.ListWorkerSessionsResponse.FromString, _registered_method=True)
        self.BrokerInfo = channel.unary_unary('/conveyor.v1.AdminService/BrokerInfo', request_serializer=conveyor_dot_v1_dot_service__pb2.BrokerInfoRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_service__pb2.BrokerInfoResponse.FromString, _registered_method=True)
        self.WatchEvents = channel.unary_stream('/conveyor.v1.AdminService/WatchEvents', request_serializer=conveyor_dot_v1_dot_service__pb2.WatchEventsRequest.SerializeToString, response_deserializer=conveyor_dot_v1_dot_task__pb2.TaskEvent.FromString, _registered_method=True)

class AdminServiceServicer(object):
    """AdminService is the inspection and operations API.
    """

    def ListQueues(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def PauseQueue(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ResumeQueue(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListRateLimits(self, request, context):
        """ListRateLimits returns every per-queue dispatch-rate override.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetQueueRateLimit(self, request, context):
        """SetQueueRateLimit sets a queue's dispatch-rate override.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteQueueRateLimit(self, request, context):
        """DeleteQueueRateLimit clears a queue's override, reverting it to the default.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListConcurrencyLimits(self, request, context):
        """ListConcurrencyLimits returns every per-queue concurrency limit.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetQueueConcurrencyLimit(self, request, context):
        """SetQueueConcurrencyLimit sets a queue's per-key concurrency limit.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteQueueConcurrencyLimit(self, request, context):
        """DeleteQueueConcurrencyLimit clears a queue's concurrency limit, leaving its keys unbounded.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListGroupConfigs(self, request, context):
        """ListGroupConfigs returns every per-group aggregation override.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetGroupConfig(self, request, context):
        """SetGroupConfig sets a group's aggregation override (max size, max delay, grace period).
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteGroupConfig(self, request, context):
        """DeleteGroupConfig clears a group's override, reverting it to the queue-wide or global default.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListTasks(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CancelTask(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteTask(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RunTask(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RescheduleTask(self, request, context):
        """RescheduleTask moves a waiting (scheduled, pending, or retry) task's due
        time to a new instant.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ArchiveTask(self, request, context):
        """ArchiveTask dead-letters a waiting (scheduled, pending, or retry) task.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BatchDeleteTasks(self, request, context):
        """BatchDeleteTasks deletes each listed task, reporting per-id outcomes.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BatchRunTasks(self, request, context):
        """BatchRunTasks makes each listed task due immediately.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BatchCancelTasks(self, request, context):
        """BatchCancelTasks cancels each listed task.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BatchArchiveTasks(self, request, context):
        """BatchArchiveTasks dead-letters each listed task.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListCron(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpsertCron(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def PauseCron(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ResumeCron(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteCron(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ClusterInfo(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListWorkerSessions(self, request, context):
        """ListWorkerSessions lists the worker sessions connected to the node serving
        the request, for the operations dashboard's worker-topology view.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def BrokerInfo(self, request, context):
        """BrokerInfo reports the storage engine's driver and runtime statistics,
        the analogue of a backing-store health page.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def WatchEvents(self, request, context):
        """WatchEvents streams task lifecycle transitions as they occur, replacing
        polling for live dashboards, alerting, audit logs, and event-driven
        chaining. Delivery is best-effort and non-durable: a watcher receives
        events from the moment it subscribes, and a watcher too slow to keep up
        has events dropped rather than stalling task processing. The optional
        queue and event-type filters narrow the stream server-side.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_AdminServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {'ListQueues': grpc.unary_unary_rpc_method_handler(servicer.ListQueues, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListQueuesRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListQueuesResponse.SerializeToString), 'PauseQueue': grpc.unary_unary_rpc_method_handler(servicer.PauseQueue, request_deserializer=conveyor_dot_v1_dot_service__pb2.PauseQueueRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.PauseQueueResponse.SerializeToString), 'ResumeQueue': grpc.unary_unary_rpc_method_handler(servicer.ResumeQueue, request_deserializer=conveyor_dot_v1_dot_service__pb2.ResumeQueueRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ResumeQueueResponse.SerializeToString), 'ListRateLimits': grpc.unary_unary_rpc_method_handler(servicer.ListRateLimits, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListRateLimitsRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListRateLimitsResponse.SerializeToString), 'SetQueueRateLimit': grpc.unary_unary_rpc_method_handler(servicer.SetQueueRateLimit, request_deserializer=conveyor_dot_v1_dot_service__pb2.SetQueueRateLimitRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.SetQueueRateLimitResponse.SerializeToString), 'DeleteQueueRateLimit': grpc.unary_unary_rpc_method_handler(servicer.DeleteQueueRateLimit, request_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueRateLimitRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueRateLimitResponse.SerializeToString), 'ListConcurrencyLimits': grpc.unary_unary_rpc_method_handler(servicer.ListConcurrencyLimits, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListConcurrencyLimitsRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListConcurrencyLimitsResponse.SerializeToString), 'SetQueueConcurrencyLimit': grpc.unary_unary_rpc_method_handler(servicer.SetQueueConcurrencyLimit, request_deserializer=conveyor_dot_v1_dot_service__pb2.SetQueueConcurrencyLimitRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.SetQueueConcurrencyLimitResponse.SerializeToString), 'DeleteQueueConcurrencyLimit': grpc.unary_unary_rpc_method_handler(servicer.DeleteQueueConcurrencyLimit, request_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueConcurrencyLimitRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.DeleteQueueConcurrencyLimitResponse.SerializeToString), 'ListGroupConfigs': grpc.unary_unary_rpc_method_handler(servicer.ListGroupConfigs, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListGroupConfigsRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListGroupConfigsResponse.SerializeToString), 'SetGroupConfig': grpc.unary_unary_rpc_method_handler(servicer.SetGroupConfig, request_deserializer=conveyor_dot_v1_dot_service__pb2.SetGroupConfigRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.SetGroupConfigResponse.SerializeToString), 'DeleteGroupConfig': grpc.unary_unary_rpc_method_handler(servicer.DeleteGroupConfig, request_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteGroupConfigRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.DeleteGroupConfigResponse.SerializeToString), 'ListTasks': grpc.unary_unary_rpc_method_handler(servicer.ListTasks, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListTasksRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListTasksResponse.SerializeToString), 'CancelTask': grpc.unary_unary_rpc_method_handler(servicer.CancelTask, request_deserializer=conveyor_dot_v1_dot_service__pb2.CancelTaskRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.CancelTaskResponse.SerializeToString), 'DeleteTask': grpc.unary_unary_rpc_method_handler(servicer.DeleteTask, request_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteTaskRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.DeleteTaskResponse.SerializeToString), 'RunTask': grpc.unary_unary_rpc_method_handler(servicer.RunTask, request_deserializer=conveyor_dot_v1_dot_service__pb2.RunTaskRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.RunTaskResponse.SerializeToString), 'RescheduleTask': grpc.unary_unary_rpc_method_handler(servicer.RescheduleTask, request_deserializer=conveyor_dot_v1_dot_service__pb2.RescheduleTaskRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.RescheduleTaskResponse.SerializeToString), 'ArchiveTask': grpc.unary_unary_rpc_method_handler(servicer.ArchiveTask, request_deserializer=conveyor_dot_v1_dot_service__pb2.ArchiveTaskRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ArchiveTaskResponse.SerializeToString), 'BatchDeleteTasks': grpc.unary_unary_rpc_method_handler(servicer.BatchDeleteTasks, request_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.SerializeToString), 'BatchRunTasks': grpc.unary_unary_rpc_method_handler(servicer.BatchRunTasks, request_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.SerializeToString), 'BatchCancelTasks': grpc.unary_unary_rpc_method_handler(servicer.BatchCancelTasks, request_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.SerializeToString), 'BatchArchiveTasks': grpc.unary_unary_rpc_method_handler(servicer.BatchArchiveTasks, request_deserializer=conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.SerializeToString), 'ListCron': grpc.unary_unary_rpc_method_handler(servicer.ListCron, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListCronRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListCronResponse.SerializeToString), 'UpsertCron': grpc.unary_unary_rpc_method_handler(servicer.UpsertCron, request_deserializer=conveyor_dot_v1_dot_service__pb2.UpsertCronRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.UpsertCronResponse.SerializeToString), 'PauseCron': grpc.unary_unary_rpc_method_handler(servicer.PauseCron, request_deserializer=conveyor_dot_v1_dot_service__pb2.PauseCronRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.PauseCronResponse.SerializeToString), 'ResumeCron': grpc.unary_unary_rpc_method_handler(servicer.ResumeCron, request_deserializer=conveyor_dot_v1_dot_service__pb2.ResumeCronRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ResumeCronResponse.SerializeToString), 'DeleteCron': grpc.unary_unary_rpc_method_handler(servicer.DeleteCron, request_deserializer=conveyor_dot_v1_dot_service__pb2.DeleteCronRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.DeleteCronResponse.SerializeToString), 'ClusterInfo': grpc.unary_unary_rpc_method_handler(servicer.ClusterInfo, request_deserializer=conveyor_dot_v1_dot_service__pb2.ClusterInfoRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ClusterInfoResponse.SerializeToString), 'ListWorkerSessions': grpc.unary_unary_rpc_method_handler(servicer.ListWorkerSessions, request_deserializer=conveyor_dot_v1_dot_service__pb2.ListWorkerSessionsRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.ListWorkerSessionsResponse.SerializeToString), 'BrokerInfo': grpc.unary_unary_rpc_method_handler(servicer.BrokerInfo, request_deserializer=conveyor_dot_v1_dot_service__pb2.BrokerInfoRequest.FromString, response_serializer=conveyor_dot_v1_dot_service__pb2.BrokerInfoResponse.SerializeToString), 'WatchEvents': grpc.unary_stream_rpc_method_handler(servicer.WatchEvents, request_deserializer=conveyor_dot_v1_dot_service__pb2.WatchEventsRequest.FromString, response_serializer=conveyor_dot_v1_dot_task__pb2.TaskEvent.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('conveyor.v1.AdminService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('conveyor.v1.AdminService', rpc_method_handlers)

class AdminService(object):
    """AdminService is the inspection and operations API.
    """

    @staticmethod
    def ListQueues(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListQueues', conveyor_dot_v1_dot_service__pb2.ListQueuesRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListQueuesResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def PauseQueue(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/PauseQueue', conveyor_dot_v1_dot_service__pb2.PauseQueueRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.PauseQueueResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ResumeQueue(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ResumeQueue', conveyor_dot_v1_dot_service__pb2.ResumeQueueRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ResumeQueueResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListRateLimits(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListRateLimits', conveyor_dot_v1_dot_service__pb2.ListRateLimitsRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListRateLimitsResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def SetQueueRateLimit(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/SetQueueRateLimit', conveyor_dot_v1_dot_service__pb2.SetQueueRateLimitRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.SetQueueRateLimitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def DeleteQueueRateLimit(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/DeleteQueueRateLimit', conveyor_dot_v1_dot_service__pb2.DeleteQueueRateLimitRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.DeleteQueueRateLimitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListConcurrencyLimits(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListConcurrencyLimits', conveyor_dot_v1_dot_service__pb2.ListConcurrencyLimitsRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListConcurrencyLimitsResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def SetQueueConcurrencyLimit(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/SetQueueConcurrencyLimit', conveyor_dot_v1_dot_service__pb2.SetQueueConcurrencyLimitRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.SetQueueConcurrencyLimitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def DeleteQueueConcurrencyLimit(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/DeleteQueueConcurrencyLimit', conveyor_dot_v1_dot_service__pb2.DeleteQueueConcurrencyLimitRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.DeleteQueueConcurrencyLimitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListGroupConfigs(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListGroupConfigs', conveyor_dot_v1_dot_service__pb2.ListGroupConfigsRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListGroupConfigsResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def SetGroupConfig(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/SetGroupConfig', conveyor_dot_v1_dot_service__pb2.SetGroupConfigRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.SetGroupConfigResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def DeleteGroupConfig(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/DeleteGroupConfig', conveyor_dot_v1_dot_service__pb2.DeleteGroupConfigRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.DeleteGroupConfigResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListTasks(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListTasks', conveyor_dot_v1_dot_service__pb2.ListTasksRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListTasksResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def CancelTask(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/CancelTask', conveyor_dot_v1_dot_service__pb2.CancelTaskRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.CancelTaskResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def DeleteTask(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/DeleteTask', conveyor_dot_v1_dot_service__pb2.DeleteTaskRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.DeleteTaskResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def RunTask(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/RunTask', conveyor_dot_v1_dot_service__pb2.RunTaskRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.RunTaskResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def RescheduleTask(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/RescheduleTask', conveyor_dot_v1_dot_service__pb2.RescheduleTaskRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.RescheduleTaskResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ArchiveTask(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ArchiveTask', conveyor_dot_v1_dot_service__pb2.ArchiveTaskRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ArchiveTaskResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def BatchDeleteTasks(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/BatchDeleteTasks', conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def BatchRunTasks(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/BatchRunTasks', conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def BatchCancelTasks(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/BatchCancelTasks', conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def BatchArchiveTasks(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/BatchArchiveTasks', conveyor_dot_v1_dot_service__pb2.BatchTasksRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.BatchTasksResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListCron(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListCron', conveyor_dot_v1_dot_service__pb2.ListCronRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListCronResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def UpsertCron(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/UpsertCron', conveyor_dot_v1_dot_service__pb2.UpsertCronRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.UpsertCronResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def PauseCron(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/PauseCron', conveyor_dot_v1_dot_service__pb2.PauseCronRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.PauseCronResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ResumeCron(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ResumeCron', conveyor_dot_v1_dot_service__pb2.ResumeCronRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ResumeCronResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def DeleteCron(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/DeleteCron', conveyor_dot_v1_dot_service__pb2.DeleteCronRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.DeleteCronResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ClusterInfo(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ClusterInfo', conveyor_dot_v1_dot_service__pb2.ClusterInfoRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ClusterInfoResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListWorkerSessions(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/ListWorkerSessions', conveyor_dot_v1_dot_service__pb2.ListWorkerSessionsRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.ListWorkerSessionsResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def BrokerInfo(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/conveyor.v1.AdminService/BrokerInfo', conveyor_dot_v1_dot_service__pb2.BrokerInfoRequest.SerializeToString, conveyor_dot_v1_dot_service__pb2.BrokerInfoResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def WatchEvents(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_stream(request, target, '/conveyor.v1.AdminService/WatchEvents', conveyor_dot_v1_dot_service__pb2.WatchEventsRequest.SerializeToString, conveyor_dot_v1_dot_task__pb2.TaskEvent.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)