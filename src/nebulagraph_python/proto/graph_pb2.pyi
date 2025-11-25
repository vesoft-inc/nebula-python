from . import nebula_common_pb2 as _nebula_common_pb2
from . import vector_pb2 as _vector_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryStats(_message.Message):
    __slots__ = ["num_affected_nodes", "num_affected_edges", "exported_paths", "num_exported_records"]
    NUM_AFFECTED_NODES_FIELD_NUMBER: _ClassVar[int]
    NUM_AFFECTED_EDGES_FIELD_NUMBER: _ClassVar[int]
    EXPORTED_PATHS_FIELD_NUMBER: _ClassVar[int]
    NUM_EXPORTED_RECORDS_FIELD_NUMBER: _ClassVar[int]
    num_affected_nodes: int
    num_affected_edges: int
    exported_paths: _containers.RepeatedScalarFieldContainer[bytes]
    num_exported_records: int
    def __init__(self, num_affected_nodes: _Optional[int] = ..., num_affected_edges: _Optional[int] = ..., exported_paths: _Optional[_Iterable[bytes]] = ..., num_exported_records: _Optional[int] = ...) -> None: ...

class PlanInfo(_message.Message):
    __slots__ = ["id", "name", "details", "columns", "time_ms", "rows", "memory_kib", "blocked_ms", "queued_ms", "consume_ms", "produce_ms", "finish_ms", "batches", "concurrency", "other_stats_json", "children"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TIME_MS_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_KIB_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_MS_FIELD_NUMBER: _ClassVar[int]
    QUEUED_MS_FIELD_NUMBER: _ClassVar[int]
    CONSUME_MS_FIELD_NUMBER: _ClassVar[int]
    PRODUCE_MS_FIELD_NUMBER: _ClassVar[int]
    FINISH_MS_FIELD_NUMBER: _ClassVar[int]
    BATCHES_FIELD_NUMBER: _ClassVar[int]
    CONCURRENCY_FIELD_NUMBER: _ClassVar[int]
    OTHER_STATS_JSON_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: bytes
    details: bytes
    columns: _containers.RepeatedScalarFieldContainer[bytes]
    time_ms: float
    rows: int
    memory_kib: float
    blocked_ms: float
    queued_ms: float
    consume_ms: float
    produce_ms: float
    finish_ms: float
    batches: int
    concurrency: int
    other_stats_json: bytes
    children: _containers.RepeatedCompositeFieldContainer[PlanInfo]
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[bytes] = ..., details: _Optional[bytes] = ..., columns: _Optional[_Iterable[bytes]] = ..., time_ms: _Optional[float] = ..., rows: _Optional[int] = ..., memory_kib: _Optional[float] = ..., blocked_ms: _Optional[float] = ..., queued_ms: _Optional[float] = ..., consume_ms: _Optional[float] = ..., produce_ms: _Optional[float] = ..., finish_ms: _Optional[float] = ..., batches: _Optional[int] = ..., concurrency: _Optional[int] = ..., other_stats_json: _Optional[bytes] = ..., children: _Optional[_Iterable[_Union[PlanInfo, _Mapping]]] = ...) -> None: ...

class ElapsedTime(_message.Message):
    __slots__ = ["total_server_time_us", "build_time_us", "optimize_time_us", "serialize_time_us", "parse_time_us"]
    TOTAL_SERVER_TIME_US_FIELD_NUMBER: _ClassVar[int]
    BUILD_TIME_US_FIELD_NUMBER: _ClassVar[int]
    OPTIMIZE_TIME_US_FIELD_NUMBER: _ClassVar[int]
    SERIALIZE_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PARSE_TIME_US_FIELD_NUMBER: _ClassVar[int]
    total_server_time_us: int
    build_time_us: int
    optimize_time_us: int
    serialize_time_us: int
    parse_time_us: int
    def __init__(self, total_server_time_us: _Optional[int] = ..., build_time_us: _Optional[int] = ..., optimize_time_us: _Optional[int] = ..., serialize_time_us: _Optional[int] = ..., parse_time_us: _Optional[int] = ...) -> None: ...

class Summary(_message.Message):
    __slots__ = ["elapsed_time", "explain_type", "plan_info", "query_stats", "log_stream", "num_warnings"]
    ELAPSED_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPLAIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    PLAN_INFO_FIELD_NUMBER: _ClassVar[int]
    QUERY_STATS_FIELD_NUMBER: _ClassVar[int]
    LOG_STREAM_FIELD_NUMBER: _ClassVar[int]
    NUM_WARNINGS_FIELD_NUMBER: _ClassVar[int]
    elapsed_time: ElapsedTime
    explain_type: bytes
    plan_info: PlanInfo
    query_stats: QueryStats
    log_stream: bytes
    num_warnings: int
    def __init__(self, elapsed_time: _Optional[_Union[ElapsedTime, _Mapping]] = ..., explain_type: _Optional[bytes] = ..., plan_info: _Optional[_Union[PlanInfo, _Mapping]] = ..., query_stats: _Optional[_Union[QueryStats, _Mapping]] = ..., log_stream: _Optional[bytes] = ..., num_warnings: _Optional[int] = ...) -> None: ...

class ExecuteRequest(_message.Message):
    __slots__ = ["session_id", "stmt"]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    STMT_FIELD_NUMBER: _ClassVar[int]
    session_id: int
    stmt: bytes
    def __init__(self, session_id: _Optional[int] = ..., stmt: _Optional[bytes] = ...) -> None: ...

class ExecuteResponse(_message.Message):
    __slots__ = ["status", "result", "summary", "cursor"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    status: _nebula_common_pb2.Status
    result: _vector_pb2.VectorResultTable
    summary: Summary
    cursor: bytes
    def __init__(self, status: _Optional[_Union[_nebula_common_pb2.Status, _Mapping]] = ..., result: _Optional[_Union[_vector_pb2.VectorResultTable, _Mapping]] = ..., summary: _Optional[_Union[Summary, _Mapping]] = ..., cursor: _Optional[bytes] = ...) -> None: ...

class AuthRequest(_message.Message):
    __slots__ = ["username", "auth_info", "client_info"]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    AUTH_INFO_FIELD_NUMBER: _ClassVar[int]
    CLIENT_INFO_FIELD_NUMBER: _ClassVar[int]
    username: bytes
    auth_info: bytes
    client_info: _nebula_common_pb2.ClientInfo
    def __init__(self, username: _Optional[bytes] = ..., auth_info: _Optional[bytes] = ..., client_info: _Optional[_Union[_nebula_common_pb2.ClientInfo, _Mapping]] = ...) -> None: ...

class AuthResponse(_message.Message):
    __slots__ = ["status", "session_id", "version"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    status: _nebula_common_pb2.Status
    session_id: int
    version: bytes
    def __init__(self, status: _Optional[_Union[_nebula_common_pb2.Status, _Mapping]] = ..., session_id: _Optional[int] = ..., version: _Optional[bytes] = ...) -> None: ...
