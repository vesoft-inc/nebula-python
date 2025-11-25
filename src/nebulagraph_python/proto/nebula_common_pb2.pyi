from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ServiceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    UNKNOWN: _ClassVar[ServiceType]
    STORAGE: _ClassVar[ServiceType]
    GRAPH: _ClassVar[ServiceType]
    META: _ClassVar[ServiceType]
    ANALYTIC: _ClassVar[ServiceType]
    ALL: _ClassVar[ServiceType]

class RoutePolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    LEADER_ONLY: _ClassVar[RoutePolicy]
    LEADER_FIRST: _ClassVar[RoutePolicy]
    ZONE_AFFINITY: _ClassVar[RoutePolicy]
    ZONE_STRICT: _ClassVar[RoutePolicy]
UNKNOWN: ServiceType
STORAGE: ServiceType
GRAPH: ServiceType
META: ServiceType
ANALYTIC: ServiceType
ALL: ServiceType
LEADER_ONLY: RoutePolicy
LEADER_FIRST: RoutePolicy
ZONE_AFFINITY: RoutePolicy
ZONE_STRICT: RoutePolicy
PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
protocol_version: _descriptor.FieldDescriptor
SUPPORTED_VERSIONS_FIELD_NUMBER: _ClassVar[int]
supported_versions: _descriptor.FieldDescriptor
META_PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
meta_protocol_version: _descriptor.FieldDescriptor
META_SUPPORTED_VERSIONS_FIELD_NUMBER: _ClassVar[int]
meta_supported_versions: _descriptor.FieldDescriptor

class Duration(_message.Message):
    __slots__ = ["is_month_based", "year", "month", "day", "hour", "minute", "sec", "microsec"]
    IS_MONTH_BASED_FIELD_NUMBER: _ClassVar[int]
    YEAR_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    DAY_FIELD_NUMBER: _ClassVar[int]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SEC_FIELD_NUMBER: _ClassVar[int]
    MICROSEC_FIELD_NUMBER: _ClassVar[int]
    is_month_based: bool
    year: int
    month: int
    day: int
    hour: int
    minute: int
    sec: int
    microsec: int
    def __init__(self, is_month_based: bool = ..., year: _Optional[int] = ..., month: _Optional[int] = ..., day: _Optional[int] = ..., hour: _Optional[int] = ..., minute: _Optional[int] = ..., sec: _Optional[int] = ..., microsec: _Optional[int] = ...) -> None: ...

class Date(_message.Message):
    __slots__ = ["year", "month", "day"]
    YEAR_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    DAY_FIELD_NUMBER: _ClassVar[int]
    year: int
    month: int
    day: int
    def __init__(self, year: _Optional[int] = ..., month: _Optional[int] = ..., day: _Optional[int] = ...) -> None: ...

class LocalTime(_message.Message):
    __slots__ = ["hour", "minute", "sec", "microsec"]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SEC_FIELD_NUMBER: _ClassVar[int]
    MICROSEC_FIELD_NUMBER: _ClassVar[int]
    hour: int
    minute: int
    sec: int
    microsec: int
    def __init__(self, hour: _Optional[int] = ..., minute: _Optional[int] = ..., sec: _Optional[int] = ..., microsec: _Optional[int] = ...) -> None: ...

class ZonedTime(_message.Message):
    __slots__ = ["hour", "minute", "sec", "microsec", "offset"]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SEC_FIELD_NUMBER: _ClassVar[int]
    MICROSEC_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    hour: int
    minute: int
    sec: int
    microsec: int
    offset: int
    def __init__(self, hour: _Optional[int] = ..., minute: _Optional[int] = ..., sec: _Optional[int] = ..., microsec: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class LocalDatetime(_message.Message):
    __slots__ = ["year", "month", "day", "hour", "minute", "sec", "microsec"]
    YEAR_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    DAY_FIELD_NUMBER: _ClassVar[int]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SEC_FIELD_NUMBER: _ClassVar[int]
    MICROSEC_FIELD_NUMBER: _ClassVar[int]
    year: int
    month: int
    day: int
    hour: int
    minute: int
    sec: int
    microsec: int
    def __init__(self, year: _Optional[int] = ..., month: _Optional[int] = ..., day: _Optional[int] = ..., hour: _Optional[int] = ..., minute: _Optional[int] = ..., sec: _Optional[int] = ..., microsec: _Optional[int] = ...) -> None: ...

class ZonedDatetime(_message.Message):
    __slots__ = ["year", "month", "day", "hour", "minute", "sec", "microsec", "offset"]
    YEAR_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    DAY_FIELD_NUMBER: _ClassVar[int]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SEC_FIELD_NUMBER: _ClassVar[int]
    MICROSEC_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    year: int
    month: int
    day: int
    hour: int
    minute: int
    sec: int
    microsec: int
    offset: int
    def __init__(self, year: _Optional[int] = ..., month: _Optional[int] = ..., day: _Optional[int] = ..., hour: _Optional[int] = ..., minute: _Optional[int] = ..., sec: _Optional[int] = ..., microsec: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class List(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[Value]
    def __init__(self, values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ...) -> None: ...

class Set(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[Value]
    def __init__(self, values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ...) -> None: ...

class Map(_message.Message):
    __slots__ = ["keys", "values"]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[Value]
    values: _containers.RepeatedCompositeFieldContainer[Value]
    def __init__(self, keys: _Optional[_Iterable[_Union[Value, _Mapping]]] = ..., values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ...) -> None: ...

class Vector(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class Record(_message.Message):
    __slots__ = ["values"]
    class ValuesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.MessageMap[str, Value]
    def __init__(self, values: _Optional[_Mapping[str, Value]] = ...) -> None: ...

class Node(_message.Message):
    __slots__ = ["node_id", "graph", "type", "labels", "properties"]
    class PropertiesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    graph: str
    type: str
    labels: _containers.RepeatedScalarFieldContainer[str]
    properties: _containers.MessageMap[str, Value]
    def __init__(self, node_id: _Optional[int] = ..., graph: _Optional[str] = ..., type: _Optional[str] = ..., labels: _Optional[_Iterable[str]] = ..., properties: _Optional[_Mapping[str, Value]] = ...) -> None: ...

class Edge(_message.Message):
    __slots__ = ["src_id", "dst_id", "direction", "graph", "type", "labels", "rank", "properties"]
    class Direction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
        DIRECTED: _ClassVar[Edge.Direction]
        UNDIRECTED: _ClassVar[Edge.Direction]
    DIRECTED: Edge.Direction
    UNDIRECTED: Edge.Direction
    class PropertiesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...
    SRC_ID_FIELD_NUMBER: _ClassVar[int]
    DST_ID_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    src_id: int
    dst_id: int
    direction: Edge.Direction
    graph: str
    type: str
    labels: _containers.RepeatedScalarFieldContainer[str]
    rank: int
    properties: _containers.MessageMap[str, Value]
    def __init__(self, src_id: _Optional[int] = ..., dst_id: _Optional[int] = ..., direction: _Optional[_Union[Edge.Direction, str]] = ..., graph: _Optional[str] = ..., type: _Optional[str] = ..., labels: _Optional[_Iterable[str]] = ..., rank: _Optional[int] = ..., properties: _Optional[_Mapping[str, Value]] = ...) -> None: ...

class Decimal(_message.Message):
    __slots__ = ["sval"]
    SVAL_FIELD_NUMBER: _ClassVar[int]
    sval: str
    def __init__(self, sval: _Optional[str] = ...) -> None: ...

class Path(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[Value]
    def __init__(self, values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ...) -> None: ...

class Ref(_message.Message):
    __slots__ = ["code", "offset"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    code: int
    offset: int
    def __init__(self, code: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class Coordinate(_message.Message):
    __slots__ = ["coords"]
    COORDS_FIELD_NUMBER: _ClassVar[int]
    coords: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, coords: _Optional[_Iterable[float]] = ...) -> None: ...

class Point(_message.Message):
    __slots__ = ["coord"]
    COORD_FIELD_NUMBER: _ClassVar[int]
    coord: Coordinate
    def __init__(self, coord: _Optional[_Union[Coordinate, _Mapping]] = ...) -> None: ...

class LineString(_message.Message):
    __slots__ = ["coords"]
    COORDS_FIELD_NUMBER: _ClassVar[int]
    coords: _containers.RepeatedCompositeFieldContainer[Coordinate]
    def __init__(self, coords: _Optional[_Iterable[_Union[Coordinate, _Mapping]]] = ...) -> None: ...

class Polygon(_message.Message):
    __slots__ = ["rowIndexes", "coords"]
    ROWINDEXES_FIELD_NUMBER: _ClassVar[int]
    COORDS_FIELD_NUMBER: _ClassVar[int]
    rowIndexes: _containers.RepeatedScalarFieldContainer[int]
    coords: _containers.RepeatedCompositeFieldContainer[Coordinate]
    def __init__(self, rowIndexes: _Optional[_Iterable[int]] = ..., coords: _Optional[_Iterable[_Union[Coordinate, _Mapping]]] = ...) -> None: ...

class GeoBase(_message.Message):
    __slots__ = ["shape", "srid", "point", "line_string", "polygon"]
    class GeoShape(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
        Point: _ClassVar[GeoBase.GeoShape]
        PointM: _ClassVar[GeoBase.GeoShape]
        PointZ: _ClassVar[GeoBase.GeoShape]
        PointZM: _ClassVar[GeoBase.GeoShape]
        LineString: _ClassVar[GeoBase.GeoShape]
        LineStringM: _ClassVar[GeoBase.GeoShape]
        LineStringZ: _ClassVar[GeoBase.GeoShape]
        LineStringZM: _ClassVar[GeoBase.GeoShape]
        Polygon: _ClassVar[GeoBase.GeoShape]
        PolygonM: _ClassVar[GeoBase.GeoShape]
        PolygonZ: _ClassVar[GeoBase.GeoShape]
        PolygonZM: _ClassVar[GeoBase.GeoShape]
    Point: GeoBase.GeoShape
    PointM: GeoBase.GeoShape
    PointZ: GeoBase.GeoShape
    PointZM: GeoBase.GeoShape
    LineString: GeoBase.GeoShape
    LineStringM: GeoBase.GeoShape
    LineStringZ: GeoBase.GeoShape
    LineStringZM: GeoBase.GeoShape
    Polygon: GeoBase.GeoShape
    PolygonM: GeoBase.GeoShape
    PolygonZ: GeoBase.GeoShape
    PolygonZM: GeoBase.GeoShape
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    SRID_FIELD_NUMBER: _ClassVar[int]
    POINT_FIELD_NUMBER: _ClassVar[int]
    LINE_STRING_FIELD_NUMBER: _ClassVar[int]
    POLYGON_FIELD_NUMBER: _ClassVar[int]
    shape: GeoBase.GeoShape
    srid: int
    point: Point
    line_string: LineString
    polygon: Polygon
    def __init__(self, shape: _Optional[_Union[GeoBase.GeoShape, str]] = ..., srid: _Optional[int] = ..., point: _Optional[_Union[Point, _Mapping]] = ..., line_string: _Optional[_Union[LineString, _Mapping]] = ..., polygon: _Optional[_Union[Polygon, _Mapping]] = ...) -> None: ...

class Geography(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: GeoBase
    def __init__(self, data: _Optional[_Union[GeoBase, _Mapping]] = ...) -> None: ...

class Geometry(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: GeoBase
    def __init__(self, data: _Optional[_Union[GeoBase, _Mapping]] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = ["bool_value", "int8_value", "uint8_value", "int16_value", "uint16_value", "int32_value", "uint32_value", "int64_value", "uint64_value", "float_value", "double_value", "string_value", "list_value", "record_value", "node_value", "edge_value", "path_value", "duration_value", "local_time_value", "zoned_time_value", "date_value", "local_datetime_value", "zoned_datetime_value", "ref_value", "decimal_value", "vector_value", "point_value", "line_string_value", "polygon_value", "geography_value", "geometry_value", "set_value", "map_value"]
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
        kNull: _ClassVar[Value.Type]
        kBool: _ClassVar[Value.Type]
        kInt8: _ClassVar[Value.Type]
        kUInt8: _ClassVar[Value.Type]
        kInt16: _ClassVar[Value.Type]
        kUInt16: _ClassVar[Value.Type]
        kInt32: _ClassVar[Value.Type]
        kUInt32: _ClassVar[Value.Type]
        kInt64: _ClassVar[Value.Type]
        kUInt64: _ClassVar[Value.Type]
        kFloat: _ClassVar[Value.Type]
        kDouble: _ClassVar[Value.Type]
        kString: _ClassVar[Value.Type]
        kList: _ClassVar[Value.Type]
        kRecord: _ClassVar[Value.Type]
        kNode: _ClassVar[Value.Type]
        kEdge: _ClassVar[Value.Type]
        kPath: _ClassVar[Value.Type]
        kDuration: _ClassVar[Value.Type]
        kLocalTime: _ClassVar[Value.Type]
        kZonedTime: _ClassVar[Value.Type]
        kDate: _ClassVar[Value.Type]
        kLocalDatetime: _ClassVar[Value.Type]
        kZonedDatetime: _ClassVar[Value.Type]
        kRef: _ClassVar[Value.Type]
        kDecimal: _ClassVar[Value.Type]
        kVector: _ClassVar[Value.Type]
        kPoint: _ClassVar[Value.Type]
        kLineString: _ClassVar[Value.Type]
        kPolygon: _ClassVar[Value.Type]
        kGeography: _ClassVar[Value.Type]
        kGeometry: _ClassVar[Value.Type]
        kSet: _ClassVar[Value.Type]
        kMap: _ClassVar[Value.Type]
    kNull: Value.Type
    kBool: Value.Type
    kInt8: Value.Type
    kUInt8: Value.Type
    kInt16: Value.Type
    kUInt16: Value.Type
    kInt32: Value.Type
    kUInt32: Value.Type
    kInt64: Value.Type
    kUInt64: Value.Type
    kFloat: Value.Type
    kDouble: Value.Type
    kString: Value.Type
    kList: Value.Type
    kRecord: Value.Type
    kNode: Value.Type
    kEdge: Value.Type
    kPath: Value.Type
    kDuration: Value.Type
    kLocalTime: Value.Type
    kZonedTime: Value.Type
    kDate: Value.Type
    kLocalDatetime: Value.Type
    kZonedDatetime: Value.Type
    kRef: Value.Type
    kDecimal: Value.Type
    kVector: Value.Type
    kPoint: Value.Type
    kLineString: Value.Type
    kPolygon: Value.Type
    kGeography: Value.Type
    kGeometry: Value.Type
    kSet: Value.Type
    kMap: Value.Type
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT8_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT8_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT16_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT16_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT32_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT32_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT64_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT64_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    LIST_VALUE_FIELD_NUMBER: _ClassVar[int]
    RECORD_VALUE_FIELD_NUMBER: _ClassVar[int]
    NODE_VALUE_FIELD_NUMBER: _ClassVar[int]
    EDGE_VALUE_FIELD_NUMBER: _ClassVar[int]
    PATH_VALUE_FIELD_NUMBER: _ClassVar[int]
    DURATION_VALUE_FIELD_NUMBER: _ClassVar[int]
    LOCAL_TIME_VALUE_FIELD_NUMBER: _ClassVar[int]
    ZONED_TIME_VALUE_FIELD_NUMBER: _ClassVar[int]
    DATE_VALUE_FIELD_NUMBER: _ClassVar[int]
    LOCAL_DATETIME_VALUE_FIELD_NUMBER: _ClassVar[int]
    ZONED_DATETIME_VALUE_FIELD_NUMBER: _ClassVar[int]
    REF_VALUE_FIELD_NUMBER: _ClassVar[int]
    DECIMAL_VALUE_FIELD_NUMBER: _ClassVar[int]
    VECTOR_VALUE_FIELD_NUMBER: _ClassVar[int]
    POINT_VALUE_FIELD_NUMBER: _ClassVar[int]
    LINE_STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    POLYGON_VALUE_FIELD_NUMBER: _ClassVar[int]
    GEOGRAPHY_VALUE_FIELD_NUMBER: _ClassVar[int]
    GEOMETRY_VALUE_FIELD_NUMBER: _ClassVar[int]
    SET_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAP_VALUE_FIELD_NUMBER: _ClassVar[int]
    bool_value: bool
    int8_value: int
    uint8_value: int
    int16_value: int
    uint16_value: int
    int32_value: int
    uint32_value: int
    int64_value: int
    uint64_value: int
    float_value: float
    double_value: float
    string_value: bytes
    list_value: List
    record_value: Record
    node_value: Node
    edge_value: Edge
    path_value: Path
    duration_value: Duration
    local_time_value: LocalTime
    zoned_time_value: ZonedTime
    date_value: Date
    local_datetime_value: LocalDatetime
    zoned_datetime_value: ZonedDatetime
    ref_value: Ref
    decimal_value: Decimal
    vector_value: Vector
    point_value: Point
    line_string_value: LineString
    polygon_value: Polygon
    geography_value: Geography
    geometry_value: Geometry
    set_value: Set
    map_value: Map
    def __init__(self, bool_value: bool = ..., int8_value: _Optional[int] = ..., uint8_value: _Optional[int] = ..., int16_value: _Optional[int] = ..., uint16_value: _Optional[int] = ..., int32_value: _Optional[int] = ..., uint32_value: _Optional[int] = ..., int64_value: _Optional[int] = ..., uint64_value: _Optional[int] = ..., float_value: _Optional[float] = ..., double_value: _Optional[float] = ..., string_value: _Optional[bytes] = ..., list_value: _Optional[_Union[List, _Mapping]] = ..., record_value: _Optional[_Union[Record, _Mapping]] = ..., node_value: _Optional[_Union[Node, _Mapping]] = ..., edge_value: _Optional[_Union[Edge, _Mapping]] = ..., path_value: _Optional[_Union[Path, _Mapping]] = ..., duration_value: _Optional[_Union[Duration, _Mapping]] = ..., local_time_value: _Optional[_Union[LocalTime, _Mapping]] = ..., zoned_time_value: _Optional[_Union[ZonedTime, _Mapping]] = ..., date_value: _Optional[_Union[Date, _Mapping]] = ..., local_datetime_value: _Optional[_Union[LocalDatetime, _Mapping]] = ..., zoned_datetime_value: _Optional[_Union[ZonedDatetime, _Mapping]] = ..., ref_value: _Optional[_Union[Ref, _Mapping]] = ..., decimal_value: _Optional[_Union[Decimal, _Mapping]] = ..., vector_value: _Optional[_Union[Vector, _Mapping]] = ..., point_value: _Optional[_Union[Point, _Mapping]] = ..., line_string_value: _Optional[_Union[LineString, _Mapping]] = ..., polygon_value: _Optional[_Union[Polygon, _Mapping]] = ..., geography_value: _Optional[_Union[Geography, _Mapping]] = ..., geometry_value: _Optional[_Union[Geometry, _Mapping]] = ..., set_value: _Optional[_Union[Set, _Mapping]] = ..., map_value: _Optional[_Union[Map, _Mapping]] = ...) -> None: ...

class HostAddress(_message.Message):
    __slots__ = ["host", "port"]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    host: bytes
    port: int
    def __init__(self, host: _Optional[bytes] = ..., port: _Optional[int] = ...) -> None: ...

class RoutePolicyInfo(_message.Message):
    __slots__ = ["route_policy"]
    ROUTE_POLICY_FIELD_NUMBER: _ClassVar[int]
    route_policy: RoutePolicy
    def __init__(self, route_policy: _Optional[_Union[RoutePolicy, str]] = ...) -> None: ...

class Status(_message.Message):
    __slots__ = ["code", "message"]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: bytes
    message: bytes
    def __init__(self, code: _Optional[bytes] = ..., message: _Optional[bytes] = ...) -> None: ...

class DirInfo(_message.Message):
    __slots__ = ["install_path", "data_paths"]
    INSTALL_PATH_FIELD_NUMBER: _ClassVar[int]
    DATA_PATHS_FIELD_NUMBER: _ClassVar[int]
    install_path: bytes
    data_paths: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, install_path: _Optional[bytes] = ..., data_paths: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ClientInfo(_message.Message):
    __slots__ = ["lang", "protocol_version", "version"]
    class Language(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
        UNKNOWN: _ClassVar[ClientInfo.Language]
        CPP: _ClassVar[ClientInfo.Language]
        GO: _ClassVar[ClientInfo.Language]
        JAVA: _ClassVar[ClientInfo.Language]
        PYTHON: _ClassVar[ClientInfo.Language]
        JAVASCRIPT: _ClassVar[ClientInfo.Language]
    UNKNOWN: ClientInfo.Language
    CPP: ClientInfo.Language
    GO: ClientInfo.Language
    JAVA: ClientInfo.Language
    PYTHON: ClientInfo.Language
    JAVASCRIPT: ClientInfo.Language
    LANG_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    lang: ClientInfo.Language
    protocol_version: bytes
    version: bytes
    def __init__(self, lang: _Optional[_Union[ClientInfo.Language, str]] = ..., protocol_version: _Optional[bytes] = ..., version: _Optional[bytes] = ...) -> None: ...
