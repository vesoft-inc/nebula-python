# Copyright 2025 vesoft-inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import nebula_common_pb2 as _nebula_common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Row(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[_nebula_common_pb2.Value]
    def __init__(self, values: _Optional[_Iterable[_Union[_nebula_common_pb2.Value, _Mapping]]] = ...) -> None: ...

class ValueType(_message.Message):
    __slots__ = ["value_type"]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    value_type: bytes
    def __init__(self, value_type: _Optional[bytes] = ...) -> None: ...

class RowType(_message.Message):
    __slots__ = ["num_columns", "column_names", "column_types"]
    NUM_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    COLUMN_NAMES_FIELD_NUMBER: _ClassVar[int]
    COLUMN_TYPES_FIELD_NUMBER: _ClassVar[int]
    num_columns: int
    column_names: _containers.RepeatedScalarFieldContainer[str]
    column_types: _containers.RepeatedCompositeFieldContainer[ValueType]
    def __init__(self, num_columns: _Optional[int] = ..., column_names: _Optional[_Iterable[str]] = ..., column_types: _Optional[_Iterable[_Union[ValueType, _Mapping]]] = ...) -> None: ...

class VectorCommonMetaData(_message.Message):
    __slots__ = ["num_records", "vector_content_type"]
    NUM_RECORDS_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    num_records: int
    vector_content_type: int
    def __init__(self, num_records: _Optional[int] = ..., vector_content_type: _Optional[int] = ...) -> None: ...

class NodeType(_message.Message):
    __slots__ = ["node_type_id", "node_type_name", "label"]
    NODE_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    node_type_id: int
    node_type_name: bytes
    label: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, node_type_id: _Optional[int] = ..., node_type_name: _Optional[bytes] = ..., label: _Optional[_Iterable[bytes]] = ...) -> None: ...

class EdgeType(_message.Message):
    __slots__ = ["edge_type_id", "edge_type_name", "label"]
    EDGE_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    EDGE_TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    edge_type_id: int
    edge_type_name: bytes
    label: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, edge_type_id: _Optional[int] = ..., edge_type_name: _Optional[bytes] = ..., label: _Optional[_Iterable[bytes]] = ...) -> None: ...

class PropertyGraphSchema(_message.Message):
    __slots__ = ["graph_id", "graph_name", "node_type", "edge_type"]
    GRAPH_ID_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    NODE_TYPE_FIELD_NUMBER: _ClassVar[int]
    EDGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    graph_id: int
    graph_name: bytes
    node_type: _containers.RepeatedCompositeFieldContainer[NodeType]
    edge_type: _containers.RepeatedCompositeFieldContainer[EdgeType]
    def __init__(self, graph_id: _Optional[int] = ..., graph_name: _Optional[bytes] = ..., node_type: _Optional[_Iterable[_Union[NodeType, _Mapping]]] = ..., edge_type: _Optional[_Iterable[_Union[EdgeType, _Mapping]]] = ...) -> None: ...

class VectorTableMetaData(_message.Message):
    __slots__ = ["table_type", "num_records", "row_type", "num_batches", "time_zone_offset", "is_little_endian", "graph_schema"]
    TABLE_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUM_RECORDS_FIELD_NUMBER: _ClassVar[int]
    ROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUM_BATCHES_FIELD_NUMBER: _ClassVar[int]
    TIME_ZONE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    IS_LITTLE_ENDIAN_FIELD_NUMBER: _ClassVar[int]
    GRAPH_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    table_type: int
    num_records: int
    row_type: RowType
    num_batches: int
    time_zone_offset: int
    is_little_endian: bool
    graph_schema: _containers.RepeatedCompositeFieldContainer[PropertyGraphSchema]
    def __init__(self, table_type: _Optional[int] = ..., num_records: _Optional[int] = ..., row_type: _Optional[_Union[RowType, _Mapping]] = ..., num_batches: _Optional[int] = ..., time_zone_offset: _Optional[int] = ..., is_little_endian: bool = ..., graph_schema: _Optional[_Iterable[_Union[PropertyGraphSchema, _Mapping]]] = ...) -> None: ...

class NestedVector(_message.Message):
    __slots__ = ["num_nested_vectors", "common_meta_data", "special_meta_data", "vector_data", "null_bit_map", "nested_vectors"]
    NUM_NESTED_VECTORS_FIELD_NUMBER: _ClassVar[int]
    COMMON_META_DATA_FIELD_NUMBER: _ClassVar[int]
    SPECIAL_META_DATA_FIELD_NUMBER: _ClassVar[int]
    VECTOR_DATA_FIELD_NUMBER: _ClassVar[int]
    NULL_BIT_MAP_FIELD_NUMBER: _ClassVar[int]
    NESTED_VECTORS_FIELD_NUMBER: _ClassVar[int]
    num_nested_vectors: int
    common_meta_data: VectorCommonMetaData
    special_meta_data: bytes
    vector_data: bytes
    null_bit_map: bytes
    nested_vectors: _containers.RepeatedCompositeFieldContainer[NestedVector]
    def __init__(self, num_nested_vectors: _Optional[int] = ..., common_meta_data: _Optional[_Union[VectorCommonMetaData, _Mapping]] = ..., special_meta_data: _Optional[bytes] = ..., vector_data: _Optional[bytes] = ..., null_bit_map: _Optional[bytes] = ..., nested_vectors: _Optional[_Iterable[_Union[NestedVector, _Mapping]]] = ...) -> None: ...

class VectorBatch(_message.Message):
    __slots__ = ["vectors"]
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    vectors: _containers.RepeatedCompositeFieldContainer[NestedVector]
    def __init__(self, vectors: _Optional[_Iterable[_Union[NestedVector, _Mapping]]] = ...) -> None: ...

class VectorResultTable(_message.Message):
    __slots__ = ["data_layout_version", "meta", "batch"]
    DATA_LAYOUT_VERSION_FIELD_NUMBER: _ClassVar[int]
    META_FIELD_NUMBER: _ClassVar[int]
    BATCH_FIELD_NUMBER: _ClassVar[int]
    data_layout_version: bytes
    meta: VectorTableMetaData
    batch: _containers.RepeatedCompositeFieldContainer[VectorBatch]
    def __init__(self, data_layout_version: _Optional[bytes] = ..., meta: _Optional[_Union[VectorTableMetaData, _Mapping]] = ..., batch: _Optional[_Iterable[_Union[VectorBatch, _Mapping]]] = ...) -> None: ...

class ResultTable(_message.Message):
    __slots__ = ["column_names", "records"]
    COLUMN_NAMES_FIELD_NUMBER: _ClassVar[int]
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    column_names: _containers.RepeatedScalarFieldContainer[bytes]
    records: _containers.RepeatedCompositeFieldContainer[Row]
    def __init__(self, column_names: _Optional[_Iterable[bytes]] = ..., records: _Optional[_Iterable[_Union[Row, _Mapping]]] = ...) -> None: ...
