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

from collections.abc import Sequence
from enum import Enum
from functools import cache
from io import BytesIO
from typing import Dict, List, Optional

from nebulagraph_python.proto.vector_pb2 import (
    EdgeType as ProtoEdgeType,
)
from nebulagraph_python.proto.vector_pb2 import (
    NodeType as ProtoNodeType,
)
from nebulagraph_python.proto.vector_pb2 import (
    PropertyGraphSchema,
)


class ByteOrder(str, Enum):
    LITTLE_ENDIAN = "little"
    BIG_ENDIAN = "big"


# Define charset constant to match Java
charset = "utf-8"


class NodeSchema:
    def __init__(self, node_type: ProtoNodeType):
        self.node_type_id = node_type.node_type_id
        self.node_type_name = node_type.node_type_name.decode(charset)
        self.node_labels = []
        for label in node_type.label:
            self.node_labels.append(label.decode(charset))

    def get_node_type_id(self) -> int:
        return self.node_type_id

    def get_node_type_name(self) -> str:
        return self.node_type_name

    def get_node_labels(self) -> List[str]:
        return self.node_labels


class EdgeSchema:
    def __init__(self, edge_type: ProtoEdgeType):
        self.edge_type_id = edge_type.edge_type_id
        self.edge_type_name = edge_type.edge_type_name.decode(charset)
        self.edge_labels = []
        for label in edge_type.label:
            self.edge_labels.append(label.decode(charset))

    def get_edge_type_id(self) -> int:
        return self.edge_type_id

    def get_edge_type_name(self) -> str:
        return self.edge_type_name

    def get_edge_labels(self) -> List[str]:
        return self.edge_labels


class ResultGraphSchemas:
    graph_schemas: Dict[int, "GraphSchema"]

    def __init__(self, graph_schemas: Optional[Sequence[PropertyGraphSchema]] = None):
        self.graph_schemas: Dict[int, GraphSchema] = {}
        if graph_schemas:
            for schema in graph_schemas:
                self.graph_schemas[schema.graph_id] = GraphSchema(schema)

    def get_graph_schema(self, graph_id: int) -> "GraphSchema":
        return self.graph_schemas[graph_id]

    def get_graph_schema_map(self) -> Dict[int, "GraphSchema"]:
        return self.graph_schemas


class GraphSchema:
    graph_id: int
    graph_name: str
    node_schemas: Dict[int, "NodeSchema"]
    edge_schemas: Dict[int, "EdgeSchema"]

    def __init__(self, graph_schema: PropertyGraphSchema):
        self.graph_id = graph_schema.graph_id
        self.graph_name = graph_schema.graph_name.decode(charset)
        self.node_schemas: Dict[int, NodeSchema] = {}
        self.edge_schemas: Dict[int, EdgeSchema] = {}
        for node_type in graph_schema.node_type:
            self.node_schemas[node_type.node_type_id] = NodeSchema(node_type)

        for edge_type in graph_schema.edge_type:
            self.edge_schemas[edge_type.edge_type_id] = EdgeSchema(edge_type)

    def get_graph_id(self) -> int:
        return self.graph_id

    def get_graph_name(self) -> str:
        return self.graph_name

    def get_node_schema(self, node_type_id: int) -> "NodeSchema":
        return self.node_schemas[node_type_id]

    def get_edge_schema(self, edge_type_id: int) -> "EdgeSchema":
        return self.edge_schemas[edge_type_id]


class ListHeader:
    def __init__(self, data: bytes, byte_order: ByteOrder):
        buffer = BytesIO(data)
        self.offset = int.from_bytes(buffer.read(4), byte_order.value)  # uint32
        self.size = int.from_bytes(buffer.read(4), byte_order.value)  # uint32

    def get_offset(self) -> int:
        return self.offset

    def get_size(self) -> int:
        return self.size


class NodeHeader:
    def __init__(self, data: bytes, byte_order: ByteOrder):
        buffer = BytesIO(data)
        self.node_id = int.from_bytes(buffer.read(8), byte_order.value)  # int64
        self.node_type_id = self.node_id >> 48  # first 16 bytes
        self.graph_id = int.from_bytes(buffer.read(4), byte_order.value)  # int32

    def get_node_id(self) -> int:
        return self.node_id

    def get_graph_id(self) -> int:
        return self.graph_id

    def get_node_type_id(self) -> int:
        return self.node_type_id


class EdgeHeader:
    def __init__(self, data: bytes, byte_order: ByteOrder):
        buffer = BytesIO(data)
        self.src_id = int.from_bytes(buffer.read(8), byte_order.value)  # int64
        self.dst_id = int.from_bytes(buffer.read(8), byte_order.value)  # int64
        self.rank = int.from_bytes(buffer.read(8), byte_order.value)  # int64
        self.graph_id = int.from_bytes(buffer.read(4), byte_order.value)  # int32
        self.edge_type_id = int.from_bytes(buffer.read(4), byte_order.value)  # int32

    def get_edge_type_id(self) -> int:
        return self.edge_type_id

    def get_graph_id(self) -> int:
        return self.graph_id

    def get_rank(self) -> int:
        return self.rank

    def get_dst_id(self) -> int:
        return self.dst_id

    def get_src_id(self) -> int:
        return self.src_id


class PathHeader:
    """PathHeader is stored in Vector data, path header is 16 bytes.
    - headNodeId: 8 bytes
    - tailNodeId: 8 bytes
    - size: 4 bytes
    - length: 4 bytes
    - headOffset: 4 bytes
    - tailOffset: 4 bytes
    """

    def __init__(self, data: bytes, byte_order: ByteOrder):
        buffer = BytesIO(data)
        self.size = (
            int.from_bytes(buffer.read(4), byte_order.value) & 0xFFFFFFFF
        )  # uint32
        self.head_node_index = (
            int.from_bytes(buffer.read(2), byte_order.value) & 0xFFFF
        )  # uint16
        self.tail_node_index = (
            int.from_bytes(buffer.read(2), byte_order.value) & 0xFFFF
        )  # uint16
        self.head_offset = int.from_bytes(buffer.read(4), byte_order.value)  # uint32
        self.tail_offset = int.from_bytes(buffer.read(4), byte_order.value)  # uint32

    def get_head_node_index(self) -> int:
        return self.head_node_index

    def get_tail_node_index(self) -> int:
        return self.tail_node_index

    def get_size(self) -> int:
        return self.size

    def get_head_offset(self) -> int:
        return self.head_offset

    def get_tail_offset(self) -> int:
        return self.tail_offset


class PathAdjHeader:
    """The PathAdjHeader is stored in the path adj vector, the adj vector is Long type,
    each value is a int64 number, includes isEnd, isEdge, vecIdx, offset.

    Considering a path: node1 -> edge1 -> node2 -> edge2 -> edge3 -> edge4 -> node3

    For each element (node1, node2, ..., edge1, ...), we use an int64 to encode its
    neighbor's information. It helps us to construct a path from a set of nodes/edges.

    The structure of this int64 is:
    | isEnd | isEdge | padding | vecIdx  | offset  |
    | 1 bit | 1 bit  | 14 bits | 16 bits | 32 bits |
    `isEnd` : whether this element is the end of path, true for node3
    `isEdge`: whether the next element is edge, true for node1, node2, edge2, edge3
    `vecIdx`: which Vector the next element belongs to
    `offset`: which offset the next element is placed at (in its Vector)
    """

    def __init__(self, value: int):
        # Convert to unsigned 64-bit integer if negative
        if value < 0:
            value = value + (1 << 64)

        # The bits should be interpreted as:
        # MSB: 0x80 = 1000 0000 -> isEnd=1, isNextEdge=0
        self._is_end = ((value >> 63) & 1) == 1
        self._is_next_edge = ((value >> 62) & 1) == 1

        # Get vecIdx from bits 32-47 (16 bits)
        self._vec_idx_of_next_ele = (value >> 32) & 0xFFFF

        # Get offset from bits 0-31 (32 bits)
        self._offset_of_next_ele = value & 0xFFFFFFFF

    def is_end(self) -> bool:
        return self._is_end

    def is_next_edge(self) -> bool:
        return self._is_next_edge

    def get_vec_idx_of_next_ele(self) -> int:
        return self._vec_idx_of_next_ele

    def get_offset_of_next_ele(self) -> int:
        return self._offset_of_next_ele


class ColumnType(Enum):
    NODE = 0x1
    EDGE = 0x2
    NULL = 0x3
    BOOL = 0x4
    INT8 = 0x5
    UINT8 = 0x6
    INT16 = 0x7
    UINT16 = 0x8
    INT32 = 0x9
    UINT32 = 0xA
    INT64 = 0xB
    UINT64 = 0xC
    FLOAT32 = 0xD
    FLOAT64 = 0xE
    STRING = 0x10
    LIST = 0x11
    PATH = 0x12
    RECORD = 0x13
    EMBEDDINGVECTOR = 0x14
    LOCALTIME = 0x15
    DURATION = 0x16
    DATE = 0x17
    LOCALDATETIME = 0x18
    ZONEDTIME = 0x19
    ZONEDDATETIME = 0x20
    REFERENCE = 0x21
    DECIMAL = 0x22
    ANY = 0xFE
    INVALID = 0xFF

    def is_basic(self) -> bool:
        """Check if type is a basic type"""
        basic_types = {
            ColumnType.BOOL,
            ColumnType.INT8,
            ColumnType.UINT8,
            ColumnType.INT16,
            ColumnType.UINT16,
            ColumnType.INT32,
            ColumnType.UINT32,
            ColumnType.INT64,
            ColumnType.UINT64,
            ColumnType.FLOAT32,
            ColumnType.FLOAT64,
            ColumnType.LOCALTIME,
            ColumnType.DURATION,
            ColumnType.DATE,
            ColumnType.LOCALDATETIME,
            ColumnType.ZONEDTIME,
            ColumnType.ZONEDDATETIME,
        }
        return self in basic_types

    def is_composite(self) -> bool:
        """Check if type is a composite type"""
        composite_types = {
            ColumnType.NODE,
            ColumnType.EDGE,
            ColumnType.LIST,
            ColumnType.PATH,
            ColumnType.RECORD,
            ColumnType.EMBEDDINGVECTOR,
        }
        return self in composite_types

    def get_byte_size(self) -> int:
        """Get byte size for fixed-length types"""
        size_map = {
            ColumnType.BOOL: 1,
            ColumnType.INT8: 1,
            ColumnType.UINT8: 1,
            ColumnType.INT16: 2,
            ColumnType.UINT16: 2,
            ColumnType.INT32: 4,
            ColumnType.UINT32: 4,
            ColumnType.INT64: 8,
            ColumnType.UINT64: 8,
            ColumnType.FLOAT32: 4,
            ColumnType.FLOAT64: 8,
            ColumnType.DATE: 4,
            ColumnType.LOCALTIME: 8,
            ColumnType.LOCALDATETIME: 8,
            ColumnType.ZONEDTIME: 8,
            ColumnType.ZONEDDATETIME: 8,
            ColumnType.DURATION: 8,
        }
        return size_map.get(self, 0)  # Return 0 for variable-length types


class AnyHeader:
    def __init__(self, data: bytes, column_type: ColumnType, byte_order: ByteOrder):
        self.chunk_index = 0
        self.offset = 0

        # Only parse chunk_index and offset for non-basic types
        if column_type not in [
            ColumnType.NULL,
            ColumnType.BOOL,
            ColumnType.INT8,
            ColumnType.UINT8,
            ColumnType.INT16,
            ColumnType.UINT16,
            ColumnType.INT32,
            ColumnType.UINT32,
            ColumnType.INT64,
            ColumnType.UINT64,
            ColumnType.FLOAT32,
            ColumnType.FLOAT64,
            ColumnType.DATE,
            ColumnType.LOCALTIME,
            ColumnType.ZONEDTIME,
            ColumnType.LOCALDATETIME,
            ColumnType.ZONEDDATETIME,
            ColumnType.DURATION,
            ColumnType.DECIMAL,
        ]:
            buffer = BytesIO(data)
            # chunk_index is uint32, add 1 to match Java implementation
            self.chunk_index = int.from_bytes(buffer.read(4), byte_order.value) + 1
            # offset is uint32
            self.offset = int.from_bytes(buffer.read(4), byte_order.value)

    def get_chunk_index(self) -> int:
        return self.chunk_index

    def get_offset(self) -> int:
        return self.offset


class DataType:
    def __init__(self, column_type: ColumnType):
        self.type = column_type

    def get_type(self) -> ColumnType:
        return self.type


class BasicType(DataType):
    def __init__(self, column_type: ColumnType):
        super().__init__(column_type)


class EdgeType(DataType):
    def __init__(self, graph_edge_types: Dict[int, Dict[int, Dict[str, DataType]]]):
        super().__init__(ColumnType.EDGE)
        self.graph_edge_types = graph_edge_types

    def get_edge_types(self) -> Dict[int, Dict[int, Dict[str, DataType]]]:
        return self.graph_edge_types


class NodeType(DataType):
    def __init__(self, graph_node_types: Dict[int, Dict[int, Dict[str, DataType]]]):
        super().__init__(ColumnType.NODE)
        self.graph_node_types = graph_node_types

    def get_node_types(self) -> Dict[int, Dict[int, Dict[str, DataType]]]:
        return self.graph_node_types


class RecordType(DataType):
    def __init__(self, field_types: Dict[str, DataType]):
        super().__init__(ColumnType.RECORD)
        self.field_types = field_types

    def get_field_types(self) -> Dict[str, DataType]:
        return self.field_types


class PathType(DataType):
    def __init__(self, data_types: List[DataType]):
        super().__init__(ColumnType.PATH)
        self.data_types: List[DataType] = data_types
        self.node_types: List[NodeType] = []
        self.edge_types: List[EdgeType] = []

        for data_type in data_types:
            if isinstance(data_type, NodeType):
                self.node_types.append(data_type)
            elif isinstance(data_type, EdgeType):
                self.edge_types.append(data_type)
            else:
                raise ValueError(f"Unsupported data type: {data_type}")

    @cache  # noqa: B019
    def get_data_types(self) -> List[DataType]:
        return self.data_types

    @cache  # noqa: B019
    def get_node_types(self) -> Dict[int, Dict[int, Dict[str, DataType]]]:
        node_types_map: Dict[int, Dict[int, Dict[str, DataType]]] = {}
        for node_type in self.node_types:
            for graph_id, type_dict in node_type.get_node_types().items():
                if graph_id not in node_types_map:
                    node_types_map[graph_id] = {}
                for type_id, props in type_dict.items():
                    node_types_map.setdefault(graph_id, {}).setdefault(
                        type_id, {}
                    ).update(props)
        return node_types_map

    @cache  # noqa: B019
    def get_edge_types(self) -> Dict[int, Dict[int, Dict[str, DataType]]]:
        edge_types_map: Dict[int, Dict[int, Dict[str, DataType]]] = {}
        for edge_type in self.edge_types:
            for graph_id, type_dict in edge_type.get_edge_types().items():
                if graph_id not in edge_types_map:
                    edge_types_map[graph_id] = {}
                for type_id, props in type_dict.items():
                    edge_types_map.setdefault(graph_id, {}).setdefault(
                        type_id, {}
                    ).update(props)
        return edge_types_map


class ListType(DataType):
    def __init__(self, value_type: DataType):
        super().__init__(ColumnType.LIST)
        self.value_type = value_type

    def get_value_type(self) -> DataType:
        return self.value_type


class EmbeddingVectorType(DataType):
    """EmbeddingVectorType is used to represent the embedding vector data type.

    Note: we dont introduce an element type for EmbeddingVectorType, because the
    element type is always float32 for now and not performance-wise reasonable to
    hold a data type object for each element.
    """

    dimension: int
    value_type: ColumnType

    def __init__(self, dimension: int, value_type: ColumnType):
        super().__init__(ColumnType.EMBEDDINGVECTOR)
        self.dimension = dimension
        self.value_type = value_type

    def get_dimension(self) -> int:
        return self.dimension
