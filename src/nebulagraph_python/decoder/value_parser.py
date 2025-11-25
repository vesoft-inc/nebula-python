import datetime
import decimal
import struct
from typing import Any, Dict

from nebulagraph_python.decoder.data_types import (
    AnyHeader,
    BasicType,
    ByteOrder,
    ColumnType,
    DataType,
    EdgeHeader,
    EdgeType,
    EmbeddingVectorType,
    ListHeader,
    ListType,
    NodeHeader,
    NodeType,
    PathAdjHeader,
    PathHeader,
    PathType,
    RecordType,
    ResultGraphSchemas,
    charset,
)
from nebulagraph_python.decoder.decode import BytesReader, VectorType, VectorWrapper
from nebulagraph_python.decoder.decode_utils import (
    bytes_to_bool,
    bytes_to_double,
    bytes_to_float,
    bytes_to_int8,
    bytes_to_int16,
    bytes_to_int32,
    bytes_to_int64,
    bytes_to_sized_string,
    bytes_to_uint8,
    bytes_to_uint16,
)
from nebulagraph_python.decoder.size_constant import (
    ANY_HEADER_SIZE,
    BOOL_SIZE,
    CHUNK_INDEX_LENGTH_IN_STRING_HEADER,
    CHUNK_INDEX_START_POSITION_IN_STRING_HEADER,
    CHUNK_OFFSET_LENGTH_IN_STRING_HEADER,
    CHUNK_OFFSET_START_POSITION_IN_STRING_HEADER,
    DATE_SIZE,
    DATE_TIME_SIZE,
    DAY_SIZE,
    DOUBLE_SIZE,
    DURATION_SIZE,
    EDGE_TYPE_ID_SIZE,
    ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE,
    ELEMENT_NUMBER_SIZE_FOR_VECTOR_VALUE,
    EMBEDDING_VECTOR_DIM_SIZE,
    EMBEDDING_VECTOR_FLOAT_VALUE_SIZE,
    FLOAT32_SIZE,
    FLOAT_SIZE,
    GRAPH_ELEMENT_TYPE_NUM_SIZE,
    GRAPH_ID_SIZE,
    INT8_SIZE,
    INT16_SIZE,
    INT32_SIZE,
    INT64_SIZE,
    LIST_HEADER_SIZE,
    LOCAL_TIME_SIZE,
    MICRO_SECONDS_OF_MINUTE,
    MICRO_SECONDS_OF_SECOND,
    MONTH_SIZE,
    NODE_ID_SIZE,
    NODE_TYPE_ID_SIZE,
    PATH_ELEMENT_NUM_SIZE,
    PROPERTY_NUM_SIZE,
    RANK_SIZE,
    RECORD_FIELD_NUM_SIZE,
    STRING_MAX_VALUE_LENGTH_IN_HEADER,
    STRING_SIZE,
    STRING_VALUE_LENGTH_SIZE,
    VALUE_TYPE_SIZE,
    VECTOR_EDGE_HEADER_SIZE,
    VECTOR_NODE_HEADER_SIZE,
    VECTOR_PATH_HEADER_SIZE,
    YEAR_SIZE,
    ZONED_DATE_TIME_SIZE,
    ZONED_TIME_SIZE,
)
from nebulagraph_python.error import InternalError
from nebulagraph_python.proto.vector_pb2 import NestedVector
from nebulagraph_python.py_data_types import (
    Edge,
    NDuration,
    Node,
    NRecord,
    NVector,
    Path,
)
from nebulagraph_python.value_wrapper import ValueWrapper


class AnyValue:
    """The value for any type and its actual data type."""

    def __init__(self, value: Any, type: ColumnType):
        # the value for any type
        self.value = value
        # the actual data type for any type
        self.type = type

    def get_type(self) -> ColumnType:
        return self.type

    def get_value(self) -> Any:
        return self.value


class ValueParser:
    graph_schemas: ResultGraphSchemas
    timezone_offset: int
    byte_order: ByteOrder

    def __init__(
        self,
        graph_schemas: ResultGraphSchemas,
        timezone_offset: int,
        byte_order: ByteOrder,
    ):
        self.graph_schemas = graph_schemas
        self.timezone_offset = timezone_offset
        self.byte_order = byte_order

    def decode_value_wrapper(
        self,
        vector: VectorWrapper,
        data_type: DataType,
        row_idx: int,
    ) -> ValueWrapper:
        """Decode value and wrap in ValueWrapper"""
        value = self._decode_value(vector, data_type, row_idx)

        if data_type.get_type() == ColumnType.ANY:
            any_value = value  # AnyValue instance
            return ValueWrapper(any_value.value, any_value.type)
        return ValueWrapper(value, data_type.get_type())

    def _decode_value(
        self,
        vector: VectorWrapper,
        data_type: DataType,
        row_idx: int,
    ) -> Any:
        """Main decode method matching Java's decodeValue"""
        # Check if value at index is null
        if not vector.is_null_all_set() and vector.get_null_bit_map():
            byte_idx = row_idx // 8
            bit_idx = row_idx % 8
            k_one_bitmasks = [
                1 << 0,  # 0000 0001
                1 << 1,  # 0000 0010
                1 << 2,  # 0000 0100
                1 << 3,  # 0000 1000
                1 << 4,  # 0001 0000
                1 << 5,  # 0010 0000
                1 << 6,  # 0100 0000
                1 << 7,  # 1000 0000
            ]
            if (vector.get_null_bit_map()[byte_idx] & k_one_bitmasks[bit_idx]) == 0:
                return None

        value = None
        vector_type = vector.get_vector_type()
        if vector_type == VectorType.FLAT_VECTOR:
            value = self._decode_flat_value(vector, data_type, row_idx)
        elif vector_type == VectorType.CONST_VECTOR:
            if not hasattr(vector, "const_value") or vector.const_value is None:
                vector_data = vector.get_vector_data()
                reader = BytesReader(vector_data)
                const_value = self._decode_const_value(reader, data_type.get_type())
                vector.const_value = const_value
            value = vector.const_value
        else:
            raise RuntimeError(f"Do not support vector type: {vector_type}")

        return value

    def _decode_flat_value(
        self,
        vector: VectorWrapper,
        data_type: DataType,
        row_idx: int,
    ) -> Any:
        """Decode flat vector value at given row index"""
        vector_data = vector.get_vector_data()
        column_type = data_type.get_type()

        if column_type == ColumnType.NULL:
            return None

        if column_type in [ColumnType.INT8, ColumnType.UINT8]:
            value_data = self._get_sub_bytes(vector_data, INT8_SIZE, row_idx)
            return int.from_bytes(
                value_data,
                byteorder=(
                    "little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big"
                ),
                signed=column_type == ColumnType.INT8,
            )

        if column_type in [ColumnType.INT16, ColumnType.UINT16]:
            value_data = self._get_sub_bytes(vector_data, INT16_SIZE, row_idx)
            return int.from_bytes(
                value_data,
                byteorder=(
                    "little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big"
                ),
                signed=column_type == ColumnType.INT16,
            )

        if column_type in [ColumnType.INT32, ColumnType.UINT32]:
            value_data = self._get_sub_bytes(vector_data, INT32_SIZE, row_idx)
            return int.from_bytes(
                value_data,
                byteorder=(
                    "little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big"
                ),
                signed=column_type == ColumnType.INT32,
            )

        if column_type in [ColumnType.INT64, ColumnType.UINT64]:
            value_data = self._get_sub_bytes(vector_data, INT64_SIZE, row_idx)
            return int.from_bytes(
                value_data,
                byteorder=(
                    "little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big"
                ),
                signed=column_type == ColumnType.INT64,
            )

        if column_type == ColumnType.FLOAT32:
            value_data = self._get_sub_bytes(vector_data, FLOAT_SIZE, row_idx)
            return struct.unpack(
                "<f" if self.byte_order == ByteOrder.LITTLE_ENDIAN else ">f",
                value_data,
            )[0]

        if column_type == ColumnType.FLOAT64:
            value_data = self._get_sub_bytes(vector_data, DOUBLE_SIZE, row_idx)
            return struct.unpack(
                "<d" if self.byte_order == ByteOrder.LITTLE_ENDIAN else ">d",
                value_data,
            )[0]

        if column_type == ColumnType.BOOL:
            value_data = self._get_sub_bytes(vector_data, BOOL_SIZE, row_idx)
            return bool(int.from_bytes(value_data, "little"))

        if column_type == ColumnType.DECIMAL:
            value_data = self._get_sub_bytes(
                vector_data,
                STRING_SIZE,
                row_idx,
            )  # STRING_SIZE
            return self.string_to_decimal(
                self.bytes_to_string(value_data, vector.vector),
            )

        if column_type == ColumnType.STRING:
            value_data = self._get_sub_bytes(
                vector_data,
                STRING_SIZE,
                row_idx,
            )  # STRING_SIZE
            return self.bytes_to_string(value_data, vector.vector)

        if column_type == ColumnType.DATE:
            value_data = self._get_sub_bytes(
                vector_data,
                DATE_SIZE,
                row_idx,
            )  # DATE_SIZE
            return self.bytes_to_date(value_data)

        if column_type == ColumnType.LOCALTIME:
            value_data = self._get_sub_bytes(
                vector_data,
                LOCAL_TIME_SIZE,
                row_idx,
            )  # LOCAL_TIME_SIZE
            return self.bytes_to_local_time(value_data)

        if column_type == ColumnType.ZONEDTIME:
            value_data = self._get_sub_bytes(
                vector_data,
                ZONED_TIME_SIZE,
                row_idx,
            )  # ZONED_TIME_SIZE
            return self.bytes_to_zoned_time(value_data)

        if column_type == ColumnType.LOCALDATETIME:
            value_data = self._get_sub_bytes(
                vector_data,
                DATE_TIME_SIZE,
                row_idx,
            )  # DATE_TIME_SIZE
            return self.bytes_to_local_datetime(value_data)

        if column_type == ColumnType.ZONEDDATETIME:
            value_data = self._get_sub_bytes(
                vector_data,
                ZONED_DATE_TIME_SIZE,
                row_idx,
            )  # ZONED_DATE_TIME_SIZE
            return self.bytes_to_zoned_datetime(value_data)

        if column_type == ColumnType.DURATION:
            value_data = self._get_sub_bytes(
                vector_data,
                ZONED_DATE_TIME_SIZE,
                row_idx,
            )  # DURATION_SIZE
            return self.bytes_to_duration(value_data)

        if column_type == ColumnType.LIST:
            value_data = self._get_sub_bytes(
                vector_data,
                LIST_HEADER_SIZE,
                row_idx,
            )  # LIST_HEADER_SIZE
            list_header = ListHeader(value_data, self.byte_order)
            if isinstance(data_type, ListType):
                value_type = data_type.get_value_type()
            else:
                raise InternalError("Expected ListType for LIST column type")

            elements = []
            for i in range(list_header.size):
                element = self._decode_value(
                    vector.vector_wrappers[0],
                    value_type,
                    list_header.offset + i,
                )
                elements.append(ValueWrapper(element, value_type.get_type()))
            return elements

        if column_type == ColumnType.RECORD:
            if not isinstance(data_type, RecordType):
                raise ValueError("Expected RecordType for RECORD column type")

            special_meta_data = vector.get_special_meta_data()
            if special_meta_data is None:
                raise RuntimeError("Special metadata is missing")
            field_types = data_type.get_field_types()
            record = {}

            reader = BytesReader(special_meta_data)
            for i in range(len(field_types)):
                field_name = reader.read_sized_string(self.byte_order)
                value = self._decode_value(
                    vector.vector_wrappers[i],
                    field_types[field_name],
                    row_idx,
                )
                record[field_name] = ValueWrapper(
                    value,
                    field_types[field_name].get_type(),
                )
            return NRecord(record)

        if column_type == ColumnType.NODE:
            # import pdb; pdb.set_trace()
            if not isinstance(data_type, NodeType):
                raise ValueError("Expected NodeType for NODE column type")

            node_prop_types = data_type.get_node_types()
            node_prop_vector_index = (
                vector.get_graph_element_type_id_and_prop_vector_index_map(
                    NODE_TYPE_ID_SIZE,
                )
            )

            node_header_binary = self._get_sub_bytes(
                vector_data,
                VECTOR_NODE_HEADER_SIZE,
                row_idx,
            )
            node_header = NodeHeader(node_header_binary, self.byte_order)
            if (
                node_header.graph_id not in node_prop_types
                or node_header.node_type_id not in node_prop_types[node_header.graph_id]
            ):
                raise RuntimeError(
                    f"Value type for NODE does not contain graphId {node_header.graph_id} "
                    f"or node type id {node_header.node_type_id}",
                )

            prop_type_map = node_prop_types[node_header.graph_id][
                node_header.node_type_id
            ]
            props = {}

            for prop_name, prop_type in prop_type_map.items():
                vector_index = node_prop_vector_index[node_header.graph_id][
                    node_header.node_type_id
                ][prop_name]
                prop_value = self._decode_value(
                    vector.vector_wrappers[vector_index],
                    prop_type,
                    row_idx,
                )
                props[prop_name] = ValueWrapper(prop_value, prop_type.get_type())

            return Node(
                node_header.graph_id,
                node_header.node_type_id,
                node_header.node_id,
                props,
                self.graph_schemas,
            )

        if column_type == ColumnType.EDGE:
            if not isinstance(data_type, EdgeType):
                raise ValueError("Expected EdgeType for EDGE column type")

            edge_prop_types = data_type.get_edge_types()
            edge_prop_vector_index = (
                vector.get_graph_element_type_id_and_prop_vector_index_map(
                    EDGE_TYPE_ID_SIZE,
                )
            )

            edge_header_binary = self._get_sub_bytes(
                vector_data,
                VECTOR_EDGE_HEADER_SIZE,
                row_idx,
            )
            edge_header = EdgeHeader(edge_header_binary, self.byte_order)

            no_directed_type_id = edge_header.edge_type_id & 0x3FFFFFFF

            if (
                edge_header.graph_id not in edge_prop_types
                or no_directed_type_id not in edge_prop_types[edge_header.graph_id]
            ):
                raise RuntimeError(
                    f"Value type for EDGE does not contain graphId {edge_header.graph_id} "
                    f"or edge type id {no_directed_type_id}",
                )

            edge_prop_type_map = edge_prop_types[edge_header.graph_id][
                no_directed_type_id
            ]
            edge_props = {}

            for prop_name, prop_type in edge_prop_type_map.items():
                vector_index = edge_prop_vector_index[edge_header.graph_id][
                    no_directed_type_id
                ][prop_name]
                prop_value = self._decode_value(
                    vector.vector_wrappers[vector_index],
                    prop_type,
                    row_idx,
                )
                edge_props[prop_name] = ValueWrapper(prop_value, prop_type.get_type())

            return Edge(
                edge_header.graph_id,
                edge_header.edge_type_id,
                edge_header.rank,
                edge_header.src_id,
                edge_header.dst_id,
                edge_props,
                self.graph_schemas,
            )

        if column_type == ColumnType.PATH:
            if not isinstance(data_type, PathType):
                raise ValueError("Expected PathType for PATH column type")

            path_type = data_type

            # decode the vector data: path header
            path_header_binary = self._get_sub_bytes(
                vector_data,
                VECTOR_PATH_HEADER_SIZE,
                row_idx,
            )
            path_header = PathHeader(path_header_binary, self.byte_order)

            # decode the special meta data into:
            # graphId -> (NodeTypeId -> vecIndex),  graphId -> (EdgeTypeId -> vecIndex)
            # uint16 pair index-> (node vector, adj vector)
            # uint16 pair index-> (edge vector, adj vector)
            path_special_meta_data = vector.get_path_special_meta_data()
            if path_special_meta_data is None:
                raise RuntimeError("Path special metadata is missing")

            # node_types = path_special_meta_data.graph_id_and_node_types
            # edge_types = path_special_meta_data.graph_id_and_edge_types

            # construct map: uint16 pair index-> (node vector, adj vector)
            index_and_nodes = path_special_meta_data.index_and_nodes
            # construct map: uint16 pair index-> (edge vector, adj vector)
            # index_and_edges = path_special_meta_data.index_and_edges

            # decode path value
            elements = []
            adj_data_type = BasicType(ColumnType.INT64)

            # if path has no element, return empty path
            if path_header.size <= 0:
                return Path(elements)

            # decode the first node of path
            first_node_pair = index_and_nodes[path_header.head_node_index]
            first_node_vector = first_node_pair.get_vector()
            first_node_adj_vector = first_node_pair.get_adj_vector()

            first_node = self._decode_value(
                first_node_vector,
                path_type.get_data_types()[0],
                path_header.get_head_offset(),
            )
            elements.append(ValueWrapper(first_node, ColumnType.NODE))

            path_adj_header = PathAdjHeader(
                ValueWrapper(
                    self._decode_value(
                        first_node_adj_vector,
                        adj_data_type,
                        path_header.head_offset,
                    ),
                    adj_data_type.get_type(),
                ).as_long(),
            )
            adj_vector = None
            while not path_adj_header.is_end():
                vec_index = path_adj_header.get_vec_idx_of_next_ele()
                vec_offset = path_adj_header.get_offset_of_next_ele()

                if path_adj_header.is_next_edge():
                    edge_vector_pair = path_special_meta_data.index_and_edges[vec_index]
                    edge = self._decode_value(
                        edge_vector_pair.get_vector(),
                        EdgeType(path_type.get_edge_types()),
                        vec_offset,
                    )
                    adj_vector = edge_vector_pair.get_adj_vector()
                    elements.append(ValueWrapper(edge, ColumnType.EDGE))
                else:
                    node_vector_pair = path_special_meta_data.index_and_nodes[vec_index]
                    node = self._decode_value(
                        node_vector_pair.get_vector(),
                        NodeType(path_type.get_node_types()),
                        vec_offset,
                    )
                    adj_vector = node_vector_pair.get_adj_vector()
                    elements.append(ValueWrapper(node, ColumnType.NODE))

                path_adj_header = PathAdjHeader(
                    ValueWrapper(
                        self._decode_value(adj_vector, adj_data_type, vec_offset),
                        adj_data_type.get_type(),
                    ).as_long(),
                )
            return Path(elements)

        if column_type == ColumnType.EMBEDDINGVECTOR:
            if not isinstance(data_type, EmbeddingVectorType):
                raise ValueError(
                    "Expected EmbeddingVectorType for EMBEDDINGVECTOR column type",
                )

            dimension = data_type.get_dimension()
            offset = row_idx * dimension * FLOAT32_SIZE

            # Use memoryview to avoid copying data
            vector_view = memoryview(vector_data)

            # Pre-allocate list for better performance
            values = [0.0] * dimension

            # Process chunks of bytes directly
            for i in range(dimension):
                start = offset + i * FLOAT32_SIZE
                values[i] = bytes_to_float(
                    vector_view[start : start + FLOAT32_SIZE].tobytes(),
                    self.byte_order,
                )

            return NVector(values)

        if column_type == ColumnType.ANY:
            value_data = self._get_sub_bytes(vector_data, ANY_HEADER_SIZE, row_idx)
            return self.bytes_to_any(value_data, vector, row_idx)

        raise ValueError(f"Unsupported type for flat vector: {column_type}")

    def _decode_const_value(self, reader: BytesReader, column_type: ColumnType) -> Any:
        """Decode constant value from binary reader"""
        if column_type == ColumnType.NULL:
            return None

        if ColumnType.is_basic(column_type):
            return self.bytes_basic_to_object(reader, column_type)

        if column_type == ColumnType.STRING:
            return reader.read_sized_string(self.byte_order)

        if column_type == ColumnType.DECIMAL:
            return self.bytes_to_decimal(reader)

        if ColumnType.is_composite(column_type):
            return self._decode_composite_value(reader, column_type)

        if column_type == ColumnType.ANY:
            return self.bytes_to_const_any(reader)

        raise ValueError(f"Unsupported type for const value: {column_type}")

    def _get_sub_bytes(self, vector_data: bytes, size: int, row_idx: int) -> bytes:
        """Get subset of bytes for given row index and size"""
        start = row_idx * size
        end = start + size
        return vector_data[start:end]

    def _get_node_type_id_from_node_id(self, node_id: int) -> int:
        """Get node type id from node id"""
        return node_id >> 48

    def bytes_to_string(self, string_header: bytes, vector: NestedVector) -> str:
        """Convert bytes to string using string header and vector data"""
        # Get string value length from first 4 bytes
        string_value_length = bytes_to_int32(
            string_header[0:STRING_VALUE_LENGTH_SIZE],
            self.byte_order,
        )

        # If string is small enough, read directly from header
        if string_value_length <= STRING_MAX_VALUE_LENGTH_IN_HEADER:
            return string_header[
                STRING_VALUE_LENGTH_SIZE : STRING_VALUE_LENGTH_SIZE
                + string_value_length
            ].decode(charset)

        # Get chunk index and offset for longer strings
        chunk_index = bytes_to_int32(
            string_header[
                CHUNK_INDEX_START_POSITION_IN_STRING_HEADER : CHUNK_INDEX_START_POSITION_IN_STRING_HEADER
                + CHUNK_INDEX_LENGTH_IN_STRING_HEADER
            ],
            self.byte_order,
        )

        chunk_offset = bytes_to_int32(
            string_header[
                CHUNK_OFFSET_START_POSITION_IN_STRING_HEADER : CHUNK_OFFSET_START_POSITION_IN_STRING_HEADER
                + CHUNK_OFFSET_LENGTH_IN_STRING_HEADER
            ],
            self.byte_order,
        )

        # Get string data from chunk
        string_chunk_vector = vector.nested_vectors[chunk_index]
        value_data = string_chunk_vector.vector_data[
            chunk_offset : chunk_offset + string_value_length
        ]
        return value_data.decode(charset)

    def bytes_to_date(self, data: bytes) -> datetime.date:
        """Convert bytes to date"""
        year = bytes_to_uint16(data[0:YEAR_SIZE], self.byte_order)
        month = bytes_to_uint8(data[YEAR_SIZE : YEAR_SIZE + MONTH_SIZE])
        day = bytes_to_uint8(
            data[YEAR_SIZE + MONTH_SIZE : YEAR_SIZE + MONTH_SIZE + DAY_SIZE],
        )
        return datetime.date(year, month, day)

    def bytes_to_local_time(self, data: bytes) -> datetime.time:
        """Convert bytes to local time"""
        # We can work directly with byte slices in Python
        hour = int.from_bytes(
            data[0:1],
            byteorder="little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big",
        )
        minute = int.from_bytes(
            data[1:2],
            byteorder="little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big",
        )
        second = int.from_bytes(
            data[2:3],
            byteorder="little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big",
        )
        # Skip padding byte at index 3
        microsecond = int.from_bytes(
            data[4:8],
            byteorder="little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big",
        )

        return datetime.time(hour, minute, second, microsecond)

    def bytes_to_zoned_time(self, data: bytes) -> datetime.time:
        """Convert bytes to zoned time"""
        hour = int.from_bytes(data[0:1], byteorder="little", signed=True)
        current_offset = self.timezone_offset

        if hour < 0:
            hour = -hour

        minute = int.from_bytes(data[1:2], byteorder="little")
        second = int.from_bytes(data[2:3], byteorder="little")
        # Skip padding byte at index 3
        microsecond = int.from_bytes(
            data[4:8],
            byteorder=self.byte_order.value,
        )

        # Create base time and add timezone offset minutes
        base_time = datetime.time(hour, minute, second, microsecond)
        adjusted_time = (
            datetime.datetime.combine(datetime.date.today(), base_time)
            + datetime.timedelta(minutes=current_offset)
        ).time()

        # Create timezone with offset
        tz = datetime.timezone(datetime.timedelta(seconds=self.timezone_offset * 60))
        return adjusted_time.replace(tzinfo=tz)

    def bytes_to_local_datetime(self, data: bytes) -> datetime.datetime:
        """Convert bytes to local datetime"""
        qword = int.from_bytes(
            data,
            byteorder=self.byte_order.value,
        )

        year = qword & 0xFFFF
        qword >>= 16
        month = qword & 0xF
        qword >>= 4
        day = qword & 0x1F
        qword >>= 5
        hour = qword & 0x1F
        qword >>= 5
        minute = qword & 0x3F
        qword >>= 6
        second = qword & 0x3F
        qword >>= 6
        microsecond = qword & 0x3FFFFF

        return datetime.datetime(year, month, day, hour, minute, second, microsecond)

    def bytes_to_zoned_datetime(self, data: bytes) -> datetime.datetime:
        """Convert bytes to zoned datetime"""
        # First get local datetime
        local_dt = self.bytes_to_local_datetime(data)
        # Add timezone offset
        local_dt = local_dt + datetime.timedelta(seconds=self.timezone_offset * 60)
        # Create timezone with offset
        tz = datetime.timezone(datetime.timedelta(seconds=self.timezone_offset * 60))
        return local_dt.replace(tzinfo=tz)

    def bytes_to_duration(self, data: bytes) -> "NDuration":
        """Convert bytes to duration"""
        # Read the 8-byte long value
        qword = int.from_bytes(
            data[0:8],
            byteorder="little" if self.byte_order == ByteOrder.LITTLE_ENDIAN else "big",
        )

        # Extract month-based flag and duration value
        is_month_based = (qword & 0x1) == 1
        duration_value = qword >> 1

        # Initialize all fields
        month, second, micro_sec = 0, 0, 0
        if is_month_based:
            # For month-based duration
            month = int(duration_value % 12)
        else:
            # For time-based duration
            second = int(
                (duration_value % MICRO_SECONDS_OF_MINUTE) // MICRO_SECONDS_OF_SECOND,
            )
            micro_sec = int(duration_value % MICRO_SECONDS_OF_SECOND)

        return NDuration(
            seconds=second,
            microseconds=micro_sec,
            months=month,
        )

    def bytes_to_any(
        self,
        value: bytes,
        vector: VectorWrapper,
        row_idx: int,
    ) -> "AnyValue":
        """Convert bytes to AnyValue for flat vector"""
        # Get data type from first vector wrapper
        data_type_vector = vector.get_vector_wrapper(0)
        value_type = ColumnType(
            bytes_to_int8(
                self._get_sub_bytes(
                    data_type_vector.get_vector_data(),
                    VALUE_TYPE_SIZE,
                    row_idx,
                ),
            ),
        )

        # Create any header to parse value
        any_header = AnyHeader(value, value_type, self.byte_order)
        obj = None

        if value_type.is_basic():
            # Handle basic types
            basic_reader = BytesReader(value)
            obj = self.bytes_basic_to_object(basic_reader, value_type)

        if value_type in (ColumnType.STRING, ColumnType.DECIMAL):
            # Handle string and decimal types
            string_vec = vector.get_vector_wrapper(any_header.chunk_index)
            obj = bytes_to_sized_string(
                data=string_vec.get_vector_data(),
                start_pos=any_header.offset,
                byte_order=self.byte_order,
            )

        if value_type.is_composite():
            # Handle composite types
            sub_vector = vector.get_vector_wrapper(any_header.chunk_index)
            reader = BytesReader(sub_vector.get_vector_data()[any_header.offset :])
            obj = self._decode_composite_value(reader, value_type)

        return AnyValue(obj, value_type)

    def bytes_to_const_any(self, reader: BytesReader) -> "AnyValue":
        """Convert bytes to AnyValue for const vector"""
        column_type = ColumnType(bytes_to_uint8(reader.read(VALUE_TYPE_SIZE)))

        if column_type.is_basic():
            obj = self.bytes_basic_to_object(reader, column_type)
        elif column_type == ColumnType.STRING:
            obj = reader.read_sized_string(self.byte_order)
        elif column_type == ColumnType.DECIMAL:
            obj = self.bytes_to_decimal(reader)
        elif column_type.is_composite():
            obj = self._decode_composite_value(reader, column_type)
        else:
            raise RuntimeError(f"do not support type: {column_type}")

        return AnyValue(obj, column_type)

    def bytes_basic_to_object(
        self,
        reader: BytesReader,
        column_type: ColumnType,
    ) -> Any:
        """Convert bytes to basic type object"""
        obj = None
        if column_type == ColumnType.NULL:
            pass

        elif column_type == ColumnType.BOOL:
            obj = bytes_to_bool(reader.read(BOOL_SIZE))

        elif column_type == ColumnType.INT8:
            obj = bytes_to_int8(reader.read(INT8_SIZE))

        elif column_type == ColumnType.UINT8:
            obj = bytes_to_uint8(reader.read(INT8_SIZE))

        elif column_type == ColumnType.INT16:
            obj = bytes_to_int16(reader.read(INT16_SIZE), self.byte_order)

        elif column_type == ColumnType.UINT16:
            obj = bytes_to_uint16(reader.read(INT16_SIZE), self.byte_order)

        elif column_type in [ColumnType.INT32, ColumnType.UINT32]:
            obj = bytes_to_int32(reader.read(INT32_SIZE), self.byte_order)

        elif column_type in [ColumnType.INT64, ColumnType.UINT64]:
            obj = bytes_to_int64(reader.read(INT64_SIZE), self.byte_order)

        elif column_type == ColumnType.FLOAT32:
            obj = bytes_to_float(reader.read(FLOAT_SIZE), self.byte_order)

        elif column_type == ColumnType.FLOAT64:
            obj = bytes_to_double(reader.read(DOUBLE_SIZE), self.byte_order)

        elif column_type == ColumnType.DATE:
            obj = self.bytes_to_date(reader.read(DATE_SIZE))

        elif column_type == ColumnType.LOCALTIME:
            obj = self.bytes_to_local_time(reader.read(LOCAL_TIME_SIZE))

        elif column_type == ColumnType.ZONEDTIME:
            obj = self.bytes_to_zoned_time(reader.read(ZONED_TIME_SIZE))

        elif column_type == ColumnType.LOCALDATETIME:
            obj = self.bytes_to_local_datetime(reader.read(DATE_TIME_SIZE))

        elif column_type == ColumnType.ZONEDDATETIME:
            obj = self.bytes_to_zoned_datetime(reader.read(ZONED_DATE_TIME_SIZE))

        elif column_type == ColumnType.DURATION:
            obj = self.bytes_to_duration(reader.read(DURATION_SIZE))

        else:
            raise RuntimeError(f"type is not basic: {column_type}")

        return obj

    def bytes_to_decimal(self, reader: BytesReader) -> decimal.Decimal:
        """Convert bytes to decimal"""
        decimal_str = reader.read_sized_string(self.byte_order)
        return self.string_to_decimal(decimal_str)

    def string_to_decimal(self, decimal_str: str) -> decimal.Decimal:
        """Convert string to decimal"""
        return decimal.Decimal(decimal_str)

    def _decode_composite_value(
        self,
        reader: BytesReader,
        column_type: ColumnType,
    ) -> Any:
        """Decode composite types from binary reader"""
        if column_type == ColumnType.NULL:
            return None

        if column_type == ColumnType.BOOL:
            return bytes_to_bool(reader.read(BOOL_SIZE))

        if column_type == ColumnType.INT8:
            return bytes_to_int8(reader.read(INT8_SIZE))

        if column_type == ColumnType.UINT8:
            return bytes_to_uint8(reader.read(INT8_SIZE))

        if column_type == ColumnType.INT16:
            return bytes_to_int16(reader.read(INT16_SIZE), self.byte_order)

        if column_type == ColumnType.UINT16:
            return bytes_to_uint16(reader.read(INT16_SIZE), self.byte_order)

        if column_type in [ColumnType.INT32, ColumnType.UINT32]:
            return bytes_to_int32(reader.read(INT32_SIZE), self.byte_order)

        if column_type in [ColumnType.INT64, ColumnType.UINT64]:
            return bytes_to_int64(reader.read(INT64_SIZE), self.byte_order)

        if column_type == ColumnType.FLOAT32:
            return bytes_to_float(reader.read(FLOAT_SIZE), self.byte_order)

        if column_type == ColumnType.FLOAT64:
            return bytes_to_double(reader.read(DOUBLE_SIZE), self.byte_order)

        if column_type == ColumnType.DATE:
            return self.bytes_to_date(reader.read(DATE_SIZE))

        if column_type == ColumnType.LOCALDATETIME:
            return self.bytes_to_local_datetime(reader.read(DATE_TIME_SIZE))

        if column_type == ColumnType.ZONEDDATETIME:
            return self.bytes_to_zoned_datetime(reader.read(ZONED_DATE_TIME_SIZE))

        if column_type == ColumnType.LOCALTIME:
            return self.bytes_to_local_time(reader.read(LOCAL_TIME_SIZE))

        if column_type == ColumnType.ZONEDTIME:
            return self.bytes_to_zoned_time(reader.read(ZONED_TIME_SIZE))

        if column_type == ColumnType.DURATION:
            return self.bytes_to_duration(reader.read(DURATION_SIZE))

        if column_type == ColumnType.DECIMAL:
            return decimal.Decimal(reader.read_sized_string(self.byte_order))

        if column_type == ColumnType.STRING:
            return reader.read_sized_string(self.byte_order)

        if column_type == ColumnType.LIST:
            ele_type = ColumnType(bytes_to_int8(reader.read(VALUE_TYPE_SIZE)))
            list_size = bytes_to_int16(
                reader.read(ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE),
                self.byte_order,
            )
            null_bit_size = (list_size + 7) // 8
            null_bit_bytes = reader.read(null_bit_size)

            values = []
            for i in range(list_size):
                if (null_bit_bytes[i // 8] & (1 << (i % 8))) == 0:
                    values.append(None)
                else:
                    value = self._decode_composite_value(reader, ele_type)
                    values.append(ValueWrapper(value, ele_type))
            return values

        if column_type == ColumnType.RECORD:
            record_size = bytes_to_int16(
                reader.read(ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE),
                self.byte_order,
            )
            record_map = {}

            for _ in range(record_size):
                field_name = reader.read_sized_string(self.byte_order)
                field_type = ColumnType(bytes_to_uint8(reader.read(VALUE_TYPE_SIZE)))
                field_value = self._decode_composite_value(reader, field_type)
                record_map[field_name] = ValueWrapper(field_value, field_type)

            return NRecord(record_map)

        if column_type == ColumnType.NODE:
            node_id = bytes_to_int64(reader.read(NODE_ID_SIZE), self.byte_order)
            node_type_id = self._get_node_type_id_from_node_id(node_id)
            graph_id = bytes_to_int32(reader.read(GRAPH_ID_SIZE), self.byte_order)
            prop_num = bytes_to_int16(
                reader.read(ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE),
                self.byte_order,
            )

            properties = {}
            for _ in range(prop_num):
                prop_name = reader.read_sized_string(self.byte_order)
                prop_type = ColumnType(bytes_to_uint8(reader.read(VALUE_TYPE_SIZE)))
                prop_value = self._decode_composite_value(reader, prop_type)
                properties[prop_name] = ValueWrapper(prop_value, prop_type)

            return Node(graph_id, node_type_id, node_id, properties, self.graph_schemas)

        if column_type == ColumnType.EDGE:
            src_id = bytes_to_int64(reader.read(NODE_ID_SIZE), self.byte_order)
            dst_id = bytes_to_int64(reader.read(NODE_ID_SIZE), self.byte_order)
            rank = bytes_to_int64(reader.read(RANK_SIZE), self.byte_order)
            graph_id = bytes_to_int32(reader.read(GRAPH_ID_SIZE), self.byte_order)
            edge_type_id = bytes_to_int32(
                reader.read(EDGE_TYPE_ID_SIZE),
                self.byte_order,
            )
            prop_num = bytes_to_int16(
                reader.read(ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE),
                self.byte_order,
            )

            properties = {}
            for _ in range(prop_num):
                prop_name = reader.read_sized_string(self.byte_order)
                prop_type = ColumnType(bytes_to_uint8(reader.read(VALUE_TYPE_SIZE)))
                prop_value = self._decode_composite_value(reader, prop_type)
                properties[prop_name] = ValueWrapper(prop_value, prop_type)

            return Edge(
                graph_id,
                edge_type_id,
                rank,
                src_id,
                dst_id,
                properties,
                self.graph_schemas,
            )

        if column_type == ColumnType.PATH:
            element_num = bytes_to_int16(
                reader.read(ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE),
                self.byte_order,
            )
            elements = []

            for _ in range(element_num):
                element_type = ColumnType(bytes_to_uint8(reader.read(VALUE_TYPE_SIZE)))
                element = self._decode_composite_value(reader, element_type)
                elements.append(ValueWrapper(element, element_type))

            return Path(elements)

        if column_type == ColumnType.EMBEDDINGVECTOR:
            dimension = bytes_to_int16(
                reader.read(ELEMENT_NUMBER_SIZE_FOR_VECTOR_VALUE),
                self.byte_order,
            )

            # Pre-allocate list for better performance
            values = [0.0] * dimension

            # Process chunks of bytes directly
            for i in range(dimension):
                value_bytes = reader.read(EMBEDDING_VECTOR_FLOAT_VALUE_SIZE)
                values[i] = bytes_to_float(value_bytes, self.byte_order)

            return NVector(values=values)

        raise RuntimeError(f"do not support type: {column_type}")


class ValueTypeParser:
    byte_order: ByteOrder

    def __init__(self, byte_order: ByteOrder):
        self.byte_order = byte_order

    def get_data_type(self, reader: BytesReader) -> DataType:
        return self.decode_value_type(reader)

    def decode_value_type(self, reader: BytesReader) -> DataType:
        """Decode value type from bytes reader"""
        value_type_data = reader.read(VALUE_TYPE_SIZE)
        column_type = ColumnType(bytes_to_uint8(value_type_data))

        if column_type in [
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
            ColumnType.STRING,
            ColumnType.DATE,
            ColumnType.LOCALTIME,
            ColumnType.ZONEDTIME,
            ColumnType.LOCALDATETIME,
            ColumnType.ZONEDDATETIME,
            ColumnType.DURATION,
            ColumnType.REFERENCE,
            ColumnType.ANY,
            ColumnType.INVALID,
        ]:
            return BasicType(column_type)

        if column_type == ColumnType.DECIMAL:
            # Skip 4 bytes (2 for precision, 2 for scale)
            reader.read(4)
            return BasicType(column_type)

        if column_type == ColumnType.NODE:
            node_types = self._get_property_name_and_type_from_value_type(
                reader,
                NODE_TYPE_ID_SIZE,
            )
            return NodeType(node_types)

        if column_type == ColumnType.EDGE:
            edge_types = self._get_property_name_and_type_from_value_type(
                reader,
                EDGE_TYPE_ID_SIZE,
            )
            return EdgeType(edge_types)

        if column_type == ColumnType.PATH:
            element_num = bytes_to_int32(
                reader.read(PATH_ELEMENT_NUM_SIZE),
                self.byte_order,
            )
            data_types = []
            for _ in range(element_num):
                data_types.append(self.decode_value_type(reader))
            return PathType(data_types)

        if column_type == ColumnType.LIST:
            data_type = self.decode_value_type(reader)
            return ListType(data_type)

        if column_type == ColumnType.RECORD:
            field_num = bytes_to_int32(
                reader.read(RECORD_FIELD_NUM_SIZE),
                self.byte_order,
            )
            field_types = {}
            for _ in range(field_num):
                field_name = reader.read_sized_string(self.byte_order)
                field_types[field_name] = self.decode_value_type(reader)
            return RecordType(field_types)

        if column_type == ColumnType.EMBEDDINGVECTOR:
            dimension = bytes_to_int32(
                reader.read(EMBEDDING_VECTOR_DIM_SIZE),
                self.byte_order,
            )
            value_type = ColumnType(bytes_to_uint8(reader.read(VALUE_TYPE_SIZE)))
            return EmbeddingVectorType(dimension, value_type)

        raise RuntimeError(f"unsupported type: {column_type}")

    def _get_property_name_and_type_from_value_type(
        self,
        reader: BytesReader,
        type_id_size: int,
    ) -> Dict[int, Dict[int, Dict[str, DataType]]]:
        """Get property name and type mapping for nodes/edges
        Returns mapping: graph_id -> (type_id -> (prop_name -> prop_type))
        """
        # 1-5: node or edge type number, 4 bytes
        type_num = bytes_to_int32(
            reader.read(GRAPH_ELEMENT_TYPE_NUM_SIZE),
            self.byte_order,
        )

        graph_type_fields = {}
        for _ in range(type_num):
            # graphID
            graph_id = bytes_to_int32(reader.read(GRAPH_ID_SIZE), self.byte_order)
            if graph_id not in graph_type_fields:
                graph_type_fields[graph_id] = {}

            # node type ID or edge type ID
            type_id = (
                bytes_to_int16(reader.read(type_id_size), self.byte_order)
                if type_id_size == NODE_TYPE_ID_SIZE
                else bytes_to_int32(reader.read(type_id_size), self.byte_order)
            )

            # node or edge type property number, 4 bytes
            type_property_num = bytes_to_int32(
                reader.read(PROPERTY_NUM_SIZE),
                self.byte_order,
            )

            # read the property name and data type for node or edge type, property name end with \0
            property_and_type = {}
            for _ in range(type_property_num):
                property_name = reader.read_sized_string(self.byte_order)
                # TODO:
                # it's found when return node with vec prop, the property name is not parsed properly
                # need to further check if it's server side problem
                # data type, 1 byte
                data_type = self.decode_value_type(reader)
                property_and_type[property_name] = data_type

            graph_type_fields[graph_id][type_id] = property_and_type

        return graph_type_fields
