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

from enum import Enum
from typing import Any, Dict, List, Optional

from nebulagraph_python.decoder.data_types import ByteOrder, charset
from nebulagraph_python.decoder.decode_utils import (
    bytes_to_int16,
    bytes_to_int32,
    is_null_bit_map_all_set,
)
from nebulagraph_python.decoder.size_constant import (
    EDGE_TYPE_ID_SIZE,
    ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE,
    GRAPH_ELEMENT_TYPE_NUM_SIZE,
    GRAPH_ID_SIZE,
    INT32_SIZE,
    NODE_TYPE_ID_SIZE,
    PATH_META_DATA_NODE_EDGE_TYPE_INDEX,
    PROPERTY_NUM_SIZE,
    VECTOR_INDEX_SIZE,
)
from nebulagraph_python.proto.vector_pb2 import NestedVector, VectorBatch


class VectorType(Enum):
    INVALID_VECTOR = 0
    FLAT_VECTOR = 1
    CONST_VECTOR = 2
    PARALLEL_VECTOR = 3

    @staticmethod
    def get_vector_type(type_value: int) -> "VectorType":
        """Get vector type from type value"""
        if type_value == 0:
            return VectorType.INVALID_VECTOR
        if type_value == 1:
            return VectorType.CONST_VECTOR
        if type_value == 2:
            return VectorType.FLAT_VECTOR
        if type_value == 3:
            return VectorType.PARALLEL_VECTOR
        raise ValueError(f"Unsupported vector type: {type_value}")


class PathSpecialMetaData:
    def __init__(self, vector: NestedVector, byte_order: ByteOrder):
        self.graph_id_and_node_types = {}  # graphId -> nodeTypeId -> pairIndex
        self.graph_id_and_edge_types = {}  # graphId -> edgeTypeId -> pairIndex
        self.index_and_nodes = {}  # pairIndex -> PathVectorPair
        self.index_and_edges = {}  # pairIndex -> PathVectorPair

        meta_data = vector.special_meta_data
        reader = BytesReader(meta_data)
        nested_vector_index = 0

        # Parse node types
        node_type_num = bytes_to_int32(
            reader.read(GRAPH_ELEMENT_TYPE_NUM_SIZE),
            byte_order,
        )
        for _ in range(node_type_num):
            graph_id = bytes_to_int32(reader.read(GRAPH_ID_SIZE), byte_order)
            node_type_id = bytes_to_int16(reader.read(NODE_TYPE_ID_SIZE), byte_order)
            node_type_pair_index = bytes_to_int16(
                reader.read(PATH_META_DATA_NODE_EDGE_TYPE_INDEX),
                byte_order,
            )

            if graph_id not in self.graph_id_and_node_types:
                self.graph_id_and_node_types[graph_id] = {}
            self.graph_id_and_node_types[graph_id][node_type_id] = node_type_pair_index

            pair = PathVectorPair(
                VectorWrapper(vector.nested_vectors[nested_vector_index], byte_order),
                VectorWrapper(
                    vector.nested_vectors[nested_vector_index + 1],
                    byte_order,
                ),
            )
            self.index_and_nodes[node_type_pair_index] = pair
            nested_vector_index += 2

        # Parse edge types
        edge_type_num = bytes_to_int32(
            reader.read(GRAPH_ELEMENT_TYPE_NUM_SIZE),
            byte_order,
        )
        for _ in range(edge_type_num):
            graph_id = bytes_to_int32(reader.read(GRAPH_ID_SIZE), byte_order)
            edge_type_id = bytes_to_int32(reader.read(EDGE_TYPE_ID_SIZE), byte_order)
            edge_type_pair_index = bytes_to_int16(
                reader.read(PATH_META_DATA_NODE_EDGE_TYPE_INDEX),
                byte_order,
            )

            if graph_id not in self.graph_id_and_edge_types:
                self.graph_id_and_edge_types[graph_id] = {}
            self.graph_id_and_edge_types[graph_id][edge_type_id] = edge_type_pair_index

            pair = PathVectorPair(
                VectorWrapper(vector.nested_vectors[nested_vector_index], byte_order),
                VectorWrapper(
                    vector.nested_vectors[nested_vector_index + 1],
                    byte_order,
                ),
            )
            self.index_and_edges[edge_type_pair_index] = pair
            nested_vector_index += 2


class VectorWrapper:
    def __init__(self, vector: NestedVector, byte_order: ByteOrder):
        self.byte_order = byte_order
        self.vector = vector
        self.null_all_set = is_null_bit_map_all_set(self.vector)
        self.vector_wrappers = [
            VectorWrapper(vec, byte_order) for vec in vector.nested_vectors
        ]
        self.graph_element_type_id_and_prop_vector_index_map = None
        self.path_special_meta_data = None
        self.const_value = None

    def get_vector(self):
        return self.vector

    def get_num_nested_vectors(self):
        return self.vector.num_nested_vectors

    def get_vector_num_records(self):
        return self.vector.common_meta_data.num_records

    def get_vector_type(self) -> VectorType:
        """Match Java's DecodeUtils.getVectorType"""
        if (
            not hasattr(self.vector, "common_meta_data")
            or not self.vector.common_meta_data
        ):
            return VectorType.INVALID_VECTOR
        content_type = self.vector.common_meta_data.vector_content_type
        type_val = content_type & 0xFF
        return VectorType.get_vector_type(type_val)

    def get_vector_data(self) -> bytes:
        return self.vector.vector_data

    def get_null_bit_map(self) -> bytes:
        return self.vector.null_bit_map

    def get_nested_vectors(self) -> List["VectorWrapper"]:
        return self.vector_wrappers

    def get_vector_wrapper(self, index: int) -> "VectorWrapper":
        return self.vector_wrappers[index]

    def is_null_all_set(self) -> bool:
        return self.null_all_set

    def set_const_value(self, value: Any):
        self.const_value = value

    def get_const_value(self) -> Any:
        return self.const_value

    def get_graph_element_type_id_and_prop_vector_index_map(
        self,
        type_id_size: int,
    ) -> Dict[int, Dict[int, Dict[str, int]]]:
        if self.graph_element_type_id_and_prop_vector_index_map is None:
            self.graph_element_type_id_and_prop_vector_index_map = {}
            reader = BytesReader(self.vector.special_meta_data)

            # First read total number of property names
            property_num = bytes_to_int32(reader.read(INT32_SIZE), self.byte_order)

            # Read all property names first
            prop_names = []
            for _ in range(property_num):
                prop_names.append(reader.read_sized_string(self.byte_order))

            # Read number of types
            type_num = bytes_to_int32(reader.read(INT32_SIZE), self.byte_order)

            for _ in range(type_num):
                # Get graph ID
                graph_id = bytes_to_int32(reader.read(GRAPH_ID_SIZE), self.byte_order)

                # Get type ID based on size (2 bytes for node, 4 bytes for edge)
                type_id = (
                    bytes_to_int16(reader.read(type_id_size), self.byte_order)
                    if type_id_size == 2
                    else bytes_to_int32(reader.read(type_id_size), self.byte_order)
                )

                # Get number of properties for this type
                node_prop_num = bytes_to_int32(
                    reader.read(PROPERTY_NUM_SIZE),
                    self.byte_order,
                )

                # Map property names to vector indices
                prop_name_to_vector_index = {}
                for _ in range(node_prop_num):
                    prop_vector_index = bytes_to_int32(
                        reader.read(VECTOR_INDEX_SIZE),
                        self.byte_order,
                    )
                    prop_name_to_vector_index[prop_names[prop_vector_index]] = (
                        prop_vector_index
                    )

                if graph_id not in self.graph_element_type_id_and_prop_vector_index_map:
                    self.graph_element_type_id_and_prop_vector_index_map[graph_id] = {}

                self.graph_element_type_id_and_prop_vector_index_map[graph_id][
                    type_id
                ] = prop_name_to_vector_index

        return self.graph_element_type_id_and_prop_vector_index_map

    def get_path_special_meta_data(self) -> Optional[PathSpecialMetaData]:
        """Get path special metadata, matching Java's getPathSpecialMetaData()"""
        if not self.vector or not self.vector.special_meta_data:
            return None
        return PathSpecialMetaData(self.vector, self.byte_order)

    def get_special_meta_data(self) -> Optional[bytes]:
        """Get special metadata bytes, matching Java's getSpecialMetaData()"""
        return self.vector.special_meta_data if self.vector else None


class Batch:
    def __init__(self, batch: VectorBatch, byte_order: ByteOrder):
        """Initialize Batch with VectorBatch and byte order

        Args:
        ----
            batch (VectorBatch): The vector batch
            byte_order (str): Byte order ('little' or 'big')

        """
        self.batch = batch
        self.vectors = []
        for vector in batch.vectors:
            self.vectors.append(VectorWrapper(vector, byte_order))

    def get_vectors_count(self) -> int:
        """Get count of vectors in this Batch

        Returns
        -------
            int: Count of vectors

        """
        return len(self.batch.vectors)

    def get_vectors(self, index: int) -> "VectorWrapper":
        """Get the VectorWrapper with specific index of the batch

        Args:
        ----
            index (int): Index of vector to get

        Returns:
        -------
            VectorWrapper: Vector wrapper at specified index

        """
        return self.vectors[index]

    def get_batch_row_size(self) -> int:
        """Get the row size of this batch

        Returns
        -------
            int: Row size

        """
        if self.get_vectors_count() > 0:
            return self.batch.vectors[0].common_meta_data.num_records
        return 0


class BytesReader:
    def __init__(self, data: bytes):
        """Initialize BytesReader with bytes data

        Args:
        ----
            data (bytes): The bytes data to read from

        """
        self.data = data
        self.index = 0

    def read(self, size: int) -> bytes:
        """Read size bytes from current position and advance position

        Args:
        ----
            size (int): Number of bytes to read

        Returns:
        -------
            bytes: The bytes read

        """
        data = self.data[self.index : self.index + size]
        self.index += size
        return data

    def read_sized_string(self, byte_order: ByteOrder) -> str:
        """Read a string prefixed with its 2-byte length

        Args:
        ----
            byte_order (str): Byte order for decoding length

        Returns:
        -------
            str: The decoded string

        """
        length = bytes_to_int16(
            self.read(ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE),
            byte_order,
        )
        start_index = self.index
        self.index += length
        string_bytes = self.data[start_index : start_index + length]
        return string_bytes.decode(charset)


class PathVectorPair:
    """The PathVectorPair represents a Node or Edge element and its neighbor information in the path"""

    def __init__(self, node_vector: "VectorWrapper", adj_vector: "VectorWrapper"):
        """Initialize PathVectorPair with node/edge vector and adjacency vector

        Args:
        ----
            node_vector (VectorWrapper): Node or Edge Vector
            adj_vector (VectorWrapper): The adjacency Long Vector

        """
        self.node_vector = node_vector
        self.adj_vector = adj_vector

    def get_vector(self) -> "VectorWrapper":
        """Get the node/edge vector

        Returns
        -------
            VectorWrapper: The node or edge vector

        """
        return self.node_vector

    def get_adj_vector(self) -> "VectorWrapper":
        """Get the adjacency vector

        Returns
        -------
            VectorWrapper: The adjacency vector

        """
        return self.adj_vector
