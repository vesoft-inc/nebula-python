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

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum

# Forward references for type hints
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Type, Union

from typing_extensions import TypedDict

from nebulagraph_python.decoder.data_types import ColumnType, ResultGraphSchemas
from nebulagraph_python.decoder.size_constant import EDGE_TYPE_ID_SIZE

if TYPE_CHECKING:
    from nebulagraph_python.value_wrapper import ValueWrapper


logger = logging.getLogger(__name__)


class NodePrimitive(TypedDict):
    id: int
    type: str
    labels: List[str]
    properties: Dict[str, Any]


class EdgePrimitive(TypedDict):
    src_id: int
    dst_id: int
    rank: int
    type: str
    labels: List[str]
    properties: Dict[str, Any]
    direction: str


class PathPrimitive(TypedDict, total=False):
    nodes: List[NodePrimitive]
    edges: List[EdgePrimitive]
    length: int
    start_node: NodePrimitive
    end_node: NodePrimitive
    string_representation: str


class PathPrimitiveField(str, Enum):
    NODES = "nodes"
    EDGES = "edges"
    LENGTH = "length"
    START_NODE = "start_node"
    END_NODE = "end_node"
    STRING_REPRESENTATION = "string_representation"


class BaseDataObject:
    def __init__(self):
        self._decode_type = "utf-8"

    def get_decode_type(self) -> str:
        return self._decode_type

    def set_decode_type(self, decode_type: str) -> "BaseDataObject":
        self._decode_type = decode_type
        return self


class CompositeDataObject(BaseDataObject, ABC):
    @abstractmethod
    def cast_primitive(self) -> Any:
        pass


class Node(CompositeDataObject):
    def __init__(
        self,
        graph_id: int,
        node_type_id: int,
        node_id: int,
        properties: Dict[str, "ValueWrapper"],
        graph_schemas: "ResultGraphSchemas",
    ):
        super().__init__()
        self.graph_id = graph_id
        self.graph_name = graph_schemas.get_graph_schema(graph_id).get_graph_name()
        self.node_type_id = node_type_id
        self.node_type_name = (
            graph_schemas.get_graph_schema(graph_id)
            .get_node_schema(node_type_id)
            .get_node_type_name()
        )
        self.labels = (
            graph_schemas.get_graph_schema(graph_id)
            .get_node_schema(node_type_id)
            .get_node_labels()
        )
        self.node_id = node_id
        self.properties = properties

    def get_graph(self) -> str:
        """Get graph

        Returns
        -------
            str: graph name

        """
        return self.graph_name

    def get_type(self) -> str:
        """Get node type name

        Returns
        -------
            str: node type name

        """
        return self.node_type_name

    def get_node_type_id(self) -> int:
        """Get node type id

        Returns
        -------
            int: node type id

        """
        return self.node_type_id

    def get_labels(self) -> List[str]:
        """Get node label list

        Returns
        -------
            List[str]: list of labels

        """
        return self.labels

    def get_id(self) -> int:
        """Get vid

        Returns
        -------
            int: node id

        """
        return self.node_id

    def get_column_names(self) -> List[str]:
        """Get property names from the node

        Returns
        -------
            List[str]: the list of property names

        """
        return list(self.properties.keys())

    def get_values(self) -> List["ValueWrapper"]:
        """Get all property values

        Returns
        -------
            List[ValueWrapper]: the List of property values

        """
        values = []
        for _, value in self.properties.items():
            values.append(value)
        return values

    def get_properties(self) -> Dict[str, "ValueWrapper"]:
        """Get all properties for node

        Returns
        -------
            Dict[str, ValueWrapper]: property name -> property value

        """
        return self.properties

    def cast_primitive(self) -> NodePrimitive:
        """Convert node to a dictionary format with id, type, labels and properties

        Returns
        -------
            NodePrimitive: A typed dictionary containing node information in format:
                {
                    'id': node_id,
                    'type': node_type,
                    'labels': list of labels,
                    'properties': dict of property name -> primitive value
                }

        """
        properties = {}
        for prop_name, prop_value in self.properties.items():
            properties[prop_name] = prop_value.cast_primitive()

        return {
            "id": self.node_id,
            "type": self.get_type(),
            "labels": self.labels,
            "properties": properties,
        }

    def __eq__(self, other):
        if self is other:
            return True
        if other is None or not isinstance(other, Node):
            return False
        return self.get_id() == other.get_id()

    def __hash__(self):
        return hash(
            (self.graph_id, self.node_type_id, self.node_id, self.get_decode_type()),
        )

    def __str__(self):
        props = self.get_properties()
        prop_strs = []
        for prop_name in props:
            prop_strs.append(f"{prop_name}:{props[prop_name].cast()}")
        return f"({self.get_id()}@{self.get_type()}:{self.get_labels()}{{{','.join(prop_strs)}}})"


class Edge(CompositeDataObject):
    def __init__(
        self,
        graph_id: int,
        edge_type_id: int,
        rank: int,
        src_id: int,
        dst_id: int,
        properties: Dict[str, "ValueWrapper"],
        graph_schemas: ResultGraphSchemas,
    ):
        """Edge is a wrapper around the Edge type returned by nebula-graph"""
        super().__init__()
        self.graph_id = graph_id
        self.graph_name = graph_schemas.get_graph_schema(graph_id).get_graph_name()
        self.edge_type_id = edge_type_id
        no_directed_type_id = edge_type_id & 0x3FFFFFFF
        self.edge_type_name = (
            graph_schemas.get_graph_schema(graph_id)
            .get_edge_schema(no_directed_type_id)
            .get_edge_type_name()
        )
        self.labels = (
            graph_schemas.get_graph_schema(graph_id)
            .get_edge_schema(no_directed_type_id)
            .get_edge_labels()
        )
        self.rank = rank
        self.properties = properties

        edge_type_move_bits = EDGE_TYPE_ID_SIZE * 8 - 2
        direction_bits = (edge_type_id >> edge_type_move_bits) & 0x3

        if direction_bits == 0x0:
            self.direction = "OUTGOING"
        elif direction_bits == 0x1:
            self.direction = "INCOMING"
        elif direction_bits in (0x2, 0x3):
            self.direction = "UNDIRECTED"
        else:
            self.direction = "KNOWN"

        if self.direction == "INCOMING":
            self.src_id = dst_id
            self.dst_id = src_id
        else:
            self.src_id = src_id
            self.dst_id = dst_id

    def get_graph(self) -> str:
        """Get graph

        Returns
        -------
            str: graph name

        """
        return self.graph_name

    def get_type(self) -> str:
        """Get edge type name

        Returns
        -------
            str: edge type name

        """
        return self.edge_type_name

    def get_edge_type_id(self) -> int:
        """Get edge type id"""
        return self.edge_type_id

    def is_directed(self) -> bool:
        """If the edge is directed

        Returns
        -------
            bool: true if edge is directed

        """
        return self.direction in ("OUTGOING", "INCOMING")

    def get_labels(self) -> List[str]:
        """Get edge labels

        Returns
        -------
            List[str]: list of edge labels

        """
        return self.labels

    def get_src_id(self) -> int:
        """Get the src id

        Returns
        -------
            int: source id

        """
        return self.src_id

    def get_dst_id(self) -> int:
        """Get the dst id from the edge

        Returns
        -------
            int: destination id

        """
        return self.dst_id

    def get_rank(self) -> int:
        """Get rank from the edge

        Returns
        -------
            int: rank

        """
        return self.rank

    def get_column_names(self) -> List[str]:
        """Get all property name

        Returns
        -------
            List[str]: the List of property names

        """
        return list(self.properties.keys())

    def get_property_values(self) -> List["ValueWrapper"]:
        """Get all property values

        Returns
        -------
            List[ValueWrapper]: the List of property values

        """
        values = []
        for _, value in self.properties.items():
            values.append(value)
        return values

    def get_properties(self) -> Dict[str, "ValueWrapper"]:
        """Get property names and values from the edge

        Returns
        -------
            Dict[str, ValueWrapper]: property name -> property value

        """
        return self.properties

    def cast_primitive(self) -> EdgePrimitive:
        """Convert edge to a dictionary format with source id, destination id, rank, type, labels and properties

        Returns
        -------
            EdgePrimitive: A typed dictionary containing edge information in format:
                {
                    'src_id': source_id,
                    'dst_id': destination_id,
                    'rank': rank,
                    'type': edge_type,
                    'labels': list of labels,
                    'properties': dict of property name -> primitive value,
                    'direction': direction
                }

        """
        properties = {}
        for prop_name, prop_value in self.properties.items():
            properties[prop_name] = prop_value.cast_primitive()

        return {
            "src_id": self.src_id,
            "dst_id": self.dst_id,
            "rank": self.rank,
            "type": self.get_type(),
            "labels": self.labels,
            "properties": properties,
            "direction": self.direction,
        }

    def __eq__(self, other):
        if self is other:
            return True
        if other is None or not isinstance(other, Edge):
            return False
        return (
            self.get_rank() == other.get_rank()
            and (
                (
                    self.get_src_id() == other.get_src_id()
                    and self.get_dst_id() == other.get_dst_id()
                )
                or (
                    self.get_src_id() == other.get_dst_id()
                    and self.get_dst_id() == other.get_src_id()
                )
            )
            and self.get_type() == other.get_type()
        )

    def __hash__(self):
        return hash(
            (
                self.graph_id,
                self.edge_type_id,
                self.rank,
                self.src_id,
                self.dst_id,
                self.get_decode_type(),
            ),
        )

    def __str__(self):
        props = self.get_properties()
        prop_strs = []
        for key in props:
            prop_strs.append(f"{key}:{props[key].cast()}")

        if self.direction != "UNDIRECTED":
            return f"({self.get_src_id()})-[{self.get_rank()}@{self.get_type()}:{self.get_labels()}{{{','.join(prop_strs)}}}]->({self.get_dst_id()})"
        return f"({self.get_src_id()})~[{self.get_rank()}@{self.get_type()}:{self.get_labels()}{{{','.join(prop_strs)}}}]~({self.get_dst_id()})"


class Path(CompositeDataObject):
    def __init__(self, values: list["ValueWrapper"]):
        self.values = values
        self.nodes: list[Node] = []
        self.edges: list[Edge] = []

        for value in values:
            if value.is_node():
                self.nodes.append(value.as_node())
            else:
                self.edges.append(value.as_edge())

    def get_nodes(self) -> list[Node]:
        """Create a list over the nodes in this path, nodes will appear in the same order as they appear
        in the path.

        Returns
        -------
            List[Node]: a List of all nodes in this path

        """
        return self.nodes

    def get_edges(self) -> list[Edge]:
        """Create a list over the edges in this path. The edges will appear
        in the same order as they appear in the path.

        Returns
        -------
            List[Edge]: a List of all edges in this path

        """
        return self.edges

    def get_values(self) -> list["ValueWrapper"]:
        """Create a list over the nodes and edges in this path. The value will appear
        in the same order as they appear in the path. The first value will be Node type, then the
        next one will be Edge type, and next one will be Node type.

        Returns
        -------
            List[ValueWrapper]: a List of all values in this path

        """
        return self.values

    def cast_primitive(
        self,
        fields: Optional[Set[PathPrimitiveField]] = None,
    ) -> PathPrimitive:
        """Convert path to a dictionary format with nodes and edges.

        Args:
        ----
            fields: List of fields to include in the dictionary.

        Example path string representation:
            (289226172909223937@Movie:Movie{id:91,name:Unpromised Land})-
            [0@WithGenre:WithGenre{}]->
            (289483299716333572@Genre:Genre{id:101,name:Staged Documentary})

        Returns:
        -------
            Dict containing the requested path information. Only fields specified in the
            fields parameter will be included.

        """
        fields = fields or set(PathPrimitiveField)
        mapping = {
            PathPrimitiveField.NODES: lambda: [
                node.cast_primitive() for node in self.nodes
            ],
            PathPrimitiveField.EDGES: lambda: [
                edge.cast_primitive() for edge in self.edges
            ],
            PathPrimitiveField.LENGTH: lambda: len(self.nodes),
            PathPrimitiveField.START_NODE: lambda: self.nodes[0].cast_primitive(),
            PathPrimitiveField.END_NODE: lambda: self.nodes[-1].cast_primitive(),
            PathPrimitiveField.STRING_REPRESENTATION: lambda: str(self),
        }
        result: PathPrimitive = {}
        for field in fields:
            result[field.value] = mapping[field]()

        return result

    def __eq__(self, other):
        if self is other:
            return True
        if other is None or not isinstance(other, Path):
            return False
        return self.values == other.values

    def __hash__(self):
        return hash(self.values)

    def __str__(self):
        if not self.values:
            return "()"

        prefix_node = self.nodes[0]
        prefix_node_prop_strs = []
        prefix_node_props = prefix_node.get_properties()
        for key in prefix_node_props:
            prefix_node_prop_strs.append(f"{key}:{prefix_node_props[key].cast()}")

        # Just one node in the path
        if not self.edges:
            template = "(%d@%s:%s{%s})"
            return template % (
                prefix_node.get_id(),
                prefix_node.get_type(),
                prefix_node.get_type(),
                ",".join(prefix_node_prop_strs),
            )

        edge_strs = []
        for i, edge in enumerate(self.edges):
            edge_prop_strs = []
            props = edge.get_properties()
            for key in props:
                edge_prop_strs.append(f"{key}:{props[key].cast()}")

            suffix_node = self.nodes[i + 1]
            suffix_node_prop_strs = []
            suffix_node_props = suffix_node.get_properties()
            for key in suffix_node_props:
                suffix_node_prop_strs.append(f"{key}:{suffix_node_props[key].cast()}")

            if i == 0:
                template = "(%d@%s:%s{%s})~[%d@%s:%s{%s}]~(%d@%s:%s{%s})"
                if edge.is_directed():
                    if edge.get_src_id() == prefix_node.get_id():
                        template = "(%d@%s:%s{%s})-[%d@%s:%s{%s}]->(%d@%s:%s{%s})"
                    else:
                        template = "(%d@%s:%s{%s})<-[%d@%s:%s{%s}]-(%d@%s:%s{%s})"

                edge_strs.append(
                    template
                    % (
                        prefix_node.get_id(),
                        prefix_node.get_type(),
                        prefix_node.get_type(),
                        ",".join(prefix_node_prop_strs),
                        edge.get_rank(),
                        edge.get_type(),
                        edge.get_type(),
                        ",".join(edge_prop_strs),
                        suffix_node.get_id(),
                        suffix_node.get_type(),
                        suffix_node.get_type(),
                        ",".join(suffix_node_prop_strs),
                    ),
                )
            else:
                template = "~[%d@%s:%s{%s}]~(%d@%s:%s{%s})"
                if edge.is_directed():
                    if edge.get_dst_id() == suffix_node.get_id():
                        template = "-[%d@%s:%s{%s}]->(%d@%s:%s{%s})"
                    else:
                        template = "<-[%d@%s:%s{%s}]-(%d@%s:%s{%s})"
                edge_strs.append(
                    template
                    % (
                        edge.get_rank(),
                        edge.get_type(),
                        edge.get_type(),
                        ",".join(edge_prop_strs),
                        suffix_node.get_id(),
                        suffix_node.get_type(),
                        suffix_node.get_type(),
                        ",".join(suffix_node_prop_strs),
                    ),
                )

        return "".join(edge_strs)


class NRecord(CompositeDataObject):
    def __init__(self, properties: Dict[str, "ValueWrapper"]):
        self.type = ColumnType.RECORD
        self.map: Dict[str, "ValueWrapper"] = {}
        if properties:
            self.map.update(properties)

    def contains_key(self, key: str) -> bool:
        """Returns true if this record contains a mapping for the specified key. The key cannot be null.

        Args:
        ----
            key: key whose presence in this record is to be checked
        Returns:
            bool: true if this record contains a mapping for the specified key
        Raises:
            NullPointerException: if the specified key is null

        """
        if key is None:
            raise ValueError("null map key")
        return key in self.map

    def get_value(self, key: str) -> Optional["ValueWrapper"]:
        """Get the Value of specified key in this record

        Args:
        ----
            key: key whose corresponding value in this record will be returned
        Returns:
            The ValueWrapper of the specified key

        """
        if not self.contains_key(key):
            return None
        return self.map[key]

    def is_empty(self) -> bool:
        """Returns true if this record has no values

        Returns
        -------
            bool: true if this record contains no key-value mappings

        """
        return not self.map

    def size(self) -> int:
        """Get the number of key-value mappings in this record.

        Returns
        -------
            int: the number of key-value mappings in this record

        """
        return len(self.map)

    def get_values_map(self) -> Dict[str, "ValueWrapper"]:
        """Get the Map object for this record

        Returns
        -------
            Dict: Map for this record

        """
        return self.map

    def cast_primitive(self) -> Dict[str, Any]:
        """Convert record(Map or Dict actually) to a dictionary format with property name -> primitive value

        Returns
        -------
            Dict: A dictionary containing property name -> primitive value

        """
        return {key: value.cast_primitive() for key, value in self.map.items()}

    def __str__(self):
        return str(self.cast_primitive())

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, NRecord):
            return False
        return self.map == other.get_values_map()

    def __hash__(self):
        return hash(frozenset(self.map.items()))


class NDuration(BaseDataObject):
    def __init__(self, seconds: int, microseconds: int, months: int):
        self.is_month_based = months != 0

        # Convert seconds and microseconds to time components
        total_seconds = abs(seconds)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        secs = total_seconds % 60

        # Convert months to year/month
        years = months // 12
        remaining_months = months % 12

        self.year = years
        self.month = remaining_months
        self.day = 0  # Not month based
        self.hour = hours
        self.minute = minutes
        self.second = secs
        self.microsec = microseconds

    def get_year(self) -> int:
        return self.year

    def get_month(self) -> int:
        return self.month

    def get_day(self) -> int:
        return self.day

    def get_hour(self) -> int:
        return self.hour

    def get_minute(self) -> int:
        return self.minute

    def get_second(self) -> int:
        return self.second

    def get_microsecond(self) -> int:
        return self.microsec

    def __str__(self) -> str:
        duration_str = ["P"]
        if self.is_month_based:
            if self.year != 0:
                duration_str.append(f"{self.year}Y")
            if self.month != 0 or self.year == 0:
                duration_str.append(f"{self.month}M")
        else:
            if self.day != 0:
                duration_str.append(f"{self.day}D")

            # Only add T if we have time components or zero duration
            if (
                self.day == 0
                or self.hour != 0
                or self.minute != 0
                or self.second != 0
                or self.microsec != 0
            ):
                duration_str.append("T")

            if self.hour != 0:
                duration_str.append(f"{self.hour}H")
            if self.minute != 0:
                duration_str.append(f"{self.minute}M")

            if (
                self.second != 0
                or self.microsec != 0
                or (
                    self.day == 0
                    and self.hour == 0
                    and self.minute == 0
                    and self.second == 0
                )
            ):
                if self.microsec == 0:
                    duration_str.append(f"{self.second}S")
                else:
                    total_microseconds = self.second * 1000000 + self.microsec
                    is_minus = total_microseconds < 0
                    if is_minus:
                        total_microseconds = -total_microseconds
                    seconds = total_microseconds // 1000000
                    microsec = total_microseconds % 1000000

                    if is_minus:
                        num_str = f"-{seconds}.{microsec:06d}"
                    else:
                        num_str = f"{seconds}.{microsec:06d}"

                    # Remove trailing zeros
                    while num_str[-1] == "0":
                        num_str = num_str[:-1]
                    duration_str.append(f"{num_str}S")

        return "".join(duration_str)

    def __eq__(self, other):
        if not isinstance(other, NDuration):
            return False
        return (
            self.is_month_based == other.is_month_based
            and self.year == other.get_year()
            and self.month == other.get_month()
            and self.day == other.get_day()
            and self.hour == other.get_hour()
            and self.minute == other.get_minute()
            and self.second == other.get_second()
            and self.microsec == other.get_microsecond()
        )

    def __hash__(self):
        return hash(
            (
                self.is_month_based,
                self.year,
                self.month,
                self.day,
                self.hour,
                self.minute,
                self.second,
                self.microsec,
            ),
        )


@dataclass(frozen=True)
class NVector(BaseDataObject):
    """Represents a fixed-dimension vector of float values."""

    values: List[float]

    @property
    def dimension(self) -> int:
        return len(self.values)

    def get_values(self) -> List[float]:
        return self.values

    def get_dimension(self) -> int:
        return self.dimension

    def __len__(self) -> int:
        """Return the dimension of the vector."""
        return self.dimension

    def __getitem__(self, index: int) -> float:
        """Get vector component at specified index."""
        if index < 0 or index >= self.dimension:
            raise IndexError(f"Index out of bounds: {index}")
        return self.values[index]

    def __eq__(self, other) -> bool:
        """Compare two vectors for equality."""
        if not isinstance(other, NVector):
            logger.warning("Expected Vector, got %s", type(other))
            return False
        return self.dimension == other.dimension and all(
            abs(a - b) < 1e-7 for a, b in zip(self.values, other.values)
        )

    def __str__(self) -> str:
        """Return string representation of the vector."""
        return f"NVector({self.values})"

    def __repr__(self) -> str:
        """Return detailed string representation of the vector."""
        return f"NVector({self.values})"


BasicTargetType = Union[
    None,
    bool,
    int,
    float,
    str,
    list,
    time,
    date,
    datetime,
    Decimal,
    NDuration,
    NVector,
]

CompositeTargetType = Union[
    Path,
    Node,
    Edge,
    NRecord,
]
TargetType = Union[BasicTargetType, CompositeTargetType]
TargetPrimitiveType = Union[
    BasicTargetType,
    PathPrimitive,
    EdgePrimitive,
    NodePrimitive,
    Dict[str, Any],
    List[float],
]
ColumnToPy: Dict[ColumnType, Type[TargetType]] = {
    ColumnType.NULL: type(None),
    ColumnType.BOOL: bool,
    ColumnType.INT8: int,
    ColumnType.INT16: int,
    ColumnType.INT32: int,
    ColumnType.INT64: int,
    ColumnType.UINT8: int,
    ColumnType.UINT16: int,
    ColumnType.UINT32: int,
    ColumnType.UINT64: int,
    ColumnType.FLOAT32: float,
    ColumnType.FLOAT64: float,
    ColumnType.STRING: str,
    ColumnType.DATE: date,
    ColumnType.LOCALTIME: time,
    ColumnType.ZONEDTIME: time,
    ColumnType.LOCALDATETIME: datetime,
    ColumnType.ZONEDDATETIME: datetime,
    ColumnType.DECIMAL: Decimal,
    ColumnType.LIST: list,
    ColumnType.DURATION: NDuration,
    ColumnType.PATH: Path,
    ColumnType.NODE: Node,
    ColumnType.EDGE: Edge,
    ColumnType.RECORD: NRecord,
    ColumnType.EMBEDDINGVECTOR: NVector,
}
