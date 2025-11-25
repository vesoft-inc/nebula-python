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

from typing import Any, Dict, Iterable, List, Optional, Tuple

from nebulagraph_python.client import NebulaBaseExecutor
from nebulagraph_python.tools.graph_type import (
    EdgeType,
    GraphType,
    NodeType,
    PropTypeRow,
)
from nebulagraph_python.tools.session_conf import get_graph_type_name


def _parse_prop_type_rows(
    res: Iterable[Dict[str, Any]],
) -> Tuple[Dict[str, PropTypeRow], List[str]]:
    return (
        {
            prop["property_name"]: PropTypeRow(
                property_name=prop["property_name"],
                data_type=prop["data_type"],
                nullable=prop["nullable"],
                default=(
                    None
                    if prop["default"] == "" and not prop["nullable"]
                    else prop["default"]
                ),
            )
            for prop in res
        },
        [
            prop["property_name"]
            for prop in res
            if (
                prop.get("primary_key", "") == "Y"
                or prop.get("multi_edge_key", "") == "Y"
            )
        ],
    )


def get_node_type(
    cli: NebulaBaseExecutor,
    node_type_name: str,
    graph_type_name: str,
) -> NodeType:
    """Get the node type of the current graph.

    Args:
        cli: The client to use for the query.
        node_type: The node type to be described.
        graph_type_name: The graph type name to be used.
    """
    # Get node type details
    node_res = cli.execute_py(
        f"DESCRIBE NODE TYPE `{node_type_name}` OF `{graph_type_name}`"
    ).as_primitive_by_row()

    # Parse properties
    node_prop, pr_keys = _parse_prop_type_rows(node_res)

    # Get labels for the node type
    type_info = cli.execute_py(
        f"DESCRIBE GRAPH TYPE `{graph_type_name}`"
    ).as_primitive_by_row()

    labels = []
    for t in type_info:
        if t["entity_type"] == "Node" and t["type_name"] == node_type_name:
            labels = t["labels"]
            break

    return NodeType(
        properties=node_prop,
        pr_or_me_keys=pr_keys,
        node_type=node_type_name,
        labels=labels,
    )


def get_edge_type(
    cli: NebulaBaseExecutor,
    edge_type_name: str,
    graph_type_name: str,
) -> EdgeType:
    """Get the edge type of the current graph.

    Args:
        cli: The client to use for the query.
        edge_type: The edge type to be described.
        graph_type_name: The graph type name to be used.
    """
    # Get edge type details
    edge_res = cli.execute_py(
        f"DESCRIBE EDGE TYPE `{edge_type_name}` OF `{graph_type_name}`"
    ).as_primitive_by_row()

    # Parse properties
    edge_prop, me_keys = _parse_prop_type_rows(edge_res)

    # Get edge pattern and labels for the edge type
    type_info = cli.execute_py(
        f"DESCRIBE GRAPH TYPE `{graph_type_name}`"
    ).as_primitive_by_row()

    edge_pattern = ""
    labels = []
    for t in type_info:
        if t["entity_type"] == "Edge" and t["type_name"] == edge_type_name:
            edge_pattern = t["type_pattern"]
            labels = t["labels"]
            break

    # Parse the edge pattern to get src_node_type and dst_node_type
    src_node_type = ""
    dst_node_type = ""
    if edge_pattern:
        # Edge pattern format is like "(Actor)-[Act]->(Movie)"
        pattern_parts = edge_pattern.strip().split("-")
        if len(pattern_parts) >= 3:
            src_node_type = pattern_parts[0].strip("()")
            dst_node_type = pattern_parts[-1].strip("()>")

    return EdgeType(
        properties=edge_prop,
        pr_or_me_keys=me_keys,
        edge_type=edge_type_name,
        src_node_type=src_node_type,
        dst_node_type=dst_node_type,
        labels=labels,
    )


def get_graph_type(
    cli: NebulaBaseExecutor,
    *,
    graph_type_name: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> GraphType:
    """Get the graph type of the current graph.

    Args:
        cli: The client to use for the query.
        graph_type_name: The graph type to be described.
            If not provided, the type of the `graph_name` will be used.
        graph_name: Only used when `graph_type_name` is not provided.
            If `graph_name` not provided, the home graph of the current session will be used.
            If the home graph is not set, an error will be raised.
    """

    graph_type_name = graph_type_name or get_graph_type_name(cli, graph_name)

    # Get all node and edge types in the graph
    types = cli.execute_py(
        f"DESCRIBE GRAPH TYPE `{graph_type_name}`"
    ).as_primitive_by_row()

    nodes: Dict[str, NodeType] = {}
    edges: Dict[str, EdgeType] = {}

    # Process each type in the graph
    for t in types:
        if t["entity_type"] == "Node":
            # Use get_node_type to get node type details
            node = get_node_type(cli, t["type_name"], graph_type_name)
            nodes[node.node_type] = node
        else:
            # Use get_edge_type to get edge type details
            edge = get_edge_type(cli, t["type_name"], graph_type_name)
            edges[edge.edge_type] = edge

    return GraphType(
        name=graph_type_name,
        nodes=nodes,
        edges=edges,
    )
