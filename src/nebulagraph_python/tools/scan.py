from typing import Iterable, List, Literal, Optional, Sequence, Tuple

from nebulagraph_python.client import NebulaBaseExecutor
from nebulagraph_python.py_data_types import Edge, Node
from nebulagraph_python.result_set import Record
from nebulagraph_python.tools.get_graph_type import (
    get_edge_type,
    get_node_type,
)
from nebulagraph_python.tools.session_conf import (
    get_graph_type_name,
    get_home_graph_name,
)


def _scan_partition(
    client: NebulaBaseExecutor,
    scan_type: Literal["node", "edge"],  # TODO: "edge_only"
    partition_id: int,
    graph_name: str,
    type_name: str,
    properties_list: Sequence[str],
    batch_size: int = 1024,
) -> Iterable[Record]:
    """
    Scan nodes/edges on graph in a given storage partition.

    Args:
        client: The client to use for the scan.
        scan_type: The type of the scan. "node" or "edge".
        partition_id: The partition id to scan.
        graph_name: Filter the name of the graph.
        type_name: Filter the name of the node/edge type.
        properties_list: Return the properties of the node/edge.
        batch_size: The batch size when scanning the nodes/edges.

    Returns:
        An iterable of records.
    """
    if scan_type == "node":
        query = "CALL cursor_node_scan({{graph_name}}, {{type_name}}, {{properties_list}}, {{partition_id}}, {{cursor}}, {{batch_size}}) RETURN *"
    elif scan_type == "edge":
        query = "CALL cursor_edge_scan({{graph_name}}, {{type_name}}, {{properties_list}}, {{partition_id}}, {{cursor}}, {{batch_size}}) RETURN *"
    # TODO: "edge_only"
    # elif scan_type == "edge_only":
    #     raise ValueError(f"Invalid scan type: {scan_type}")
    #     query = "CALL cursor_edge_only_scan({{graph_name}}, {{type_name}}, {{properties_list}}, {{partition_id}}, {{cursor}}, {{batch_size}}) RETURN *"
    else:
        raise ValueError(f"Invalid scan type: {scan_type}")

    cursor = ""
    while True:
        result = client.execute_py(
            query,
            stmt_args={
                "graph_name": graph_name,
                "type_name": type_name,
                "properties_list": properties_list,
                "partition_id": partition_id,
                "batch_size": batch_size,
                "cursor": cursor,
            },
        )
        for row in result:
            yield row
        cursor = result.extra_info.cursor
        if cursor == "":
            break


def scan(
    client: NebulaBaseExecutor,
    scan_type: Literal["node", "edge"],  # TODO: "edge_only"
    graph_name: str | None = None,
    type_name: str | None = None,
    graph_type_name: Optional[str] = None,
    properties_list: Optional[Sequence[str]] = None,
    batch_size: int = 1024,
) -> Iterable[Record]:
    """
    Scan nodes/edges on graph.

    Args:
        client: The client to use for the scan.
        scan_type: The type of the scan. "node" or "edge".
        graph_name: Filter the name of the graph.
        type_name: Filter the name of the node/edge type.
        graph_type_name: Filter the name of the graph type.
        properties_list: Return the properties of the node/edge. Default to None means all properties.
        batch_size: The batch size when scanning the nodes/edges.

    Returns:
        An iterable of records.
    """
    graph_name = graph_name or get_home_graph_name(client)
    if not type_name:
        raise ValueError("type_name is required for scan")
    graph_type_name = graph_type_name or get_graph_type_name(client, graph_name)
    properties_list = (
        properties_list
        if properties_list is not None
        else (
            list(get_node_type(client, type_name, graph_type_name).properties.keys())
            if scan_type == "node"
            else list(
                get_edge_type(client, type_name, graph_type_name).properties.keys()
            )
        )
    )
    partitions: List[int] = client.execute_py(
        "CALL show_partitions() RETURN partition_id"
    ).as_primitive_by_column()["partition_id"]
    # Filter partitions 0
    # TODO: Revisit it which may break between 5.0 and 5.1
    partitions = [p for p in partitions if p != 0]

    for partition_id in partitions:
        for record in _scan_partition(
            client=client,
            scan_type=scan_type,
            partition_id=partition_id,
            graph_name=graph_name,
            type_name=type_name,
            properties_list=properties_list,
            batch_size=batch_size,
        ):
            yield record


def scan_nodes(
    client: NebulaBaseExecutor,
    *,
    graph_name: str | None = None,
    type_name: str,
    graph_type_name: Optional[str] = None,
    properties_list: Optional[Sequence[str]] = None,
    batch_size: int = 1024,
) -> Iterable[Node]:
    graph_name = graph_name or get_home_graph_name(client)
    if not type_name:
        raise ValueError("type_name is required for scan_nodes")
    graph_type_name = graph_type_name or get_graph_type_name(client, graph_name)
    properties_list = (
        properties_list
        if properties_list is not None
        else [
            k
            for k, v in get_node_type(
                client, type_name, graph_type_name
            ).properties.items()
            if not v.data_type.startswith(
                "VECTOR"
            )  # TODO : TO BE FIXED! scan with vector properties will cause decoding error.
        ]
    )
    """
    TODO: scan with vector properties will cause error. 
    CREATE GRAPH TYPE test_scan_type {
        NODE node1 ({n STRING PRIMARY KEY, emb1 VECTOR<3, FLOAT>})
    }
CREATE GRAPH test_scan test_scan_type
SESSION SET GRAPH test_scan
INSERT (@node1 {n: "foo", emb1: VECTOR<3, FLOAT>([1.0, 2.0, 3.0])})

CALL {CALL show_partitions() RETURN collect(partition_id) AS parts GROUP BY ()}
FOR part IN parts 
CALL cursor_node_scan("test_scan", "node1", ["n", "emb1"], part, "", 999) RETURN *
"""
    for record in scan(
        client=client,
        scan_type="node",
        graph_name=graph_name,
        type_name=type_name,
        graph_type_name=graph_type_name,
        properties_list=properties_list,
        batch_size=batch_size,
    ):
        yield record["node"].as_node()


def scan_edges(
    client: NebulaBaseExecutor,
    graph_name: str | None = None,
    type_name: str | None = None,
    graph_type_name: Optional[str] = None,
    properties_list: Optional[Sequence[str]] = None,
    batch_size: int = 1024,
) -> Iterable[Tuple[Node, Edge, Node]]:
    graph_name = graph_name or get_home_graph_name(client)
    if not type_name:
        raise ValueError("type_name is required for scan_edges")
    graph_type_name = graph_type_name or get_graph_type_name(client, graph_name)
    properties_list = (
        properties_list
        if properties_list is not None
        else [
            k
            for k, v in get_edge_type(
                client, type_name, graph_type_name
            ).properties.items()
            if not v.data_type.startswith(
                "VECTOR"
            )  # TODO : TO BE FIXED! scan with vector properties will cause decoding error.
        ]
    )
    for record in scan(
        client=client,
        scan_type="edge",
        graph_name=graph_name,
        type_name=type_name,
        graph_type_name=graph_type_name,
        properties_list=properties_list,
        batch_size=batch_size,
    ):
        yield (
            record["src"].as_node(),
            record["edge"].as_edge(),
            record["dst"].as_node(),
        )
