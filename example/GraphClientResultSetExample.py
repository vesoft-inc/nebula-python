#!/usr/bin/env python3
"""Python example that mirrors Java GraphClientExample ResultSet usage.

Run with:
    PYTHONPATH=src python example/GraphClientResultSetExample.py
"""

from __future__ import annotations

import os
from typing import Iterable

from nebulagraph_python import NebulaClient
from nebulagraph_python.py_data_types import Edge, Node
from nebulagraph_python.result_set import ResultSet
from nebulagraph_python.value_wrapper import ValueWrapper


def format_value(value: ValueWrapper) -> str:
    """Format ValueWrapper similarly to Java GraphClientExample.resolve()."""
    if value.is_null():
        return ""
    if value.is_int():
        return str(value.as_int())
    if value.is_long():
        return str(value.as_long())
    if value.is_bool():
        return str(value.as_bool())
    if value.is_double():
        return str(value.as_double())
    if value.is_string():
        return value.as_string()
    if value.is_date():
        return str(value.as_date())
    if value.is_local_time():
        return str(value.as_local_time())
    if value.is_zoned_time():
        return str(value.as_zoned_time())
    if value.is_local_datetime():
        return str(value.as_local_datetime())
    if value.is_zoned_datetime():
        return str(value.as_zoned_datetime())
    if value.is_list():
        return str(value.as_list())
    if value.is_record():
        return str(value.as_record())
    if value.is_node():
        node: Node = value.as_node()
        _ = node.get_id(), node.get_type(), node.get_properties()
        return str(node)
    if value.is_edge():
        edge: Edge = value.as_edge()
        _ = edge.get_src_id(), edge.get_dst_id(), edge.get_properties()
        return str(edge)
    return str(value.cast())


def print_rows(result: ResultSet) -> None:
    if not result.is_succeeded:
        print(f"query failed: {result.status_message}")
        return

    print(f"query result row size: {result.size}")
    print(f"query latency: {result.latency_us} us")
    print(f"result columns: {result.column_names}")

    # while (resultSet.hasNext()) { Record record = resultSet.next(); ... }
    while result.has_next():
        record = result.next()
        formatted = [f"{format_value(v):>15}" for v in record.values()]
        print(" | ".join(formatted))


def run_queries(client: NebulaClient, queries: Iterable[str]) -> None:
    for query in queries:
        print(f"\n=== Execute ===\n{query}")
        result = client.execute(query)
        print_rows(result)


def main() -> None:
    host = os.getenv("NEBULA_HOST", "127.0.0.1:9669")
    user = os.getenv("NEBULA_USER", "root")
    password = os.getenv("NEBULA_PASSWORD", "nebula")

    client = NebulaClient(
        addresses=host,
        user_name=user,
        password=password,
        connect_timeout_ms=1000,
        request_timeout_ms=3000,
    )

    try:
        # Keep queries close to GraphClientExample.java's query() section.
        run_queries(
            client,
            [
                "USE nba MATCH (v:player) RETURN v.id, v.name, v.score, v.gender, v.rate",
                "USE nba MATCH ()-[e:follow]->() RETURN e.followness, e.likeness",
            ],
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()

