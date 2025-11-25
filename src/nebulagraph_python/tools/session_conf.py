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

from typing import Optional

from nebulagraph_python.client import NebulaBaseExecutor


def get_home_graph_name(cli: NebulaBaseExecutor) -> str:
    """Get the home graph name of the current session."""
    rec = cli.execute_py("SHOW CURRENT_SESSION").one()
    try:
        res = rec["graph"].cast(str)  # For version >= 5.1
    except KeyError:
        res = rec["home_graph_name"].cast(str)  # For version == 5.0
    return res


def get_graph_type_name(
    cli: NebulaBaseExecutor, graph_name: Optional[str] = None
) -> str:
    """Get the graph type of the current graph.

    Args:
        cli: The client to use for the query.
        graph_name: The name of the graph to get the type of. Default to the home graph of the current session.
    """
    return (
        cli.execute_py(f"""DESCRIBE GRAPH `{graph_name or get_home_graph_name(cli)}`""")
        .one()["graph_type_name"]
        .cast(str)
    )
