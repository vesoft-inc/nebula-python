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

import json
import logging
from abc import abstractmethod
from datetime import date
from typing import Any, Callable, Dict, Mapping, Optional, Type, Union

from minijinja import render_str

from nebulagraph_python.client.logger import debug_flag
from nebulagraph_python.error import InternalError
from nebulagraph_python.py_data_types import NVector, TargetPrimitiveType, TargetType
from nebulagraph_python.result_set import ResultSet

logger = logging.getLogger(__name__)


def unwrap_value(v: Union[TargetType, TargetPrimitiveType]) -> str:
    """Convert python value to GQL string"""

    def from_vector(x: NVector):
        return f"vector<{x.dimension}, float>([{', '.join(str(v) for v in x.values)}])"

    mapping: Dict[
        Type[Union[TargetType, TargetPrimitiveType]], Callable[[Any], str]
    ] = {
        str: lambda x: json.dumps(x, ensure_ascii=False).replace("'", "\\'"),
        int: str,
        float: str,
        bool: lambda x: str(x).upper(),
        list: lambda x: "[%s]" % ", ".join(unwrap_value(i) for i in x),
        dict: lambda x: "{ %s }"
        % ", ".join(f"`{k}`: {unwrap_value(v)}" for k, v in x.items()),
        type(None): lambda x: "NULL",
        NVector: from_vector,
        date: lambda x: f'date("{x.strftime("%Y-%m-%d")}")',
    }
    try:
        return mapping[type(v)](v)
    except KeyError as e:
        raise InternalError(f"Unsupported value type: {type(v)}") from e


def unwrap_props(props: Mapping[str, Any]):
    return ", ".join(f"`{k}`: {unwrap_value(v)}" for k, v in props.items())


class NebulaBaseAsyncExecutor:
    @abstractmethod
    async def execute(
        self,
        statement: str,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        pass

    async def execute_py(
        self,
        stmt: str,
        stmt_args: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        """This is an util method to execute a statement and raise an error if the statement fails,

        it will render the stmt_template with the stmt_args using Jinja2,

        and then execute the statement, raise an error if the statement fails.
        """

        if stmt_args is not None:
            unwrap_args = {k: unwrap_value(v) for k, v in stmt_args.items()}
            stmt = render_str(stmt, **unwrap_args)
        logger.debug("Executing NebulaGraph statement:\n%s", stmt)
        return await self.execute(stmt, timeout=timeout, do_ping=do_ping)


class NebulaBaseExecutor:
    @abstractmethod
    def execute(
        self,
        statement: str,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        pass

    def execute_py(
        self,
        stmt: str,
        stmt_args: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        """This is an util method to execute a statement and raise an error if the statement fails,

        it will render the stmt_template with the stmt_args using Jinja2,

        and then execute the statement, raise an error if the statement fails.
        """

        if stmt_args is not None:
            unwrap_args = {k: unwrap_value(v) for k, v in stmt_args.items()}
            stmt = render_str(stmt, **unwrap_args)
        logger.debug("Executing NebulaGraph statement:\n%s", stmt)
        return self.execute(stmt, timeout=timeout, do_ping=do_ping)

    def print_query_result(
        self,
        query: str,
        style: str = "table",
        width: Optional[int] = None,
        min_width: int = 8,
        max_width: Optional[int] = None,
        padding: int = 1,
        collapse_padding: bool = False,
    ) -> None:
        """Execute a query and print the results in a formatted way using rich

        Args:
        ----
            query: The nGQL query to execute
            style: Output style - either "table" (default) or "rows"
            width: Fixed width for all columns. If None, width will be auto-calculated
            min_width: Minimum width of columns when using table style
            max_width: Maximum width of columns. If None, no maximum is enforced
            padding: Number of spaces around cell contents in table style
            collapse_padding: Reduce padding when cell contents are too wide

        Raises:
        ------
            Exception if execution fails

        """
        try:
            result = self.execute(query)
            result.print(
                style=style,
                width=width,
                min_width=min_width,
                max_width=max_width,
                padding=padding,
                collapse_padding=collapse_padding,
            )
        except Exception as e:
            from rich.console import Console
            from rich.traceback import Traceback

            console = Console()
            console.print(f"[bold red]Error executing query:[/bold red] {e!s}")
            if debug_flag:
                console.print(Traceback.from_exception(type(e), e, e.__traceback__))

    def pq(self, query: str, **kwargs):
        """Print query result using rich"""
        self.print_query_result(query, **kwargs)
