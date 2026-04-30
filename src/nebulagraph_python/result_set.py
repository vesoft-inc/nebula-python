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

from collections.abc import Iterable, Iterator
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from nebulagraph_python.data import ExtraInfo, PlanInfoNode
from nebulagraph_python.decoder.data_types import ResultGraphSchemas
from nebulagraph_python.decoder.decode import Batch, BytesReader
from nebulagraph_python.decoder.decode_utils import ByteOrder
from nebulagraph_python.decoder.value_parser import (
    DataType,
    ValueParser,
    ValueTypeParser,
)
from nebulagraph_python.error import ErrorCode, InternalError, NebulaGraphRemoteError
from nebulagraph_python.proto.graph_pb2 import ExecuteResponse
from nebulagraph_python.proto.vector_pb2 import VectorResultTable
from nebulagraph_python.value_wrapper import Row, ValueWrapper

if TYPE_CHECKING:
    from pandas import DataFrame
    from rich.console import Console

logger = getLogger(__name__)


class ResultTable:
    """Stateful iterator over batched result data, matching Java's ResultTable.next() semantics.

    Rows are decoded on-demand as next() is called, rather than all at once.
    """

    def __init__(self, table: Any):
        if not isinstance(table, VectorResultTable):
            raise InternalError("table must be a VectorResultTable")

        self.result_table = table

        graph_schemas = ResultGraphSchemas(table.meta.graph_schema)
        time_zone_offset = table.meta.time_zone_offset

        if table.meta.is_little_endian:
            self.byte_order = ByteOrder.LITTLE_ENDIAN
        else:
            self.byte_order = ByteOrder.BIG_ENDIAN

        self.parser = ValueParser(graph_schemas, time_zone_offset, self.byte_order)
        self.num_batches = table.meta.num_batches
        if self.num_batches != len(table.batch):
            raise RuntimeError("the number of batch is not equal to numBatches")

        self.column_names = list(table.meta.row_type.column_names)
        self.column_data_types: List[DataType] = []
        value_type_parser = ValueTypeParser(self.byte_order)
        for col_type in table.meta.row_type.column_types:
            if not col_type.value_type:
                raise ValueError("Invalid column type: empty value_type")
            data_type = value_type_parser.get_data_type(
                BytesReader(col_type.value_type),
            )
            self.column_data_types.append(data_type)

        self.total_num_records = table.meta.num_records

        # Iterator state (mirrors Java's batchIndex / currentBatchRowIndex)
        self._batch_index: int = 0
        self._current_batch_row_index: int = 0
        self._current_batch: Optional[Batch] = (
            Batch(table.batch[0], self.byte_order) if self.num_batches > 0 else None
        )

    def _get_row_by_index(self, batch: Batch, index: int) -> Row:
        """Decode one row from the given batch at the specified index."""
        row = Row()
        decode_value_wrapper = self.parser.decode_value_wrapper
        get_vectors = batch.get_vectors
        column_data_types = self.column_data_types
        for i in range(batch.get_vectors_count()):
            value = decode_value_wrapper(
                get_vectors(i),
                column_data_types[i],
                index,
            )
            row.add_value(value)
        return row

    def next(self) -> Row:
        """Return the next decoded row, advancing internal state.

        Mirrors Java ResultTable.next():
        - Skip empty or exhausted batches automatically.
        - Raises StopIteration when all rows have been consumed.
        """
        if self._current_batch is None:
            raise StopIteration("no more batch data")

        current_batch_row_size = 0
        if self._current_batch.get_vectors_count() != 0:
            current_batch_row_size = self._current_batch.get_batch_row_size()

        # Current batch is empty or exhausted – advance to next batch
        if (
            self._current_batch.get_vectors_count() == 0
            or self._current_batch_row_index >= current_batch_row_size
        ):
            self._batch_index += 1
            if self._batch_index >= self.num_batches:
                raise StopIteration("no more batch data")
            self._current_batch_row_index = 0
            self._current_batch = Batch(
                self.result_table.batch[self._batch_index], self.byte_order
            )

        row = self._get_row_by_index(self._current_batch, self._current_batch_row_index)
        self._current_batch_row_index += 1
        return row

    def __iter__(self):
        return self

    def __next__(self) -> Row:
        return self.next()


class Record:
    column_names: List[str]
    col_values: List[ValueWrapper]
    mapping: Optional[Dict[str, int]]

    def __init__(self, column_names: Optional[List[str]], row: Row):
        self.col_values: List[ValueWrapper] = []
        self.mapping: Optional[Dict[str, int]] = None

        if column_names is None or row is None or not row.values:
            self.column_names: List[str] = []
            self.mapping = {}
            return
        self.column_names = column_names
        self.col_values = row.values

    def _get_mapping(self) -> Dict[str, int]:
        mapping = self.mapping
        if mapping is None:
            mapping = {name: idx for idx, name in enumerate(self.column_names)}
            self.mapping = mapping
        return mapping

    def __iter__(self) -> Iterator[Tuple[str, ValueWrapper]]:
        return self.items()

    def __str__(self) -> str:
        value_strs = [str(v.cast()) for v in self.col_values]
        return f"ColumnName: {self.column_names}, Values: {value_strs}"

    def get(self, key: Union[int, str]) -> ValueWrapper:
        if isinstance(key, str):
            try:
                key = self._get_mapping()[key]
            except KeyError as e:
                raise KeyError(
                    f"Cannot get field because the columnName '{key}' is not exists",
                ) from e
        try:
            return self.col_values[key]
        except IndexError as e:
            raise IndexError(
                f"Cannot get field because the key '{key}' out of range",
            ) from e

    def __getitem__(self, key: Union[int, str]) -> ValueWrapper:
        return self.get(key)

    def values(self) -> List[ValueWrapper]:
        return self.col_values

    def items(self) -> Iterator[Tuple[str, ValueWrapper]]:
        for col in self.column_names:
            yield col, self[col]

    def size(self) -> int:
        return len(self.column_names)

    def contains(self, column_name: str) -> bool:
        return column_name in self.column_names

    def for_each(self, action):
        for value in self.col_values:
            action(value)

    def spliterator(self):
        return self.col_values.__iter__()

    def as_primitive(self) -> Dict[str, Any]:
        return {col: val.cast_primitive() for col, val in self.items()}


class ResultSet:
    column_names: List[str]
    result_table: Optional[ResultTable]
    status_code: str
    status_message: str
    latency_us: int
    plan_desc: PlanInfoNode
    size: int
    extra_info: ExtraInfo

    def __init__(self, response: Any):
        if not isinstance(response, ExecuteResponse):
            raise InternalError(f"got {type(response)} object for server's response")

        if response.HasField("result"):
            self.result_table = ResultTable(response.result)
            self.column_names = self.result_table.column_names
            self.size = self.result_table.total_num_records
        else:
            self.result_table = None
            self.column_names = []
            self.size = 0

        self._is_empty: bool = self.size == 0

        # Cursor position (mirrors Java's AtomicInteger index)
        self._index: int = 0

        if not response.HasField("status"):
            raise InternalError("status is not set in response")
        self.status_code = response.status.code.decode("utf-8")
        self.status_message = response.status.message.decode("utf-8")
        self.is_succeeded = self.status_code == "00000"

        if not response.HasField("summary"):
            raise InternalError("summary is not set in response")
        self.latency_us = response.summary.elapsed_time.total_server_time_us
        self.plan_desc = PlanInfoNode(response.summary.plan_info)
        self.extra_info = ExtraInfo(
            cursor=response.cursor.decode("utf-8"),
            affected_nodes=response.summary.query_stats.num_affected_nodes,
            affected_edges=response.summary.query_stats.num_affected_edges,
            total_server_time_us=response.summary.elapsed_time.total_server_time_us,
            build_time_us=response.summary.elapsed_time.build_time_us,
            optimize_time_us=response.summary.elapsed_time.optimize_time_us,
            serialize_time_us=response.summary.elapsed_time.serialize_time_us,
        )

    def raise_on_error(self) -> "ResultSet":
        if not self.is_succeeded:
            try:
                error_code = ErrorCode(self.status_code)
            except ValueError:
                logger.error("Unknown error code: %s", self.status_code)
                error_code = ErrorCode.UNKNOWN
            raise NebulaGraphRemoteError(
                code=error_code,
                message=self.status_message,
                result=self,
            )
        return self

    def is_empty(self) -> bool:
        """Return True when the result set contains no rows."""
        return self._is_empty

    # ------------------------------------------------------------------
    # Java-style hasNext / next interface
    # ------------------------------------------------------------------

    def has_next(self) -> bool:
        """Return True if there are more rows to consume.

        Mirrors Java ResultSet.hasNext().
        """
        if self._is_empty:
            return False
        return self._index < self.size

    def next(self) -> Record:
        """Decode and return the next row as a Record.

        Mirrors Java ResultSet.next(): each call advances the internal
        cursor and decodes exactly one row on demand.

        Raises
        ------
            StopIteration: when no more rows are available.
        """
        if not self.has_next():
            raise StopIteration("no more row record data")
        if self.result_table is None:
            raise InternalError("result table is not initialized")
        row = self.result_table.next()
        self._index += 1
        return Record(self.column_names, row)

    # ------------------------------------------------------------------
    # Python iterator protocol (delegates to has_next / next)
    # ------------------------------------------------------------------

    def __iter__(self) -> "ResultSet":
        return self

    def __next__(self) -> Record:
        if not self.has_next():
            raise StopIteration
        return self.next()

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def records(self) -> Iterator[Record]:
        """Generator that yields all remaining Records.

        Compatible with existing code that calls ``for record in result.records()``.
        """
        while self.has_next():
            yield self.next()

    def __str__(self) -> str:
        if not self.is_succeeded:
            return self.status_message
        return f"ColumnName: {self.column_names}, RowSize: {self.size}, Latency: {self.latency_us}"

    def as_primitive_by_row(self) -> Iterable[Dict[str, Any]]:
        for record in self.records():
            yield record.as_primitive()

    def as_primitive_by_column(self) -> Dict[str, List[Any]]:
        answer = {col: [] for col in self.column_names}
        for record in self.records():
            for col, val in record.as_primitive().items():
                answer[col].append(val)
        return answer

    def as_pandas_df(self) -> "DataFrame":
        """Convert result set to pandas DataFrame."""
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required to use this method. Please install it using 'pip install pandas'",
            ) from e

        if not self.is_succeeded:
            raise RuntimeError(f"Query failed: {self.status_message}")

        if self.size == 0:
            return pd.DataFrame(columns=self.column_names)

        rows = []
        for record in self.records():
            row = []
            for val in record.values():
                row.append(val.cast_primitive() if val is not None else None)
            rows.append(row)

        return pd.DataFrame(rows, columns=self.column_names)

    def as_ascii_table(
        self,
        console: Optional["Console"] = None,
        style: str = "table",
        width: Optional[int] = None,
        min_width: int = 8,
        max_width: Optional[int] = None,
        padding: int = 1,
        collapse_padding: bool = False,
    ) -> Optional[str]:
        """Print query results in a formatted table or row-by-row format.

        Return the string if console is not provided.
        """
        try:
            from io import StringIO

            from rich import box
            from rich.console import Console
            from rich.table import Table
        except ImportError as e:
            raise ImportError(
                "The 'rich' library is required to use this method. Please install it using 'pip install rich'.",
            ) from e

        have_console: bool = console is not None
        console = console or Console(file=StringIO(), force_terminal=False)

        if not self.is_succeeded:
            console.print(f"[bold red]Error:[/bold red] {self.status_message}")
            if not have_console:
                console.file.seek(0)
                return console.file.read()

        if self.size == 0:
            console.print("[yellow]Empty result set[/yellow]")
            if not have_console:
                console.file.seek(0)
                return console.file.read()

        if style == "rows":
            row_num = 1
            for record in self.records():
                console.print(f"\n[bold blue]Row {row_num}[/bold blue]")
                for col, val in zip(self.column_names, record.values(), strict=True):
                    console.print(f"  [cyan]{col}:[/cyan] {val.cast_primitive()}")
                row_num += 1
        else:
            table = Table(
                box=box.DOUBLE_EDGE,
                show_header=True,
                header_style="bold cyan",
                width=width,
                min_width=min_width,
                padding=padding,
                collapse_padding=collapse_padding,
                highlight=True,
            )

            for header in self.column_names:
                table.add_column(header, max_width=max_width, overflow="fold")

            for record in self.records():
                table.add_row(*[str(v.cast_primitive()) for v in record.values()])

            console.print(table)

        console.print("\n[bold green]Summary[/bold green]")
        console.print(f"├── [green]Rows:[/green] {self.size}")
        console.print(f"└── [blue]Latency:[/blue] {self.latency_us}μs")

        if not have_console:
            console.file.seek(0)
            return console.file.read()

    def one(self) -> Record:
        return self.next()

    def one_or_none(self) -> Optional[Record]:
        try:
            return self.next()
        except StopIteration:
            return None

    def print(
        self,
        style: str = "table",
        width: Optional[int] = None,
        min_width: int = 8,
        max_width: Optional[int] = None,
        padding: int = 1,
        collapse_padding: bool = False,
    ):
        """Print the results directly to console with rich formatting."""
        from rich.console import Console

        console = Console()
        self.as_ascii_table(
            console,
            style,
            width,
            min_width,
            max_width,
            padding,
            collapse_padding,
        )
