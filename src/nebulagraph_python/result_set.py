from collections.abc import Iterable, Iterator
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from nebulagraph_python.data import ExtraInfo, PlanInfoNode
from nebulagraph_python.decoder.data_types import ByteOrder, ResultGraphSchemas
from nebulagraph_python.decoder.decode import Batch, BytesReader
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
    result_table: VectorResultTable
    byte_order: ByteOrder
    parser: ValueParser
    num_batches: int
    column_names: List[str]
    column_data_types: List[DataType]
    total_num_records: int

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
        self.column_data_types = []
        value_type_parser = ValueTypeParser(self.byte_order)
        for col_type in table.meta.row_type.column_types:
            if not col_type.value_type:
                raise ValueError("Invalid column type: empty value_type")
            data_type = value_type_parser.get_data_type(
                BytesReader(col_type.value_type),
            )
            self.column_data_types.append(data_type)

        self.total_num_records = table.meta.num_records

    def _get_row_by_index(self, batch: Batch, index: int) -> Row:
        """Parse row record from batch

        Args:
        ----
            batch: the batch to parse from
            index: the position of each vector in current batch

        Returns:
        -------
            Row: row record

        """

        row = Row()
        for i in range(batch.get_vectors_count()):
            value = self.parser.decode_value_wrapper(
                batch.get_vectors(i),
                self.column_data_types[i],
                index,
            )
            row.add_value(value)
        return row

    def __iter__(self):
        return self.rows()

    def rows(self):
        """Generator that yields rows from the result table.

        Returns
        -------
            Iterator[Row]: iterator of row records

        Raises
        ------
            InternalError: if no result table data
        """
        for batch_index in range(self.num_batches):
            current_batch = Batch(self.result_table.batch[batch_index], self.byte_order)

            # each VectorMetaData has the same numRecords value,
            # just use the first one to get the numRecord for this batch
            current_batch_row_size = 0
            if current_batch.get_vectors_count() != 0:
                current_batch_row_size = current_batch.get_batch_row_size()

            # Skip empty batches
            if current_batch.get_vectors_count() == 0:
                continue

            # Process rows in current batch
            for row_index in range(current_batch_row_size):
                row = self._get_row_by_index(current_batch, row_index)
                yield row


class Record:
    column_names: List[str]
    col_values: List[ValueWrapper]
    mapping: Dict[str, int]

    def __init__(self, column_names: Optional[List[str]], row: Row):
        self.col_values: List[ValueWrapper] = []
        self.mapping = {}

        if column_names is None or row is None or not row.values:
            self.column_names: List[str] = []
            return
        self.column_names = column_names

        for idx, value in enumerate(row.values):
            self.col_values.append(value)
            self.mapping[column_names[idx]] = len(self.col_values) - 1

    def __iter__(self) -> Iterator[Tuple[str, ValueWrapper]]:
        return self.items()

    def __str__(self) -> str:
        value_strs = [str(v.cast()) for v in self.col_values]
        return f"ColumnName: {self.column_names}, Values: {value_strs}"

    def get(self, key: Union[int, str]) -> ValueWrapper:
        if isinstance(key, str):
            try:
                key = self.mapping[key]
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

    def __iter__(self) -> Iterator[Record]:
        return self.records()

    def records(self):
        if self.result_table is None:
            raise InternalError("result table is not initialized")
        for row in self.result_table:
            yield Record(self.column_names, row)

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
        """Convert result set to pandas DataFrame.

        Returns
        -------
            pandas.DataFrame: DataFrame containing the query results

        Raises
        ------
            ImportError: If pandas is not installed

        """
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

        # Reset index to start
        self._index = 0

        # Build list of rows
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
        """Print query results in a formatted table or row-by-row format. Return the string if console is not provided.

        Args:
        ----
            console: rich.console.Console instance to use for printing. If None, a new instance will be created.
            style: Output style - either "table" (default) or "rows"
            width: Fixed width for all columns. If None, width will be auto-calculated
            min_width: Minimum width of columns when using table style
            max_width: Maximum width of columns. If None, no maximum is enforced
            padding: Number of spaces around cell contents in table style
            collapse_padding: Reduce padding when cell contents are too wide

        Examples:
        --------
            # Print as table (default)
            result.as_ascii_table()

            # Print as rows
            result.as_ascii_table(style="rows")

            # Customize table formatting
            result.as_ascii_table(width=20, max_width=30, padding=2)

        Returns:
        -------
            Optional[str]: Formatted representation of the results, or error message if query failed.
                If console is provided, the output will be printed to the console and None will be returned.

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

        # Reset index to start
        if style == "rows":
            # Row-by-row format
            row_num = 1
            for record in self.records():
                console.print(f"\n[bold blue]Row {row_num}[/bold blue]")
                for col, val in zip(self.column_names, record.values()):
                    console.print(f"  [cyan]{col}:[/cyan] {val.cast_primitive()}")
                row_num += 1

        else:
            # Table format
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

        # Print summary
        console.print("\n[bold green]Summary[/bold green]")
        console.print(f"├── [green]Rows:[/green] {self.size}")
        console.print(f"└── [blue]Latency:[/blue] {self.latency_us}μs")

        if not have_console:
            console.file.seek(0)
            return console.file.read()

    def one(self) -> Record:
        return next(self.records())

    def one_or_none(self) -> Optional[Record]:
        try:
            return next(self.records())
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
        """Print the results directly to console with rich formatting.

        Args are the same as as_ascii_table().
        """
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
