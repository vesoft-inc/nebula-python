import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, List, Optional, Type, TypeVar, Union, overload

from nebulagraph_python.decoder.data_types import ColumnType
from nebulagraph_python.py_data_types import (
    ColumnToPy,
    CompositeDataObject,
    Edge,
    NDuration,
    Node,
    NRecord,
    NVector,
    Path,
    TargetType,
)

logger = logging.getLogger(__name__)


T = TypeVar("T", bound=TargetType)


class ValueWrapper:
    def __init__(self, value: Any, data_type: ColumnType):
        self.value = value
        self.data_type = data_type

    def is_null(self) -> bool:
        """If the value is null

        Returns
        -------
            bool: true if value is null

        """
        return self.value is None

    def is_bool(self) -> bool:
        """Check if the Value is Boolean type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_BOOL

        """
        return self.data_type == ColumnType.BOOL

    def is_long(self) -> bool:
        """Check if the Value is Long type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_UINT64 or COLUMN_TYPE_INT64

        """
        return self.data_type in [ColumnType.UINT64, ColumnType.INT64]

    def is_int(self) -> bool:
        """Check if the Value is Int type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_UINT8 or COLUMN_TYPE_INT8 or COLUMN_TYPE_UINT16
                or COLUMN_TYPE_INT16 or COLUMN_TYPE_UINT32 or COLUMN_TYPE_INT32

        """
        return self.data_type in [
            ColumnType.UINT8,
            ColumnType.INT8,
            ColumnType.UINT16,
            ColumnType.INT16,
            ColumnType.UINT32,
            ColumnType.INT32,
        ]

    def is_float(self) -> bool:
        """Check if the Value is Float type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_FLOAT32

        """
        return self.data_type == ColumnType.FLOAT32

    def is_double(self) -> bool:
        """Check if the Value is Double type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_FLOAT64

        """
        return self.data_type == ColumnType.FLOAT64

    def is_string(self) -> bool:
        """Check if the Value is String type.

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_STRING

        """
        return self.data_type == ColumnType.STRING

    def is_list(self) -> bool:
        """Check if the Value is List type.

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_LIST

        """
        return self.data_type == ColumnType.LIST

    def is_node(self) -> bool:
        """Check if the Value is Node type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_NODE

        """
        return self.data_type == ColumnType.NODE

    def is_edge(self) -> bool:
        """Check if the Value is Edge type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_EDGE

        """
        return self.data_type == ColumnType.EDGE

    def is_local_time(self) -> bool:
        """Check if the Value is Local Time type.

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_LOCALTIME

        """
        return self.data_type == ColumnType.LOCALTIME

    def is_zoned_time(self) -> bool:
        """Check if the Value is Zoned Time type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_ZONEDTIME

        """
        return self.data_type == ColumnType.ZONEDTIME

    def is_local_datetime(self) -> bool:
        """Check if the Value is Local Datetime type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_LOCALDATETIME

        """
        return self.data_type == ColumnType.LOCALDATETIME

    def is_zoned_datetime(self) -> bool:
        """Check if the Value is Zoned Datetime type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_ZONEDDATETIME

        """
        return self.data_type == ColumnType.ZONEDDATETIME

    def is_date(self) -> bool:
        """Check if the Value is Date type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_DATE

        """
        return self.data_type == ColumnType.DATE

    def is_record(self) -> bool:
        """Check if the Value is Record type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_RECORD

        """
        return self.data_type == ColumnType.RECORD

    def is_duration(self) -> bool:
        """Check if the Value is Duration type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_DURATION

        """
        return self.data_type == ColumnType.DURATION

    def is_path(self) -> bool:
        """Check if the Value is Path type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_PATH

        """
        return self.data_type == ColumnType.PATH

    def is_decimal(self) -> bool:
        """Check if the Value is Decimal type

        Returns
        -------
            bool: true if Value's type is COLUMN_TYPE_DECIMAL

        """
        return self.data_type == ColumnType.DECIMAL

    def as_bool(self):
        return self.cast(bool)

    def as_int(self):
        return self.cast(int)

    def as_long(self):
        return self.cast(int)

    def as_string(self):
        return self.cast(str)

    def as_float(self):
        return self.cast(float)

    def as_double(self):
        return self.cast(float)

    def as_list(self):
        return self.cast(list)

    def as_node(self):
        return self.cast(Node)

    def as_edge(self):
        return self.cast(Edge)

    def as_local_time(self):
        return self.cast(time)

    def as_zoned_time(self):
        return self.cast(time)

    def as_date(self):
        return self.cast(date)

    def as_local_datetime(self):
        return self.cast(datetime)

    def as_zoned_datetime(self):
        return self.cast(datetime)

    def as_duration(self):
        return self.cast(NDuration)

    def as_record(self):
        return self.cast(NRecord)

    def as_path(self):
        return self.cast(Path)

    def as_decimal(self):
        return self.cast(Decimal)

    def as_embedding_vector(self):
        return self.cast(NVector)

    @overload
    def cast(self) -> TargetType:
        """Get self.value"""
        ...

    @overload
    def cast(self, target_type: Type[T]) -> T:
        """Get self.value, with target_type check"""
        ...

    def cast(
        self,
        target_type: Optional[Type[T]] = None,
    ) -> Union[T, TargetType]:
        if self.value is None and target_type is None:
            return None
        target_type = ColumnToPy[self.data_type] if target_type is None else target_type  # type: ignore

        if target_type is not None:
            if isinstance(self.value, target_type):
                return self.value
            else:
                raise TypeError(
                    f"Cannot cast {type(self.value)} to {target_type}. ColumnType and value:",
                    self.data_type,
                    self.value,
                )

    def cast_primitive(self) -> Any:
        """Convert the wrapped value to primitive Python types recursively.

        For basic types, uses cast()
        For composite types (Path, Node, Edge), calls cast_primitive()
        For containers (Map, List, Record), recursively calls cast_primitive() on elements
        """
        outer = self.cast()

        # Handle composite types
        if isinstance(outer, CompositeDataObject):
            return outer.cast_primitive()

        # Handle list recursively
        if isinstance(outer, list):
            ans = []
            for i, v in enumerate(outer):
                if isinstance(v, ValueWrapper):
                    ans.append(v.cast_primitive())
                else:
                    raise TypeError(
                        f"Cannot cast list, where list[{i}] have type {type(v)}, not ValueWrapper. list[{i}]: ",
                        v,
                    )
            return ans

        return outer

    def __eq__(self, other):
        if not isinstance(other, ValueWrapper):
            return False
        return self.value == other.value and self.data_type == other.data_type


class Row:
    def __init__(self, values: Optional[List[ValueWrapper]] = None):
        self.values = values if values is not None else []

    def add_value(self, value: ValueWrapper):
        """Append one value into row.

        Args:
        ----
            value (ValueWrapper): one value of the row

        """
        self.values.append(value)

    def get_values(self) -> list[ValueWrapper]:
        """Get the values of this row.

        Returns
        -------
            List[ValueWrapper]: list of values in this row

        """
        return self.values
