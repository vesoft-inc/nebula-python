import datetime
from abc import abstractmethod
from typing import Dict, Any, Optional
from nebula3.data.ResultSet import ResultSet
from nebula3.common.ttypes import ErrorCode, Value, NList, Date, Time, DateTime


class BaseExecutor:
    @abstractmethod
    def execute_parameter(
        self, stmt: str, params: Optional[Dict[str, Any]]
    ) -> ResultSet:
        pass

    @abstractmethod
    def execute_json_with_parameter(
        self, stmt: str, params: Optional[Dict[str, Any]]
    ) -> bytes:
        pass

    def execute(self, stmt: str) -> ResultSet:
        return self.execute_parameter(stmt, None)

    def execute_json(self, stmt: str) -> bytes:
        return self.execute_json_with_parameter(stmt, None)

    def execute_py_params(
        self, stmt: str, params: Optional[Dict[str, Any]]
    ) -> ResultSet:
        """**Recommended** Execute a statement with parameters in Python type instead of thrift type."""
        return self.execute_parameter(stmt, _build_byte_param(params))


def _build_byte_param(params: dict) -> dict:
    byte_params = {}
    for k, v in params.items():
        if isinstance(v, Value):
            byte_params[k] = v
        elif str(type(v)).startswith("nebula3.common.ttypes"):
            byte_params[k] = v
        else:
            byte_params[k] = _cast_value(v)
    return byte_params


def _cast_value(value: Any) -> Value:
    """
    Cast the value to nebula Value type
    ref: https://github.com/vesoft-inc/nebula/blob/master/src/common/datatypes/Value.cpp
    :param value: the value to be casted
    :return: the casted value
    """
    casted_value = Value()
    if isinstance(value, bool):
        casted_value.set_bVal(value)
    elif isinstance(value, int):
        casted_value.set_iVal(value)
    elif isinstance(value, str):
        casted_value.set_sVal(value)
    elif isinstance(value, float):
        casted_value.set_fVal(value)
    elif isinstance(value, datetime.date):
        date_value = Date(year=value.year, month=value.month, day=value.day)
        casted_value.set_dVal(date_value)
    elif isinstance(value, datetime.time):
        time_value = Time(
            hour=value.hour,
            minute=value.minute,
            sec=value.second,
            microsec=value.microsecond,
        )
        casted_value.set_tVal(time_value)
    elif isinstance(value, datetime.datetime):
        datetime_value = DateTime(
            year=value.year,
            month=value.month,
            day=value.day,
            hour=value.hour,
            minute=value.minute,
            sec=value.second,
            microsec=value.microsecond,
        )
        casted_value.set_dtVal(datetime_value)
    # TODO: add support for GeoSpatial
    elif isinstance(value, list):
        byte_list = []
        for item in value:
            byte_list.append(_cast_value(item))
        casted_value.set_lVal(NList(values=byte_list))
    elif isinstance(value, dict):
        # TODO: add support for NMap
        raise TypeError("Unsupported type: dict")
    else:
        raise TypeError(f"Unsupported type: {type(value)}")
    return casted_value
