#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import pytz
from datetime import datetime, timezone, timedelta
from nebula3.Exception import (
    InvalidValueTypeException,
    InvalidKeyException,
    OutOfRangeException,
)

from nebula3.common.ttypes import (
    Geography,
    Value,
    Vertex,
    Edge,
    NullType,
    DateTime,
    Time,
    Duration,
)


def date_time_convert_with_timezone(date_time: DateTime, timezone_offset: int):
    """the function to convert utc date_time to local date_time

    :param date_time: the utc date_time
    :param timezone_offset: the timezone offset
    :return: the date_time with timezone
    """
    native_date_time = datetime(
        date_time.year,
        date_time.month,
        date_time.day,
        date_time.hour,
        date_time.minute,
        date_time.sec,
        date_time.microsec,
        pytz.timezone("utc"),
    )
    local_date_time = native_date_time.astimezone(
        timezone(timedelta(seconds=timezone_offset))
    )
    new_date_time = DateTime()
    new_date_time.year = local_date_time.year
    new_date_time.month = local_date_time.month
    new_date_time.day = local_date_time.day
    new_date_time.hour = local_date_time.hour
    new_date_time.minute = local_date_time.minute
    new_date_time.sec = local_date_time.second
    new_date_time.microsec = local_date_time.microsecond
    return new_date_time


def time_convert_with_timezone(n_time: Time, timezone_offset: int):
    """the function to convert utc date_time to local date_time

    :param n_time: the utc time
    :param timezone_offset: the timezone offset
    :return: the time with the timezone
    """
    native_date_time = datetime(
        1,
        1,
        1,
        n_time.hour,
        n_time.minute,
        n_time.sec,
        n_time.microsec,
        pytz.timezone("utc"),
    )
    local_date_time = native_date_time.astimezone(
        timezone(timedelta(seconds=timezone_offset))
    )
    local_time = Time()
    local_time.hour = local_date_time.hour
    local_time.minute = local_date_time.minute
    local_time.sec = local_date_time.second
    local_time.microsec = local_date_time.microsecond
    return local_time


class BaseObject(object):
    def __init__(self):
        self._decode_type = 'utf-8'
        self._timezone_offset = 0

    def set_decode_type(self, decode_type):
        self._decode_type = decode_type
        return self

    def set_timezone_offset(self, timezone_offset):
        self._timezone_offset = timezone_offset
        return self

    def get_decode_type(self):
        return self._decode_type

    def get_timezone_offset(self):
        return self._timezone_offset


class Record(object):
    def __init__(self, values, names, decode_type='utf-8', timezone_offset: int = 0):
        assert len(names) == len(
            values
        ), 'len(names): {} != len(values): {}, names: {}, values: {}'.format(
            len(names), len(values), str(names), str(values)
        )
        self._record = list()
        self._names = names

        for val in values:
            self._record.append(
                ValueWrapper(
                    val, decode_type=decode_type, timezone_offset=timezone_offset
                )
            )

    def __iter__(self):
        return iter(self._record)

    def size(self):
        """the size of record

        :return: record size
        """
        return len(self._names)

    def get_value(self, index):
        """get value by specified index

        :param index: the index of column
        :return: ValueWrapper
        """
        if index >= len(self._names):
            raise OutOfRangeException()
        return self._record[index]

    def get_value_by_key(self, key):
        """get value by key

        :return: Value
        """
        try:
            return self._record[self._names.index(key)]
        except Exception:
            raise InvalidKeyException(key)

    def keys(self):
        """get column names of record

        :return: the column names
        """
        return self._names

    def values(self):
        """get all values

        :return: values
        """
        return self._record

    def __repr__(self):
        return "{}".format('\n'.join([str(val_wrap) for val_wrap in self._record]))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__


class DataSetWrapper(object):
    def __init__(self, data_set, decode_type='utf-8', timezone_offset: int = 0):
        assert data_set is not None
        self._decode_type = decode_type
        self._timezone_offset = timezone_offset
        self._data_set = data_set
        self._column_names = []
        self._key_indexes = {}
        self._pos = -1
        for index, name in enumerate(self._data_set.column_names):
            d_name = name.decode(self._decode_type)
            self._column_names.append(d_name)
            self._key_indexes[d_name] = index

    def get_row_size(self):
        return len(self._data_set.rows)

    def get_col_names(self):
        return self._column_names

    def get_rows(self):
        return self._data_set.rows

    def get_row_types(self):
        """Get row types

        :param empty
        :return: list<int>
          ttypes.Value.__EMPTY__ = 0
          ttypes.Value.NVAL = 1
          ttypes.Value.BVAL = 2
          ttypes.Value.IVAL = 3
          ttypes.Value.FVAL = 4
          ttypes.Value.SVAL = 5
          ttypes.Value.DVAL = 6
          ttypes.Value.TVAL = 7
          ttypes.Value.DTVAL = 8
          ttypes.Value.VVAL = 9
          ttypes.Value.EVAL = 10
          ttypes.Value.PVAL = 11
          ttypes.Value.LVAL = 12
          ttypes.Value.MVAL = 13
          ttypes.Value.UVAL = 14
          ttypes.Value.GVAL = 15
          ttypes.Value.GGVAL = 16
          ttypes.Value.DUVAL = 17
        """
        if len(self._data_set.rows) == 0:
            return []
        return [(value.getType()) for value in self._data_set.rows[0].values]

    def row_values(self, row_index):
        """get row values

        :param row_index: the Record index
        :return: list<ValueWrapper>
        """
        if row_index >= len(self._data_set.rows):
            raise OutOfRangeException()
        return [
            ValueWrapper(
                value=value,
                decode_type=self._decode_type,
                timezone_offset=self._timezone_offset,
            )
            for value in self._data_set.rows[row_index].values
        ]

    def column_values(self, key):
        """get column values

        :param key: the col name
        :return: list<ValueWrapper>
        """
        if key not in self._column_names:
            raise InvalidKeyException(key)

        return [
            ValueWrapper(
                value=row.values[self._key_indexes[key]],
                decode_type=self._decode_type,
                timezone_offset=self._timezone_offset,
            )
            for row in self._data_set.rows
        ]

    def __iter__(self):
        self._pos = -1
        return self

    def __next__(self):
        """The record iterator

        :return: record
        """
        if len(self._data_set.rows) == 0 or self._pos >= len(self._data_set.rows) - 1:
            raise StopIteration
        self._pos = self._pos + 1
        return Record(
            values=self._data_set.rows[self._pos].values,
            names=self._column_names,
            decode_type=self._decode_type,
            timezone_offset=self._timezone_offset,
        )

    def __repr__(self):
        data_str = []
        for i in range(self.get_row_size()):
            data_str.append(str(self.row_values(i)))
        value_str = ','.join(data_str)
        return 'keys: {}, values: {}'.format(self._column_names, value_str)


class Null(object):
    __NULL__ = NullType.__NULL__
    NaN = NullType.NaN
    BAD_DATA = NullType.BAD_DATA
    BAD_TYPE = NullType.BAD_TYPE
    ERR_OVERFLOW = NullType.ERR_OVERFLOW
    UNKNOWN_PROP = NullType.UNKNOWN_PROP
    DIV_BY_ZERO = NullType.DIV_BY_ZERO
    OUT_OF_RANGE = NullType.OUT_OF_RANGE

    def __init__(self, type):
        self._type = type

    def __repr__(self):
        return NullType._VALUES_TO_NAMES[self._type]

    def __eq__(self, other):
        return self._type == other._type


class ValueWrapper(object):
    def __init__(self, value, decode_type='utf-8', timezone_offset: int = 0):
        self._value = value
        self._decode_type = decode_type
        self._timezone_offset = timezone_offset

    def get_value(self):
        """get raw data

        :return: Value
        """
        return self._value

    def is_null(self):
        """check if the value is Null type

        :return: true or false
        """
        return self._value.getType() == Value.NVAL

    def is_empty(self):
        """check if the value is Empty type

        :return: true or false
        """
        return self._value.getType() == Value.__EMPTY__

    def is_bool(self):
        """check if the value is Bool type

        :return: true or false
        """
        return self._value.getType() == Value.BVAL

    def is_int(self):
        """check if the value is Int type

        :return: true or false
        """
        return self._value.getType() == Value.IVAL

    def is_double(self):
        """check if the value is Double type

        :return: true or false
        """
        return self._value.getType() == Value.FVAL

    def is_string(self):
        """check if the value is String type

        :return: true or false
        """
        return self._value.getType() == Value.SVAL

    def is_list(self):
        """check if the value is List type

        :return: true or false
        """
        return self._value.getType() == Value.LVAL

    def is_set(self):
        """check if the value is Set type

        :return: true or false
        """
        return self._value.getType() == Value.UVAL

    def is_map(self):
        """check if the value is Map type

        :return: true or false
        """
        return self._value.getType() == Value.MVAL

    def is_time(self):
        """check if the value is Time type

        :return: true or false
        """
        return self._value.getType() == Value.TVAL

    def is_date(self):
        """check if the value is Date type

        :return: true or false
        """
        return self._value.getType() == Value.DVAL

    def is_datetime(self):
        """check if the value is Datetime type

        :return: true or false
        """
        return self._value.getType() == Value.DTVAL

    def is_vertex(self):
        """check if the value is Vertex type

        :return: true or false
        """
        return self._value.getType() == Value.VVAL

    def is_edge(self):
        """check if the value is Edge type

        :return: true or false
        """
        return self._value.getType() == Value.EVAL

    def is_path(self):
        """check if the value is Path type

        :return: true or false
        """
        return self._value.getType() == Value.PVAL

    def is_geography(self):
        """check if the value is Geography type

        :return: true or false
        """
        return self._value.getType() == Value.GGVAL

    def is_duration(self):
        """check if the value is Duration type

        :return: true or false
        """
        return self._value.getType() == Value.DUVAL

    def as_null(self):
        """converts the original data type to Null type

        :return: Null value
        """
        if self._value.getType() == Value.NVAL:
            return Null(self._value.get_nVal())
        raise InvalidValueTypeException(
            "expect NULL type, but is " + self._get_type_name()
        )

    def as_bool(self):
        """converts the original data type to Bool type

        :return: Bool value
        """
        if self._value.getType() == Value.BVAL:
            return self._value.get_bVal()
        raise InvalidValueTypeException(
            "expect bool type, but is " + self._get_type_name()
        )

    def as_int(self):
        """converts the original data type to Int type

        :return: Int value
        """
        if self._value.getType() == Value.IVAL:
            return self._value.get_iVal()
        raise InvalidValueTypeException(
            "expect bool type, but is " + self._get_type_name()
        )

    def as_double(self):
        """converts the original data type to Double type

        :return: Double value
        """
        if self._value.getType() == Value.FVAL:
            return self._value.get_fVal()
        raise InvalidValueTypeException(
            "expect int type, but is " + self._get_type_name()
        )

    def as_string(self):
        """converts the original data type to String type

        :return: String value
        """
        if self._value.getType() == Value.SVAL:
            return self._value.get_sVal().decode(self._decode_type)
        raise InvalidValueTypeException(
            "expect string type, but is " + self._get_type_name()
        )

    def as_time(self):
        """converts the original data type to Time type

        :return: Time value
        """
        if self._value.getType() == Value.TVAL:
            return TimeWrapper(self._value.get_tVal()).set_timezone_offset(
                self._timezone_offset
            )
        raise InvalidValueTypeException(
            "expect time type, but is " + self._get_type_name()
        )

    def as_date(self):
        """converts the original data type to Date type

        :return: Date value
        """
        if self._value.getType() == Value.DVAL:
            return DateWrapper(self._value.get_dVal())
        raise InvalidValueTypeException(
            "expect date type, but is " + self._get_type_name()
        )

    def as_datetime(self):
        """converts the original data type to Datetime type

        :return: Datetime value
        """
        if self._value.getType() == Value.DTVAL:
            return DateTimeWrapper(self._value.get_dtVal()).set_timezone_offset(
                self._timezone_offset
            )
        raise InvalidValueTypeException(
            "expect datetime type, but is " + self._get_type_name()
        )

    def as_list(self):
        """converts the original data type to list of ValueWrapper

        :return: list<ValueWrapper>
        """
        if self._value.getType() == Value.LVAL:
            result = []
            for val in self._value.get_lVal().values:
                result.append(
                    ValueWrapper(
                        val,
                        decode_type=self._decode_type,
                        timezone_offset=self._timezone_offset,
                    )
                )
            return result
        raise InvalidValueTypeException(
            "expect list type, but is " + self._get_type_name()
        )

    def as_set(self):
        """converts the original data type to set of ValueWrapper

        :return: set<ValueWrapper>
        """
        if self._value.getType() == Value.UVAL:
            result = set()
            for val in self._value.get_uVal().values:
                result.add(
                    ValueWrapper(
                        val,
                        decode_type=self._decode_type,
                        timezone_offset=self._timezone_offset,
                    )
                )
            return result
        raise InvalidValueTypeException(
            "expect set type, but is " + self._get_type_name()
        )

    def as_map(self):
        """converts the original data type to map type

        :return: map<String, ValueWrapper>
        """
        if self._value.getType() == Value.MVAL:
            result = {}
            kvs = self._value.get_mVal().kvs
            for key in kvs.keys():
                result[key.decode(self._decode_type)] = ValueWrapper(
                    kvs[key],
                    decode_type=self._decode_type,
                    timezone_offset=self._timezone_offset,
                )
            return result
        raise InvalidValueTypeException(
            "expect map type, but is " + self._get_type_name()
        )

    def as_node(self):
        """converts the original data type to Node type

        :return: Node type
        """
        if self._value.getType() == Value.VVAL:
            return (
                Node(self._value.get_vVal())
                .set_decode_type(self._decode_type)
                .set_timezone_offset(self._timezone_offset)
            )
        raise InvalidValueTypeException(
            "expect vertex type, but is " + self._get_type_name()
        )

    def as_relationship(self):
        """converts the original data type to Relationship type

        :return: Relationship type
        """
        if self._value.getType() == Value.EVAL:
            return (
                Relationship(self._value.get_eVal())
                .set_decode_type(self._decode_type)
                .set_timezone_offset(self._timezone_offset)
            )
        raise InvalidValueTypeException(
            "expect edge type, but is " + self._get_type_name()
        )

    def as_path(self):
        """converts the original data type to PathWrapper type

        :return: PathWrapper type
        """
        if self._value.getType() == Value.PVAL:
            return (
                PathWrapper(self._value.get_pVal())
                .set_decode_type(self._decode_type)
                .set_timezone_offset(self._timezone_offset)
            )
        raise InvalidValueTypeException(
            "expect path type, but is " + self._get_type_name()
        )

    def as_geography(self):
        """converts the original data type to GeographyWrapper type

        :return: GeographyWrapper type
        """
        if self._value.getType() == Value.GGVAL:
            return (
                GeographyWrapper(self._value.get_ggVal())
                .set_decode_type(self._decode_type)
                .set_timezone_offset(self._timezone_offset)
            )
        raise InvalidValueTypeException(
            "expect geography type, but is " + self._get_type_name()
        )

    def as_duration(self):
        """converts the original data type to Duration type

        :return: DurationWrapper type
        """
        if self._value.getType() == Value.DUVAL:
            return DurationWrapper(self._value.get_duVal())
        raise InvalidValueTypeException(
            "expect duration type, but is " + self._get_type_name()
        )

    def _get_type_name(self):
        if self.is_empty():
            return "empty"
        if self.is_null():
            return "null"
        if self.is_bool():
            return "bool"
        if self.is_int():
            return "int"
        if self.is_double():
            return "double"
        if self.is_string():
            return "string"
        if self.is_list():
            return "list"
        if self.is_set():
            return "set"
        if self.is_map():
            return "map"
        if self.is_time():
            return "time"
        if self.is_date():
            return "date"
        if self.is_datetime():
            return "datetime"
        if self.is_vertex():
            return "vertex"
        if self.is_edge():
            return "edge"
        if self.is_path():
            return "path"
        if self.is_geography():
            return "geography"
        if self.is_duration():
            return "duration"
        return "unknown"

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        if self.get_value().getType() != o.get_value().getType():
            return False
        if self.is_empty():
            return o.is_empty()
        elif self.is_null():
            return self.as_null() == o.as_null()
        elif self.is_bool():
            return self.as_bool() == o.as_bool()
        elif self.is_int():
            return self.as_int() == o.as_int()
        elif self.is_double():
            return self.as_double() == o.as_double()
        elif self.is_string():
            return self.as_string() == o.as_string()
        elif self.is_list():
            return self.as_list() == o.as_list()
        elif self.is_set():
            return self.as_set() == o.as_set()
        elif self.is_map():
            return self.as_map() == o.as_map()
        elif self.is_vertex():
            return self.as_node() == o.as_node()
        elif self.is_edge():
            return self.as_relationship() == o.as_relationship()
        elif self.is_path():
            return self.as_path() == o.as_path()
        elif self.is_time():
            return self.as_time() == o.as_time()
        elif self.is_date():
            return self.as_date() == o.as_date()
        elif self.is_datetime():
            return self.as_datetime() == o.as_datetime()
        elif self.is_geography():
            return self.as_geography() == o.as_geography()
        elif self.is_duration():
            return self.as_duration() == o.as_duration()
        else:
            raise RuntimeError(
                'Unsupported type:{} to compare'.format(self._get_type_name())
            )
        return False

    def __repr__(self):
        if self.is_empty():
            return '__EMPTY__'
        elif self.is_null():
            return str(self.as_null())
        elif self.is_bool():
            return 'True' if self.as_bool() else 'False'
        elif self.is_int():
            return str(self.as_int())
        elif self.is_double():
            return str(self.as_double())
        elif self.is_string():
            return '\"{}\"'.format(self.as_string())
        elif self.is_list():
            return str(self.as_list())
        elif self.is_set():
            return str(self.as_set())
        elif self.is_map():
            return str(self.as_map())
        elif self.is_vertex():
            return str(self.as_node())
        elif self.is_edge():
            return str(self.as_relationship())
        elif self.is_path():
            return str(self.as_path())
        elif self.is_time():
            return str(self.as_time())
        elif self.is_date():
            return str(self.as_date())
        elif self.is_datetime():
            return str(self.as_datetime())
        elif self.is_geography():
            return str(self.as_geography())
        elif self.is_duration():
            return str(self.as_duration())
        else:
            raise RuntimeError(
                'Unsupported type:{} to compare'.format(self._get_type_name())
            )
        return False

    def __hash__(self):
        return self._value.__hash__()


class TimeWrapper(BaseObject):
    def __init__(self, time):
        super(TimeWrapper, self).__init__()
        self._time = time

    def get_hour(self):
        """get utc hour

        :return: hour
        """
        return self._time.hour

    def get_minute(self):
        """get utc minute

        :return: minute
        """
        return self._time.minute

    def get_sec(self):
        """get utc second

        :return: second
        """
        return self._time.sec

    def get_microsec(self):
        """get utc microseconds

        :return: microseconds
        """
        return self._time.microsec

    def get_time(self):
        """get utc time

        :return: Time value
        """
        return self._time

    def get_local_time(self):
        """get time with the timezone from graph service

        :return: Time value with timezone offset
        """
        return time_convert_with_timezone(self._time, self.get_timezone_offset())

    def get_local_time_by_timezone_offset(self, timezone_offset):
        """get local time with the specified timezone by user

        :return: Time value with timezone offset
        """
        return time_convert_with_timezone(self._time, timezone_offset)

    def get_local_time_str(self):
        """convert local time string format

        :return: return local time string format
        """
        local_time = time_convert_with_timezone(self._time, self.get_timezone_offset())
        return "%02d:%02d:%02d.%06d" % (
            local_time.hour,
            local_time.minute,
            local_time.sec,
            local_time.microsec,
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._time.hour == other.get_hour()
            and self._time.minute == other.get_minute()
            and self._time.sec == other.get_sec()
            and self._time.microsec == self.get_microsec()
        )

    def __repr__(self):
        return "utc time: %02d:%02d:%02d.%06d, timezone_offset: %d" % (
            self._time.hour,
            self._time.minute,
            self._time.sec,
            self._time.microsec,
            self.get_timezone_offset(),
        )


class DateWrapper(object):
    def __init__(self, date):
        self._date = date

    def get_year(self):
        """get year

        :return: year
        """
        return self._date.year

    def get_month(self):
        """get month

        :return: month
        """
        return self._date.month

    def get_day(self):
        """get day

        :return: day
        """
        return self._date.day

    def get_date(self):
        """get original date

        :return: Date
        """
        return self._date

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._date.year == other.get_year()
            and self._date.month == other.get_month()
            and self._date.day == other.get_day()
        )

    def __repr__(self):
        return "%d-%02d-%02d" % (self._date.year, self._date.month, self._date.day)


class DateTimeWrapper(BaseObject):
    def __init__(self, date_time):
        super(DateTimeWrapper, self).__init__()
        self._date_time = date_time

    def get_year(self):
        """get utc year

        :return: year
        """
        return self._date_time.year

    def get_month(self):
        """get utc month

        :return: month
        """
        return self._date_time.month

    def get_day(self):
        """get utc day

        :return: day
        """
        return self._date_time.day

    def get_hour(self):
        """get utc hour

        :return: hour
        """
        return self._date_time.hour

    def get_minute(self):
        """get utc minute

        :return: minute
        """
        return self._date_time.minute

    def get_sec(self):
        """get utc seconds

        :return: seconds
        """
        return self._date_time.sec

    def get_microsec(self):
        """get utc microseconds

        :return: microseconds
        """
        return self._date_time.microsec

    def get_datetime(self):
        """get utc datetime

        :return: datetime
        """
        return self._date_time

    def get_local_datetime(self):
        """get datetime with the timezone from graph service

        :return: Datetime value with timezone offset
        """
        return date_time_convert_with_timezone(
            self._date_time, self.get_timezone_offset()
        )

    def get_local_datetime_by_timezone_offset(self, timezone_offset):
        """get local datetime with the specified timezone by user

        :return: Time value with timezone offset
        """
        return date_time_convert_with_timezone(self._date_time, timezone_offset)

    def get_local_datetime_str(self):
        """convert local datetime string format

        :return: return local datetime string format
        """
        local_date_time = date_time_convert_with_timezone(
            self._date_time, self.get_timezone_offset()
        )
        return "%d-%02d-%02dT%02d:%02d:%02d.%06d" % (
            local_date_time.year,
            local_date_time.month,
            local_date_time.day,
            local_date_time.hour,
            local_date_time.minute,
            local_date_time.sec,
            local_date_time.microsec,
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._date_time.year == other.get_year()
            and self._date_time.month == other.get_month()
            and self._date_time.day == other.get_day()
            and self._date_time.hour == other.get_hour()
            and self._date_time.minute == other.get_minute()
            and self._date_time.sec == other.get_sec()
            and self._date_time.microsec == other.get_microsec()
        )

    def __repr__(self):
        return "utc datetime: %d-%02d-%02dT%02d:%02d:%02d.%06d, timezone_offset: %d" % (
            self._date_time.year,
            self._date_time.month,
            self._date_time.day,
            self._date_time.hour,
            self._date_time.minute,
            self._date_time.sec,
            self._date_time.microsec,
            self.get_timezone_offset(),
        )


class CoordinateWrapper(BaseObject):
    def __init__(self, x, y):
        super(CoordinateWrapper, self).__init__()
        self._x = x
        self._y = y

    def get_x(self):
        """get x

        :return: double
        """
        return self._x

    def get_y(self):
        """get y

        :return: double
        """
        return self._y

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._x == other.get_x() and self._y == other.get_y()

    def __repr__(self):
        return "%f %f" % (self._x, self._y)


class PointWrapper(BaseObject):
    def __init__(self, coord):
        super(PointWrapper, self).__init__()
        self._coord = coord

    def get_coordinate(self):
        """get raw data

        :return: CoordinateWrapper
        """
        return self._coord

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._coord == other.get_coordinate()

    def __repr__(self):
        return "POINT(" + str(self._coord) + ")"


class LineStringWrapper(BaseObject):
    def __init__(self, coord_list):
        super(LineStringWrapper, self).__init__()
        self._coord_list = coord_list

    def get_coordinate_list(self):
        """get raw data

        :return: list of CoordinateWrapper
        """
        return self._coord_list

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._coord_list == other.get_coordinate_list()

    def __repr__(self):
        wkt = "LINESTRING("
        for i in range(len(self._coord_list)):
            coord = self._coord_list[i]
            wkt += str(coord)
            if i < len(self._coord_list) - 1:
                wkt += ", "

        wkt += ")"
        return wkt


class PolygonWrapper(BaseObject):
    def __init__(self, coord_list_list):
        super(PolygonWrapper, self).__init__()
        self._coord_list_list = coord_list_list

    def get_coordinate_list_list(self):
        """get raw data

        :return: list of list of CoordinateWrapper
        """
        return self._coord_list_list

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._coord_list_list == other.get_coordinate_list_list()

    def __repr__(self):
        wkt = "POLYGON("
        for i in range(len(self._coord_list_list)):
            coord_list = self._coord_list_list[i]
            wkt += "("
            for j in range(len(coord_list)):
                coord = coord_list[i]
                wkt += str(coord)
                if i < len(self._coord_list) - 1:
                    wkt += ", "
            wkt += ")"
            if i < len(self._coord_list_list) - 1:
                wkt += ", "

        wkt += ")"
        return wkt


class GeographyWrapper(BaseObject):
    def __init__(self, geography):
        assert isinstance(geography, Geography)
        super(GeographyWrapper, self).__init__()
        self._geography = geography

    def get_geography(self):
        """get raw data

        :return: Geography
        """
        return self._geography

    def is_point(self):
        """judge the geography if is Point type

        :return: true or false
        """
        return self._geography.getType() == Geography.PTVAL

    def is_linestring(self):
        """judge the geography if is LineString type

        :return: true or false
        """
        return self._geography.getType() == Geography.LSVAL

    def is_polygon(self):
        """judge the geography if is Polygon type

        :return: true or false
        """
        return self._geography.getType() == Geography.PGVAL

    def as_point(self):
        """converts the original data type to Point type

        :return: PointWrapper
        """
        if self._geography.getType() == Geography.PTVAL:
            return PointWrapper(self._geography.get_ptVal())
        raise InvalidValueTypeException(
            "expect Point type, but is " + self._get_type_name()
        )

    def as_linestring(self):
        """converts the original data type to LineString type

        :return: LineStringWrapper
        """
        if self._geography.getType() == Geography.LSVAL:
            return LineStringWrapper(self._geography.get_lsVal())
        raise InvalidValueTypeException(
            "expect LineString type, but is " + self._get_type_name()
        )

    def as_polygon(self):
        """converts the original data type to Polygon type

        :return: PolygonWrapper
        """
        if self._geography.getType() == Geography.PGVAL:
            return PolygonWrapper(self._geography.get_pgVal())
        raise InvalidValueTypeException(
            "expect Polygon type, but is " + self._get_type_name()
        )

    def _get_type_name(self):
        if self.is_point():
            return "point"
        if self.is_linestring():
            return "linestring"
        if self.is_polygon():
            return "polygon"
        return "unknown"

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        if self.is_point():
            return self.as_point() == o.as_point()
        elif self.is_linestring():
            return self.as_linestring() == o.as_linestring()
        elif self.is_polygon():
            return self.as_polygon() == o.as_polygon()
        else:
            raise RuntimeError(
                'Unsupported type:{} to compare'.format(self._get_type_name())
            )

    def __repr__(self):
        if self.is_point():
            return str(self.as_point())
        elif self.is_linestring():
            return str(self.as_linestring())
        elif self.is_polygon():
            return str(self.as_polygon())
        else:
            raise RuntimeError(
                'Unsupported type:{} to compare'.format(self._get_type_name())
            )


class DurationWrapper(BaseObject):
    def __init__(self, duration):
        super(DurationWrapper, self).__init__()
        self._duration = duration

    def get_seconds(self):
        """get seconds

        :return: int64 seconds
        """
        return self._duration.seconds

    def get_microseconds(self):
        """get microseconds

        :return: int32 microseconds
        """
        return self._duration.microseconds

    def get_months(self):
        """get month

        :return: int32 month
        """
        return self._duration.months

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self._duration.seconds == other.get_seconds()
            and self._duration.microseconds == other.get_microseconds()
            and self._duration.months == other.get_months()
        )

    def __repr__(self):
        totalSeconds = self._duration.seconds + (self._duration.microseconds) // 1000000
        remainMicroSeconds = self._duration.microseconds % 1000000
        return f"P{self._duration.months}MT{totalSeconds}.{remainMicroSeconds:06d}000S"


class GenValue(object):
    @classmethod
    def gen_vertex(cls, vid, tags):
        vertex = Vertex()
        vertex.vid = vid
        vertex.tags = tags
        return vertex

    @classmethod
    def gen_edge(cls, src_id, dst_id, type, edge_name, ranking, props):
        edge = Edge()
        edge.src = src_id
        edge.dst = dst_id
        edge.type = type
        edge.name = edge_name
        edge.ranking = ranking
        edge.props = props
        return edge

    @classmethod
    def gen_segment(cls, start_node, end_node, relationship):
        segment = Segment()
        segment.start_node = start_node
        segment.end_node = end_node
        segment.relationship = relationship
        return segment


class Node(BaseObject):
    def __init__(self, vertex):
        super(Node, self).__init__()
        self._value = vertex
        self._tag_indexes = dict()
        for index, tag in enumerate(self._value.tags, start=0):
            self._tag_indexes[tag.name.decode(self.get_decode_type())] = index

    def get_id(self):
        """get the vid of Node

        :return: ValueWrapper type vid
        """
        return ValueWrapper(
            value=self._value.vid,
            decode_type=self.get_decode_type(),
            timezone_offset=self.get_timezone_offset(),
        )

    def tags(self):
        """get tag names

        :return: the list of tag name
        """
        return list(self._tag_indexes.keys())

    def has_tag(self, tag):
        """whether the specified tag is included

        :param tag: the tag name
        :return: true or false
        """
        return True if tag in self._tag_indexes.keys() else False

    def properties(self, tag):
        """get all properties of the specified tag

        :param tag: the tag name
        :return: the properties
        """
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)

        props = self._value.tags[self._tag_indexes[tag]].props
        result_props = {}
        if props is None:
            return result_props
        for key in props.keys():
            result_props[key.decode(self.get_decode_type())] = ValueWrapper(
                props[key],
                decode_type=self.get_decode_type(),
                timezone_offset=self._timezone_offset,
            )
        return result_props

    def prop_names(self, tag):
        """get the property names of the specified tag

        :param tag: the tag name
        :return: property name list
        """
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        index = self._tag_indexes[tag]
        props = self._value.tags[index].props
        if props is None:
            return []
        return [
            key.decode(self.get_decode_type())
            for key in self._value.tags[index].props.keys()
        ]

    def prop_values(self, tag):
        """get all property values of the specified tag

        :param tag: the tag name
        :return: property name list
        """
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        index = self._tag_indexes[tag]
        props = self._value.tags[index].props
        if props is None:
            return []
        return [
            ValueWrapper(
                value,
                decode_type=self.get_decode_type(),
                timezone_offset=self._timezone_offset,
            )
            for value in self._value.tags[index].props.values()
        ]

    def __repr__(self):
        tag_str_list = list()
        for tag in self._tag_indexes.keys():
            prop_strs = [
                '%s: %s' % (key, str(val)) for key, val in self.properties(tag).items()
            ]
            tag_str_list.append(':%s{%s}' % (tag, ', '.join(prop_strs)))
        return '({} {})'.format(str(self.get_id()), ' '.join(tag_str_list))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.get_id() == other.get_id()


class Relationship(BaseObject):
    def __init__(self, edge: Edge):
        super(Relationship, self).__init__()
        self._value = edge

    def start_vertex_id(self):
        """get start vertex vid, if your space vid_type is int, you can use start_vertex_id().as_int(),
        if your space vid_type is fixed_string, you can use start_vertex_id().as_string()

        :return: ValueWrapper type vid
        """
        if self._value.type > 0:
            return ValueWrapper(self._value.src, self.get_decode_type())
        else:
            return ValueWrapper(self._value.dst, self.get_decode_type())

    def end_vertex_id(self):
        """get end vertex vid, if your space vid_type is int, you can use end_vertex_id().as_int(),
        if your space vid_type is fixed_string, you can use end_vertex_id().as_string()

        :return: ValueWrapper type vid
        """
        if self._value.type > 0:
            return ValueWrapper(self._value.dst, self.get_decode_type())
        else:
            return ValueWrapper(self._value.src, self.get_decode_type())

    def edge_name(self):
        """get the edge name

        :return: edge name
        """
        return self._value.name.decode(self.get_decode_type())

    def ranking(self):
        """get the edge ranking

        :return: ranking
        """
        return self._value.ranking

    def properties(self):
        """get all properties

        :return: the properties
        """
        props = {}
        if self._value.props is None:
            return props
        for key in self._value.props.keys():
            props[key.decode(self.get_decode_type())] = ValueWrapper(
                self._value.props[key],
                decode_type=self.get_decode_type(),
                timezone_offset=self.get_timezone_offset(),
            )
        return props

    def keys(self):
        """get all property names

        :return: the property names
        """
        if self._value.props is None:
            return []
        return [(key.decode(self._decode_type)) for key in self._value.props.keys()]

    def values(self):
        """get all property values

        :return: the property values
        """
        if self._value.props is None:
            return []
        return [
            ValueWrapper(
                value,
                decode_type=self.get_decode_type(),
                timezone_offset=self.get_timezone_offset(),
            )
            for value in self._value.props.values()
        ]

    def __repr__(self):
        prop_strs = [
            '%s: %s' % (key, str(val)) for key, val in self.properties().items()
        ]
        return "(%s)-[:%s@%d{%s}]->(%s)" % (
            str(self.start_vertex_id()),
            self.edge_name(),
            self.ranking(),
            ', '.join(prop_strs),
            str(self.end_vertex_id()),
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self.start_vertex_id() == other.start_vertex_id()
            and self.end_vertex_id() == other.end_vertex_id()
            and self.edge_name() == other.edge_name()
            and self.ranking() == self.ranking()
        )


class Segment:
    start_node = None
    end_node = None
    relationship = None

    def __repr__(self):
        return "{}-[:{}@{}{}]->{}".format(
            self.start_node,
            self.relationship.edge_name(),
            self.relationship.ranking(),
            self.relationship.properties(),
            self.end_node,
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (
            self.start_node == other.start_node
            and self.end_node == other.end_node
            and self.relationship == other.relationship
        )


class PathWrapper(BaseObject):
    """
    PathWrapper is wrapper handling for the Path from the service
    """

    def __init__(self, path):
        super(PathWrapper, self).__init__()
        self._nodes = list()
        self._segments = list()
        self._relationships = list()

        self._path = path
        self._nodes.append(
            Node(path.src)
            .set_decode_type(self.get_decode_type())
            .set_timezone_offset(self.get_timezone_offset())
        )

        vids = []
        vids.append(path.src.vid)
        for step in self._path.steps:
            type = step.type
            if step.type > 0:
                start_node = self._nodes[-1]
                end_node = (
                    Node(step.dst)
                    .set_decode_type(self.get_decode_type())
                    .set_timezone_offset(self.get_timezone_offset())
                )
                src_id = vids[-1]
                dst_id = step.dst.vid
            else:
                type = -type
                end_node = self._nodes[-1]
                start_node = (
                    Node(step.dst)
                    .set_decode_type(self.get_decode_type())
                    .set_timezone_offset(self.get_timezone_offset())
                )
                dst_id = vids[-1]
                src_id = step.dst.vid
            vids.append(step.dst.vid)
            relationship = (
                Relationship(
                    GenValue.gen_edge(
                        src_id, dst_id, type, step.name, step.ranking, step.props
                    )
                )
                .set_decode_type(self.get_decode_type())
                .set_timezone_offset(self.get_timezone_offset())
            )

            self._relationships.append(relationship)
            segment = GenValue.gen_segment(start_node, end_node, relationship)
            if segment.start_node == self._nodes[-1]:
                self._nodes.append(segment.end_node)
            elif segment.end_node == self._nodes[-1]:
                self._nodes.append(segment.start_node)
            else:
                raise Exception(
                    "Relationship [{}] does not connect to the last node".format(
                        relationship
                    )
                )

            self._segments.append(segment)

    def __iter__(self):
        return iter(self._segments)

    def start_node(self):
        """get start node of the Path

        :return: start node
        """
        if len(self._nodes) == 0:
            return None
        return self._nodes[0]

    def length(self):
        """get the length of the path

        :return: path length
        """
        return len(self._segments)

    def contain_node(self, node):
        """whether the node is in the path

        :param node: the specified node
        :return: true or false
        """
        return True if node in self._nodes else False

    def contain_relationship(self, relationship):
        """whether the relationship is in the path

        :param relationship: the specified relationship
        :return: true or false
        """
        return True if relationship in self._relationships else False

    def nodes(self):
        """get all nodes of the path

        :return: nodes
        """
        return self._nodes

    def relationships(self):
        """get all relationships of the path

        :return: relationships
        """
        return self._relationships

    def segments(self):
        """get all segments of the path

        :return: segments
        """
        return self._segments

    def __repr__(self):
        edge_strs = []
        for step in self._path.steps:

            relationship = (
                Relationship(
                    GenValue.gen_edge(
                        step.dst.vid,
                        step.dst.vid,
                        type,
                        step.name,
                        step.ranking,
                        step.props,
                    )
                )
                .set_decode_type(self.get_decode_type())
                .set_timezone_offset(self.get_timezone_offset())
            )

            edge_str = ''
            prop_strs = [
                '%s: %s' % (key, str(val))
                for key, val in relationship.properties().items()
            ]
            if step.type > 0:
                edge_str = '-[:%s@%d{%s}]->%s' % (
                    relationship.edge_name(),
                    relationship.ranking(),
                    ', '.join(prop_strs),
                    Node(step.dst)
                    .set_decode_type(self.get_decode_type())
                    .set_timezone_offset(self.get_timezone_offset()),
                )
            else:
                edge_str = "<-[:%s@%d{%s}]-%s" % (
                    relationship.edge_name(),
                    relationship.ranking(),
                    ', '.join(prop_strs),
                    Node(step.dst)
                    .set_decode_type(self.get_decode_type())
                    .set_timezone_offset(self.get_timezone_offset()),
                )

            edge_strs.append(edge_str)
        return '{}{}'.format(
            Node(self._path.src)
            .set_decode_type(self.get_decode_type())
            .set_timezone_offset(self.get_timezone_offset()),
            ''.join(edge_strs),
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self._segments == other.segments()
