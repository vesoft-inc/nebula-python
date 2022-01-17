#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from nebula3.common.ttypes import ErrorCode

from nebula3.data.DataObject import DataSetWrapper


class ResultSet(object):
    def __init__(
        self, resp, all_latency, decode_type='utf-8', timezone_offset: int = 0
    ):
        """Constructor method

        :param resp: the response from the service
        :param all_latency: the execution time from the time the client
        sends the request to the time the response is received and the data is decoded
        :param decode_type: the decode_type for decode binary value, it comes from the service,
        but now the service not return, use utf-8 default
        :param timezone_offset: the timezone offset for calculate local time,
        it comes from the service
        """
        self._decode_type = decode_type
        self._resp = resp
        self._data_set_wrapper = None
        self._all_latency = all_latency
        self._timezone_offset = timezone_offset
        if self._resp.data is not None:
            self._data_set_wrapper = DataSetWrapper(
                data_set=resp.data,
                decode_type=self._decode_type,
                timezone_offset=self._timezone_offset,
            )

    def is_succeeded(self):
        """check the response from the service is succeeded

        :return: bool
        """
        return self._resp.error_code == ErrorCode.SUCCEEDED

    def error_code(self):
        """if the response is failed, the service return the error code

        :return: nebula3.common.ttypes.ErrorCode
        """
        return self._resp.error_code

    def space_name(self):
        """get the space for the current operation

        :return: space name or ''
        """
        if self._resp.space_name is None:
            return ''
        return self._resp.space_name.decode(self._decode_type)

    def error_msg(self):
        """if the response is failed, the service return the error message

        :return: error message
        """
        if self._resp.error_msg is None:
            return ''
        return self._resp.error_msg.decode(self._decode_type)

    def comment(self):
        """the comment return by service, it maybe some warning message

        :return: comment message
        """
        if self._resp.error_msg is None:
            return ''
        return self._resp.comment.decode(self._decode_type)

    def latency(self):
        """the time the server processes the request

        :return: latency
        """
        return self._resp.latency_in_us

    def whole_latency(self):
        """the execution time from the time the client
        sends the request to the time the response is received and the data is decoded

        :return: all_latency
        """
        return self._all_latency

    def plan_desc(self):
        """get plan desc, whe user want to get the execute plan use `PROFILE` and `EXPLAIN`

        :return:plan desc
        """
        return self._resp.plan_desc

    def is_empty(self):
        """the data of response is empty

        :return: true of false
        """
        return (
            self._data_set_wrapper is None or self._data_set_wrapper.get_row_size() == 0
        )

    def keys(self):
        """get the column names

        :return: column names
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_col_names()

    def row_size(self):
        """get the row size

        :return: row size
        """
        if self._data_set_wrapper is None:
            return 0
        return len(self._data_set_wrapper.get_rows())

    def col_size(self):
        """get column size

        :return: column size
        """
        if self._data_set_wrapper is None:
            return 0
        return len(self._data_set_wrapper.get_col_names())

    def get_row_types(self):
        """get the value type of the row

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
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_row_types()

    def row_values(self, row_index):
        """get row values

        :param row_index:
        :return: list<ValueWrapper>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.row_values(row_index)

    def column_values(self, key):
        """get column values

        :param key: the specified column name
        :return: list<ValueWrapper>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.column_values(key)

    def rows(self):
        """get all rows

        :return: list<Row>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_rows()

    def __iter__(self):
        """the iterator for per row

        :return: iter
        """
        return iter(self._data_set_wrapper)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._data_set_wrapper)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
