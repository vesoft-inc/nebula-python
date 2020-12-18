#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula2.graph import ttypes

from nebula2.data.DataObject import DataSetWrapper


class ResultSet(object):
    def __init__(self, resp, decode_type='utf-8'):
        """
        get data from ResultSet
        """
        self._decode_type = decode_type
        self._resp = resp
        self._data_set_wrapper = None
        if self._resp.data is not None:
            self._data_set_wrapper = DataSetWrapper(resp.data, self._decode_type)

    def is_succeeded(self):
        return self._resp.error_code == ttypes.ErrorCode.SUCCEEDED

    def error_code(self):
        return self._resp.error_code

    def space_name(self):
        if self._resp.space_name is None:
            return ''
        return self._resp.space_name.decode(self._decode_type)

    def error_msg(self):
        if self._resp.error_msg is None:
            return ''
        return self._resp.error_msg.decode(self._decode_type)

    def comment(self):
        if self._resp.error_msg is None:
            return ''
        return self._resp.comment.decode(self._decode_type)

    def latency(self):
        """
        unit us
        """
        return self._resp.latency_in_us

    def plan_desc(self):
        return self._resp.plan_desc

    def is_empty(self):
        return self._data_set_wrapper is None or self._data_set_wrapper.get_row_size() == 0

    def keys(self):
        """
        get colNames
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_col_names()

    def row_size(self):
        """
        get one row size
        """
        if self._data_set_wrapper is None:
            return 0
        return len(self._data_set_wrapper.get_rows())

    def col_size(self):
        """
        get one col size
        """
        if self._data_set_wrapper is None:
            return 0
        return len(self._data_set_wrapper.get_col_names())

    def get_row_types(self):
        """
        Get row types
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
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_row_types()

    def row_values(self, row_index):
        """
        Get row values
        :param index: the Record index
        :return: list<ValueWrapper>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.row_values(row_index)

    def column_values(self, key):
        """
        get column values
        :param key: the col name
        :return: list<ValueWrapper>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.column_values(key)

    def rows(self):
        """
        get all rows
        :return: list<Row>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_rows()

    def __iter__(self):
        return iter(self._data_set_wrapper)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._data_set_wrapper)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


