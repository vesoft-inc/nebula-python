#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from typing import Dict

import pandas as pd
import prettytable
from nebula3.data.DataObject import Value, ValueWrapper
from nebula3.data.ResultSet import ResultSet


################################
#     Method 1 (Recommended)   #
################################
def result_to_df(result: ResultSet) -> pd.DataFrame:
    """
    build list for each column, and transform to dataframe
    """
    assert result.is_succeeded()
    columns = result.keys()
    d: Dict[str, list] = {}
    for col_num in range(result.col_size()):
        col_name = columns[col_num]
        col_list = result.column_values(col_name)
        d[col_name] = [x.cast() for x in col_list]
    return pd.DataFrame.from_dict(d, columns=columns)


################################
#     Method 2   (Customize)   #
################################
cast_as = {
    Value.NVAL: "as_null",
    Value.__EMPTY__: "as_empty",
    Value.BVAL: "as_bool",
    Value.IVAL: "as_int",
    Value.FVAL: "as_double",
    Value.SVAL: "as_string",
    Value.LVAL: "as_list",
    Value.UVAL: "as_set",
    Value.MVAL: "as_map",
    Value.TVAL: "as_time",
    Value.DVAL: "as_date",
    Value.DTVAL: "as_datetime",
    Value.VVAL: "as_node",
    Value.EVAL: "as_relationship",
    Value.PVAL: "as_path",
    Value.GGVAL: "as_geography",
    Value.DUVAL: "as_duration",
}


def customized_cast_with_dict(val: ValueWrapper):
    _type = val._value.getType()
    method = cast_as.get(_type)
    if method is not None:
        return getattr(val, method, lambda *args, **kwargs: None)()
    raise KeyError("No such key: {}".format(_type))


def print_resp(resp: ResultSet):
    assert resp.is_succeeded()
    output_table = prettytable.PrettyTable()
    output_table.field_names = resp.keys()
    for recode in resp:
        value_list = []
        for col in recode:
            val = customized_cast_with_dict(col)
            value_list.append(val)
        output_table.add_row(value_list)
    print(output_table)
