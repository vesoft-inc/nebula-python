#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import prettytable

from nebula3.data.DataObject import ValueWrapper


def cast(val: ValueWrapper):
    if val.is_empty():
        return '__EMPTY__'
    elif val.is_null():
        return '__NULL__'
    elif val.is_bool():
        return val.as_bool()
    elif val.is_int():
        return val.as_int()
    elif val.is_double():
        return val.as_double()
    elif val.is_string():
        return val.as_string()
    elif val.is_time():
        return val.as_time()
    elif val.is_date():
        return val.as_date()
    elif val.is_datetime():
        return val.as_datetime()
    elif val.is_list():
        return [cast(x) for x in val.as_list()]
    elif val.is_set():
        return {cast(x) for x in val.as_set()}
    elif val.is_map():
        return {k: cast(v) for k, v in val.as_map()}
    elif val.is_vertex():
        return val.as_node()
    elif val.is_edge():
        return val.as_relationship()
    elif val.is_path():
        return val.as_path()
    elif val.is_geography():
        return val.as_geography()
    else:
        print("ERROR: Type unsupported")
        return None


def print_resp(resp):
    assert resp.is_succeeded()
    output_table = prettytable.PrettyTable()
    output_table.field_names = resp.keys()
    for recode in resp:
        value_list = []
        for col in recode:
            val = cast(col)
            value_list.append(val)
        output_table.add_row(value_list)
    print(output_table)
