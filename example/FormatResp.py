#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import prettytable


def print_resp(resp):
    assert resp.is_succeeded()
    output_table = prettytable.PrettyTable()
    output_table.field_names = resp.keys()
    for recode in resp:
        value_list = []
        for col in recode:
            if col.is_empty():
                value_list.append('__EMPTY__')
            elif col.is_null():
                value_list.append('__NULL__')
            elif col.is_bool():
                value_list.append(col.as_bool())
            elif col.is_int():
                value_list.append(col.as_int())
            elif col.is_double():
                value_list.append(col.as_double())
            elif col.is_string():
                value_list.append(col.as_string())
            elif col.is_time():
                value_list.append(col.as_time())
            elif col.is_date():
                value_list.append(col.as_date())
            elif col.is_datetime():
                value_list.append(col.as_datetime())
            elif col.is_list():
                value_list.append(col.as_list())
            elif col.is_set():
                value_list.append(col.as_set())
            elif col.is_map():
                value_list.append(col.as_map())
            elif col.is_vertex():
                value_list.append(col.as_node())
            elif col.is_edge():
                value_list.append(col.as_relationship())
            elif col.is_path():
                value_list.append(col.as_path())
            elif col.is_geography():
                value_list.append(col.as_geography())
            else:
                print('ERROR: Type unsupported')
                return
        output_table.add_row(value_list)
    print(output_table)
