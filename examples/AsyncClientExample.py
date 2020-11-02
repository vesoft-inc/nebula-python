# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula Async Client example.
"""

import sys
import time
import asyncio
import prettytable
import datetime

from nebula.graph import ttypes
from nebula.AsyncClient import AsyncGraphClient
from nebula.Common import *


def print_value(column_names, rows):
    output_table = prettytable.PrettyTable()
    output_table.field_names = column_names
    for row in rows:
        value_list = []
        for col in row.columns:
            if col.getType() == ttypes.ColumnValue.__EMPTY__:
                print('ERROR: type is empty')
                return
            elif col.getType() == ttypes.ColumnValue.BOOL_VAL:
                value_list.append(col.get_bool_val())
            elif col.getType() == ttypes.ColumnValue.INTEGER:
                value_list.append(col.get_integer())
            elif col.getType() == ttypes.ColumnValue.ID:
                value_list.append(col.get_id())
            elif col.getType() == ttypes.ColumnValue.STR:
                value_list.append(col.get_str().decode('utf-8'))
            elif col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                value_list.append(col.get_double_precision())
            elif col.getType() == ttypes.ColumnValue.TIMESTAMP:
                value_list.append(col.get_timestamp())
            else:
                print('ERROR: Type unsupported')
                return
        output_table.add_row(value_list)
    print(output_table)


def main_test(client):
    try:
        space_name = 'test_space'
        # auth
        auth_resp = client.authenticate('user', 'password')
        if auth_resp.error_code:
            print('auth failed')
            raise AuthException("Auth failed")

        def query_callback(resp):
            if resp.error_code == 0:
                print_value(resp.column_names, resp.rows)
            else:
                print('Do query cmd failed: {}'.format(resp.error_msg))

        def simple_callback(resp):
            if resp.error_code == 0:
                print('Do cmd succeeded')
            else:
                print('Do cmd failed: {}'.format(resp.error_msg))

        asyncio.run(client.async_execute_query('SHOW SPACES', query_callback))

        # Create space mySpace
        async def prepare():
            await client.async_execute('CREATE SPACE IF NOT EXISTS ' + space_name, simple_callback)

            await client.async_execute('USE ' + space_name, simple_callback)
            time.sleep(1)

            # Create tag and edge
            await client.async_execute('CREATE TAG IF NOT EXISTS person(name string, age int);'
                                       'CREATE EDGE IF NOT EXISTS like(likeness double)',
                                       simple_callback)

            # It should large than the cycle of loading the schema
            time.sleep(6)

            # Insert vertex and edge
            await client.async_execute('INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10);'
                                       'INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9);'
                                       'INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10), '
                                       '4:(\'Jerry\', 13), 5:(\'John\', 11);'
                                       'INSERT EDGE like(likeness) VALUES 1->2:(80.0);'
                                       'INSERT EDGE like(likeness) VALUES 1->3:(70.0);'
                                       'INSERT EDGE like(likeness) VALUES 2->4:(84.0),'
                                       '3->5:(68.3), 1->5:(97.2)', simple_callback)

        client.get_loop().run_until_complete(prepare())

        # Query data, use sync
        async def sync_query():
            await client.async_execute_query('GO FROM 1 OVER like YIELD $$.person.name, '
                                             '$$.person.age, like.likeness', query_callback)
            await client.async_execute_query('GO FROM 2 OVER like YIELD $$.person.name, '
                                             '$$.person.age, like.likeness', query_callback)
            await client.async_execute_query('GO FROM 3 OVER like YIELD $$.person.name, '
                                             '$$.person.age, like.likeness', query_callback)

        start = datetime.datetime.now()
        client.get_loop().run_until_complete(sync_query())
        sync_cost = datetime.datetime.now() - start

        # Query data, use async
        task1 = client.get_loop().create_task(client.async_execute_query(
            'GO FROM 1 OVER like YIELD $$.person.name, $$.person.age, like.likeness', query_callback))
        task2 = client.get_loop().create_task(client.async_execute_query(
            'GO FROM 2 OVER like YIELD $$.person.name, $$.person.age, like.likeness', query_callback))
        task3 = client.get_loop().create_task(client.async_execute_query(
            'GO FROM 3 OVER like YIELD $$.person.name, $$.person.age, like.likeness', query_callback))
        start = datetime.datetime.now()
        task = list()
        task.append(task1)
        task.append(task2)
        task.append(task3)
        client.get_loop().run_until_complete(asyncio.wait(task))
        async_cost = datetime.datetime.now() - start
        print('Sync cost time: %s us' % sync_cost.microseconds)
        print('Async cost time: %s us' % async_cost.microseconds)
        client.sign_out()
    except Exception as x:
        print(x)

        client.sign_out()
        exit(1)


if __name__ == '__main__':

    g_ip = '127.0.0.1'
    g_port = 3699

    print('input argv num is %d' % len(sys.argv))
    if len(sys.argv) == 3:
        print('input argv num is 3')
        g_ip = sys.argv[1]
        print('ip: %s' % g_ip)
        g_port = sys.argv[2]
        print('port: %s' % g_port)
    client = AsyncGraphClient(g_ip, g_port)
    main_test(client)
