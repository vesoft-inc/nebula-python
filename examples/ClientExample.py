# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula Client example.
"""

import sys
import time
import threading
import prettytable

from distutils.sysconfig import get_python_lib
import platform

# get the site-packages path
python_version = platform.python_version()[0:3]
lib_path = get_python_lib() + '/nebula_python-1.0.0-py' + python_version + '.egg/nebula'
sys.path.insert(0, lib_path)

from nebula.graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient, ExecutionException, AuthException


def print_value(column_names, rows):
    output_table = prettytable.PrettyTable()
    output_table.field_names = column_names
    for row in rows:
        value_list = []
        for col in row.columns:
            if col.getType() == ttypes.ColumnValue.__EMPTY__:
                print('ERROR: type is empty')
                return
            if col.getType() == ttypes.ColumnValue.BOOL_VAL:
                value_list.append(col.get_bool_val())
                continue
            if col.getType() == ttypes.ColumnValue.INTEGER:
                value_list.append(col.get_integer())
                continue
            if col.getType() == ttypes.ColumnValue.ID:
                value_list.append(col.get_id())
                continue
            if col.getType() == ttypes.ColumnValue.STR:
                value_list.append(col.get_str().decode('utf-8'))
                continue
            if col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                value_list.append(col.get_double_precision())
                continue
            if col.getType() == ttypes.ColumnValue.TIMESTAMP:
                value_list.append(col.get_timestamp())
                continue
            print('ERROR: Type unsupported')
            return
        output_table.add_row(value_list)
    print(output_table)


def do_simple_execute(client, cmd):
    print("do execute %s" %cmd)
    resp = client.execute(cmd)
    if resp.error_code != 0:
        print('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg))
        raise ExecutionException('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg))


def has_space(rows, space_name):
    for row in rows:
        if len(row.columns) != 1:
            raise ExecutionException('The row of SHOW SPACES has wrong size of columns')
        if row.columns[0].get_str().decode('utf-8') == space_name:
            return True
    return False


def main_test():
    client = None
    try:
        space_name = 'space_' + threading.current_thread().getName()
        print('thread name: %s, space_name : %s' % 
                (threading.current_thread().getName(), space_name))
        # Get one client
        client = GraphClient(connection_pool)
        auth_resp = client.authenticate('user', 'password')
        if auth_resp.error_code:
            raise AuthException("Auth failed")

        query_resp = client.execute_query('SHOW SPACES')
        if has_space(query_resp.rows, space_name):
            print('has %s, drop it' % space_name)
            do_simple_execute(client, 'DROP SPACE %s' % space_name)

        # Create space mySpace
        do_simple_execute(client, 'CREATE SPACE %s(partition_num=1, replica_factor=1)'
                          % space_name)

        do_simple_execute(client, 'USE %s' % space_name)
        time.sleep(1)

        # Create tag and edge
        do_simple_execute(client, 'CREATE TAG person(name string, age int); '
                                  'CREATE EDGE like(likeness double)')

        # It should large than the cycle of loading the schema
        time.sleep(6)

        # Insert vertex and edge
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)')
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)')
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10)')
        do_simple_execute(client, 'INSERT EDGE like(likeness) VALUES 1->2:(80.0)')
        do_simple_execute(client, 'INSERT EDGE like(likeness) VALUES 1->3:(70.0)')

        # Query data
        query_resp = client.execute_query('GO FROM 1 OVER like YIELD $$.person.name, '
                                          '$$.person.age, like.likeness')
        if query_resp.error_code:
            print('Execute failed: %s' % query_resp.error_msg)
            exit(1)

        # Print the result of query
        print(' \n====== The query result of thread[%s]======\n '
              % threading.current_thread().getName())
        print_value(query_resp.column_names, query_resp.rows)
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

    # init connection pool
    connection_pool = ConnectionPool(g_ip, g_port)

    # Test multi thread
    thread1 = threading.Thread(target=main_test, name='thread1')
    thread2 = threading.Thread(target=main_test, name='thread2')
    thread3 = threading.Thread(target=main_test, name='thread3')
    thread4 = threading.Thread(target=main_test, name='thread4')

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()

    # close connect pool
    connection_pool.close()
