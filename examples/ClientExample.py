# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula Client 2.0 example.
"""

import sys
import time
import threading
import prettytable

sys.path.insert(0, '../')

from nebula2.common import ttypes as CommonTtypes
from nebula2.graph import ttypes
from nebula2.ConnectionPool import ConnectionPool
from nebula2.Client import GraphClient
from nebula2.Common import *


def print_value(data):
    output_table = prettytable.PrettyTable()
    output_table.field_names = data.column_names
    for row in data.rows:
        value_list = []
        for col in row.values:
            if col.getType() == CommonTtypes.Value.__EMPTY__:
                value_list.append('__EMPTY__')
            elif col.getType() == CommonTtypes.Value.NVAL:
                value_list.append('__NULL__')
            elif col.getType() == CommonTtypes.Value.BVAL:
                value_list.append(col.get_bVal())
            elif col.getType() == CommonTtypes.Value.IVAL:
                value_list.append(col.get_iVal())
            elif col.getType() == CommonTtypes.Value.FVAL:
                value_list.append(col.get_fVal())
            elif col.getType() == CommonTtypes.Value.SVAL:
                value_list.append(col.get_sVal().decode('utf-8'))
            elif col.getType() == CommonTtypes.Type.DVAL:
                value_list.append(col.get_dVal().decode('utf-8'))
            elif col.getType() == CommonTtypes.Type.DATETIME:
                value_list.append(col.get_tVal())
            else:
                print('ERROR: Type unsupported')
                return
        output_table.add_row(value_list)
    print(output_table)


def do_simple_execute(client, cmd):
    print("do execute %s" %cmd)
    resp = client.execute(cmd)
    if resp.error_code != 0:
        print('Execute failed: %s, error msg: %s' % (cmd, resp.error_msg.decode('utf-8')))
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

        # Create space mySpace
        do_simple_execute(client, 'CREATE SPACE IF NOT EXISTS %s'
                          % space_name)

        time.sleep(5)

        do_simple_execute(client, 'USE %s' % space_name)
        time.sleep(1)

        # Create tag and edge
        do_simple_execute(client, 'CREATE TAG IF NOT EXISTS person(name string, age int); ')

        do_simple_execute(client, 'CREATE EDGE IF NOT EXISTS like(likeness double)')
        # It should large than the cycle of loading the schema
        time.sleep(6)

        # Insert vertex and edge
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES \'Bob\':(\'Bob\', 10)')
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES \'Lily\':(\'Lily\', 9)')
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES \'Tom\':(\'Tom\', 10)')
        do_simple_execute(client, 'INSERT VERTEX person(name, age) VALUES \'Jerry\':(\'Jerry\', 13), \'John\':(\'John\', 11)')
        do_simple_execute(client, 'INSERT EDGE like(likeness) VALUES \'Bob\'->\'Lily\':(80.0)')
        do_simple_execute(client, 'INSERT EDGE like(likeness) VALUES \'Bob\'->\'Tom\':(70.0)')
        do_simple_execute(client, 'INSERT EDGE like(likeness) VALUES \'Lily\'->\'Jerry\':(84.0), \'Tom\'->\'Jerry\':(68.3), \'Bob\'->\'John\':(97.2)')

        # Query data
        query_resp = client.execute_query('GO FROM \"Bob\" OVER like YIELD $^.person.name, '
                                          '$^.person.age, like.likeness')
        if query_resp.error_code:
            print('Execute failed: %s' % query_resp.error_msg)
            exit(1)

        # Print the result of query
        print(' \n====== The query result of thread[%s]======\n '
              % threading.current_thread().getName())
        print_value(query_resp.data)
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
