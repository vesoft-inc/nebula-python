# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


"""
Nebula Client tests.
"""

import pytest
import sys
import os
import time
import threading

sys.path.insert(0, '../')

from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient


def get_port():
    return 3699


def create_pool(port):
    return ConnectionPool('127.0.0.1', port)


@pytest.fixture(scope='module')
def get_pool():
    return create_pool(get_port())


@pytest.fixture(scope='function')
def get_client(get_pool):
    return GraphClient(get_pool)


def check_result(rows, expect):
    if len(rows) != len(expect):
        print('len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect)))
        return False

    for row, i in zip(rows, range(0, len(expect))):
        for col, j in zip(row.columns, range(0, len(expect[i]))):
            if col.getType() == ttypes.ColumnValue.__EMPTY__:
                print('ERROR: type is empty')
                return False
            if col.getType() == ttypes.ColumnValue.BOOL_VAL:
                if col.get_bool_val() != expect[i][j]:
                    print('result: %d, expect: %d' % (col.get_bool_val(), expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.INTEGER:
                if col.get_integer() != expect[i][j]:
                    print('result: %d, expect: %d' % (col.get_integer(), expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.ID:
                if col.get_id() != expect[i][j]:
                    print('result: %d, expect: %d' % (col.get_id(), expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.STR:
                if col.get_str().decode('utf-8') != expect[i][j]:
                    print('result: %s, expect: %s' % (col.get_str().decode('utf-8'),
                                                              expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                if col.get_double_precision() != expect[i][j]:
                    print('result: %d, expect: %d' % (col.get_double_precision(),
                                                              expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.TIMESTAMP:
                if col.get_timestamp() != expect[i][j]:
                    print('result: %d, expect: %d' % (col.get_timestamp(),
                                                              expect[i][j]))
                    return False
                continue
            print('ERROR: Type unsupported')
            return False
    return True


def test_create_schema(get_client):
    try:
        client = get_client
        if client is None:
            assert False
            return
        resp = client.authenticate('user', 'password')
        assert resp.error_code == 0, resp.error_msg
        client.execute('DROP SPACE space1')

        resp = client.execute('CREATE SPACE space1')
        assert resp.error_code == 0, resp.error_msg
        time.sleep(5)
        count = 0
        while count < 100:
            resp = client.execute('USE space1')
            if resp.error_code == 0:
                break
            print(resp.error_msg)
            count += 1

        resp = client.execute('CREATE TAG person(name string, age int)')
        assert resp.error_code == 0, resp.error_msg

        client.execute('CREATE EDGE like(likeness double)')
        assert resp.error_code == 0, resp.error_msg

        time.sleep(12)

        resp = client.execute_query('SHOW TAGS')
        assert resp.error_code == 0, resp.error_msg
        assert len(resp.rows) == 1, resp.error_msg
        assert resp.rows[0].columns[1].get_str().decode('utf-8') == 'person', resp.error_msg

        resp = client.execute_query('SHOW EDGES')
        assert resp.error_code == 0, resp.error_msg
        assert len(resp.rows) == 1, resp.error_msg
        assert resp.rows[0].columns[1].get_str().decode('utf-8') == 'like', resp.error_msg

        time.sleep(10)
        client.sign_out()

    except Exception as ex:
        print(ex)
        client.sign_out()
        assert False


def test_insert_data(get_client):
    try:
        client = get_client
        if client is None:
            assert False, "client is None"
            return
        resp = client.authenticate('user', 'password')
        assert resp.error_code == 0, resp.error_msg

        resp = client.execute('USE space1')
        assert resp.error_code == 0, resp.error_msg

        time.sleep(1)
        resp = client.execute('INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)')
        assert resp.error_code == 0, resp.error_msg

        resp = client.execute('INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)')
        assert resp.error_code == 0, resp.error_msg

        resp = client.execute('INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10)')
        assert resp.error_code == 0, resp.error_msg

        resp = client.execute('INSERT EDGE like(likeness) VALUES 1->2:(80.0)')
        assert resp.error_code == 0, resp.error_msg

        resp = client.execute('INSERT EDGE like(likeness) VALUES 1->3:(90.0)')
        assert resp.error_code == 0, resp.error_msg

        client.sign_out()

    except Exception as ex:
        print(ex)
        client.sign_out()
        assert False


def test_query_data(get_client):
    try:
        client = get_client
        if client is None:
            assert False
            return
        resp = client.authenticate('user', 'password')
        if resp.error_code != 0:
            print("ERROR: %s" % resp.error_msg)
        assert resp.error_code == 0

        time.sleep(1)
        resp = client.execute('USE space1')
        assert resp.error_code == 0

        resp = client.execute_query('GO FROM 1 OVER like YIELD $$.person.name, '
                                    '$$.person.age, like.likeness')
        assert resp.error_code == 0
        assert len(resp.rows) == 2

        expect_result = [['Lily', 9, 80.0], ['Tom', 10, 90.0]]
        assert check_result(resp.rows, expect_result)
        client.sign_out()

    except Exception as ex:
        print(ex)
        client.sign_out()
        assert False


def test_multi_thread():
    # Test multi thread
    connection_pool = ConnectionPool('127.0.0.1', get_port())

    global success_flag
    success_flag = True

    def main_test():
        client = None
        global success_flag
        try:
            client = GraphClient(connection_pool)
            if client.is_none():
                print("ERROR: None client")
                success_flag = False
                return
            space_name = 'space_' + threading.current_thread().getName()
            resp = client.authenticate('user', 'password')
            if resp.error_code != 0:
                raise AuthException('Auth failed')

            client.execute('DROP SPACE %s' % space_name)
            resp = client.execute('CREATE SPACE %s' % space_name)
            if resp.error_code != 0:
                raise ExecutionException('CREATE SPACE failed')

            time.sleep(3)
            count = 0
            while count < 100:
                resp = client.execute('USE %s' % space_name)
                if resp.error_code == 0:
                    break
                print(resp.error_msg)
                count += 1
            resp = client.execute('USE %s' % space_name)
            if resp.error_code != 0:
                raise ExecutionException('USE SPACE failed')

            client.sign_out()
        except Exception as x:
            print(x)
            client.sign_out()
            success_flag = False
            return

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

    connection_pool.close()
    assert success_flag
