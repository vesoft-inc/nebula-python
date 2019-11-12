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
import logging

sys.path.insert(0, '../nebula/dependence')
sys.path.insert(0, '../nebula/gen-py')
sys.path.insert(0, '../nebula')

from graph import ttypes
from ConnectionPool import ConnectionPool
from client import GraphClient


logging.basicConfig(level=logging.DEBUG)


class TestPool:
    def __init__(self, port):
        self.connection_pool = ConnectionPool('192.168.8.6', port)


def get_port():
    port = 3777
    port_str = os.popen("docker-compose ps|grep 3699|cut -d ':' -f 2 | cut -d '-' -f 1").read()

    if len(port_str) != 0 and len(port_str) < 7:
        port = int(port_str[0:-1])
    return port


@pytest.fixture(scope='module')
def get_pool():
    return TestPool(get_port())


@pytest.fixture(scope='function')
def get_client(get_pool):
    return GraphClient(get_pool.connection_pool)


def check_result(rows, expect):
    if len(rows) != len(expect):
        logging.error('len(rows)[%d] != len(expect)[%d]' % (len(rows), len(expect)))
        return False

    for row, i in zip(rows, range(0, len(expect))):
        for col, j in zip(row.columns, range(0, len(expect[i]))):
            if col.getType() == ttypes.ColumnValue.__EMPTY__:
                logging.exception('ERROR: type is empty')
                return False
            if col.getType() == ttypes.ColumnValue.BOOL_VAL:
                if col.get_bool_val() != expect[i][j]:
                    logging.error('result: %d, expect: %d' % (col.get_bool_val(), expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.INTEGER:
                if col.get_integer() != expect[i][j]:
                    logging.error('result: %d, expect: %d' % (col.get_integer(), expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.ID:
                if col.get_id() != expect[i][j]:
                    logging.error('result: %d, expect: %d' % (col.get_id(), expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.STR:
                if col.get_str().decode('utf-8') != expect[i][j]:
                    logging.error('result: %s, expect: %s' % (col.get_str().decode('utf-8'),
                                                              expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                if col.get_double_precision() != expect[i][j]:
                    logging.error('result: %d, expect: %d' % (col.get_double_precision(),
                                                              expect[i][j]))
                    return False
                continue
            if col.getType() == ttypes.ColumnValue.TIMESTAMP:
                if col.get_timestamp() != expect[i][j]:
                    logging.error('result: %d, expect: %d' % (col.get_timestamp(),
                                                              expect[i][j]))
                    return False
                continue
            logging.exception('ERROR: Type unsupported')
            return False
    return True


def test_create_schema(get_client):
    try:
        client = get_client
        if client is None:
            assert False
            return
        resp = client.authenticate('user', 'password')
        assert resp.error_code == 0
        client.execute('DROP SPACE space1')
        resp = client.execute('CREATE SPACE space1(partition_num=1, replica_factor=1)')
        assert resp.error_code == 0
        resp = client.execute('USE space1')
        assert resp.error_code == 0
        resp = client.execute('CREATE TAG person(name string, age int)')
        assert resp.error_code == 0
        client.execute('CREATE EDGE like(likeness double)')
        assert resp.error_code == 0

        resp = client.executeQuery('SHOW TAGS')
        assert resp.error_code == 0
        assert len(resp.rows) == 1
        assert resp.rows[0].columns[0].get_str().decode('utf-8') == 'person'

        resp = client.executeQuery('SHOW EDGES')
        assert resp.error_code == 0
        assert len(resp.rows) == 1
        assert resp.rows[0].columns[0].get_str().decode('utf-8') == 'like'

        time.sleep(6)
        client.signout()

    except Exception as ex:
        logging.error(ex)
        assert False


def test_insert_data(get_client):
    try:
        client = get_client
        if client is None:
            assert False
            return
        resp = client.authenticate('user', 'password')
        assert resp.error_code == 0
        resp = client.execute('USE space1')
        assert resp.error_code == 0
        resp = client.execute('INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)')
        assert resp.error_code == 0
        resp = client.execute('INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)')
        assert resp.error_code == 0
        resp = client.execute('INSERT VERTEX person(name, age) VALUES 3:(\'Tom\', 10)')
        assert resp.error_code == 0
        resp = client.execute('INSERT EDGE like(likeness) VALUES 1->2:(80.0)')
        assert resp.error_code == 0
        resp = client.execute('INSERT EDGE like(likeness) VALUES 1->3:(90.0)')
        assert resp.error_code == 0
        client.signout()
    except Exception as ex:
        logging.error(ex)
        assert False


def test_query_data(get_client):
    try:
        client = get_client
        if client is None:
            assert False
            return
        resp = client.authenticate('user', 'password')
        assert resp.error_code == 0
        resp = client.execute('USE space1')
        assert resp.error_code == 0
        resp = client.executeQuery('GO FROM 1 OVER like YIELD $$.person.name, '
                                   '$$.person.age, like.likeness')
        assert resp.error_code == 0
        assert len(resp.rows) == 2
        expect_result = [['Lily', 9, 80.0], ['Tom', 10, 90.0]]
        assert check_result(resp.rows, expect_result)
        client.signout()
    except Exception as ex:
        logging.error(ex)
        assert False


def test_multi_thread():
    # Test multi thread
    connection_pool = ConnectionPool('192.168.8.6', get_port())

    def main_test():
        client = None
        try:
            client = GraphClient(connection_pool)
            if client is None:
                assert False
                return
            space_name = 'space_' + threading.current_thread().getName()
            resp = client.authenticate('user', 'password')
            assert resp.error_code == 0
            resp = client.execute('DROP SPACE %s' % space_name)
            assert resp.error_code == 0
            resp = client.execute('CREATE SPACE %s(partition_num=1, replica_factor=1)' % space_name)
            assert resp.error_code == 0
            resp = client.execute('USE %s' % space_name)
            assert resp.error_code == 0
            client.signout()
        except Exception as x:
            logging.exception(x)

            if not client:
                client.signout()
            assert False

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
