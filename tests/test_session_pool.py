#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import sys
import os
import threading
import time
import pytest

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from unittest import TestCase
from nebula3.common.ttypes import ErrorCode
from nebula3.gclient.net.SessionPool import SessionPool
from nebula3.gclient.net import Connection
from nebula3.Config import SessionPoolConfig

from nebula3.Exception import (
    NoValidSessionException,
    InValidHostname,
    IOErrorException,
)


class TestSessionPool(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(('127.0.0.1', 9669))
        self.addresses.append(('127.0.0.1', 9670))
        self.configs = SessionPoolConfig()
        self.configs.min_size = 2
        self.configs.max_size = 4
        self.configs.idle_time = 2000
        self.configs.interval_check = 2

        # prepare space
        conn = Connection()
        conn.open('127.0.0.1', 9669, 1000)
        auth_result = conn.authenticate('root', 'nebula')
        assert auth_result.get_session_id() != 0
        resp = conn.execute(
            auth_result._session_id,
            'CREATE SPACE IF NOT EXISTS session_pool_test(vid_type=FIXED_STRING(30))',
        )
        assert resp.error_code == ErrorCode.SUCCEEDED
        # insert data need to sleep after create schema
        time.sleep(10)

        self.session_pool = SessionPool(
            'root', 'nebula', 'session_pool_test', self.addresses
        )
        assert self.session_pool.init(self.configs)

    def test_right_hostname(self):
        session_pool = SessionPool(
            'root', 'nebula', 'session_pool_test', self.addresses
        )
        assert session_pool.init(self.configs)

    def test_wrong_hostname(self):
        session_pool = SessionPool(
            'root', 'nebula', 'session_pool_test', ('wrong_host', 9669)
        )
        try:
            session_pool.init(self.configs)
            assert False
        except InValidHostname:
            assert True

    def test_ping(self):
        assert self.pool.ping(('127.0.0.1', 9669))
        assert self.pool.ping(('127.0.0.1', 5000)) is False

    def test_init_failed(self):
        # init succeeded
        pool1 = SessionPool()
        addresses = list()
        addresses.append(('127.0.0.1', 9669))
        addresses.append(('127.0.0.1', 9670))
        assert pool1.init(addresses, Config())

        # init failed, connected failed
        pool2 = SessionPool()
        addresses = list()
        addresses.append(('127.0.0.1', 3800))
        try:
            pool2.init(addresses, Config())
            assert False
        except Exception:
            assert True

        # init failed, hostname not existed
        try:
            pool3 = SessionPool()
            addresses = list()
            addresses.append(('not_exist_hostname', 3800))
            assert not pool3.init(addresses, Config())
        except InValidHostname:
            assert True, "We expected get the exception"


def test_multi_thread():
    # Test multi thread
    addresses = [('127.0.0.1', 9669), ('127.0.0.1', 9670)]
    configs = Config()
    configs.max_connection_pool_size = 4
    pool = SessionPool()
    assert pool.init(addresses, configs)

    global success_flag
    success_flag = True

    def main_test():
        session = None
        global success_flag
        try:
            session = pool.get_session('root', 'nebula')
            if session is None:
                success_flag = False
                return
            space_name = 'space_' + threading.current_thread().getName()

            session.execute('DROP SPACE %s' % space_name)
            resp = session.execute(
                'CREATE SPACE IF NOT EXISTS %s(vid_type=FIXED_STRING(8))' % space_name
            )
            if not resp.is_succeeded():
                raise RuntimeError('CREATE SPACE failed: {}'.format(resp.error_msg()))

            time.sleep(3)
            resp = session.execute('USE %s' % space_name)
            if not resp.is_succeeded():
                raise RuntimeError('USE SPACE failed:{}'.format(resp.error_msg()))

        except Exception as x:
            print(x)
            success_flag = False
            return
        finally:
            if session is not None:
                session.release()

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

    pool.close()
    assert success_flag
