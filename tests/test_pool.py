#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os
import threading
import time
import pytest

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from unittest import TestCase

from nebula2.gclient.net import ConnectionPool

from nebula2.Config import Config

from nebula2.Exception import (
    NotValidConnectionException,
    InValidHostname,
    IOErrorException
)


class TestConnectionPool(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(('127.0.0.1', 9669))
        self.addresses.append(('127.0.0.1', 9670))
        self.configs = Config()
        self.configs.min_connection_pool_size = 2
        self.configs.max_connection_pool_size = 4
        self.configs.idle_time = 2000
        self.configs.interval_check = 2
        self.pool = ConnectionPool()
        assert self.pool.init(self.addresses, self.configs)
        assert self.pool.connnects() == 2

    def test_right_hostname(self):
        pool = ConnectionPool()
        assert pool.init([('localhost', 9669)], Config())

    def test_wrong_hostname(self):
        pool = ConnectionPool()
        try:
            pool.init([('wrong_host', 9669)], Config())
            assert False
        except InValidHostname:
            assert True

    def test_ping(self):
        assert self.pool.ping(('127.0.0.1', 9669))
        assert self.pool.ping(('127.0.0.1', 5000)) is False

    def test_init_failed(self):
        # init succeeded
        pool1 = ConnectionPool()
        addresses = list()
        addresses.append(('127.0.0.1', 9669))
        addresses.append(('127.0.0.1', 9670))
        assert pool1.init(addresses, Config())

        # init failed, connected failed
        pool2 = ConnectionPool()
        addresses = list()
        addresses.append(('127.0.0.1', 3800))
        try:
            pool2.init(addresses, Config())
            assert False
        except Exception:
            assert True

        # init failed, hostname not existed
        try:
            pool3 = ConnectionPool()
            addresses = list()
            addresses.append(('not_exist_hostname', 3800))
            assert not pool3.init(addresses, Config())
        except InValidHostname:
            assert True, "We expected get the exception"

    def test_get_session(self):
        # get session succeeded
        sessions = list()
        for num in range(0, self.configs.max_connection_pool_size):
            session = self.pool.get_session('root', 'nebula')
            resp = session.execute('SHOW SPACES')
            assert resp.is_succeeded()
            sessions.append(session)

        # get session failed
        try:
            self.pool.get_session('root', 'nebula')
        except NotValidConnectionException:
            assert True

        assert self.pool.in_used_connects() == 4
        # release session
        for session in sessions:
            session.release()

        assert self.pool.in_used_connects() == 0
        assert self.pool.connnects() == 4

        # test get session after release
        for num in range(0, self.configs.max_connection_pool_size - 1):
            session = self.pool.get_session('root', 'nebula')
            resp = session.execute('SHOW SPACES')
            assert resp.is_succeeded()
            sessions.append(session)

        assert self.pool.in_used_connects() == 3
        assert self.pool.connnects() == 4
        # test the idle connection delete
        time.sleep(5)
        assert self.pool.connnects() == 3

    def test_stop_close(self):
        session = self.pool.get_session('root', 'nebula')
        assert session is not None
        resp = session.execute('SHOW SPACES')
        assert resp.is_succeeded()
        self.pool.close()
        try:
            new_session = self.pool.get_session('root', 'nebula')
        except NotValidConnectionException:
            assert True
        except Exception as e:
            assert False, "We don't expect reach here:{}".format(e)

        try:
            session.execute('SHOW SPACES')
        except IOErrorException:
            assert True
        except Exception as e:
            assert False, "We don't expect reach here:".format(e)

    @pytest.mark.skip(reason="the test data without nba")
    def test_timeout(self):
        config = Config()
        config.timeout = 1000
        config.max_connection_pool_size = 1
        pool = ConnectionPool()
        assert pool.init([('127.0.0.1', 9669)], config)
        session = pool.get_session('root', 'nebula')
        try:
            resp = session.execute('USE nba;GO 1000 STEPS FROM \"Tim Duncan\" OVER like')
            assert False
        except IOErrorException as e:
            assert True
            assert str(e).find("Read timed out")
        session.release()
        try:
            session = pool.get_session('root', 'nebula')
        except IOErrorException as e:
            assert False


def test_multi_thread():
    # Test multi thread
    addresses = [('127.0.0.1', 9669), ('127.0.0.1', 9670)]
    configs = Config()
    configs.max_connection_pool_size = 4
    pool = ConnectionPool()
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
            resp = session.execute('CREATE SPACE IF NOT EXISTS %s(vid_type=FIXED_STRING(8))' % space_name)
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

