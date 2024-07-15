#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import os
import threading
import time
from unittest import TestCase

import pytest

from nebula3.Config import Config
from nebula3.Exception import (
    InValidHostname,
    IOErrorException,
    NotValidConnectionException,
)
from nebula3.gclient.net import ConnectionPool


class TestConnectionPool(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(("127.0.0.1", 9669))
        self.addresses.append(("127.0.0.1", 9670))
        self.configs = Config()
        self.configs.min_connection_pool_size = 2
        self.configs.max_connection_pool_size = 4
        self.configs.idle_time = 2000
        self.configs.interval_check = 2
        self.pool = ConnectionPool()
        assert self.pool.init(self.addresses, self.configs)
        assert self.pool.connects() == 2

    def test_right_hostname(self):
        pool = ConnectionPool()
        assert pool.init([("localhost", 9669)], Config())

    def test_wrong_hostname(self):
        pool = ConnectionPool()
        try:
            pool.init([("wrong_host", 9669)], Config())
            assert False
        except InValidHostname:
            assert True

    def test_ping(self):
        assert self.pool.ping(("127.0.0.1", 9669))
        assert self.pool.ping(("127.0.0.1", 5000)) is False

    def test_init_failed(self):
        # init succeeded
        pool1 = ConnectionPool()
        addresses = list()
        addresses.append(("127.0.0.1", 9669))
        addresses.append(("127.0.0.1", 9670))
        assert pool1.init(addresses, Config())

        # init failed, connected failed
        pool2 = ConnectionPool()
        addresses = list()
        addresses.append(("127.0.0.1", 3800))
        try:
            pool2.init(addresses, Config())
            assert False
        except Exception:
            assert True

        # init failed, hostname not existed
        try:
            pool3 = ConnectionPool()
            addresses = list()
            addresses.append(("not_exist_hostname", 3800))
            assert not pool3.init(addresses, Config())
        except InValidHostname:
            assert True, "We expected get the exception"

    def test_get_session(self):
        # get session succeeded
        sessions = list()
        for num in range(0, self.configs.max_connection_pool_size):
            session = self.pool.get_session("root", "nebula")
            resp = session.execute("SHOW SPACES")
            assert resp.is_succeeded()
            sessions.append(session)

        # get session failed
        try:
            self.pool.get_session("root", "nebula")
        except NotValidConnectionException:
            assert True

        assert self.pool.in_used_connects() == 4
        # release session
        for session in sessions:
            session.release()

        assert self.pool.in_used_connects() == 0
        assert self.pool.connects() == 4

        # test get session after release
        for num in range(0, self.configs.max_connection_pool_size - 1):
            session = self.pool.get_session("root", "nebula")
            resp = session.execute("SHOW SPACES")
            assert resp.is_succeeded()
            sessions.append(session)

        assert self.pool.in_used_connects() == 3
        assert self.pool.connects() == 4
        # test the idle connection delete
        time.sleep(5)
        assert self.pool.connects() == 3

    def test_stop_close(self):
        session = self.pool.get_session("root", "nebula")
        assert session is not None
        resp = session.execute("SHOW SPACES")
        assert resp.is_succeeded()
        self.pool.close()
        try:
            new_session = self.pool.get_session("root", "nebula")
        except NotValidConnectionException:
            assert True
        except Exception as e:
            assert False, "We don't expect reach here:{}".format(e)

        try:
            session.execute("SHOW SPACES")
        except IOErrorException:
            assert True
        except Exception:
            assert False, "We don't expect reach here:".format()

    @pytest.mark.skip(reason="the test data without nba")
    def test_timeout(self):
        config = Config()
        config.timeout = 1000
        config.max_connection_pool_size = 1
        pool = ConnectionPool()
        assert pool.init([("127.0.0.1", 9669)], config)
        session = pool.get_session("root", "nebula")
        try:
            resp = session.execute('USE nba;GO 1000 STEPS FROM "Tim Duncan" OVER like')
            assert False
        except IOErrorException as e:
            assert True
            assert str(e).find("Read timed out")
        session.release()
        try:
            session = pool.get_session("root", "nebula")
        except IOErrorException:
            assert False


def test_multi_thread():
    # Test multi thread
    addresses = [("127.0.0.1", 9669), ("127.0.0.1", 9670)]
    configs = Config()
    thread_num = 6
    configs.max_connection_pool_size = thread_num
    pool = ConnectionPool()
    assert pool.init(addresses, configs)

    global success_flag
    success_flag = True

    def pool_multi_thread_test():
        session = None
        global success_flag
        try:
            session = pool.get_session("root", "nebula")
            if session is None:
                success_flag = False
                return
            space_name = "space_" + threading.current_thread().getName()

            session.execute("DROP SPACE IF EXISTS %s" % space_name)
            resp = session.execute(
                "CREATE SPACE IF NOT EXISTS %s(vid_type=FIXED_STRING(8))" % space_name
            )
            if not resp.is_succeeded():
                raise RuntimeError("CREATE SPACE failed: {}".format(resp.error_msg()))

            time.sleep(3)
            resp = session.execute("USE %s" % space_name)
            if not resp.is_succeeded():
                raise RuntimeError("USE SPACE failed:{}".format(resp.error_msg()))

        except Exception as x:
            print(x)
            success_flag = False
            return
        finally:
            if session is not None:
                session.release()

    threads = []
    for num in range(0, thread_num):
        thread = threading.Thread(
            target=pool_multi_thread_test, name="test_pool_thread" + str(num)
        )
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()
    assert success_flag

    pool.close()


def test_session_context_multi_thread():
    # Test multi thread
    addresses = [("127.0.0.1", 9669), ("127.0.0.1", 9670)]
    configs = Config()
    thread_num = 50
    configs.max_connection_pool_size = thread_num
    pool = ConnectionPool()
    assert pool.init(addresses, configs)

    global success_flag
    success_flag = True

    def pool_session_context_multi_thread_test():
        session = None
        global success_flag
        try:
            with pool.session_context("root", "nebula") as session:
                if session is None:
                    success_flag = False
                    return
                space_name = "space_" + threading.current_thread().getName()

                session.execute("DROP SPACE IF EXISTS %s" % space_name)
                resp = session.execute(
                    "CREATE SPACE IF NOT EXISTS %s(vid_type=FIXED_STRING(8))"
                    % space_name
                )
                if not resp.is_succeeded():
                    raise RuntimeError(
                        "CREATE SPACE failed: {}".format(resp.error_msg())
                    )

                time.sleep(3)
                resp = session.execute("USE %s" % space_name)
                if not resp.is_succeeded():
                    raise RuntimeError("USE SPACE failed:{}".format(resp.error_msg()))

        except Exception as x:
            print(x)
            success_flag = False
            return

    threads = []
    for num in range(0, thread_num):
        thread = threading.Thread(
            target=pool_session_context_multi_thread_test,
            name="test_session_context_thread" + str(num),
        )
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()
    assert success_flag

    pool.close()


def test_remove_invalid_connection():
    addresses = [("127.0.0.1", 9669), ("127.0.0.1", 9670), ("127.0.0.1", 9671)]
    configs = Config()
    configs.min_connection_pool_size = 30
    configs.max_connection_pool_size = 45
    pool = ConnectionPool()

    try:
        assert pool.init(addresses, configs)

        # turn down one server('127.0.0.1', 9669) so the connection to it is invalid
        os.system("docker stop tests_graphd0_1 || docker stop tests-graphd0-1")
        time.sleep(3)

        # get connection from the pool, we should be able to still get 30 connections even though one server is down
        for i in range(0, 30):
            conn = pool.get_connection()
            assert conn is not None

        # total connection should still be 30
        assert pool.connects() == 30

        # the number of connections to the down server should be 0
        assert len(pool._connections[addresses[0]]) == 0

        # the number of connections to the 2nd('127.0.0.1', 9670) and 3rd server('127.0.0.1', 9671) should be 15
        assert len(pool._connections[addresses[1]]) == 15
        assert len(pool._connections[addresses[2]]) == 15

    finally:
        os.system("docker start tests_graphd0_1 || docker start tests-graphd0-1")
        time.sleep(3)
