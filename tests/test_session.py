#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import os
import time
from unittest import TestCase

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


class TestSession(TestCase):
    @classmethod
    def setup_class(self):
        self.user_name = "root"
        self.password = "nebula"
        self.configs = Config()
        self.configs.max_connection_pool_size = 6
        self.pool = ConnectionPool()
        assert self.pool.init(
            [("127.0.0.1", 9669), ("127.0.0.1", 9670), ("127.0.0.1", 9671)],
            self.configs,
        )
        assert self.pool.connects() == 0
        assert self.pool.in_used_connects() == 0

    def test_1_release_by_del(self):
        def get_local_session(pool):
            session = pool.get_session("root", "nebula")
            assert pool.in_used_connects() == 1

        get_local_session(self.pool)
        assert self.pool.in_used_connects() == 0

    def test_2_reconnect(self):
        try:
            session = self.pool.get_session("root", "nebula")
            time.sleep(2)

            # wait for the session space info to be updated to meta service
            resp = session.execute(
                "CREATE SPACE IF NOT EXISTS test_session(vid_type=FIXED_STRING(8)); USE test_session;"
            )
            assert resp.is_succeeded(), resp.error_msg()
            time.sleep(10)
            for i in range(0, 5):
                if i == 3:
                    os.system(
                        "docker stop tests_graphd0_1 || docker stop tests-graphd0-1"
                    )
                    os.system(
                        "docker stop tests_graphd1_1 || docker stop tests-graphd1-1"
                    )
                    time.sleep(3)
                resp = session.execute("SHOW SESSIONS")
                assert resp.is_succeeded(), resp.error_msg()
                assert resp.space_name() == "test_session"
                time.sleep(2)
            session.release()
            new_session = self.pool.get_session("root", "nebula")
            new_session.execute("SHOW SPACES")
        except Exception as e:
            assert False, e
        finally:
            os.system("docker start tests_graphd0_1 || docker start tests-graphd0-1")
            os.system("docker start tests_graphd1_1 || docker start tests-graphd1-1")
            time.sleep(2)

    def test_3_session_context(self):
        in_used_connects = self.pool.in_used_connects()
        with self.pool.session_context("root", "nebula") as session:
            assert self.pool.in_used_connects() == in_used_connects + 1
        assert self.pool.in_used_connects() == in_used_connects

    def test_4_timeout(self):
        try:
            configs = Config()
            configs.timeout = 100
            configs.max_connection_pool_size = 1
            pool = ConnectionPool()
            assert pool.init([("127.0.0.1", 9669)], configs)
            session = pool.get_session(self.user_name, self.password)
            ngql = ""
            for n in range(0, 500):
                ngql = ngql + "show hosts;"
            session.execute(ngql)
            assert False, "expect to get exception"
        except Exception as ex:
            assert str(ex).find("timed out") > 0
            assert True, ex
