#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from unittest import TestCase
from nebula2.graph.ttypes import ErrorCode
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config


class TestSession(TestCase):
    @classmethod
    def setup_class(self):
        self.user_name = 'root'
        self.password = 'nebula'
        self.configs = Config()
        self.configs.max_connection_pool_size = 6
        self.pool = ConnectionPool()
        assert self.pool.init([('127.0.0.1', 9669),
                               ('127.0.0.1', 9670),
                               ('127.0.0.1', 9671)],
                              self.configs)
        assert self.pool.connnects() == 0
        assert self.pool.in_used_connects() == 0

    def test_1_release_by_del(self):
        def get_local_session(pool):
            session = pool.get_session('root', 'nebula')
            assert pool.in_used_connects() == 1

        get_local_session(self.pool)
        assert self.pool.in_used_connects() == 0

    def test_2_reconnect(self):
        try:
            session = self.pool.get_session('root', 'nebula')
            for i in range(0, 5):
                if i == 3:
                    os.system('docker stop nebula-docker-compose_graphd0_1')
                    os.system('docker stop nebula-docker-compose_graphd1_1')
                    time.sleep(3)
                resp = session.execute('SHOW SPACES')
                if i >= 3:
                    assert resp.error_code() == ErrorCode.E_SESSION_INVALID
                else:
                    assert resp.is_succeeded()
                time.sleep(2)
            session.release()
            new_session = self.pool.get_session('root', 'nebula')
            new_session.execute('SHOW SPACES')
        except Exception as e:
            assert False, e
        finally:
            os.system('docker start nebula-docker-compose_graphd0_1')
            os.system('docker start nebula-docker-compose_graphd1_1')
            time.sleep(5)

    def test_3_session_context(self):
        in_used_connects = self.pool.in_used_connects()
        with self.pool.session_context('root', 'nebula') as session:
            assert self.pool.in_used_connects() == in_used_connects + 1
        assert self.pool.in_used_connects() == in_used_connects
