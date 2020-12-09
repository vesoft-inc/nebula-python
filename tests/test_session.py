#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import concurrent
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
        assert self.pool.init([('127.0.0.1', 3699),
                               ('127.0.0.1', 3700),
                               ('127.0.0.1', 3701)],
                              self.configs)
        assert self.pool.active_conn_num() == 0
        assert self.pool.in_used_conn_num() == 0

    def test_1_release_by_del(self):
        def get_local_session(pool):
            session = pool.get_session('root', 'nebula')
            session.execute('SHOW HOSTS')
            assert pool.in_used_conn_num() == 1

        get_local_session(self.pool)
        assert self.pool.in_used_conn_num() == 0

    def test_2_safe_execute(self):
        session = self.pool.get_session('root', 'nebula')
        session.safe_execute('CREATE SPACE IF NOT EXISTS test;'
                             'USE test;CREATE TAG IF NOT EXISTS a()')

        def exeucte_nqgl():
            return session.safe_execute('SHOW TAGS')

        with concurrent.futures.ThreadPoolExecutor(4) as executor:
            do_jobs = []
            succeeded_num = 0
            for i in range(0, 4):
                future = executor.submit(exeucte_nqgl)
                do_jobs.append(future)
            for future in concurrent.futures.as_completed(do_jobs):
                if future.exception() is not None:
                    assert False, future.exception()
                else:
                    resp = future.result()
                    if resp.is_succeeded():
                        succeeded_num = succeeded_num + 1

            assert succeeded_num == 4
        test_session = self.pool.get_session('root', 'nebula')
        test_session.release()

    def test_3_reconnect(self):
        try:
            session = self.pool.get_session('root', 'nebula')
            safe_session = self.pool.get_session('root', 'nebula')
            for i in range(0, 5):
                if i == 3:
                    os.system('docker stop nebula-docker-compose_graphd_1')
                    os.system('docker stop nebula-docker-compose_graphd1_1')
                    time.sleep(3)
                resp = session.execute('SHOW SPACES')
                resp1 = safe_session.safe_execute('SHOW SPACES')
                if i >= 3:
                    assert resp.error_code() == ErrorCode.E_SESSION_INVALID
                    assert resp1.error_code() == ErrorCode.E_SESSION_INVALID
                else:
                    assert resp.is_succeeded()
                    assert resp1.is_succeeded()
                time.sleep(2)
            session.release()
            safe_session.release()
            new_session = self.pool.get_session('root', 'nebula')
            new_session.execute('SHOW SPACES')
        except Exception as e:
            assert False, e
        finally:
            os.system('docker start nebula-docker-compose_graphd_1')
            os.system('docker start nebula-docker-compose_graphd1_1')
            time.sleep(5)
