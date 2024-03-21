#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import json
import threading
import time
from unittest import TestCase

from nebula3.common.ttypes import ErrorCode
from nebula3.Config import SessionPoolConfig
from nebula3.Exception import (
    InValidHostname,
)
from nebula3.gclient.net import Connection
from nebula3.gclient.net.SessionPool import SessionPool

# ports for test
test_port = 9669
test_port2 = 9670


def prepare_space(space_name="session_pool_test"):
    # prepare space
    conn = Connection()
    conn.open("127.0.0.1", test_port, 1000)
    auth_result = conn.authenticate("root", "nebula")
    assert auth_result.get_session_id() != 0
    resp = conn.execute(
        auth_result._session_id,
        "CREATE SPACE IF NOT EXISTS {}(partition_num=32, replica_factor=1, vid_type = FIXED_STRING(30))".format(
            space_name
        ),
    )
    assert resp.error_code == ErrorCode.SUCCEEDED


def drop_space(space_name="session_pool_test"):
    # drop space
    conn = Connection()
    conn.open("127.0.0.1", test_port, 1000)
    auth_result = conn.authenticate("root", "nebula")
    assert auth_result.get_session_id() != 0

    # drop space
    resp = conn.execute(
        auth_result._session_id,
        "DROP SPACE IF EXISTS {}".format(space_name),
    )
    assert resp.error_code == ErrorCode.SUCCEEDED


class TestSessionPoolBasic(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(("127.0.0.1", test_port))
        self.addresses.append(("127.0.0.1", test_port2))
        self.configs = SessionPoolConfig()
        self.configs.min_size = 2
        self.configs.max_size = 4
        self.configs.idle_time = 2000
        self.configs.interval_check = 2

        # prepare space
        prepare_space("session_pool_test")
        prepare_space("session_pool_test_2")

        # insert data need to sleep after create schema
        time.sleep(10)

        self.session_pool = SessionPool(
            "root", "nebula", "session_pool_test", self.addresses
        )
        assert self.session_pool.init(self.configs)

    def tearDown_Class(self):
        drop_space("session_pool_test")
        drop_space("session_pool_test_2")

    def test_pool_init(self):
        # basic
        session_pool = SessionPool(
            "root", "nebula", "session_pool_test", self.addresses
        )
        assert session_pool.init(self.configs)

        # handle wrong service port
        pool = SessionPool(
            "root", "nebula", "session_pool_test", [("127.0.0.1", 3800)]
        )  # wrong port
        try:
            pool.init(self.configs)
            assert False
        except Exception:
            assert True

        # handle invalid hostname
        try:
            session_pool = SessionPool(
                "root", "nebula", "session_pool_test", [("wrong_host", test_port)]
            )
            session_pool.init(self.configs)
            assert False
        except InValidHostname:
            assert True, "We expected get the exception"

    def test_ping(self):
        assert self.session_pool.ping(self.addresses[0])
        assert self.session_pool.ping(("127.0.0.1", 5000)) is False

    def test_execute(self):
        resp = self.session_pool.execute("SHOW HOSTS")
        assert resp.is_succeeded()

    def test_execute_json(self):
        resp = self.session_pool.execute_json("SHOW HOSTS")
        json_obj = json.loads(resp)
        # Get errorcode
        resp_error_code = json_obj["errors"][0]["code"]
        assert 0 == resp_error_code

    def test_switch_space(self):
        # This test is used to test if the space bond to session is the same as the space in the session pool config after executing
        # a query contains `USE <space_name>` statement.
        session_pool = SessionPool(
            "root", "nebula", "session_pool_test", self.addresses
        )
        configs = SessionPoolConfig()
        configs.min_size = 1
        configs.max_size = 1
        assert session_pool.init(configs)

        resp = session_pool.execute("USE session_pool_test_2; SHOW HOSTS;")
        assert resp.is_succeeded()

        # The space in the session pool config should be the same as the space in the session.
        resp = session_pool.execute("SHOW HOSTS;")
        assert resp.is_succeeded()
        assert resp.space_name() == "session_pool_test"

    def test_session_renew_when_invalid(self):
        # This test is used to test if the session will be renewed when the session is invalid.
        session_pool = SessionPool(
            "root", "nebula", "session_pool_test", self.addresses
        )
        configs = SessionPoolConfig()
        configs.min_size = 1
        configs.max_size = 1
        assert session_pool.init(configs)

        # kill all sessions of the pool, size 1 here though
        for session in session_pool._idle_sessions:
            session_id = session._session_id
            session.execute(f"KILL SESSION {session_id}")
        try:
            session_pool.execute("SHOW HOSTS;")
        except Exception:
            pass
        # - session_id is not in the pool
        # - session_pool is still usable after renewing
        assert (
            session_id not in session_pool._idle_sessions
        ), "session should be renewed"
        resp = session_pool.execute("SHOW HOSTS;")
        assert resp.is_succeeded(), "session_pool should be usable after renewing"
        session_pool.close()


def test_session_pool_multi_thread():
    # prepare space
    prepare_space()

    # Test multi thread
    addresses = [("127.0.0.1", test_port), ("127.0.0.1", test_port2)]
    configs = SessionPoolConfig()
    configs.min_size = 2
    configs.max_size = 4
    configs.idle_time = 2000
    configs.interval_check = 2

    session_pool = SessionPool("root", "nebula", "session_pool_test", addresses)
    assert session_pool.init(configs)

    global success_flag
    success_flag = True

    def main_test():
        global success_flag
        try:
            resp = session_pool.execute("SHOW HOSTS")
            if not resp.is_succeeded():
                raise RuntimeError(
                    "Failed to execute the query in thread {} : {}".format(
                        threading.current_thread().getName(), resp.error_msg()
                    )
                )

        except Exception as x:
            print(x)
            success_flag = False
            return

    thread1 = threading.Thread(target=main_test, name="thread1")
    thread2 = threading.Thread(target=main_test, name="thread2")
    thread3 = threading.Thread(target=main_test, name="thread3")
    thread4 = threading.Thread(target=main_test, name="thread4")

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    assert len(session_pool._active_sessions) == 0
    assert success_flag
