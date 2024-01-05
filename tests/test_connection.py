#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import sys
import os
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from unittest import TestCase
from nebula3.gclient.net import Connection
from nebula3.common import ttypes
from nebula3.Exception import IOErrorException

AddrIp = ['127.0.0.1']
port = 9669
version="3.0.0"

class TestConnection(TestCase):
    def test_create(self):
        for ip in AddrIp:
            try:
                conn = Connection()
                conn.open(ip, port, 1000,version)
                auth_result = conn.authenticate('root', 'nebula')
                assert auth_result.get_session_id() != 0
                conn.close()
            except Exception as ex:
                assert False, ex

    def test_release(self):
        for ip in AddrIp:
            try:
                conn = Connection()
                conn.open(ip, port, 1000,version)
                auth_result = conn.authenticate('root', 'nebula')
                session_id = auth_result.get_session_id()
                assert session_id != 0
                resp = conn.execute(session_id, 'SHOW SPACES')
                assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg
                conn.signout(session_id)
                # the session delete later
                time.sleep(12)
                resp = conn.execute(session_id, 'SHOW SPACES')
                assert resp.error_code != ttypes.ErrorCode.SUCCEEDED
                conn.close()
            except Exception as ex:
                assert False, ex

    def test_close(self):
        for ip in AddrIp:
            conn = Connection()
            conn.open(ip, port, 1000,version)
            auth_result = conn.authenticate('root', 'nebula')
            assert auth_result.get_session_id() != 0
            conn.close()
            try:
                conn.authenticate('root', 'nebula')
            except IOErrorException:
                assert True
