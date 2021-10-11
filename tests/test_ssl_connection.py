#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import sys
import os
import time
import ssl

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from unittest import TestCase
from nebula2.Exception import IOErrorException
from nebula2.common import ttypes
from nebula2.gclient.net import Connection
from nebula2.Config import SSL_config

# set SSL config
ssl_config = SSL_config()
ssl_config.cert_reqs = ssl.CERT_OPTIONAL
ssl_config.ca_certs = os.path.join(current_dir, 'secrets/test.ca.pem')
ssl_config.keyfile = os.path.join(current_dir, 'secrets/test.client.key')
ssl_config.certfile = os.path.join(current_dir, 'secrets/test.client.crt')

# self signed SSL config
ssl_selfs_signed_config = SSL_config()
ssl_selfs_signed_config.cert_reqs = ssl.CERT_OPTIONAL
ssl_selfs_signed_config.cert_reqs = ssl.CERT_OPTIONAL
ssl_selfs_signed_config.ca_certs = os.path.join(current_dir, 'secrets/test.self-signed.pem')
ssl_selfs_signed_config.keyfile = os.path.join(current_dir, 'secrets/test.self-signed.key')
ssl_selfs_signed_config.certfile = os.path.join(current_dir, 'secrets/test.self-signed.pem')

host = '127.0.0.1'
port = 9669

class TestSSLConnection(TestCase):
    def test_create(self):
        try:
            conn = Connection()
            conn.open_SSL(host, port, 1000, ssl_config)
            auth_result = conn.authenticate('root', 'nebula')
            assert auth_result.get_session_id() != 0
            conn.close()
        except Exception as ex:
            assert False, ex

    def test_release(self):
        try:
            conn = Connection()
            conn.open_SSL(host, port, 1000, ssl_config)
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
        conn = Connection()
        conn.open_SSL(host, port, 1000, ssl_config)
        auth_result = conn.authenticate('root', 'nebula')
        assert auth_result.get_session_id() != 0
        conn.close()
        try:
            conn.authenticate('root', 'nebula')
        except IOErrorException:
            assert True

class TestSSLConnectionSelfSigned(TestCase):
    def test_create_self_signed(self):
        try:
            conn = Connection()
            conn.open_SSL(host, port, 1000, ssl_selfs_signed_config)
            auth_result = conn.authenticate('root', 'nebula')
            assert auth_result.get_session_id() != 0
            conn.close()
        except Exception as ex:
            assert False, ex

    def test_release_self_signed(self):
        try:
            conn = Connection()
            conn.open_SSL(host, port, 1000, ssl_selfs_signed_config)
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

    def test_close_self_signed(self):
        conn = Connection()
        conn.open_SSL(host, port, 1000, ssl_selfs_signed_config)
        auth_result = conn.authenticate('root', 'nebula')
        assert auth_result.get_session_id() != 0
        conn.close()
        try:
            conn.authenticate('root', 'nebula')
        except IOErrorException:
            assert True
