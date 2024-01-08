#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
import copy
import os
import ssl
from unittest import TestCase

import pytest

from nebula3.Config import Config, SSL_config
from nebula3.gclient.net import ConnectionPool

current_dir = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.SSL
class TestConnectionPool(TestCase):
    @classmethod
    def setup_class(self):
        self.addresses = list()
        self.addresses.append(("127.0.0.1", 9669))
        self.configs = Config()
        self.configs.min_connection_pool_size = 2
        self.configs.max_connection_pool_size = 4
        self.configs.idle_time = 2000
        self.configs.interval_check = 2

        # set SSL config
        self.ssl_config = SSL_config()
        self.ssl_config.cert_reqs = ssl.CERT_OPTIONAL
        self.ssl_config.ca_certs = os.path.join(current_dir, "secrets/root.crt")
        self.ssl_config.keyfile = os.path.join(current_dir, "secrets/client.key")
        self.ssl_config.certfile = os.path.join(current_dir, "secrets/client.crt")
        # self signed SSL config
        self.ssl_selfs_signed_config = SSL_config()
        self.ssl_selfs_signed_config.cert_reqs = ssl.CERT_OPTIONAL
        self.ssl_selfs_signed_config.ca_certs = os.path.join(
            current_dir, "secrets/root.crt"
        )
        self.ssl_selfs_signed_config.keyfile = os.path.join(
            current_dir, "secrets/client.key"
        )
        self.ssl_selfs_signed_config.certfile = os.path.join(
            current_dir, "secrets/client.crt"
        )

    def test_ssl_with_ca(self):
        pool = ConnectionPool()
        assert pool.init(self.addresses, self.configs, self.ssl_config)
        session = pool.get_session("root", "nebula")
        resp = session.execute("SHOW HOSTS")
        assert resp.is_succeeded()

    def test_ssl_with_invalid_ca(self):
        pool = ConnectionPool()
        config = copy.copy(self.ssl_config)
        config.ca_certs = "invalid"

        with self.assertRaises(Exception):
            pool.init(self.addresses, self.configs, config)
