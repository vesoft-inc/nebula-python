#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import pytest
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
from nebula3.utils.hash import hash as murmur_hash


@pytest.fixture(scope="module")
def nebula_session():
    config = Config()
    config.max_connection_pool_size = 10
    pool = ConnectionPool()
    pool.init([("127.0.0.1", 9669)], config)
    session = pool.get_session("root", "nebula")
    yield session
    pool.close()


@pytest.mark.parametrize(
    "data", ["", "a", "abcdefgh", "abcdefghi", "to_be_hashed", "中文"]
)
def test_hash_against_server(nebula_session, data):
    # Local Computing
    expected = murmur_hash(data)
    result = nebula_session.execute(f'YIELD hash("{data}")')
    assert result.is_succeeded(), result.error_msg()
    actual = result.row_values(0)[0].as_int()
    assert actual == expected
