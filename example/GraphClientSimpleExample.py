#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import time
import json

from nebula3.gclient.net import ConnectionPool

from nebula3.Config import Config
from nebula3.common import *
from FormatResp import print_resp

if __name__ == '__main__':
    client = None
    try:
        config = Config()
        config.max_connection_pool_size = 2
        # init connection pool
        connection_pool = ConnectionPool()
        assert connection_pool.init([('192.168.15.8', 9669)], config)

        # get session from the pool
        client = connection_pool.get_session('root', 'nebula')
        assert client is not None

        # insert vertex
        resp = client.execute(
            'use sf100;lookup on Person yield id(vertex) as vid | limit 500000 | go from $-.vid over LIKES  yield distinct LIKES._src as src, LIKES._dst as dst'
        )
        assert resp.is_succeeded(), resp.error_msg()

        print("Example finished")

    except Exception as x:
        import traceback

        print(traceback.format_exc())
        if client is not None:
            client.release()
        exit(1)
