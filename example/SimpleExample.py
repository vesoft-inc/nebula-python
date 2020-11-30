#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import time

sys.path.insert(0, '../')

from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config
from FormatResp import print_resp

if __name__ == '__main__':
    client = None
    try:
        config = Config()
        config.max_connection_pool_size = 2
        # init connection pool
        connection_pool = ConnectionPool()
        assert connection_pool.init([('127.0.0.1', 3700), ('127.0.0.1', 3699)], config)

        # get session from the pool
        client = connection_pool.get_session('root', 'nebula')
        assert client is not None

        client.execute('CREATE SPACE IF NOT EXISTS test; USE test;'
                       'CREATE TAG IF NOT EXISTS person(name string, age int);')

        # insert data need to sleep after create schema
        time.sleep(6)

        # insert vertex
        resp = client.execute('INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10)')
        assert resp.is_succeeded(), resp.error_msg()

        resp = client.execute('FETCH PROP ON person "Bob"')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

    except Exception as x:
        import traceback
        print(traceback.format_exc())
        if client is not None:
            client.release()
        exit(1)
