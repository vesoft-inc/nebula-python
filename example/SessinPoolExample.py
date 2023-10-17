#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import time
from nebula3.common.ttypes import ErrorCode

from nebula3.gclient.net import Connection
from nebula3.gclient.net.SessionPool import SessionPool
from nebula3.Config import SessionPoolConfig
from FormatResp import print_resp

if __name__ == '__main__':
    ip = '127.0.0.1'
    port = 9669

    try:
        config = SessionPoolConfig()

        # prepare space
        conn = Connection()
        conn.open(ip, port, 1000)
        auth_result = conn.authenticate('root', 'nebula')
        assert auth_result.get_session_id() != 0
        resp = conn.execute(
            auth_result._session_id,
            'CREATE SPACE IF NOT EXISTS session_pool_test(vid_type=FIXED_STRING(30))',
        )
        assert resp.error_code == ErrorCode.SUCCEEDED
        # insert data need to sleep after create schema
        time.sleep(10)

        # init session pool
        session_pool = SessionPool('root', 'nebula', 'session_pool_test', [(ip, port)])
        assert session_pool.init(config)

        # add schema
        resp = session_pool.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int);'
            'CREATE EDGE like (likeness double);'
        )

        time.sleep(6)

        # insert vertex
        resp = session_pool.execute(
            'INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10), "Lily":("Lily", 9)'
        )
        assert resp.is_succeeded(), resp.error_msg()

        # insert edges
        resp = session_pool.execute(
            'INSERT EDGE like(likeness) VALUES "Bob"->"Lily":(80.0);'
        )
        assert resp.is_succeeded(), resp.error_msg()

        resp = session_pool.execute('FETCH PROP ON person "Bob" YIELD vertex as node')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        resp = session_pool.execute('FETCH PROP ON like "Bob"->"Lily" YIELD edge as e')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        # drop space
        conn.execute(
            auth_result._session_id,
            'DROP SPACE session_pool_test',
        )

        print("Example finished")

    except Exception as x:
        import traceback

        print(traceback.format_exc())
        exit(1)
