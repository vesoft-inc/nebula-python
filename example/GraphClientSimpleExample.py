#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import json

from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config
from nebula2.common import *
from FormatResp import print_resp

if __name__ == '__main__':
    client = None
    try:
        config = Config()
        config.max_connection_pool_size = 2
        # init connection pool
        connection_pool = ConnectionPool()
        assert connection_pool.init([('127.0.0.1', 9669)], config)

        # get session from the pool
        client = connection_pool.get_session('root', 'nebula')
        assert client is not None

        # get the result in json format
        resp_json = client.execute_json("yield 1")
        json_obj = json.loads(resp_json)
        print(json.dumps(json_obj, indent=2, sort_keys=True))

        client.execute('CREATE SPACE IF NOT EXISTS test(vid_type=FIXED_STRING(30)); USE test;'
                       'CREATE TAG IF NOT EXISTS person(name string, age int);'
                       'CREATE EDGE like (likeness double);')

        # insert data need to sleep after create schema
        time.sleep(6)

        # insert vertex
        resp = client.execute(
            'INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10), "Lily":("Lily", 9)')
        assert resp.is_succeeded(), resp.error_msg()

        # insert edges
        resp = client.execute(
            'INSERT EDGE like(likeness) VALUES "Bob"->"Lily":(80.0);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = client.execute('FETCH PROP ON person "Bob"')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        bval = ttypes.Value()
        bval.set_bVal(True)
        ival = ttypes.Value()
        ival.set_iVal(3)
        sval = ttypes.Value()
        sval.set_sVal("Cypher Parameter")
        params={"p1":ival,"p2":bval,"p3":sval}

        # test parameter interface
        resp = client.execute_parameter('RETURN abs($p1)+3, toBoolean($p2) and false, toLower($p3)+1',params)
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)
        # test compatibility
        resp = client.execute('RETURN 3')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        # get the result in json format
        resp_json = client.execute_json_with_parameter("yield 1", params)
        json_obj = json.loads(resp_json)
        print(json.dumps(json_obj, indent=2, sort_keys=True))

        # drop space
        resp = client.execute('DROP SPACE test')
        assert resp.is_succeeded(), resp.error_msg()

    except Exception as x:
        import traceback
        print(traceback.format_exc())
        if client is not None:
            client.release()
        exit(1)
