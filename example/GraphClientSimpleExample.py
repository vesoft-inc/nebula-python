#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

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

        # prepare space and insert data
        print("\n Prepare space data...\n")
        resp = client.execute(
            'CREATE SPACE IF NOT EXISTS test(vid_type=FIXED_STRING(30));'
        )
        assert resp.is_succeeded(), resp.error_msg()
        time.sleep(6)
        resp = client.execute('USE test;')
        assert resp.is_succeeded(), resp.error_msg()
        resp = client.execute(
            'CREATE TAG IF NOT EXISTS person(name string, age int);'
            'CREATE EDGE like (likeness double);'
        )

        time.sleep(6)
        # insert data need to sleep after create schema
        resp = client.execute('CREATE TAG INDEX person_age_index on person(age)')
        time.sleep(6)
        # insert vertex
        resp = client.execute(
            'INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10), "Lily":("Lily", 9)'
        )
        assert resp.is_succeeded(), resp.error_msg()
        # insert edges
        resp = client.execute('INSERT EDGE like(likeness) VALUES "Bob"->"Lily":(80.0);')
        assert resp.is_succeeded(), resp.error_msg()
        resp = client.execute('REBUILD TAG INDEX person_age_index')
        assert resp.is_succeeded(), resp.error_msg()
        print("\n Data preparation is completed\n")

        # test fetch prop on statement
        print("\n Test fetch prop on statement...\n")
        resp = client.execute('FETCH PROP ON person "Bob" YIELD vertex as node')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)
        resp = client.execute('FETCH PROP ON like "Bob"->"Lily" YIELD edge as e')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        # get the result in json format
        print("\n Test execute_json interface...\n")
        resp_json = client.execute_json("yield 1")
        json_obj = json.loads(resp_json)
        print(json.dumps(json_obj, indent=2, sort_keys=True))

        # test parameter interface
        print("\n Test cypher parameter...\n")
        bval = ttypes.Value()
        bval.set_bVal(True)
        ival = ttypes.Value()
        ival.set_iVal(3)
        sval = ttypes.Value()
        sval.set_sVal("Cypher Parameter")
        params = {"p1": ival, "p2": bval, "p3": sval}
        resp = client.execute_parameter(
            'RETURN abs($p1)+3, toBoolean($p2) and false, toLower($p3)+1', params
        )
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)
        resp = client.execute_parameter(
            'MATCH (v:person)--() WHERE v.age>abs($p1)+3 RETURN v,v.age AS vage ORDER BY vage, $p3 LIMIT $p1+1',
            params,
        )
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        # drop space
        resp = client.execute('DROP SPACE test')
        assert resp.is_succeeded(), resp.error_msg()

    except Exception as x:
        import traceback

        print(traceback.format_exc())
        if client is not None:
            client.release()
        exit(1)
