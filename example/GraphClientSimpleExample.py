#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import json
import ssl

from nebula2.gclient.net import ConnectionPool

from nebula2.Config import Config, SSL_config
from nebula2.common import *
from FormatResp import print_resp

if __name__ == '__main__':
    client = None
    try:
        config = Config()
        ssl_config = SSL_config()
        ssl_config.cert_reqs = ssl.CERT_REQUIRED
        ssl_config.ca_certs = '../tests/secrets/test.ca.pem'
        ssl_config.keyfile = '../tests/secrets/test.client.key'
        ssl_config.certfile = '../tests/secrets/test.client.crt'

        config.max_connection_pool_size = 2
        # init connection pool
        connection_pool = ConnectionPool()
        if ssl_config is None:
            assert connection_pool.init(
                [('127.0.0.1', 9562)], config)
        else:
            print("not none\n")
            assert connection_pool.init_SSL(
                [('127.0.0.1', 29562)], config, ssl_config)

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

        resp = client.execute('FETCH PROP ON like "Bob"->"Lily"')
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
