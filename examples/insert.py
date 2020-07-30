# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula Client example.
"""

import sys
import time
import datetime
import threading
import gevent
import multiprocessing
import cProfile

sys.path.insert(0, '../')

from concurrent.futures import ThreadPoolExecutor
from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient, ExecutionException, AuthException
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from graph import GraphService
from graph.ttypes import ErrorCode

if __name__ == '__main__':

    g_ip = '192.168.8.6'
    g_port = 3777

    try:
        # init connection pool
        connection_pool = ConnectionPool(g_ip, g_port, 1, 10)
        client = GraphClient(connection_pool)
        resp = client.authenticate('user', 'password')
        resp = client.execute('use default_space');
        client.set_space('default_space')
        '''
        for i in range(1, 505):
            try:
                resp = client.execute('INSERT VERTEX commodity() VALUES {}:()'.format(i));
                if resp.error_code != ErrorCode.SUCCEEDED:
                    print('=== insert vertex failed ===={}'.format(resp.error_msg))

            except Exception as x:
                print('===== catch Exception: {}  ===='.format(x))
        for i in range(1, 505):
            try:
                resp = client.execute('INSERT EDGE same_kind() VALUES 1->{}:()'.format(i));
                if resp.error_code != ErrorCode.SUCCEEDED:
                    print('=== insert edge failed ===={}'.format(resp.error_msg))

            except Exception as x:
                print('===== catch Exception: {}  ===='.format(x))
        '''
        start_time = time.time()
        query = "go from 1 over same_kind YIELD $$.commodity.name, $$.commodity.time, $$.commodity.count, $$.commodity.start, $$.commodity.end, $$.commodity.cost, $$.commodity.fromAddr , $$.commodity.manufacturer, $$.commodity.description, $$.commodity.period, $$.commodity.wify"
        client.execute(query)
        #cProfile.run("client.execute(query)")
        end_time = time.time()
        print('cost {} microseconds '.format((end_time - start_time)*1000000))
        '''
        while True:
            query = "go from 1 over same_kind YIELD $$.commodity.name, $$.commodity.time, $$.commodity.count, $$.commodity.start, $$.commodity.end, $$.commodity.cost, $$.commodity.fromAddr , $$.commodity.manufacturer, $$.commodity.description, $$.commodity.period, $$.commodity.wify"
            #query = "go from 1 over same_kind"
            start_time = time.time()
            resp = client.execute(query);
            end_time = time.time()
            print('cost {} seconds'.format(end_time - start_time))
            if resp.error_code != ErrorCode.SUCCEEDED:
                print('=== insert vertex failed ===={}'.format(resp.error_msg))
        '''
    except Exception as x:
        print('===== catch Exception ====')
        print(x)
        exit(1)

