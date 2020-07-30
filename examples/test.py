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

    '''
    transport = TSocket.TSocket(g_ip, g_port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    transport.open()
    client = GraphService.Client(protocol)
    resp = client.authenticate('user', 'password')
    session_id = resp.session_id
    '''
    
    try:
        # init connection pool
        connection_pool = ConnectionPool(g_ip, g_port, 1, 0)
        client = GraphClient(connection_pool)
        resp = client.authenticate('user', 'password')
        resp = client.execute('use test');
        client.set_space('test')
        '''
        start_time = time.time()
        resp = client.execute(session_id, 'UPSERT VERTEX -7796464702011970474 SET Sentence.keyno="38d3c3611d90ae2d18234d114be5f8fe", Sentence.name="强生北中国全域营销重点客户总监王旭光也表示, "字面意思是在家乐福能买到李施德林,或者是每个家� e";')
        #print( "row.size = {}, latency_in_us = {}".format(len(resp.rows), resp.latency_in_us))
        print('=== error : {}'.format(resp.error_msg))
        end_time = time.time()
        print('cost time {}'.format((int)(round((end_time-start_time)*1000000))))
        '''
        for i in range(0, 100):
            print('now is {}'.format(i))
            try:
                resp = client.execute('show tags');
                if resp.error_code == ErrorCode.SUCCEEDED:
                    print('=== show tag success====')
                else:
                    print('=== show tag failed===={}'.format(resp.error_msg))

            except Exception as x:
                print('===== catch Exception: {}  ===='.format(x))
            time.sleep(1)
    except Exception as x:
        print('===== catch Exception ====')
        print(x)
        exit(1)
        #time.sleep(5)
        #transport.open()
        print('reconnect server')
        #resp = client.execute('show spaces');

