# --coding:utf-8--
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import threading

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from graph import GraphService


class ConnectionPool(object):
    DEFAULT_TIMEOUT = 1000
    DEFAULT_CONNECT_SIZE = 2

    def __init__(self,
                 ip,
                 port,
                 socket_num=DEFAULT_CONNECT_SIZE,
                 network_timeout=DEFAULT_TIMEOUT):
        self._ip = ip
        self._port = port

        self._timeout = network_timeout
        self._num = socket_num

        self._closed = False
        try:
            import Queue
        except ImportError:
            import queue as Queue
        self._semaphore = threading.BoundedSemaphore(socket_num)
        self._connection_queue = Queue.LifoQueue(socket_num)
        self._QueueEmpty = Queue.Empty

    def close(self):
        """ close all connection in the pool
        Returns: None
        """
        self._closed = True
        while not self._connection_queue.empty():
            try:
                conn = self._connection_queue.get(block=False)
                try:
                    self._close_connection(conn)
                except Exception:
                    pass
            except self._QueueEmpty:
                pass

    def get_connection(self):
        """ get a connection from the pool. This blocks until one is available.
        Returns: None
        """
        self._semaphore.acquire()
        if self._closed:
            print('ERROR: connection pool closed')
            return None
        try:
            return self._connection_queue.get(block=False)
        except self._QueueEmpty:
            try:
                return self._create_connection()
            except Exception:
                self._semaphore.release()
                return None
        except Exception as ex:
            print(ex)
            return None

    def return_connection(self, conn):
        """ return a thrift connection to the pool.
        Returns: None
        """
        if self._closed:
            self._close_connection(conn)
            return
        self._connection_queue.put(conn)
        self._semaphore.release()

    def release_conn(self, conn):
        """ call when the connect is no usable anymore
        Returns: None
        """
        try:
            self._close_connection(conn)
        except Exception:
            pass
        if not self._closed:
            self._semaphore.release()

    def _create_connection(self):
        transport = TSocket.TSocket(self._ip, self._port)
        if self._timeout > 0:
            transport.setTimeout(self._timeout)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        connection = GraphService.Client(protocol)
        return connection

    def _close_connection(self, conn):
        try:
            conn._iprot.trans.close()
        except Exception:
            print('WARNING: failed to close iprot trans on ', conn)
            pass
        try:
            conn._oprot.trans.close()
        except Exception:
            print('WARNING: failed to close oprot trans on ', conn)
            pass
