#--coding:utf-8--

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import logging

sys.path.insert(0, './dependence')
sys.path.insert(0, './gen-py')


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
                 is_async=False,
                 network_timeout=DEFAULT_TIMEOUT):
        self._ip = ip
        self._port = port

        self._timeout = network_timeout
        self._num = socket_num

        self._closed = False
        self._async = is_async
        if self._async:
            import gevent.queue
            try:
                from gevent import lock as glock
            except ImportError:
                # gevent < 1.0
                from gevent import coros as glock
            self._semaphore = glock.BoundedSemaphore(socket_num)
            self._connection_queue = gevent.queue.LifoQueue(socket_num)
            self._QueueEmpty = gevent.queue.Empty

        else:
            import threading
            try:
                import Queue
            except ImportError:
                import queue as Queue
            self._semaphore = threading.BoundedSemaphore(socket_num)
            self._connection_queue = Queue.LifoQueue(socket_num)
            self._QueueEmpty = Queue.Empty

    def close(self):
        self._closed = True
        while not self._connection_queue.empty():
            try:
                conn = self._connection_queue.get(block=False)
                try:
                    self._close_thrift_connection(conn)
                except:
                    pass
            except self._QueueEmpty:
                pass

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
        except:
            logging.warning('warn: failed to close iprot trans on {}', conn)
            pass
        try:
            conn._oprot.trans.close()
        except:
            logging.error('warn: failed to close oprot trans on {}', conn)
            pass

    def get_connection(self):
        """ get a connection from the pool. This blocks until one is available.
        """
        self._semaphore.acquire()
        if self._closed:
            raise RuntimeError('connection pool closed')
        try:
            return self._connection_queue.get(block=False)
        except self._QueueEmpty:
            try:
                return self._create_connection()
            except:
                self._semaphore.release()
                raise

    def return_connection(self, conn):
        """ return a thrift connection to the pool.
        """
        if self._closed:
            self._close_connection(conn)
            return
        self._connection_queue.put(conn)
        self._semaphore.release()

    def release_conn(self, conn):
        """ call when the connect is no usable anymore
        """
        try:
            self._close_connection(conn)
        except:
            pass
        if not self._closed:
            self._semaphore.release()
