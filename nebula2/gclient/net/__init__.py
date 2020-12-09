#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import queue
import logging
import threading
import time
import socket

from threading import RLock
from typing import Dict
from nebula2.Config import Config
from nebula2.common.ttypes import HostAddr
from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TTransportException
from thrift.protocol import TBinaryProtocol

from nebula2.graph import (
    ttypes,
    GraphService
)

from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
    NotValidConnectionException,
    InValidHostname
)

from nebula2.data.ResultSet import ResultSet

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(filename)s:%(lineno)s]:%(message)s')


class Session(object):
    def __init__(self, address, session_id, pool, retry_connect=True):
        self.session_id = session_id
        self._address = address
        self._connection = None
        self._timezone = 0
        self._pool = pool
        self._retry_connect = retry_connect
        self._lock = threading.RLock()
        self._do_execute = False
        self._do_safe_execute = False

    def execute(self, stmt):
        """
        execute statement
        :param stmt: the ngql
        :return: ResultSet
        """
        with self._lock:
            if self._do_safe_execute:
                raise IOErrorException(
                    IOErrorException.E_UNKNOWN,
                    'The session is already called safe_execute, '
                    'You can only use execute or safe_execute in the session')
            self._do_execute = True
        try:
            if self._connection is None:
                self._connection = self._pool.get_connection(self._address)
            return ResultSet(self._connection.execute(self.session_id, stmt))
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.set_invalidate_connection(self._connection)
                if self._retry_connect:
                    if not self._reconnect():
                        logging.warning('Retry connect failed')
                        raise IOErrorException(IOErrorException.E_ALL_BROKEN,
                                               'All connections are broken')
                    try:
                        return ResultSet(self._connection.execute(self.session_id, stmt))
                    except Exception:
                        raise
            raise
        except Exception:
            raise

    def safe_execute(self, stmt):
        """
        safe_execute: it can use by multi thread. and every call,
        it will use a connection get from the pool
        :param stmt: the ngql
        :return: ResultSet
        """
        with self._lock:
            if self._do_execute:
                raise IOErrorException(
                    IOErrorException.E_UNKNOWN,
                    'The session is already called execute, '
                    'You can only use execute or safe_execute in the session')
            self._do_safe_execute = True
        connection = None
        try:
            if self._address is None:
                raise IOErrorException(IOErrorException.E_UNKNOWN, 'Input none address')
            connection = self._pool.get_connection(self._address)
            return ResultSet(connection.execute(self.session_id, stmt))
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.set_invalidate_connection(connection)
                connection = None
                if self._retry_connect:
                    try_count = self._pool.can_use_num()
                    while try_count > 0:
                        try_count = try_count - 1
                        connection = self._pool.get_connection()
                        if connection is None:
                            continue
                        if connection.ping():
                            self._connection = connection
                            self._address = self._connection.get_address()
                            return ResultSet(connection.execute(self.session_id, stmt))
            raise
        except Exception:
            raise
        finally:
            if connection is not None:
                self._pool.return_connection(connection)

    def release(self):
        """
        release the connection to pool
        """
        if self._connection is None:
            self._connection = self._pool.get_connection(self._address)
        self._connection.signout(self.session_id)
        self._pool.return_connection(self._connection)
        self._connection = None

    def ping(self):
        """
        check the connection is ok
        :return Boolean
        """
        if self._connection is None:
            return False
        return self._connection.ping()

    def _reconnect(self):
        try:
            self._pool.set_invalidate_connection(self._connection)
            self._connection = None
            try_count = self._pool.can_use_num()
            while try_count > 0:
                try_count = try_count - 1
                conn = self._pool.get_connection()
                if conn is None:
                    return False
                if conn.ping():
                    self._connection = conn
                    self._address = self._connection.get_address()
                    return True
        except NotValidConnectionException:
            return False
        return True

    def __del__(self):
        logging.debug("Call __del__ to release()")
        self.release()


class ConnectionPool(object):
    S_OK = 0
    S_BAD = 1
    T_CHECK_PERIOD = 5 * 60  # unit seconds

    def __init__(self):
        # all addresses of servers
        self._addresses = list()
        # all connections
        self._connections = dict()
        # the config of pool
        self._configs = None
        self._lock = RLock()
        self._close = False
        self._load_balance = None
        # the conn in use
        self._in_use_conn_num = 0
        # the return conn num
        self._return_conn_num = 0
        # the active conn num
        self._active_conn_num = 0
        # the idle conn num
        self._idle_conn_num = 0
        # the max conn num
        self._max_conn_num = 0

    def __del__(self):
        self.close()

    def init(self, addresses, configs=Config()):
        """
        init the connection pool
        :param addresses: the graphd servers' addresses
        :param configs: the config
        :return: if all addresses are ok, return True else return False.
        """
        if self._close:
            logging.error('The pool has init or closed.')
            raise RuntimeError('The pool has init or closed.')

        self._configs = configs
        for address in addresses:
            if address not in self._addresses:
                try:
                    ip = socket.gethostbyname(address[0])
                except Exception:
                    raise InValidHostname(str(address[0]))
                ip_port = (ip, address[1])
                self._addresses.append(ip_port)
                self._connections[ip_port] = queue.Queue(
                    self._configs.max_connection_pool_size)

        self._load_balance = RoundRobinLoadBalance(self._addresses, self)

        self._load_balance.update_servers_status()
        if not self._load_balance.is_ok():
            raise RuntimeError('The services status exception: {}'.format(
                self._load_balance.get_services_status()))

        if self._configs.min_connection_pool_size > 0:
            for i in range(0, self._configs.min_connection_pool_size):
                addr = self._load_balance.get_address()
                connection = Connection()
                connection.open(addr[0], addr[1], self._configs.timeout)
                self._connections[addr].put(connection)
        self._idle_conn_num = self._configs.min_connection_pool_size
        self._active_conn_num = self._configs.min_connection_pool_size
        self._max_conn_num = self._configs.max_connection_pool_size

        self._period_check()
        return True

    def get_session(self, user_name, password, retry_connect=True):
        """
        get session
        :param user_name:
        :param password:
        :param retry_connect: if auto retry connect
        :return: void
        """
        connection = self.get_connection(None)
        if connection is None:
            raise NotValidConnectionException()
        try:
            session_id = connection.authenticate(user_name, password)
            return Session(connection.get_address(),
                           session_id,
                           self,
                           retry_connect)
        except Exception:
            raise
        finally:
            self.return_connection(connection)

    def get_connection(self, key: HostAddr = None):
        """
        get available connection
        :type key: object
        :return: Connection Object
        """
        new_key = key
        if new_key is None:
            new_key = self._load_balance.get_address()
        if new_key is None:
            raise IOErrorException(IOErrorException.E_ALL_BROKEN,
                                   'All server is broken')
        with self._lock:
            is_succeed = False
            if new_key not in self._connections.keys():
                raise IOErrorException(IOErrorException.E_UNKNOWN,
                                       'Use the unknown key: {}'.format(new_key))
            try:
                if self._close:
                    logging.error('The pool is closed')
                    raise NotValidConnectionException()

                if self._idle_conn_num <= 0 \
                        and self._active_conn_num >= self._max_conn_num:
                    raise NotValidConnectionException()
                if self._idle_conn_num > 0:
                    if self._connections[new_key].qsize() > 0:
                        self._idle_conn_num -= 1
                        is_succeed = True
                        return self._connections[new_key].get(block=False)
                    elif self._active_conn_num < self._max_conn_num:
                        self._create_connection(new_key)
                        is_succeed = True
                        return self._connections[new_key].get(block=False)

                if self._active_conn_num < self._max_conn_num:
                    self._create_connection(new_key)
                    is_succeed = True
                    return self._connections[new_key].get(block=False)
            except Exception as e:
                raise e
            finally:
                if is_succeed:
                    self._in_use_conn_num += 1

    def close(self):
        """
        close all connections in pool
        :return: void
        """
        with self._lock:
            try:
                for addr in self._connections.keys():
                    for i in range(0, self._connections[addr].qsize()):
                        self._connections[addr].get(block=False).close()
            except Exception as e:
                logging.warning(e)
            finally:
                self._close = True

    def idle_conn_num(self):
        """
        get the number of idle connections
        :return: int
        """
        with self._lock:
            return self._idle_conn_num

    def active_conn_num(self):
        """
        get the number of existing connections
        :return: int
        """
        with self._lock:
            return self._active_conn_num

    def in_used_conn_num(self):
        """
        get the number of the used connections
        :return: int
        """
        with self._lock:
            return self._in_use_conn_num

    def can_use_num(self):
        """
        get the can use number of the pool
        :return: int
        """
        with self._lock:
            return self._max_conn_num - self._active_conn_num + self._idle_conn_num

    def return_connection(self, connection):
        """
        return the connection to the pool
        :param connection:
        :return:
        """
        with self._lock:
            if connection.get_address() not in self._connections.keys():
                return
            connection.reset()
            self._connections[connection.get_address()].put(connection)
            self._idle_conn_num += 1
            self._in_use_conn_num -= 1

    def _remove_idle_connection(self):
        if self._configs.idle_time <= 0:
            return
        with self._lock:
            if self._idle_conn_num > self._configs.min_connection_pool_size:
                for key in self._connections.keys():
                    connection = self._connections[key].get(block=False)
                    count = self._connections[key].qsize()
                    while count > 0:
                        count = count - 1
                        if connection.idle_time() >= self._configs.idle_time:
                            logging.info(
                                'Remove the idle connection for address:{}'.format(key))
                            self._active_conn_num -= 1
                            self._idle_conn_num -= 1
                            continue
                        else:
                            self._connections[key].put(connection)

    def _period_check(self):
        if self._close:
            return
        self._remove_idle_connection()
        timer = threading.Timer(self.T_CHECK_PERIOD, self._period_check)
        timer.setDaemon(True)
        timer.start()

    def set_invalidate_connection(self, connection):
        with self._lock:
            if connection.get_address() not in self._connections.keys():
                return
            connection.close()
            self._active_conn_num -= 1
            self._in_use_conn_num -= 1
            self._load_balance.update_servers_status()

    def _create_connection(self, key: HostAddr):
        connection = Connection()
        connection.open(key[0], key[1], self._configs.timeout)
        self._connections[key].put(connection)
        self._active_conn_num += 1


def ping(addr, timeout=3000):
    try:
        connection = Connection()
        connection.open(addr[0], addr[1], timeout)
        connection.close()
        return True
    except Exception:
        return False


class Connection(object):
    is_used = False

    def __init__(self):
        self._connection = None
        self.start_use_time = 0
        self._ip = None
        self._port = None

    def open(self, ip, port, timeout):
        self._ip = ip
        self._port = port
        try:
            s = TSocket.TSocket(self._ip, self._port)
            if timeout > 0:
                s.setTimeout(timeout)
            transport = TTransport.TBufferedTransport(s)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            transport.open()
            self._connection = GraphService.Client(protocol)
        except Exception:
            raise

    def authenticate(self, user_name, password):
        try:
            resp = self._connection.authenticate(user_name, password)
            if resp.error_code != ttypes.ErrorCode.SUCCEEDED:
                raise AuthFailedException(resp.error_msg)
            return resp.session_id
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise IOErrorException(IOErrorException.E_CONNECT_BROKEN, te.message)

    def execute(self, session_id, stmt):
        try:
            resp = self._connection.execute(session_id, stmt)
            return resp
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise IOErrorException(IOErrorException.E_CONNECT_BROKEN, te.message)

    def signout(self, session_id):
        try:
            self._connection.signout(session_id)
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()

    def close(self):
        """
        :return: void
        """
        try:
            self._connection._iprot.trans.close()
        except Exception as e:
            logging.error('Close connection to {}:{} failed:{}'.format(self._ip, self._port, e))

    def ping(self):
        """
        check the connection if ok
        :return: Boolean
        """
        try:
            self._connection.execute(0, 'YIELD 1;')
            return True
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                return False
        return True

    def reset(self):
        self.start_use_time = time.time()

    def idle_time(self):
        return time.time() - self.start_use_time

    def get_address(self):
        return (self._ip, self._port)


class RoundRobinLoadBalance(object):
    S_OK = 0
    S_BAD = 1

    def __init__(self, servers_addresses, pool):
        self._servers_addresses = servers_addresses
        self._servers_status = {}
        self._pos = 0
        self._period_time = 5 * 60
        self._pool = pool
        self._servers_status: Dict[HostAddr, int]
        for addr in self._servers_addresses:
            self._servers_status[addr] = self.S_BAD

    def get_address(self):
        try_count = 0
        while try_count < len(self._servers_addresses):
            new_pos = self._pos % len(self._servers_addresses)
            addr = self._servers_addresses[new_pos]
            try_count = try_count + 1
            self._pos = self._pos + 1
            if self._servers_status[addr] == self.S_OK:
                return addr
        self._pos = self._pos + 1
        return None

    def update_servers_status(self):
        for addr in self._servers_addresses:
            if ping(addr):
                self._servers_status[addr] = self.S_OK
            else:
                self._servers_status[addr] = self.S_BAD

    def is_ok(self):
        for addr in self._servers_status:
            if self._servers_status[addr] == self.S_BAD:
                return False
        return True

    def get_services_status(self):
        msg_list = []
        for addr in self._servers_status.keys():
            status = 'OK'
            if self._servers_status[addr] != self.S_OK:
                status = 'BAD'
            msg_list.append('[services: {}, status: {}]'.format(addr, status))
        return ', '.join(msg_list)
