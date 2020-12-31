#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


import threading
import logging
import time
import socket

from collections import deque
from threading import RLock

from nebula2.fbthrift.transport import TSocket, TTransport
from nebula2.fbthrift.transport.TTransport import TTransportException
from nebula2.fbthrift.protocol import TBinaryProtocol

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

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]:%(message)s')


class Session(object):
    def __init__(self, connection, session_id, pool, retry_connect=True):
        self.session_id = session_id
        self._connection = connection
        self._timezone = 0
        self._pool = pool
        self._retry_connect = retry_connect

    def execute(self, stmt):
        """
        execute statement
        :param stmt: the ngql
        :return: ResultSet
        """
        if self._connection is None:
            raise RuntimeError('The session has released')
        try:
            return ResultSet(self._connection.execute(self.session_id, stmt))
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
                if self._retry_connect:
                    if not self._reconnect():
                        logging.warning('Retry connect failed')
                        raise IOErrorException(IOErrorException.E_ALL_BROKEN, 'All connections are broken')
                    try:
                        return ResultSet(self._connection.execute(self.session_id, stmt))
                    except Exception:
                        raise
            raise
        except Exception:
            raise

    def release(self):
        """
        release the connection to pool
        """
        if self._connection is None:
            return
        self._connection.signout(self.session_id)
        self._connection.is_used = False
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
            conn = self._pool.get_connection()
            if conn is None:
                return False
            self._connection = conn
        except NotValidConnectionException:
            return False
        return True

    def __del__(self):
        self.release()


class ConnectionPool(object):
    S_OK = 0
    S_BAD = 1

    def __init__(self):
        # all addresses of servers
        self._addresses = list()

        # server's status
        self._addresses_status = dict()

        # all connections
        self._connections = dict()
        self._configs = None
        self._lock = RLock()
        self._pos = -1
        self._check_delay = 5 * 60  # unit seconds
        self._close = False

    def __del__(self):
        self.close()

    def init(self, addresses, configs):
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
                self._addresses_status[ip_port] = self.S_BAD
                self._connections[ip_port] = deque()

        # detect the services
        self._period_detect()

        # init min connections
        ok_num = self.get_ok_servers_num()
        if ok_num < len(self._addresses):
            raise RuntimeError('The services status exception: {}'.format(self._get_services_status()))

        conns_per_address = int(self._configs.min_connection_pool_size / ok_num)
        for addr in self._addresses:
            for i in range(0, conns_per_address):
                connection = Connection()
                connection.open(addr[0], addr[1], self._configs.timeout)
                self._connections[addr].append(connection)
        return True

    def get_session(self, user_name, password, retry_connect=True):
        """
        get session
        :param user_name:
        :param password:
        :param retry_connect: if auto retry connect
        :return: void
        """
        connection = self.get_connection()
        if connection is None:
            raise NotValidConnectionException()
        try:
            session_id = connection.authenticate(user_name, password)
            return Session(connection, session_id, self, retry_connect)
        except Exception:
            raise

    def get_connection(self):
        """
        get available connection
        :return: Connection Object
        """
        with self._lock:
            if self._close:
                logging.error('The pool is closed')
                raise NotValidConnectionException()

            try:
                ok_num = self.get_ok_servers_num()
                if ok_num == 0:
                    return None
                max_con_per_address = int(self._configs.max_connection_pool_size / ok_num)
                try_count = 0
                while try_count <= len(self._addresses):
                    self._pos = (self._pos + 1) % len(self._addresses)
                    addr = self._addresses[self._pos]
                    if self._addresses_status[addr] == self.S_OK:
                        for connection in self._connections[addr]:
                            if not connection.is_used:
                                if connection.ping():
                                    connection.is_used = True
                                    logging.info('Get connection to {}'.format(addr))
                                    return connection

                        if len(self._connections[addr]) < max_con_per_address:
                            connection = Connection()
                            connection.open(addr[0], addr[1], self._configs.timeout)
                            connection.is_used = True
                            self._connections[addr].append(connection)
                            logging.info('Get connection to {}'.format(addr))
                            return connection
                    else:
                        for connection in list(self._connections[addr]):
                            if not connection.is_used:
                                self._connections[addr].remove(connection)
                    try_count = try_count + 1
                return None
            except Exception as ex:
                logging.error('Get connection failed: {}'.format(ex))
                import traceback
                print(traceback.format_exc())
                return None

    def ping(self, address):
        """
        check the server is ok
        :param address: the server address want to connect
        :return: True or False
        """
        try:
            conn = Connection()
            conn.open(address[0], address[1], 1000)
            conn.close()
            return True
        except Exception as ex:
            logging.warning('Connect {}:{} failed: {}'.format(address[0], address[1], ex))
            return False

    def close(self):
        """
        close all connections in pool
        :return: void
        """
        with self._lock:
            for addr in self._connections.keys():
                for connection in self._connections[addr]:
                    if connection.is_used:
                        logging.error('The connection using by someone, but now want to close it')
                    connection.close()
            self._close = True

    def connnects(self):
        """
        get the number of existing connections
        :return: int
        """
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                count = count + len(self._connections[addr])
            return count

    def in_used_connects(self):
        """
        get the number of the used connections
        :return: int
        """
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                for connection in self._connections[addr]:
                    if connection.is_used:
                        count = count + 1
            return count

    def get_ok_servers_num(self):
        """
        get the number of the ok servers
        :return: int
        """
        count = 0
        for addr in self._addresses_status.keys():
            if self._addresses_status[addr] == self.S_OK:
                count = count + 1
        return count

    def _get_services_status(self):
        msg_list = []
        for addr in self._addresses_status.keys():
            status = 'OK'
            if self._addresses_status[addr] != self.S_OK:
                status = 'BAD'
            msg_list.append('[services: {}, status: {}]'.format(addr, status))
        return ', '.join(msg_list)

    def update_servers_status(self):
        """
        update the servers' status
        """
        for address in self._addresses:
            if self.ping(address):
                self._addresses_status[address] = self.S_OK
            else:
                self._addresses_status[address] = self.S_BAD

    def _remove_idle_unusable_connection(self):
        if self._configs.idle_time == 0:
            return
        with self._lock:
            for addr in self._connections.keys():
                conns = self._connections[addr]
                for connection in conns:
                    if not connection.is_used:
                        if not connection.ping():
                            logging.debug('Remove the not unusable connection to {}'.format(connection.get_address()))
                            conns.remove(connection)
                            continue
                        if self._configs.idle_time != 0 and connection.idle_time() > self._configs.idle_time:
                            logging.debug('Remove the idle connection to {}'.format(connection.get_address()))
                            conns.remove(connection)

    def _period_detect(self):
        if self._close:
            return
        self.update_servers_status()
        self._remove_idle_unusable_connection()
        timer = threading.Timer(self._check_delay, self._period_detect)
        timer.setDaemon(True)
        timer.start()


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
        if not self.is_used:
            return 0
        return time.time() - self.start_use_time

    def get_address(self):
        return (self._ip, self._port)
