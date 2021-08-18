#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


import contextlib
import threading
import logging
import time
import socket

from collections import deque
from threading import RLock

from nebula2.fbthrift.transport import TSocket, TTransport
from nebula2.fbthrift.transport.TTransport import TTransportException
from nebula2.fbthrift.protocol import TBinaryProtocol

from nebula2.common.ttypes import ErrorCode

from nebula2.graph import GraphService

from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
    NotValidConnectionException,
    InValidHostname
)

from nebula2.data.ResultSet import ResultSet

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]:%(message)s')


class AuthResult(object):
    def __init__(self, session_id, timezone_offset, timezone_name):
        self._session_id = session_id
        self._timezone_offset = timezone_offset
        self._timezone_name = timezone_name

    def get_session_id(self):
        return self._session_id

    def get_timezone_offset(self):
        return self._timezone_offset

    def get_timezone_name(self):
        return self._timezone_name


class Session(object):
    def __init__(self, connection, auth_result: AuthResult, pool, retry_connect=True):
        self._session_id = auth_result.get_session_id()
        self._timezone_offset = auth_result.get_timezone_offset()
        self._connection = connection
        self._timezone = 0
        self._pool = pool
        self._retry_connect = retry_connect

    def execute(self, stmt):
        """execute statement

        :param stmt: the ngql
        :return: ResultSet
        """
        if self._connection is None:
            raise RuntimeError('The session has released')
        try:
            start_time = time.time()
            resp = self._connection.execute(self._session_id, stmt)
            end_time = time.time()
            return ResultSet(resp,
                             all_latency=int((end_time - start_time) * 1000000),
                             timezone_offset=self._timezone_offset)
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
                if self._retry_connect:
                    if not self._reconnect():
                        logging.warning('Retry connect failed')
                        raise IOErrorException(IOErrorException.E_ALL_BROKEN, ie.message)
                    resp = self._connection.execute(self._session_id, stmt)
                    end_time = time.time()
                    return ResultSet(resp,
                                     all_latency=int((end_time - start_time) * 1000000),
                                     timezone_offset=self._timezone_offset)
            raise
        except Exception:
            raise

    def release(self):
        """release the connection to pool, and the session couldn't been use again

        :return:
        """
        if self._connection is None:
            return
        self._connection.signout(self._session_id)
        self._connection.is_used = False
        self._connection = None

    def ping(self):
        """check the connection is ok

        :return: True or False
        """
        if self._connection is None:
            return False
        return self._connection.ping()

    def _reconnect(self):
        try:
            self._connection.is_used = False
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
        self._close = False

    def __del__(self):
        self.close()

    def init(self, addresses, configs):
        """init the connection pool

        :param addresses: the graphd servers' addresses
        :param configs: the config of the pool
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

        self.update_servers_status()

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
        """get session

        :param user_name: the user name to authenticate graphd
        :param password: the password to authenticate graphd
        :param retry_connect:
        :return: Session
        """
        connection = self.get_connection()
        if connection is None:
            raise NotValidConnectionException()
        try:
            auth_result = connection.authenticate(user_name, password)
            return Session(connection, auth_result, self, retry_connect)
        except Exception:
            raise

    @contextlib.contextmanager
    def session_context(self, *args, **kwargs):
        """
        session_context is to be used with a contextlib.contextmanager.
        It returns a connection session from the pool, with same params
        as the method get_session().

        When session_context is exited, the connection will be released.

        :param user_name: the user name to authenticate graphd
        :param password: the password to authenticate graphd
        :param retry_connect: if auto retry connect
        :return: contextlib._GeneratorContextManager
        """
        session = None
        try:
            session = self.get_session(*args, **kwargs)
            yield session
        except Exception:
            raise
        finally:
            if session:
                session.release()

    def get_connection(self):
        """get available connection

        :return: Connection
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
                return None

    def ping(self, address):
        """check the server is ok

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
        """close all connections in pool

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
        """get the number of existing connections

        :return: the number of connections
        """
        with self._lock:
            count = 0
            for addr in self._connections.keys():
                count = count + len(self._connections[addr])
            return count

    def in_used_connects(self):
        """get the number of the used connections

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
        """get the number of the ok servers

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
        """update the servers' status
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
                for connection in list(conns):
                    if not connection.is_used:
                        if not connection.ping():
                            logging.debug('Remove the not unusable connection to {}'.format(connection.get_address()))
                            conns.remove(connection)
                            continue
                        if self._configs.idle_time != 0 and connection.idle_time() > self._configs.idle_time:
                            logging.debug('Remove the idle connection to {}'.format(connection.get_address()))
                            conns.remove(connection)

    def _period_detect(self):
        if self._close or self._configs.interval_check < 0:
            return
        self.update_servers_status()
        self._remove_idle_unusable_connection()
        timer = threading.Timer(self._configs.interval_check, self._period_detect)
        timer.setDaemon(True)
        timer.start()


class Connection(object):
    is_used = False

    def __init__(self):
        self._connection = None
        self.start_use_time = time.time()
        self._ip = None
        self._port = None
        self._timeout = 0

    def open(self, ip, port, timeout):
        """open the connection

        :param ip: the server ip
        :param port: the server port
        :param timeout: the timeout for connect and execute
        :return: void
        """
        self._ip = ip
        self._port = port
        self._timeout = timeout
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

    def _reopen(self):
        """reopen the connection

        :return:
        """
        self.close()
        self.open(self._ip, self._port, self._timeout)

    def authenticate(self, user_name, password):
        """authenticate to graphd

        :param user_name: the user name
        :param password: the password
        :return:
        """
        try:
            resp = self._connection.authenticate(user_name, password)
            if resp.error_code != ErrorCode.SUCCEEDED:
                raise AuthFailedException(resp.error_msg)
            return AuthResult(resp.session_id, resp.time_zone_offset_seconds, resp.time_zone_name)
        except TTransportException as te:
            if te.message.find("timed out"):
                self._reopen()
            if te.type == TTransportException.END_OF_FILE:
                self.close()
            raise IOErrorException(IOErrorException.E_CONNECT_BROKEN, te.message)

    def execute(self, session_id, stmt):
        """execute interface with session_id and ngql

        :param session_id: the session id get from result of authenticate interface
        :param stmt: the ngql
        :return: ExecutionResponse
        """
        try:
            resp = self._connection.execute(session_id, stmt)
            return resp
        except Exception as te:
            if isinstance(te, TTransportException):
                if te.message.find("timed out") > 0:
                    self._reopen()
                    raise IOErrorException(IOErrorException.E_TIMEOUT, te.message)
                elif te.type == TTransportException.END_OF_FILE:
                    raise IOErrorException(IOErrorException.E_CONNECT_BROKEN, te.message)
                elif te.type == TTransportException.NOT_OPEN:
                    raise IOErrorException(IOErrorException.E_NOT_OPEN, te.message)
                else:
                    raise IOErrorException(IOErrorException.E_UNKNOWN, te.message);
            raise

    def signout(self, session_id):
        """tells the graphd can release the session info

        :param session_id:the session id
        :return: void
        """
        try:
            self._connection.signout(session_id)
        except TTransportException as te:
            if te.type == TTransportException.END_OF_FILE:
                self.close()

    def close(self):
        """close the connection

        :return: void
        """
        try:
            self._connection._iprot.trans.close()
        except Exception as e:
            logging.error('Close connection to {}:{} failed:{}'.format(self._ip, self._port, e))

    def ping(self):
        """check the connection if ok
        :return: True or False
        """
        try:
            resp = self._connection.execute(0, 'YIELD 1;')
            return True
        except Exception:
            return False

    def reset(self):
        """reset the idletime

        :return: void
        """
        self.start_use_time = time.time()

    def idle_time(self):
        """get idletime of connection

        :return: idletime
        """
        if self.is_used:
            return 0
        return (time.time() - self.start_use_time) * 1000

    def get_address(self):
        """get the address of the connected service

        :return: (ip, port)
        """
        return (self._ip, self._port)
