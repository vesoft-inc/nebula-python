# --coding:utf-8--
#
# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import contextlib
import socket

from collections import deque
from threading import RLock, Timer

from nebula3.Exception import NotValidConnectionException, InValidHostname

from nebula3.gclient.net.Session import Session
from nebula3.gclient.net.Connection import Connection
from nebula3.logger import logger


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
        self._ssl_configs = None
        self._lock = RLock()
        self._pos = -1
        self._close = False

    def __del__(self):
        self.close()

    def init(self, addresses, configs, ssl_conf=None):
        """init the connection pool

        :param addresses: the graphd servers' addresses
        :param configs: the config of the pool
        :param ssl_conf: the config of SSL socket
        :return: if all addresses are ok, return True else return False.
        """
        if self._close:
            logger.error('The pool has init or closed.')
            raise RuntimeError('The pool has init or closed.')
        self._configs = configs
        self._ssl_configs = ssl_conf
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
            raise RuntimeError(
                'The services status exception: {}'.format(self._get_services_status())
            )

        conns_per_address = int(self._configs.min_connection_pool_size / ok_num)

        if self._ssl_configs is None:
            for addr in self._addresses:
                for i in range(0, conns_per_address):
                    connection = Connection()
                    connection.open(addr[0], addr[1], self._configs.timeout)
                    self._connections[addr].append(connection)
        else:
            for addr in self._addresses:
                for i in range(0, conns_per_address):
                    connection = Connection()
                    connection.open_SSL(
                        addr[0], addr[1], self._configs.timeout, self._ssl_configs
                    )
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
                logger.error('The pool is closed')
                raise NotValidConnectionException()

            try:
                ok_num = self.get_ok_servers_num()
                if ok_num == 0:
                    return None
                max_con_per_address = int(
                    self._configs.max_connection_pool_size / ok_num
                )
                try_count = 0
                while try_count <= len(self._addresses):
                    self._pos = (self._pos + 1) % len(self._addresses)
                    addr = self._addresses[self._pos]
                    if self._addresses_status[addr] == self.S_OK:
                        for connection in self._connections[addr]:
                            if not connection.is_used:
                                if connection.ping():
                                    connection.is_used = True
                                    logger.info('Get connection to {}'.format(addr))
                                    return connection

                        if len(self._connections[addr]) < max_con_per_address:
                            connection = Connection()
                            if self._ssl_configs is None:
                                connection.open(addr[0], addr[1], self._configs.timeout)
                            else:
                                connection.open_SSL(
                                    addr[0],
                                    addr[1],
                                    self._configs.timeout,
                                    self._ssl_configs,
                                )
                            connection.is_used = True
                            self._connections[addr].append(connection)
                            logger.info('Get connection to {}'.format(addr))
                            return connection
                    else:
                        for connection in list(self._connections[addr]):
                            if not connection.is_used:
                                self._connections[addr].remove(connection)
                    try_count = try_count + 1
                return None
            except Exception as ex:
                logger.error('Get connection failed: {}'.format(ex))
                return None

    def ping(self, address):
        """check the server is ok

        :param address: the server address want to connect
        :return: True or False
        """
        try:
            conn = Connection()
            if self._ssl_configs is None:
                conn.open(address[0], address[1], 1000)
            else:
                conn.open_SSL(address[0], address[1], 1000, self._ssl_configs)
            conn.close()
            return True
        except Exception as ex:
            logger.warning(
                'Connect {}:{} failed: {}'.format(address[0], address[1], ex)
            )
            return False

    def close(self):
        """close all connections in pool

        :return: void
        """
        with self._lock:
            for addr in self._connections.keys():
                for connection in self._connections[addr]:
                    if connection.is_used:
                        logger.error(
                            'The connection using by someone, but now want to close it'
                        )
                    connection.close()
            self._close = True

    def connects(self):
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
        """update the servers' status"""
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
                            logger.debug(
                                'Remove the not unusable connection to {}'.format(
                                    connection.get_address()
                                )
                            )
                            conns.remove(connection)
                            continue
                        if (
                            self._configs.idle_time != 0
                            and connection.idle_time() > self._configs.idle_time
                        ):
                            logger.debug(
                                'Remove the idle connection to {}'.format(
                                    connection.get_address()
                                )
                            )
                            conns.remove(connection)

    def _period_detect(self):
        if self._close or self._configs.interval_check < 0:
            return
        self.update_servers_status()
        self._remove_idle_unusable_connection()
        timer = Timer(self._configs.interval_check, self._period_detect)
        timer.setDaemon(True)
        timer.start()
