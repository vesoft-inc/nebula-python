# --coding:utf-8--
#
# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import socket

from threading import RLock, Timer

from nebula3.Exception import NotValidConnectionException, InValidHostname

from nebula3.gclient.net.Session import Session
from nebula3.gclient.net.Connection import Connection
from nebula3.logger import logger
from nebula3.Config import SessionConfig


class SessionPool(object):
    S_OK = 0
    S_BAD = 1

    def __init__(self):
        # all addresses of servers
        self._addresses = list()

        # server's status
        self._addresses_status = dict()

        # sessions that are currently in use
        self._active_sessions = list()
        # sessions that are currently available
        self._idle_sessions = list()

        self._configs = SessionConfig()
        self._ssl_configs = None
        self._lock = RLock()
        self._pos = -1
        self._close = False

    def __del__(self):
        self.close()

    def init(self, addresses, configs):
        """init the session pool

        :param configs: the config of the pool
        :return: if all addresses are valid, return True else return False.
        """
        # check configs
        try:
            self._check_configs()
        except Exception as e:
            logger.error('Invalid configs: {}'.format(e))
            return False

        if self._close:
            logger.error('The pool has init or closed.')
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

        # ping all servers
        self.update_servers_status()

        # check services status in the background
        self._period_detect()

        ok_num = self.get_ok_servers_num()
        if ok_num < len(self._addresses):
            raise RuntimeError(
                'The services status exception: {}'.format(self._get_services_status())
            )

        # iterate all addresses and create sessions to fullfil the min_size
        for i in range(self._configs.min_size):
            session = self._new_session()
            if session is None:
                raise RuntimeError('Get session failed')
            self._idle_sessions.append(session)

        return True

    def _new_session(self):
        """get a valid session with the username and password in the pool.
            also, the session is bound to the space specified in the configs.

        :return: Session
        """
        self._pos = (self._pos + 1) % len(self._addresses)
        addr = self._addresses[self._pos]
        if self._addresses_status[addr] == self.S_OK:
            if self._ssl_configs is None:
                connection = Connection()
                try:
                    connection.open(addr[0], addr[1], self._configs.timeout)
                    auth_result = connection.authenticate(
                        self._configs.username, self._configs.password
                    )
                    session = Session(connection, auth_result, self, False)
                    resp = session.execute('USE {}'.format(self._configs.space_name))
                    if resp.error_code != 0:
                        raise RuntimeError(
                            'Failed to get session, cannot set the session space to {}'.format(
                                self._configs.space_name
                            )
                        )
                    return session
                except Exception:
                    raise
        else:
            raise RuntimeError('SSL is not supported yet')

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
        """log out all sessions and close all connections

        :return: void
        """
        with self._lock:
            for session in self._idle_sessions:
                session.release()
                session.connection.close()
            for session in self._active_sessions:
                session.release()
                session.connection.close()
            self._idle_sessions.clear()
            self._close = True

    def connects(self):
        """get the number of existing connections

        :return: the number of connections
        """
        with self._lock:
            count = 0
            for addr in self._sessions.keys():
                count = count + len(self._sessions[addr])
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

    def ping_sessions(self):
        """ping all sessions in the pool"""
        with self._lock:
            for session in self._idle_sessions:
                session.execute(r'RETURN "SESSION PING"')

    def _remove_idle_unusable_session(self):
        if self._configs.idle_time == 0:
            return
        with self._lock:
            for addr in self._sessions.keys():
                conns = self._sessions[addr]
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
        """periodically detect the services status"""
        if self._close or self._configs.interval_check < 0:
            return
        self.update_servers_status()
        self._remove_idle_unusable_session()
        timer = Timer(self._configs.interval_check, self._period_detect)
        timer.setDaemon(True)
        timer.start()

    def _check_configs(self):
        """validate the configs"""
        if self._configs.min_size < 0:
            raise RuntimeError('The min_size must be greater than 0')
        if self._configs.max_size < 0:
            raise RuntimeError('The max_size must be greater than 0')
        if self._configs.min_size > self._configs.max_size:
            raise RuntimeError(
                'The min_size must be less than or equal to the max_size'
            )
        if self._configs.idle_time < 0:
            raise RuntimeError('The idle_time must be greater or equal to 0')
        if self._configs.space_name == "":
            raise RuntimeError('The space_name must be set')
        if self._configs.timeout < 0:
            raise RuntimeError('The timeout must be greater or equal to 0')
