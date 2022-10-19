# --coding:utf-8--
#
# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import json
import socket

from threading import RLock, Timer
import time

from nebula3.Exception import (
    AuthFailedException,
    NoValidSessionException,
    InValidHostname,
)

from nebula3.gclient.net.Session import Session
from nebula3.gclient.net.Connection import Connection
from nebula3.logger import logger
from nebula3.Config import SessionPoolConfig


class SessionPool(object):
    S_OK = 0
    S_BAD = 1

    def __init__(self, username, password, space_name, addresses):
        # user name and password of the session
        self._username = username
        self._password = password

        # space name bonded to the session
        self._space_name = space_name

        # all addresses of servers
        self._addresses = list()

        # server's status
        self._addresses_status = dict()

        # validate the addresses
        for address in addresses:
            try:
                ip = socket.gethostbyname(address[0])
            except Exception:
                raise InValidHostname(str(address[0]))
            ip_port = (ip, address[1])
            self._addresses.append(ip_port)
            self._addresses_status[ip_port] = self.S_BAD

        # sessions that are currently in use
        self._active_sessions = list()
        # sessions that are currently available
        self._idle_sessions = list()

        self._configs = SessionPoolConfig()
        self._ssl_configs = None
        self._lock = RLock()

        # the index of the next address to connect
        self._pos = -1

        # the flag of whether the pool is closed
        self._close = False

    def __del__(self):
        self.close()

    def init(self, configs):
        """init the session pool

        :param username: the username of the session
        :param password: the password of the session
        :param space_name: the space name of the session
        :param addresses: the addresses of the servers
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
            self._add_session_to_idle(session)

        return True

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

    def execute(self, stmt):
        """execute the given query
        Notice there are some limitations:
        1. The query should not be a plain space switch statement, e.g. "USE test_space",
        but queries like "use space xxx; match (v) return v" are accepted.
        2. If the query contains statements like "USE <space name>", the space will be set to the
        one in the pool config after the execution of the query.
        3. The query should not change the user password nor drop a user.

        :param stmt: the query string
        :return: ResultSet
        """
        return self.execute_parameter(stmt, None)

    def execute_parameter(self, stmt, params):
        """execute statement

        :param stmt: the query string
        :param params: parameter map
        :return: ResultSet
        """
        session = self._get_idle_session()
        if session is None:
            raise RuntimeError('Get session failed')
        self._add_session_to_active(session)

        try:
            resp = session.execute_parameter(stmt, params)

            # reset the space name to the pool config
            if resp.space_name() != self._space_name:
                self._set_space_to_default(session)

            # move the session back to the idle list
            self._return_session(session)

            return resp
        except Exception as e:
            logger.error('Execute failed: {}'.format(e))
            # remove the session from the pool if it is invalid
            self._active_sessions.remove(session)
            raise e

    def execute_json(self, stmt):
        """execute statement and return the result as a JSON string
            Date and Datetime will be returned in UTC
            JSON struct:
            {
                "results": [
                {
                    "columns": [],
                    "data": [
                    {
                        "row": [
                        "row-data"
                        ],
                        "meta": [
                        "metadata"
                        ]
                    }
                    ],
                    "latencyInUs": 0,
                    "spaceName": "",
                    "planDesc ": {
                    "planNodeDescs": [
                        {
                        "name": "",
                        "id": 0,
                        "outputVar": "",
                        "description": {
                            "key": ""
                        },
                        "profiles": [
                            {
                            "rows": 1,
                            "execDurationInUs": 0,
                            "totalDurationInUs": 0,
                            "otherStats": {}
                            }
                        ],
                        "branchInfo": {
                            "isDoBranch": false,
                            "conditionNodeId": -1
                        },
                        "dependencies": []
                        }
                    ],
                    "nodeIndexMap": {},
                    "format": "",
                    "optimize_time_in_us": 0
                    },
                    "comment ": ""
                }
                ],
                "errors": [
                {
                    "code": 0,
                    "message": ""
                }
                ]
            }
        :param stmt: the ngql
        :return: JSON string
        """
        return self.execute_json_with_parameter(stmt, None)

    def execute_json_with_parameter(self, stmt, params):
        session = self._get_idle_session()
        if session is None:
            raise RuntimeError('Get session failed')
        self._add_session_to_active(session)

        try:
            resp = session.execute_json_with_parameter(stmt, params)

            # reset the space name to the pool config
            json_obj = json.loads(resp)
            if json_obj["results"][0]["spaceName"] != self._space_name:
                self._set_space_to_default(session)

            # move the session back to the idle list
            self._return_session(session)

            return resp
        except Exception as e:
            logger.error('Execute failed: {}'.format(e))
            # remove the session from the pool if it is invalid
            self._active_sessions.remove(session)
            raise e

    def close(self):
        """log out all sessions and close all connections

        :return: void
        """
        with self._lock:
            for session in self._idle_sessions:
                session._sign_out()
                session._connection.close()
            for session in self._active_sessions:
                session._sign_out()
                session._connection.close()
            self._idle_sessions.clear()
            self._close = True

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

    def _get_idle_session(self):
        """get a valid session from the pool idle list.

        :return: Session
        """
        with self._lock:
            if len(self._idle_sessions) > 0:
                return self._idle_sessions.pop(0)
            elif len(self._active_sessions) < self._configs.max_size:
                return self._new_session()
            else:
                raise NoValidSessionException(
                    'The total number of sessions reaches the pool max size {}'.format(
                        self._configs.max_size
                    )
                )

    def _new_session(self):
        """construct a new session with the username and password in the pool.
            also, the session is bound to the space specified in the configs.

        :return: Session
        """
        if self._ssl_configs is not None:
            raise RuntimeError('SSL is not supported yet')

        self._pos = (self._pos + 1) % len(self._addresses)
        next_addr_index = self._pos

        # try to connect with a valid service address, the worst case it to iterate all addresses
        retries = len(self._addresses)

        while retries > 0:
            addr = self._addresses[next_addr_index]

            # if the address is bad, skip it
            if self._addresses_status[addr] == self.S_BAD:
                logger.warning('The graph service {} is not available'.format(addr))
                retries = retries - 1
                next_addr_index = (next_addr_index + 1) % len(self._addresses)
                continue

            # connect to the valid service
            connection = Connection()
            try:
                connection.open(addr[0], addr[1], self._configs.timeout)
                auth_result = connection.authenticate(self._username, self._password)
                session = Session(connection, auth_result, self, False)

                # switch to the space specified in the configs
                resp = session.execute('USE {}'.format(self._space_name))
                if not resp.is_succeeded():
                    raise RuntimeError(
                        'Failed to get session, cannot set the session space to {} error: {} {}'.format(
                            self._space_name, resp.error_code(), resp.error_msg()
                        )
                    )
                return session
            except AuthFailedException as e:
                # if auth failed because of credentials, close the pool
                if e.message.find("Invalid password") or e.message.find(
                    "User not exist"
                ):
                    logger.error(
                        'Authentication failed, because of bad credentials, close the pool {}'.format(
                            e
                        )
                    )
                    self.close()
                raise e
            except Exception:
                raise

        raise RuntimeError(
            'Failed to get a valid session, no graph service is available'
        )

    def _return_session(self, session):
        """return the session to the pool idle list when query finished.

        :param session: the session to return
        :return: void
        """
        with self._lock:
            self._active_sessions.remove(session)
            self._idle_sessions.append(session)
            session.idle_time_start = time.time()

    def _add_session_to_idle(self, session):
        """add the session to the pool idle list

        :param session: the session to add
        :return: void
        """
        with self._lock:
            self._idle_sessions.append(session)
            session.idle_time_start = time.time()

    def _add_session_to_active(self, session):
        """add the session to the pool active list

        :param session: the session to add
        :return: void
        """
        with self._lock:
            self._active_sessions.append(session)
            # reset the idle time start
            session.idle_time_start = 0

    def _set_space_to_default(self, session):
        """set the space to the default space in the pool

        :param session: the session to set
        :return: void
        """
        try:
            resp = session.execute('USE {}'.format(self._space_name))
            if not resp.is_succeeded():
                raise RuntimeError(
                    'Failed to set the session space to {}'.format(self._space_name)
                )
        except Exception:
            logger.warning(
                'Failed to set the session space to {}, the current session has been dropped'.format(
                    self._space_name
                )
            )
            session._connection.close()
            with self._lock:
                self._active_sessions.remove(session)

    def _remove_idle_unusable_session(self):
        if self._configs.idle_time == 0:
            return
        with self._lock:
            total_sessions = len(self._idle_sessions) + len(self._active_sessions)
            if total_sessions <= self._configs.min_size:
                return
            for session in self._idle_sessions:
                # calc session idle time
                idle_time = time.time() - session._idle_time_start

                # release idle session and remove from the pool
                if idle_time > self._configs.idle_time:
                    session.release()
                    session._connection.close()
                    self._idle_sessions.remove(session)

    def _period_detect(self):
        """periodically detect the services status and remove the sessions from the idle list if they expire"""
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
        if self._configs.timeout < 0:
            raise RuntimeError('The timeout must be greater or equal to 0')

        if self._space_name == "":
            raise RuntimeError('The space_name must be set')
        if self._username == "":
            raise RuntimeError('The username must be set')
        if self._password == "":
            raise RuntimeError('The password must be set')
        if self._addresses is None or len(self._addresses) == 0:
            raise RuntimeError('The addresses must be set')
