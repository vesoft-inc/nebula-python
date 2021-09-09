# --coding:utf-8--
#
# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import logging
import time

from nebula2.fbthrift.transport import TSocket, TTransport
from nebula2.fbthrift.transport.TTransport import TTransportException
from nebula2.fbthrift.protocol import TBinaryProtocol

from nebula2.common.ttypes import ErrorCode
from nebula2.graph import GraphService

from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
)

from nebula2.gclient.net.AuthResult import AuthResult


class Connection(object):

    def __init__(self):
        self._connection = None
        self.start_use_time = time.time()
        self._ip = None
        self._port = None
        self._timeout = 0
        self._is_used = False

    def __str__(self):
        return "{}:{} used: {}".format(self._ip, self._port, self._is_used)

    def is_used(self):
        return self._is_used

    def set_used(self, used=True):
        self._is_used = used

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
        if self._is_used:
            return 0
        return (time.time() - self.start_use_time) * 1000

    def get_address(self):
        """get the address of the connected service

        :return: (ip, port)
        """
        return (self._ip, self._port)
