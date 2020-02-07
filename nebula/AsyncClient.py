# signout--coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import asyncio

from .Common import *
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from graph import AsyncGraphService


class AsyncGraphClient(object):
    def __init__(self, ip, port, loop = None):
        """Initializer
        Arguments: empty
        Returns: empty
        """
        self._loop = loop or asyncio.get_event_loop()
        transport = TSocket.TSocket(ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        self._client = AsyncGraphService.Client(protocol)
        if self._client is None:
            print('client is None')
        self._iprot = protocol
        self._session_id = 0
        self._reqid_callback = {}

    def get_loop(self):
        return self._loop

    def authenticate(self, user, password):
        """authenticate to graph server
        Arguments:
            - user: the user name
            - password: the password of user
        Returns:
            AuthResponse: the response of graph
            AuthResponse's attributes:
                - error_code
                - session_id
                - error_msg
        """
        if self._client is None:
            raise AuthException("No client")

        try:
            fut = self._client.authenticate(user, password)
            (fname, mtype, rseqid) = self._iprot.readMessageBegin()
            self._client.recv_authenticate(self._iprot, mtype, rseqid)
            resp = fut.result()
            if resp.error_code == 0:
                self._session_id = resp.session_id
                print("client: %d authenticate succeed" % self._session_id)
            return resp
        except Exception as x:
            raise AuthException("Auth failed: {}".format(x))

    @asyncio.coroutine
    def async_execute(self, statement, callback=None):
        """execute statement to graph server
        Arguments:
            - statement: the statement
        Returns:
            SimpleResponse: the response of graph
            SimpleResponse's attributes:
                - error_code
                - error_msg
        """
        if self._client is None:
            raise ExecutionException("No client")

        try:
            print('async_execute: %s' % statement)
            fut = self._client.execute(self._session_id, statement)
            if callback is None:
                return
            self._reqid_callback[self._client._seqid] = callback
            yield from (asyncio.sleep(0))
            (fname, mtype, rseqid) = self._iprot.readMessageBegin()
            self._client.recv_execute(self._iprot, mtype, rseqid)
            resp = fut.result()
            cb = self._reqid_callback.get(rseqid)
            if cb is not None:
                callback(SimpleResponse(resp.error_code, resp.error_msg))
                self._reqid_callback.pop(rseqid)
        except Exception as x:
            raise ExecutionException("Execute `{}' failed: {}".format(statement, x))

    @asyncio.coroutine
    def async_execute_query(self, statement, callback=None):
        """execute query statement to graph server
        Arguments:
            - statement: the statement
        Returns:
            ExecutionResponse: the response of graph
            ExecutionResponse's attributes:
                - error_code
                - latency_in_us
                - error_msg
                - column_names
                - rows
                - space_name
        """
        if self._client is None:
            raise ExecutionException("No client")

        try:
            print('async_execute_query: %s' % statement)
            fut = self._client.execute(self._session_id, statement)
            if callback is None:
                return
            self._reqid_callback[self._client._seqid] = callback
            yield from (asyncio.sleep(0))
            (fname, mtype, rseqid) = self._iprot.readMessageBegin()
            self._client.recv_execute(self._iprot, mtype, rseqid)
            resp = fut.result()
            cb = self._reqid_callback.get(rseqid)
            if cb is not None:
                callback(resp)
                self._reqid_callback.pop(rseqid)
        except Exception as x:
            raise ExecutionException("Execute `{}' failed: {}".format(statement, x))

    def sign_out(self):
        """sign out: Users should call sign_out when catch the exception or exit
        """
        if self._client is None:
            return

        try:
            if self._session_id != 0:
                print('client: %d sign out' % self._session_id)
                self._client.signout(self._session_id)
        except Exception as x:
            raise Exception("SignOut failed: {}".format(x))

    def is_none(self):
        """is_none: determine if the client creation was successful
        Returns:
            True or False
        """
        return self._client is None
