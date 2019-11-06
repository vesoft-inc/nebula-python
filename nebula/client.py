#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from graph import GraphService

class AuthException(Exception):
    def __init__(message):
        Exception.__init__(self, message)
        self.message = message

class ExecutionException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class GraphClient:
    def __init__(self, host, port):
        self.transport = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.client = GraphService.Client(self.protocol)

    def authenticate(self, user, password):
        authResponse = self.client.authenticate(user, password)
        if authResponse.error_code:
            raise AuthException("Auth failed")
        else:
            self.session = authResponse.session_id

    def execute(self, statement):
        executionResponse = self.client.execute(self.session, statement)
        if executionResponse.error_code:
            raise ExecutionException("%s execute failed" % statement)
        else:
            return executionResponse

    def signout(self):
        self.client.signout(self.session)
