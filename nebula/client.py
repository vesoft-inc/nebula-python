#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys

sys.path.insert(0, './dependence')
sys.path.insert(0, './gen-py')
sys.path.insert(0, './')

from graph import GraphService
from graph import ttypes
from ConnectionPool import ConnectionPool


class AuthException(Exception):
    def __init__(message):
        Exception.__init__(self, message)
        self.message = message


class ExecutionException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class SimpleResponse:
    """
    Attributes:
         - error_code
         - error_msg
    """
    def __init__(self, code, msg):
        self.error_code = code
        self.error_msg = msg


class GraphClient(object):

    def __init__(self, pool):
        """Initializer
        Arguments:
            pool: the connection pool instance
        Returns: empty
        """
        self._pool = pool
        self._client = self._pool.get_connection()

    def authenticate(self, user, password):
        """authenticate to graph server
        Arguments:
            user: the user name
            password: the password of user
        Returns:
            AuthResponse: the response of graph
            AuthResponse's attributes:
                - error_code
                - session_id
                - error_msg
        """
        authResponse = self._client.authenticate(user, password)
        if authResponse.error_code:
            raise AuthException("Auth failed")
        else:
            self._session_id = authResponse.session_id
        return authResponse

    def execute(self, statement):
        """execute statement to graph server
        Arguments:
            statement: the statement
        Returns:
            SimpleResponse: the response of graph
            SimpleResponse's attributes:
                - error_code
                - error_msg
        """
        resp = self._client.execute(self._session_id, statement)
        return SimpleResponse(resp.error_code, resp.error_msg)

    def executeQuery(self, statement):
        """execute query statement to graph server
        Arguments:
            statement: the statement
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
        executionResponse = self._client.execute(self._session_id, statement)
        if executionResponse.error_code:
            raise ExecutionException("Execute failed %s, error: %s" % (statement, executionResponse.error_msg))
        else:
            return executionResponse

    def signout(self):
        self._client.signout(self._session_id)
        self._pool.return_connection(self._client)
