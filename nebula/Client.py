# signout--coding:utf-8--

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from .Common import *


class GraphClient(object):

    def __init__(self, pool):
        """Initializer
        Arguments:
            - pool: the connection pool instance
        Returns: empty
        """
        self._pool = pool
        self._client = self._pool.get_connection()
        self._session_id = 0

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
            resp = self._client.authenticate(user, password)
            if resp.error_code:
                return resp
            else:
                self._session_id = resp.session_id
                print("client: %d authenticate succeed" % self._session_id)
            return resp
        except Exception as x:
            raise AuthException("Auth failed: {}".format(x))

    def execute(self, statement):
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
            resp = self._client.execute(self._session_id, statement)
            return SimpleResponse(resp.error_code, resp.error_msg)
        except Exception as x:
            raise ExecutionException("Execute `{}' failed: {}".format(statement, x))

    def execute_query(self, statement):
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
            return self._client.execute(self._session_id, statement)
        except Exception as x:
            raise ExecutionException("Execute `{}' failed: {}".format(statement, x))

    def sign_out(self):
        """sign out: Users should call sign_out when catch the exception or exit
        """
        if self._client is None:
            return

        try:
            if self._session_id is not None:
                print('client: %d sign out' % self._session_id)
            self._client.signout(self._session_id)
            self._pool.return_connection(self._client)
        except Exception as x:
            raise Exception('SignOut failed: {}'.format(x))

    def is_none(self):
        """is_none: determine if the client creation was successful
        Returns:
            True or False
        """
        return self._client is None
