# --coding:utf-8--
#
# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import json
import time

from typing import TYPE_CHECKING

from nebula3.Exception import (
    IOErrorException,
    NotValidConnectionException,
)
from nebula3.common.ttypes import ErrorCode
from nebula3.data.ResultSet import ResultSet
from nebula3.gclient.net.AuthResult import AuthResult
from nebula3.gclient.net.base import BaseExecutor
from nebula3.logger import logger

if TYPE_CHECKING:
    from nebula3.gclient.net.ConnectionPool import ConnectionPool
    from nebula3.gclient.net.Connection import Connection


class Session(BaseExecutor, object):
    def __init__(
        self,
        connection: "Connection",
        auth_result: AuthResult,
        pool: "ConnectionPool",
        retry_connect=True,
        execution_retry_count=0,
        retry_interval_seconds=1,
    ):
        """
        Initialize the Session object.

        :param connection: The connection object associated with the session.
        :param auth_result: The result of the authentication process.
        :param pool: The pool object where the session was created.
        :param retry_connect: A boolean indicating whether to retry the connection if it fails.
        :param execution_retry_count: The number of attempts to retry the execution upon encountering an execution error(-1005), with the default being 0 (no retries).
        :param retry_interval_seconds: The interval between connection retries in seconds.
        """
        self._session_id = auth_result.get_session_id()
        self._timezone_offset = auth_result.get_timezone_offset()
        self._connection = connection
        self._timezone = 0
        # connection the where the session was created, if session pool was used
        self._pool = pool
        self._retry_connect = retry_connect
        self._execution_retry_count = execution_retry_count
        self._retry_interval_seconds = retry_interval_seconds
        # the time stamp when the session was added to the idle list of the session pool
        self._idle_time_start = 0

    def execute(self, stmt):
        """execute statement

        :param stmt: the ngql
        :return: ResultSet
        """
        return super().execute(stmt)

    def execute_parameter(self, stmt, params):
        """execute statement
        :param stmt: the ngql
        :param params: parameter map
        :return: ResultSet
        """
        if self._connection is None:
            raise RuntimeError("The session has been released")
        try:
            start_time = time.time()
            resp = self._connection.execute_parameter(self._session_id, stmt, params)
            end_time = time.time()

            if (
                self._execution_retry_count > 0
                and resp.error_code == ErrorCode.E_EXECUTION_ERROR
            ):
                for retry_count in range(1, self._execution_retry_count + 1):
                    logger.warning(
                        f"Execution error, retrying {retry_count}/{self._execution_retry_count} after {self._retry_interval_seconds}s"
                    )
                    time.sleep(self._retry_interval_seconds)
                    resp = self._connection.execute_parameter(
                        self._session_id, stmt, params
                    )
                    if resp.error_code != ErrorCode.E_EXECUTION_ERROR:
                        break

            return ResultSet(
                resp,
                all_latency=int((end_time - start_time) * 1000000),
                timezone_offset=self._timezone_offset,
            )
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
                if self._retry_connect:
                    if not self._reconnect():
                        logger.warning("Retry connect failed")
                        raise IOErrorException(
                            IOErrorException.E_ALL_BROKEN, ie.message
                        )
                    resp = self._connection.execute_parameter(
                        self._session_id, stmt, params
                    )
                    end_time = time.time()
                    return ResultSet(
                        resp,
                        all_latency=int((end_time - start_time) * 1000000),
                        timezone_offset=self._timezone_offset,
                    )
            raise
        except Exception:
            raise

    def execute_json(self, stmt):
        """execute statement and return the result as a JSON bytes
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
        :return: JSON bytes
        """
        return super().execute_json(stmt)

    def execute_json_with_parameter(self, stmt, params):
        """execute statement and return the result as a JSON bytes
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
        :param params: parameter map
        :return: JSON bytes
        """
        if self._connection is None:
            raise RuntimeError("The session has been released")
        try:
            resp_json = self._connection.execute_json_with_parameter(
                self._session_id, stmt, params
            )
            if self._execution_retry_count > 0:
                for retry_count in range(self._execution_retry_count):
                    if (
                        json.loads(resp_json).get("errors", [{}])[0].get("code")
                        != ErrorCode.E_EXECUTION_ERROR
                    ):
                        break
                    logger.warning(
                        "Execute failed, retry count:{}/{} in {} seconds".format(
                            retry_count + 1,
                            self._execution_retry_count,
                            self._retry_interval_seconds,
                        )
                    )
                    time.sleep(self._retry_interval_seconds)
                    resp_json = self._connection.execute_json_with_parameter(
                        self._session_id, stmt, params
                    )
            return resp_json

        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
                if self._retry_connect:
                    if not self._reconnect():
                        logger.warning("Retry connect failed")
                        raise IOErrorException(
                            IOErrorException.E_ALL_BROKEN, ie.message
                        )
                    resp_json = self._connection.execute_json_with_parameter(
                        self._session_id, stmt, params
                    )
                    return resp_json
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
        """ping at connection level check the connection is valid

        :return: True or False
        """
        if self._connection is None:
            return False
        return self._connection.ping()

    def ping_session(self):
        """ping at session level, check whether the session is usable"""
        resp = self.execute(r'RETURN "NEBULA PYTHON SESSION PING"')
        if resp.is_succeeded():
            return True
        else:
            logger.error(
                "failed to ping the session: error code:{}, error message:{}".format(
                    resp.error_code, resp.error_msg
                )
            )
            return False

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

    def _idle_time(self):
        """get idletime of connection

        :return: idletime
        """
        if self.is_used:
            return 0
        return (time.time() - self.start_use_time) * 1000

    def _sign_out(self):
        """sign out the session"""
        if self._connection is None:
            raise RuntimeError("The session has been released")
        self._connection.signout(self._session_id)
