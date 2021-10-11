# --coding:utf-8--
#
# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import logging
import time

from nebula2.Exception import (
    IOErrorException,
    NotValidConnectionException,
)

from nebula2.data.ResultSet import ResultSet
from nebula2.gclient.net.AuthResult import AuthResult


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
        if self._connection is None:
            raise RuntimeError('The session has released')
        try:
            resp_json = self._connection.execute_json(self._session_id, stmt)
            return resp_json
        except IOErrorException as ie:
            if ie.type == IOErrorException.E_CONNECT_BROKEN:
                self._pool.update_servers_status()
                if self._retry_connect:
                    if not self._reconnect():
                        logging.warning('Retry connect failed')
                        raise IOErrorException(
                            IOErrorException.E_ALL_BROKEN, ie.message)
                    resp_json = self._connection.execute_json(
                        self._session_id, stmt)
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
