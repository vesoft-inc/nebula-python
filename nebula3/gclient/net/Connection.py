# --coding:utf-8--
#
# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import time
import ssl

from nebula3.fbthrift.transport import (
    TSocket,
    TSSLSocket,
    TTransport,
    THeaderTransport,
    THttp2Client,
)
from nebula3.fbthrift.transport.TTransport import TTransportException
from nebula3.fbthrift.protocol import THeaderProtocol, TBinaryProtocol

from nebula3.common.ttypes import ErrorCode
from nebula3.graph import GraphService
from nebula3.graph.ttypes import VerifyClientVersionReq
from nebula3.logger import logger

from nebula3.Exception import (
    AuthFailedException,
    IOErrorException,
    ClientServerIncompatibleException,
)

from nebula3.gclient.net.AuthResult import AuthResult


class Connection(object):
    is_used = False

    def __init__(self):
        self._connection = None
        self.start_use_time = time.time()
        self._ip = None
        self._port = None
        self._timeout = 0
        self._ssl_conf = None
        self.use_http2 = False
        self.http_headers = None
        self._closed = True

    def open(self, ip, port, timeout, use_http2=False, http_headers=None):
        """open the connection

        :param ip: the server ip
        :param port: the server port
        :param timeout: the timeout for connect and execute
        :param use_http2: use http2 or not
        :param http_headers: http headers
        :return: void
        """
        self.open_SSL(ip, port, timeout, None, use_http2, http_headers)

    def open_SSL(
        self, ip, port, timeout, ssl_config=None, use_http2=False, http_headers=None
    ):
        """open the SSL connection

        :param ip: the server ip
        :param port: the server port
        :param timeout: the timeout for connect and execute
        :ssl_config: configs for SSL
        :param use_http2: use http2 or not
        :param http_headers: http headers
        :return: void
        """
        self._ip = ip
        self._port = port
        self._timeout = timeout
        self._ssl_conf = ssl_config
        self.use_http2 = use_http2
        self.http_headers = http_headers
        try:
            if use_http2 is False:
                protocol = self.__get_protocol(timeout, ssl_config)
            else:
                protocol = self.__get_protocal_http2(timeout, ssl_config, http_headers)
            self._connection = GraphService.Client(protocol)
            resp = self._connection.verifyClientVersion(VerifyClientVersionReq())
            if resp.error_code != ErrorCode.SUCCEEDED:
                self._connection._iprot.trans.close()
                raise ClientServerIncompatibleException(resp.error_msg)
        except Exception as e:
            self.close()
            raise
        self._closed = False

    def __get_protocol(self, timeout, ssl_config):
        try:
            if ssl_config is not None:
                s = TSSLSocket.TSSLSocket(
                    self._ip,
                    self._port,
                    ssl_config.unix_socket,
                    ssl_config.ssl_version,
                    ssl_config.cert_reqs,
                    ssl_config.ca_certs,
                    ssl_config.verify_name,
                    ssl_config.keyfile,
                    ssl_config.certfile,
                    ssl_config.allow_weak_ssl_versions,
                )
            else:
                s = TSocket.TSocket(self._ip, self._port)
            if timeout > 0:
                s.setTimeout(timeout)

            buffered_transport = TTransport.TBufferedTransport(s)
            header_transport = THeaderTransport.THeaderTransport(buffered_transport)
            protocol = THeaderProtocol.THeaderProtocol(header_transport)
            header_transport.open()
        except Exception as e:
            raise
        return protocol

    def __get_protocal_http2(self, timeout, ssl_config, http_headers):
        verify, certfile, keyfile, password = None, None, None, None
        if ssl_config is not None:
            # verify could be a boolean or ssl.SSLContext in httpx.
            verify = ssl.create_default_context(cafile=ssl_config.ca_certs)
            certfile = ssl_config.certfile
            keyfile = ssl_config.keyfile
            url = "https://" + self._ip + ":" + str(self._port)
        else:
            url = "http://" + self._ip + ":" + str(self._port)
        try:
            transport = THttp2Client.THttp2Client(
                url, timeout, verify, certfile, keyfile, password, http_headers
            )
            transport.open()
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
        except Exception as e:
            raise
        return protocol

    def _reopen(self):
        """reopen the connection

        :return:
        """
        self.close()
        if self._ssl_conf is not None:
            self.open_SSL(
                self._ip,
                self._port,
                self._timeout,
                self._ssl_conf,
                self.use_http2,
                self.http_headers,
            )
            self._closed = False
        else:
            self.open(
                self._ip, self._port, self._timeout, self.use_http2, self.http_headers
            )

    def authenticate(self, user_name, password):
        """authenticate to graphd

        :param user_name: the user name
        :param password: the password
        :return:
        """
        try:
            resp = self._connection.authenticate(user_name, password)
            if resp.error_code != ErrorCode.SUCCEEDED:
                self._connection.is_used = False
                raise AuthFailedException(resp.error_msg)
            return AuthResult(
                resp.session_id, resp.time_zone_offset_seconds, resp.time_zone_name
            )
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
        return self.execute_parameter(session_id, stmt, None)

    def execute_parameter(self, session_id, stmt, params):
        """execute interface with session_id and ngql
        :param session_id: the session id get from result of authenticate interface
        :param stmt: the ngql
        :param params: parameter map
        :return: ExecutionResponse
        """
        try:
            resp = self._connection.executeWithParameter(session_id, stmt, params)
            return resp
        except Exception as te:
            if isinstance(te, TTransportException):
                if te.message.find("timed out") > 0:
                    self._reopen()
                    raise IOErrorException(IOErrorException.E_TIMEOUT, te.message)
                elif te.type == TTransportException.END_OF_FILE:
                    raise IOErrorException(
                        IOErrorException.E_CONNECT_BROKEN, te.message
                    )
                elif te.type == TTransportException.NOT_OPEN:
                    raise IOErrorException(IOErrorException.E_NOT_OPEN, te.message)
                else:
                    raise IOErrorException(IOErrorException.E_UNKNOWN, te.message)
            raise

    def execute_json(self, session_id, stmt):
        """execute_json interface with session_id and ngql
        :param session_id: the session id get from result of authenticate interface
        :param stmt: the ngql
        :return: string json representing the execution result
        """
        return self.execute_json_with_parameter(session_id, stmt, None)

    def execute_json_with_parameter(self, session_id, stmt, params):
        """execute_json interface with session_id and ngql with parameter
        :param session_id: the session id get from result of authenticate interface
        :param stmt: the ngql
        :param params: parameter map
        :return: json bytes representing the execution result
        """
        try:
            resp = self._connection.executeJsonWithParameter(session_id, stmt, params)
            if not isinstance(resp, bytes):
                raise TypeError("response is not bytes")
            else:
                return resp
        except Exception as te:
            if isinstance(te, TTransportException):
                if te.message.find("timed out") > 0:
                    self._reopen()
                    raise IOErrorException(IOErrorException.E_TIMEOUT, te.message)
                elif te.type == TTransportException.END_OF_FILE:
                    raise IOErrorException(
                        IOErrorException.E_CONNECT_BROKEN, te.message
                    )
                elif te.type == TTransportException.NOT_OPEN:
                    raise IOErrorException(IOErrorException.E_NOT_OPEN, te.message)
                else:
                    raise IOErrorException(IOErrorException.E_UNKNOWN, te.message)
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
            if not self._closed:
                self._connection._iprot.trans.close()
                self._closed = True
        except Exception as e:
            logger.error(
                "Close connection to {}:{} failed:{}".format(self._ip, self._port, e)
            )

    def ping(self):
        """check the connection if ok
        :return: True or False
        """
        try:
            resp = self._connection.execute(0, "YIELD 1;")
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
        if self.is_used:
            return 0
        return (time.time() - self.start_use_time) * 1000

    def get_address(self):
        """get the address of the connected service

        :return: (ip, port)
        """
        return (self._ip, self._port)
