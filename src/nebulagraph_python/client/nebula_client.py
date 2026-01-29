# Copyright 2025 vesoft-inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""NebulaClient implementation matching Java NebulaClient"""

import asyncio
import logging
import random
import threading
import time
from typing import Dict, List, Optional, TYPE_CHECKING

import grpc

from nebulagraph_python._error_code import ErrorCode
from nebulagraph_python.client._connection import GrpcConnection, AsyncConnection, ConnectionConfig
from nebulagraph_python.client.auth_result import AuthResult
from nebulagraph_python.client.base_executor import NebulaBaseExecutor, NebulaBaseAsyncExecutor
from nebulagraph_python.client.constants import (
    DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_ENABLE_TLS,
    DEFAULT_MAX_TIMEOUT_MS,
    DEFAULT_PING_TIMEOUT_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    DEFAULT_SCAN_PARALLEL,
)
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import (
    AuthenticatingError,
    ExecutingError,
    NebulaGraphRemoteError,
)
from nebulagraph_python.result_set import ResultSet

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class NebulaClient(NebulaBaseExecutor):
    """Client to connect to NebulaGraph, matching Java NebulaClient"""

    def __init__(
        self,
        addresses: str,
        user_name: str = None,
        password: Optional[str] = None,
        *,
        connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
        request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
        server_ping_timeout_ms: int = DEFAULT_PING_TIMEOUT_MS,
        scan_parallel: int = DEFAULT_SCAN_PARALLEL,
        enable_tls: bool = DEFAULT_ENABLE_TLS,
        ssl_param: Optional[SSLParam] = None,
        auth_options: Optional[Dict[str, object]] = None,
    ):
        """Initialize NebulaClient with configuration parameters

        Args:
            addresses: NebulaGraph server addresses (e.g., "127.0.0.1:9669,127.0.0.2:9669")
            user_name: Username for authentication
            password: Password for authentication
            connect_timeout_ms: Connection timeout in milliseconds
            request_timeout_ms: Request timeout in milliseconds
            server_ping_timeout_ms: Server ping timeout in milliseconds
            scan_parallel: Scan parallel degree
            enable_tls: Enable TLS connection
            ssl_param: SSL parameters
            auth_options: Additional authentication options
        """
        self.servers: List[HostAddress] = self._validate_address(addresses)
        self.user_name: str = user_name
        self.password: Optional[str] = password
        self.auth_options: Dict[str, object] = auth_options or {}

        if password:
            self.auth_options["password"] = password

        self.connect_timeout_mills: int = connect_timeout_ms
        self.request_timeout_mills: int = request_timeout_ms
        self.server_ping_timeout_mills: int = server_ping_timeout_ms
        self.scan_parallel: int = scan_parallel
        self.enable_tls: bool = enable_tls
        self.ssl_param: Optional[SSLParam] = ssl_param

        self.connection: Optional[GrpcConnection] = None
        self.session_id: int = -1
        self.version: str = ""
        self.create_time: int = 0
        self.is_closed: bool = False
        self._lock = threading.Lock()

        self._init_client()

    def execute(
        self,
        statement: str,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        """Execute a query with optional timeout"""
        if timeout is None:
            timeout = self.request_timeout_mills
        return self.execute_with_timeout(statement, int(timeout))

    def execute_with_timeout(self, gql: str, request_timeout: int) -> ResultSet:
        """Execute a query with custom timeout"""
        with self._lock:
            self._check_closed()
            response = self.connection.execute_default_timeout(
                self.session_id, gql
            )
        return ResultSet(response)

    def get_session_id(self) -> int:
        """Get the session ID"""
        return self.session_id

    def get_version(self) -> str:
        """Get the server version"""
        return self.version

    def get_create_time(self) -> int:
        """Get the creation time"""
        return self.create_time

    def get_host(self) -> str:
        """Get the connected host address"""
        if self.connection:
            return str(self.connection.get_server_address())
        return ""

    def get_connect_timeout_mills(self) -> int:
        """Get the connection timeout"""
        return self.connect_timeout_mills

    def get_request_timeout_mills(self) -> int:
        """Get the request timeout"""
        return self.request_timeout_mills

    def get_scan_parallel(self) -> int:
        """Get the scan parallel"""
        return self.scan_parallel

    def ping(self, timeout_ms: int = DEFAULT_PING_TIMEOUT_MS) -> bool:
        """Ping the server"""
        with self._lock:
            self._check_closed()
            try:
                return self.connection.ping(self.session_id, timeout_ms)
            except ExecutingError as e:
                logger.error(f"ping error for host {self.get_host()}: {e}")
                return False

    def close(self) -> None:
        """Close the client"""
        with self._lock:
            if not self.is_closed:
                self.is_closed = True
                if self.connection is not None:
                    try:
                        self.connection.execute(
                            self.session_id, "SESSION CLOSE", 1000
                        )
                        self.connection.close()
                    except Exception as e:
                        logger.warn(f"signout failed: {e}")
                self.connection = None

    def is_closed_client(self) -> bool:
        """Check if the client is closed"""
        return self.is_closed

    def _check_closed(self) -> None:
        """Check if the client is closed and raise exception if so"""
        if self.is_closed:
            raise RuntimeError("The NebulaClient already closed.")

    def _init_client(self) -> None:
        """Initialize the client connection"""
        auth_result: Optional[AuthResult] = None
        self.connection = GrpcConnection()

        try_connect_times = len(self.servers)
        random.shuffle(self.servers)

        while try_connect_times > 0:
            try_connect_times -= 1
            try:
                self.connection.open(self.servers[try_connect_times], self)
                auth_result = self.connection.authenticate(
                    self.user_name, self.auth_options
                )
                self.session_id = auth_result.get_session_id()
                self.version = auth_result.get_version()
                self.create_time = int(time.time() * 1000)
                break
            except AuthenticatingError as e:
                logger.error(f"create NebulaClient failed: {e}")
                raise
            except Exception as e:
                if try_connect_times == 0:
                    logger.error(f"create NebulaClient failed: {e}")
                    raise

    @staticmethod
    def _validate_address(addresses: str) -> List[HostAddress]:
        """Validate and parse addresses"""
        result = []
        if isinstance(addresses, str):
            for addr in addresses.split(","):
                addr = addr.strip()
                if ":" in addr:
                    host, port = addr.rsplit(":", 1)
                    result.append(HostAddress(host, int(port)))
                else:
                    raise ValueError(f"Invalid address format: {addr}")
        return result


class AsyncNebulaClient(NebulaBaseAsyncExecutor):
    """Async client to connect to NebulaGraph, matching Java NebulaClient with async support"""

    def __init__(
        self,
        addresses: str,
        user_name: str = None,
        password: Optional[str] = None,
        *,
        connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
        request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS,
        server_ping_timeout_ms: int = DEFAULT_PING_TIMEOUT_MS,
        scan_parallel: int = DEFAULT_SCAN_PARALLEL,
        enable_tls: bool = DEFAULT_ENABLE_TLS,
        ssl_param: Optional[SSLParam] = None,
        auth_options: Optional[Dict[str, object]] = None,
    ):
        """Initialize AsyncNebulaClient with configuration parameters

        Args:
            addresses: NebulaGraph server addresses (e.g., "127.0.0.1:9669,127.0.0.2:9669")
            user_name: Username for authentication
            password: Password for authentication
            connect_timeout_ms: Connection timeout in milliseconds
            request_timeout_ms: Request timeout in milliseconds
            server_ping_timeout_ms: Server ping timeout in milliseconds
            scan_parallel: Scan parallel degree
            enable_tls: Enable TLS connection
            ssl_param: SSL parameters
            auth_options: Additional authentication options
        """
        self.servers: List[HostAddress] = self._validate_address(addresses)
        self.user_name: str = user_name
        self.password: Optional[str] = password
        self.auth_options: Dict[str, object] = auth_options or {}

        if password:
            self.auth_options["password"] = password

        self.connect_timeout_mills: int = connect_timeout_ms
        self.request_timeout_mills: int = request_timeout_ms
        self.server_ping_timeout_mills: int = server_ping_timeout_ms
        self.scan_parallel: int = scan_parallel
        self.enable_tls: bool = enable_tls
        self.ssl_param: Optional[SSLParam] = ssl_param

        self.connection: Optional[AsyncConnection] = None
        self.session_id: int = -1
        self.version: str = ""
        self.create_time: int = 0
        self.is_closed: bool = False
        self._lock = asyncio.Lock()

    async def execute(
        self,
        statement: str,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        """Execute a query with optional timeout"""
        if timeout is None:
            timeout = self.request_timeout_mills
        return await self.execute_with_timeout(statement, int(timeout))

    async def execute_with_timeout(self, gql: str, request_timeout: int) -> ResultSet:
        """Execute a query with custom timeout"""
        async with self._lock:
            self._check_closed()
            response = await self.connection.execute_default_timeout(
                self.session_id, gql
            )
        return ResultSet(response)

    def get_session_id(self) -> int:
        """Get the session ID"""
        return self.session_id

    def get_version(self) -> str:
        """Get the server version"""
        return self.version

    def get_create_time(self) -> int:
        """Get the creation time"""
        return self.create_time

    def get_host(self) -> str:
        """Get the connected host address"""
        if self.connection:
            return str(self.connection.server_addr)
        return ""

    def get_connect_timeout_mills(self) -> int:
        """Get the connection timeout"""
        return self.connect_timeout_mills

    def get_request_timeout_mills(self) -> int:
        """Get the request timeout"""
        return self.request_timeout_mills

    def get_scan_parallel(self) -> int:
        """Get the scan parallel"""
        return self.scan_parallel

    async def ping(self, timeout_ms: int = DEFAULT_PING_TIMEOUT_MS) -> bool:
        """Ping the server"""
        async with self._lock:
            self._check_closed()
            try:
                return await self.connection.ping(self.session_id, timeout_ms)
            except ExecutingError as e:
                logger.error(f"ping error for host {self.get_host()}: {e}")
                return False

    async def close(self) -> None:
        """Close the client"""
        async with self._lock:
            if not self.is_closed:
                self.is_closed = True
                if self.connection is not None:
                    try:
                        await self.connection.execute(
                            self.session_id, "SESSION CLOSE", 1000
                        )
                        await self.connection.close()
                    except Exception as e:
                        logger.warn(f"signout failed: {e}")
                self.connection = None

    def is_closed_client(self) -> bool:
        """Check if the client is closed"""
        return self.is_closed

    def _check_closed(self) -> None:
        """Check if the client is closed and raise exception if so"""
        if self.is_closed:
            raise RuntimeError("The AsyncNebulaClient already closed.")

    async def _init_client(self) -> None:
        """Initialize the client connection"""
        auth_result: Optional[AuthResult] = None

        # Create connection config
        config = ConnectionConfig.from_defaults(
            hosts=self.servers,
            ssl_param=self.enable_tls or self.ssl_param,
            connect_timeout=self.connect_timeout_mills / 1000.0,
            request_timeout=self.request_timeout_mills / 1000.0,
        )
        if self.ssl_param:
            config.ssl_param = self.ssl_param

        self.connection = AsyncConnection(config)

        try_connect_times = len(self.servers)
        random.shuffle(self.servers)

        while try_connect_times > 0:
            try_connect_times -= 1
            try:
                await self.connection.connect(self.servers[try_connect_times])
                auth_result = await self.connection.authenticate(
                    self.user_name, self.auth_options
                )
                self.session_id = auth_result.get_session_id()
                self.version = auth_result.get_version()
                self.create_time = int(time.time() * 1000)
                break
            except AuthenticatingError as e:
                logger.error(f"create AsyncNebulaClient failed: {e}")
                raise
            except Exception as e:
                if try_connect_times == 0:
                    logger.error(f"create AsyncNebulaClient failed: {e}")
                    raise

    @staticmethod
    def _validate_address(addresses: str) -> List[HostAddress]:
        """Validate and parse addresses"""
        result = []
        if isinstance(addresses, str):
            for addr in addresses.split(","):
                addr = addr.strip()
                if ":" in addr:
                    host, port = addr.rsplit(":", 1)
                    result.append(HostAddress(host, int(port)))
                else:
                    raise ValueError(f"Invalid address format: {addr}")
        return result