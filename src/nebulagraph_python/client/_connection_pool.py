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

import copy
import logging
import threading
from dataclasses import dataclass, field

from anyio import Lock

from nebulagraph_python.client._connection import (
    AsyncConnection,
    Connection,
    ConnectionConfig,
)
from nebulagraph_python.data import HostAddress
from nebulagraph_python.error import PoolError

logger = logging.getLogger(__name__)


@dataclass
class AsyncConnectionPool:
    """Manages a pool of AsyncConnections with one connection per address.

    Uses round-robin strategy for getting connections.
    If ping is enabled in ConnectionConfig and fails, the connection will try to reconnect once.
    If still fails, the next address will be tried, until all addresses are tried and failed.

    This pool is async/coroutine-level safe but not thread-safe.
    """

    conn_conf: ConnectionConfig

    _connections: dict[HostAddress, AsyncConnection] = field(
        default_factory=dict, init=False
    )
    _current_index: int = field(default=0, init=False)
    _lock: Lock = field(default_factory=Lock, init=False)

    @property
    def addresses(self) -> list[HostAddress]:
        return self.conn_conf.hosts

    @property
    def current_address(self) -> HostAddress:
        return self.addresses[self._current_index]

    async def next_address(self) -> HostAddress:
        async with self._lock:
            self._current_index = (self._current_index + 1) % len(self.addresses)
            return self.addresses[self._current_index]

    def __post_init__(self):
        """Create a new connection for the specified host address, without connecting."""
        # Create a config with only this host
        for host_addr in self.addresses:
            copied_conf = copy.copy(self.conn_conf)
            copied_conf.hosts = [host_addr]
            copied_conf.ping_before_execute = (
                False  # Because ping will be done when borrowing a connection
            )
            conn = AsyncConnection(copied_conf)
            self._connections[host_addr] = conn

    async def connect(self):
        for conn in self._connections.values():
            await conn.connect()

    async def get_connection(self, host_addr: HostAddress) -> AsyncConnection | None:
        conn = self._connections[host_addr]
        if self.conn_conf.ping_before_execute and not await conn.ping():
            try:
                await conn.reconnect()
            except Exception:
                logger.exception("Error reconnecting to server %s", host_addr)
                return None
        return self._connections[host_addr]

    async def next_connection(self) -> tuple[HostAddress, AsyncConnection]:
        for _ in range(len(self.addresses)):
            host_addr = await self.next_address()
            conn = await self.get_connection(host_addr)
            if conn is not None:
                return host_addr, conn
            else:
                continue
        raise PoolError("No connection available in the pool")

    async def close(self):
        """Close all connections in the pool."""
        for conn in self._connections.values():
            await conn.close()
        self._connections.clear()


@dataclass
class ConnectionPool:
    """Manages a pool of Connections with one connection per address.

    Uses round-robin strategy for getting connections.
    If ping is enabled in ConnectionConfig and fails, the connection will try to reconnect once.
    If still fails, the next address will be tried, until all addresses are tried and failed.

    This pool is thread-safe.
    """

    conn_conf: ConnectionConfig

    _connections: dict[HostAddress, Connection] = field(
        default_factory=dict, init=False
    )
    _current_index: int = field(default=0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    @property
    def addresses(self) -> list[HostAddress]:
        return self.conn_conf.hosts

    @property
    def current_address(self) -> HostAddress:
        return self.addresses[self._current_index]

    def next_address(self) -> HostAddress:
        with self._lock:
            self._current_index = (self._current_index + 1) % len(self.addresses)
            return self.addresses[self._current_index]

    def __post_init__(self):
        """Create a new connection for the specified host address, without connecting."""
        # Create a config with only this host
        for host_addr in self.addresses:
            copied_conf = copy.copy(self.conn_conf)
            copied_conf.hosts = [host_addr]
            copied_conf.ping_before_execute = (
                False  # Because ping will be done when borrowing a connection
            )
            conn = Connection(copied_conf)
            self._connections[host_addr] = conn

    def connect(self):
        for conn in self._connections.values():
            conn.connect()

    def get_connection(self, host_addr: HostAddress) -> Connection | None:
        conn = self._connections[host_addr]
        if self.conn_conf.ping_before_execute and not conn.ping():
            try:
                conn.reconnect()
            except Exception:
                logger.exception("Error reconnecting to server %s", host_addr)
                return None
        return self._connections[host_addr]

    def next_connection(self) -> tuple[HostAddress, Connection]:
        for _ in range(len(self.addresses)):
            host_addr = self.next_address()
            conn = self.get_connection(host_addr)
            if conn is not None:
                return host_addr, conn
            else:
                continue
        raise PoolError("No connection available in the pool")

    def close(self):
        """Close all connections in the pool."""
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()
