import logging
import time
from contextlib import contextmanager
from copy import copy
from dataclasses import dataclass
from itertools import cycle
from threading import Lock
from typing import Any, Dict, Iterator, List, Literal, Optional, Union

from nebulagraph_python.client import constants
from nebulagraph_python.client._connection import (
    ConnectionConfig,
    _parse_hosts,
)
from nebulagraph_python.client._session import SessionConfig
from nebulagraph_python.client.base_executor import NebulaBaseExecutor
from nebulagraph_python.client.client import NebulaClient
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import InternalError, PoolError

logger = logging.getLogger(__name__)


@dataclass
class NebulaPoolConfig:
    """Configuration for the NebulaGraph connection pool"""

    max_client_size: int = constants.DEFAULT_MAX_CLIENT_SIZE
    min_client_size: int = constants.DEFAULT_MIN_CLIENT_SIZE
    test_on_borrow: bool = constants.DEFAULT_TEST_ON_BORROW
    strictly_server_healthy: bool = constants.DEFAULT_STRICTLY_SERVER_HEALTHY
    max_wait: float = constants.DEFAULT_MAX_WAIT


class NebulaPool(NebulaBaseExecutor):
    """A connection pool that manages multiple NebulaGraph clients with round-robin load balancing.
    Safe for thread-level concurrency, not async/coroutine-level.

    Required to explicitly call `close()` to release all resources.
    """

    # Config
    hosts: List[HostAddress]
    username: str
    password: str
    ssl_param: Union[SSLParam, Literal[True], None]
    auth_options: Optional[Dict[str, Any]]
    pool_config: NebulaPoolConfig
    session_config: Optional[SessionConfig]
    conn_config: Optional[ConnectionConfig]

    # Owned Resources
    _clients: List[NebulaClient]

    # State
    _lock: Lock
    _client_cycle: Iterator[NebulaClient]
    _hosts_cycle: Iterator[HostAddress]
    _in_use: Dict[NebulaClient, bool]  # Track if client is in use

    def __init__(
        self,
        hosts: Union[str, List[str], List[HostAddress]],
        username: str,
        password: str,
        *,
        ssl_param: Union[SSLParam, Literal[True], None] = None,
        auth_options: Optional[Dict[str, Any]] = None,
        pool_config: Optional[NebulaPoolConfig] = None,
        session_config: Optional[SessionConfig] = None,
        conn_config: Optional[ConnectionConfig] = None,
    ):
        """Initialize NebulaGraph connection pool

        Args:
        ----
            hosts: Single host string ("hostname:port"), list of host strings,
                  or list of HostAddress objects
            username: Username for authentication
            password: Password for authentication
            ssl_param: SSL configuration
            auth_options: dict of authentication options
            pool_config: Pool configuration
            session_config: Session configuration
            connection_config: Connection configuration. If provided,
                               it will override the hosts and ssl_param
        """
        self.hosts = (
            _parse_hosts(hosts)
            if (conn_config is None or not conn_config.hosts)
            else conn_config.hosts
        )
        self.username = username
        self.password = password
        self.ssl_param = ssl_param
        self.auth_options = auth_options
        self.pool_config = pool_config or NebulaPoolConfig()
        self.session_config = session_config
        self.conn_config = conn_config

        self._clients = []
        self._lock = Lock()
        self._in_use = {}  # Initialize tracking dict
        self._hosts_cycle = cycle(self.hosts)

        # Initialize the client pool
        self.fulfill_pool()

    def fulfill_pool(self, locked: bool = False):
        """May raise exception with partial success"""
        to_fill_num = max(self.pool_config.max_client_size - len(self._clients), 0)

        def _inner_default() -> None:
            for _ in range(to_fill_num):
                client = NebulaClient(
                    hosts=self.hosts,
                    username=self.username,
                    password=self.password,
                    ssl_param=self.ssl_param,
                    auth_options=self.auth_options,
                    conn_config=self.conn_config,
                    session_config=self.session_config,
                )
                self._clients.append(client)
                self._in_use[client] = False
            # Initialize the round-robin cycle
            self._client_cycle = cycle(self._clients)

        def _inner_for_strictly_server_healthy() -> None:
            # When new pool is created and strictly_server_healthy is True,
            # we need to connect to all hosts
            for _ in range(len(self.hosts)):
                # Round-robin host address selection
                host = next(self._hosts_cycle)
                conn_config = None
                if self.conn_config:
                    conn_config = copy(self.conn_config)
                    conn_config.hosts = [host]

                client = NebulaClient(
                    hosts=[host],
                    username=self.username,
                    password=self.password,
                    ssl_param=self.ssl_param,
                    auth_options=self.auth_options,
                    conn_config=self.conn_config,
                    session_config=self.session_config,
                )
                if len(self._clients) < self.pool_config.max_client_size:
                    self._clients.append(client)
                    self._in_use[client] = False
                else:
                    client.close()
            # Initialize the round-robin cycle
            self._client_cycle = cycle(self._clients)

        def _inner():
            try:
                if not self.pool_config.strictly_server_healthy:
                    _inner_default()
                else:
                    _inner_for_strictly_server_healthy()
            except Exception as e:
                self._client_cycle = cycle(self._clients)
                raise e

        if not locked:
            with self._lock:
                _inner()
        else:
            _inner()

    def kick_from_pool(self, client: NebulaClient, locked: bool = False) -> None:
        """Kick a client from the pool and close its connection.

        Args:
        ----
            client: The client to kick from the pool

        Raises:
        ------
            InternalError: If the client is not from this pool
        """
        if client not in self._clients:
            raise InternalError("Client does not belong to this pool")

        def _inner():
            self._clients.remove(client)
            self._in_use.pop(client)
            # Close the client connection
            client.close()
            # Recreate the cycle with remaining clients
            if len(self._clients) < self.pool_config.min_client_size:
                try:
                    self.fulfill_pool(locked=True)
                except Exception:
                    logger.exception("Failed or partial success when fulfilling pool")
            else:
                self._client_cycle = cycle(self._clients)

        if not locked:
            with self._lock:
                _inner()
        else:
            _inner()

    def get_client(self) -> NebulaClient:
        """Get the next available client using round-robin selection.

        Returns:
        -------
            NebulaClient: The next available client from the pool

        Raises:
        ------
            InternalError: When kicking a client from the pool fails
            PoolError: If all clients are in use after max_wait seconds
        """

        def _inner():
            # Try one full cycle through the clients
            for _ in range(len(self._clients)):
                client = next(self._client_cycle)
                if self._in_use[client]:
                    continue
                if self.pool_config.test_on_borrow and not client.ping():
                    self.kick_from_pool(client, locked=True)
                    continue

                self._in_use[client] = True
                return client
            return None

        with self._lock:
            start_time = time.time()
            while time.time() - start_time < self.pool_config.max_wait:
                client = _inner()
                if client:
                    return client
            raise PoolError("All clients are in use")

    def return_client(self, client: NebulaClient) -> None:
        """Return a client back to the pool.

        Args:
        ----
            client: The client to return to the pool

        Raises:
        ------
            InternalError: If the client is not from this pool
        """
        if client not in self._clients:
            raise InternalError("Client does not belong to this pool")

        with self._lock:
            self._in_use[client] = False

    @contextmanager
    def borrow(self):
        """Borrow a client from the pool using a context manager.

        Returns:
        -------
            ContextManager[NebulaClient]: A context manager that yields a client

        Raises:
        ------
            PoolError: If all clients are in use after max_wait seconds
            InternalError: If kicking a client from the pool fails

        Example:
        -------
            with pool.borrow() as client:
                result = client.execute("SHOW HOSTS")
        """
        client = self.get_client()
        try:
            yield client
        finally:
            self.return_client(client)

    def execute(self, statement: str, *, timeout: Optional[float] = None):
        with self.borrow() as client:
            return client.execute(statement, timeout=timeout)

    def close(self):
        """Close all clients in the pool. No Exception will be raised but errors will be logged."""
        for client in self._clients:
            client.close()
