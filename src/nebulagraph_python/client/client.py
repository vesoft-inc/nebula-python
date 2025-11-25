import logging
from typing import Any, Dict, List, Literal, Optional, Union

from nebulagraph_python.client._connection import (
    AsyncConnection,
    Connection,
    ConnectionConfig,
)
from nebulagraph_python.client._connection_pool import (
    AsyncConnectionPool,
    ConnectionPool,
)
from nebulagraph_python.client._session import (
    AsyncSession,
    Session,
    SessionConfig,
)
from nebulagraph_python.client._session_pool import (
    AsyncSessionPool,
    SessionPool,
    SessionPoolConfig,
)
from nebulagraph_python.client.base_executor import (
    NebulaBaseAsyncExecutor,
    NebulaBaseExecutor,
)
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import PoolError
from nebulagraph_python.result_set import ResultSet

logger = logging.getLogger(__name__)


class NebulaAsyncClient(NebulaBaseAsyncExecutor):
    """The async client for connecting to NebulaGraph. It is async/coroutine-level safe but not thread-safe,
    which means you can not share the client instance across threads,
    but you can call `await client.execute()` concurrently in async coroutines.

    Required to explicitly call `close()` to release all resources.
    """

    # Owned Resources
    _conn: AsyncConnection | AsyncConnectionPool
    _sessions: dict[HostAddress, AsyncSession | AsyncSessionPool]

    def __init__(*args, **kwargs):
        raise RuntimeError(
            "Using `await NebulaAsyncClient.connect()` to create a client instance."
        )

    @classmethod
    async def connect(
        cls,
        hosts: Union[str, List[str], List[HostAddress]],
        username: str,
        password: Optional[str] = None,
        *,
        ssl_param: Union[SSLParam, Literal[True], None] = None,
        auth_options: Optional[Dict[str, Any]] = None,
        conn_config: Optional[ConnectionConfig] = None,
        session_config: Optional[SessionConfig] = None,
        session_pool_config: Optional[SessionPoolConfig] = None,
    ):
        """Connect to NebulaGraph and initialize the client

        Args:
        ----
            hosts: Single host string ("hostname:port"), list of host strings,
                  or list of HostAddress objects
            username: Username for authentication
            password: Password for authentication
            ssl_param: SSL configuration
            auth_options: dict of authentication options
            conn_config: Connection configuration. If provided, it overrides hosts and ssl_param.
            session_config: Session configuration.
        """
        self = super().__new__(cls)
        conn_conf = conn_config or ConnectionConfig.from_defults(hosts, ssl_param)
        hosts = conn_conf.hosts
        self._sessions = {}
        if len(hosts) == 1:
            self._conn = AsyncConnection(conn_conf)
            await self._conn.connect()
        else:
            self._conn = AsyncConnectionPool(conn_conf)
            await self._conn.connect()
        try:
            for host_addr in hosts:
                conn = (
                    await self._conn.get_connection(host_addr)
                    if isinstance(self._conn, AsyncConnectionPool)
                    else self._conn
                )
                if conn is None:
                    raise PoolError(
                        f"Failed to get connection to {host_addr} when initializing NebulaAsyncClient"
                    )
                if session_pool_config:
                    self._sessions[host_addr] = await AsyncSessionPool.connect(
                        conn=conn,
                        username=username,
                        password=password,
                        auth_options=auth_options or {},
                        session_config=session_config or SessionConfig(),
                        pool_config=session_pool_config,
                    )
                else:
                    self._sessions[host_addr] = AsyncSession(
                        conn=conn,
                        username=username,
                        password=password,
                        session_config=session_config or SessionConfig(),
                        auth_options=auth_options or {},
                    )
        except Exception as e:
            await self._conn.close()
            raise e
        return self

    async def execute(
        self, statement: str, *, timeout: Optional[float] = None, do_ping: bool = False
    ) -> ResultSet:
        if isinstance(self._conn, AsyncConnectionPool):
            addr, _conn = await self._conn.next_connection()
        else:
            addr = self._conn.config.hosts[0]
        _session = self._sessions[addr]

        if isinstance(_session, AsyncSessionPool):
            async with _session.borrow() as session:
                return (
                    await session.execute(statement, timeout=timeout, do_ping=do_ping)
                ).raise_on_error()
        else:
            return (
                await _session.execute(statement, timeout=timeout, do_ping=do_ping)
            ).raise_on_error()

    async def ping(self, timeout: Optional[float] = None) -> bool:
        try:
            res = (
                (await self.execute(statement="RETURN 1", timeout=timeout))
                .one()
                .as_primitive()
            )
            if not res == {"1": 1}:
                raise ValueError(f"Unexpected result from ping: {res}")
            return True
        except Exception:
            logger.exception("Failed to ping NebulaGraph")
            return False

    async def close(self):
        """Close the client connection and session. No Exception will be raised but an error will be logged."""
        for session in self._sessions.values():
            await session.close()
        await self._conn.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


class NebulaClient(NebulaBaseExecutor):
    """The client for connecting to NebulaGraph. It is thread-safe,
    which means you can share a client instance across threads and call `execute` concurrently.

    Required to explicitly call `close()` to release all resources.
    """

    # Owned Resources
    _conn: Connection | ConnectionPool
    _sessions: dict[HostAddress, Session | SessionPool]

    def __init__(
        self,
        hosts: Union[str, List[str], List[HostAddress]],
        username: str,
        password: Optional[str] = None,
        *,
        ssl_param: Union[SSLParam, Literal[True], None] = None,
        auth_options: Optional[Dict[str, Any]] = None,
        conn_config: Optional[ConnectionConfig] = None,
        session_config: Optional[SessionConfig] = None,
        session_pool_config: Optional[SessionPoolConfig] = None,
    ):
        """Initialize NebulaGraph client

        Args:
        ----
            hosts: Single host string ("hostname:port"), list of host strings,
                  or list of HostAddress objects
            username: Username for authentication
            password: Password for authentication
            ssl_param: SSL configuration
            auth_options: dict of authentication options
            conn_config: Connection configuration. If provided, it overrides hosts and ssl_param.
            session_config: Session configuration.
            session_pool_config: Session pool configuration. If provided, a session pool will be created.
        """
        conn_conf = conn_config or ConnectionConfig.from_defults(hosts, ssl_param)
        hosts = conn_conf.hosts
        self._sessions = {}
        if len(hosts) == 1:
            self._conn = Connection(conn_conf)
            self._conn.connect()
        else:
            self._conn = ConnectionPool(conn_conf)
            self._conn.connect()
        try:
            for host_addr in hosts:
                conn = (
                    self._conn.get_connection(host_addr)
                    if isinstance(self._conn, ConnectionPool)
                    else self._conn
                )
                if conn is None:
                    raise PoolError(
                        f"Failed to get connection to {host_addr} when initializing NebulaClient"
                    )
                if session_pool_config:
                    self._sessions[host_addr] = SessionPool.connect(
                        conn=conn,
                        username=username,
                        password=password,
                        auth_options=auth_options or {},
                        session_config=session_config or SessionConfig(),
                        pool_config=session_pool_config,
                    )
                else:
                    self._sessions[host_addr] = Session(
                        conn=conn,
                        username=username,
                        password=password,
                        session_config=session_config or SessionConfig(),
                        auth_options=auth_options or {},
                    )
        except Exception as e:
            self._conn.close()
            raise e

    def execute(
        self, statement: str, *, timeout: Optional[float] = None, do_ping: bool = False
    ) -> ResultSet:
        if isinstance(self._conn, ConnectionPool):
            addr, _conn = self._conn.next_connection()
        else:
            addr = self._conn.config.hosts[0]
        _session = self._sessions[addr]

        if isinstance(_session, SessionPool):
            with _session.borrow() as session:
                return session.execute(
                    statement, timeout=timeout, do_ping=do_ping
                ).raise_on_error()
        else:
            return _session.execute(
                statement, timeout=timeout, do_ping=do_ping
            ).raise_on_error()

    def ping(self, timeout: Optional[float] = None) -> bool:
        try:
            res = (
                (self.execute(statement="RETURN 1", timeout=timeout))
                .one()
                .as_primitive()
            )
            if not res == {"1": 1}:
                raise ValueError(f"Unexpected result from ping: {res}")
            return True
        except Exception:
            logger.exception("Failed to ping NebulaGraph")
            return False

    def close(self):
        """Close the client connection and session. No Exception will be raised but an error will be logged."""
        for session in self._sessions.values():
            session.close()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
