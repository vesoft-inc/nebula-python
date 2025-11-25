import logging
import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

from anyio import Lock, Semaphore, fail_after

from nebulagraph_python.client._connection import AsyncConnection, Connection
from nebulagraph_python.client._session import (
    AsyncSession,
    Session,
    SessionConfig,
)
from nebulagraph_python.client.constants import (
    DEFAULT_SESSION_POOL_SIZE,
    DEFAULT_SESSION_POOL_WAIT_TIMEOUT,
)
from nebulagraph_python.error import PoolError

logger = logging.getLogger(__name__)


@dataclass
class SessionPoolConfig:
    """Configuration for the SessionPool.
    Args:
        size: The number of sessions to be managed by the SessionPool.
        wait_timeout: The maximum time to wait for a session to be available. If None, wait indefinitely.
    """

    size: int = field(default=DEFAULT_SESSION_POOL_SIZE)
    wait_timeout: float | None = field(default=DEFAULT_SESSION_POOL_WAIT_TIMEOUT)

    def __post_init__(self):
        if self.size <= 0:
            raise ValueError(
                f"SessionPoolConfig.size must be greater than 0, but got {self.size}"
            )
        if self.wait_timeout is not None and self.wait_timeout <= 0:
            self.wait_timeout = None


class AsyncSessionPool:
    """Manage a pool of sessions. It is built upon anyio Lock and is async/coroutine-level safe but not thread-safe."""

    free_sessions_queue: Set[AsyncSession]
    busy_sessions_queue: Set[AsyncSession]
    queue_lock: Lock
    queue_count: Semaphore
    config: SessionPoolConfig

    @classmethod
    async def connect(
        cls,
        conn: AsyncConnection,
        username: str,
        password: Optional[str] = None,
        auth_options: Optional[Dict[str, Any]] = None,
        session_config: Optional[SessionConfig] = None,
        pool_config: Optional[SessionPoolConfig] = None,
    ):
        pool_config = pool_config or SessionPoolConfig()
        sessions: Set[AsyncSession] = set()
        try:
            for _ in range(pool_config.size):
                sessions.add(
                    AsyncSession(
                        conn,
                        username=username,
                        password=password,
                        session_config=session_config,
                        auth_options=auth_options,
                    )
                )
            return cls(sessions, pool_config)
        except Exception:
            # Clean up any sessions that were successfully created
            for session in sessions:
                await session.close()
            raise

    def __init__(
        self,
        sessions: Set[AsyncSession],
        config: SessionPoolConfig,
    ):
        """Initialize the SessionPool

        Args:
            sessions: The sessions to be managed by the SessionPool.
            config: Configuration for the SessionPool.
        """
        if len(sessions) != config.size:
            raise ValueError(
                f"The number of sessions ({len(sessions)}) does not match the size of the pool ({config.size})"
            )
        self.free_sessions_queue = sessions
        self.busy_sessions_queue = set()
        self.queue_lock = Lock()
        self.queue_count = Semaphore(len(sessions))
        self.config = config

    @asynccontextmanager
    async def borrow(self):
        got_session: Optional[AsyncSession] = None

        # Event-based loop (wait for free session to be available)
        while True:
            if self.config.wait_timeout is not None:
                try:
                    with fail_after(self.config.wait_timeout):
                        await self.queue_count.acquire()
                except TimeoutError:
                    break
            else:
                await self.queue_count.acquire()
            async with self.queue_lock:
                if not self.free_sessions_queue:
                    logger.error(
                        "No free sessions available after acquired semaphore, which indicates a bug in the AsyncSessionPool"
                    )
                    # Release semaphore and retry if no sessions available
                    self.queue_count.release()
                    continue
                session = self.free_sessions_queue.pop()
                self.busy_sessions_queue.add(session)
                got_session = session
                break

        if got_session is None:
            raise PoolError(
                f"No session available in the SessionPool after waiting {self.config.wait_timeout} seconds"
            )

        try:
            yield got_session
        finally:
            # Ensure session is returned to pool even if exception occurs
            async with self.queue_lock:
                if got_session in self.busy_sessions_queue:
                    self.free_sessions_queue.add(got_session)
                    self.busy_sessions_queue.remove(got_session)
            self.queue_count.release()

    async def close(self):
        # Acquire all semaphore permits to prevent new borrows
        for _ in range(self.config.size):
            await self.queue_count.acquire()
        async with self.queue_lock:
            # Close all free sessions
            for session in self.free_sessions_queue:
                await session.close()
            # Close all busy sessions (if any remain)
            for session in self.busy_sessions_queue:
                logger.error(
                    "Busy sessions remain after acquire all semaphore permits, which indicates a bug in the AsyncSessionPool"
                )
                await session.close()


class SessionPool:
    """Manage a pool of sessions. It is built upon threading.Lock and is thread-safe."""

    free_sessions_queue: Set[Session]
    busy_sessions_queue: Set[Session]
    queue_lock: threading.Lock
    queue_count: threading.Semaphore
    config: SessionPoolConfig

    @classmethod
    def connect(
        cls,
        conn: Connection,
        username: str,
        password: Optional[str] = None,
        auth_options: Optional[Dict[str, Any]] = None,
        session_config: Optional[SessionConfig] = None,
        pool_config: Optional[SessionPoolConfig] = None,
    ):
        pool_config = pool_config or SessionPoolConfig()
        sessions: Set[Session] = set()
        try:
            for _ in range(pool_config.size):
                sessions.add(
                    Session(
                        conn,
                        username=username,
                        password=password,
                        session_config=session_config,
                        auth_options=auth_options,
                    )
                )
            return cls(sessions, pool_config)
        except Exception:
            # Clean up any sessions that were successfully created
            for session in sessions:
                session.close()
            raise

    def __init__(
        self,
        sessions: Set[Session],
        config: SessionPoolConfig,
    ):
        """Initialize the SessionPool

        Args:
            sessions: The sessions to be managed by the SessionPool.
            config: Configuration for the SessionPool.
        """
        if len(sessions) != config.size:
            raise ValueError(
                f"The number of sessions ({len(sessions)}) does not match the size of the pool ({config.size})"
            )
        self.free_sessions_queue = sessions
        self.busy_sessions_queue = set()
        self.queue_lock = threading.Lock()
        self.queue_count = threading.Semaphore(len(sessions))
        self.config = config

    @contextmanager
    def borrow(self):
        got_session: Optional[Session] = None

        # Event-based loop (wait for free session to be available)
        while True:
            if self.config.wait_timeout is not None:
                acquired = self.queue_count.acquire(timeout=self.config.wait_timeout)
                if not acquired:
                    break
            else:
                self.queue_count.acquire()
            with self.queue_lock:
                if not self.free_sessions_queue:
                    logger.error(
                        "No free sessions available after acquired semaphore, which indicates a bug in the SessionPool"
                    )
                    # Release semaphore and retry if no sessions available
                    self.queue_count.release()
                    continue
                session = self.free_sessions_queue.pop()
                self.busy_sessions_queue.add(session)
                got_session = session
                break

        if got_session is None:
            raise PoolError(
                f"No session available in the SessionPool after waiting {self.config.wait_timeout} seconds"
            )

        try:
            yield got_session
        finally:
            # Ensure session is returned to pool even if exception occurs
            with self.queue_lock:
                if got_session in self.busy_sessions_queue:
                    self.free_sessions_queue.add(got_session)
                    self.busy_sessions_queue.remove(got_session)
            self.queue_count.release()

    def close(self):
        # Acquire all semaphore permits to prevent new borrows
        for _ in range(self.config.size):
            self.queue_count.acquire()
        with self.queue_lock:
            # Close all free sessions
            for session in self.free_sessions_queue:
                session.close()
            # Close all busy sessions (if any remain)
            for session in self.busy_sessions_queue:
                logger.error(
                    "Busy sessions remain after acquire all semaphore permits, which indicates a bug in the SessionPool"
                )
                session.close()
