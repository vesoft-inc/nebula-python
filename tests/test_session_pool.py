import asyncio
import threading
import time
from unittest.mock import AsyncMock, Mock, patch

import pytest

from nebulagraph_python.client._session_pool import (
    AsyncSessionPool,
    SessionPool,
    SessionPoolConfig,
)
from nebulagraph_python.client._session import (
    Session,
    AsyncSession,
    SessionConfig,
)
from nebulagraph_python.error import PoolError
from copy import copy


class TestSessionPool:
    """Test cases for SessionPool (synchronous)"""

    def test_init_basic(self):
        """Test basic initialization"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=3)
        pool = SessionPool(copy(sessions), config)
        
        assert pool.free_sessions_queue == sessions
        assert pool.busy_sessions_queue == set()
        assert len(pool.free_sessions_queue) == 3
        assert pool.queue_count._value == 3  # Semaphore initial value

    def test_init_with_config(self):
        """Test initialization with custom config"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2, wait_timeout=10.0)
        pool = SessionPool(copy(sessions), config)
        
        assert pool.config.size == 2
        assert pool.config.wait_timeout == 10.0

    def test_init_with_all_config_params(self):
        """Test initialization with all configuration parameters"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(
            size=3, 
            wait_timeout=5.0, 
        )
        pool = SessionPool(copy(sessions), config)
        
        assert pool.config.size == 3
        assert pool.config.wait_timeout == 5.0
        assert len(pool.free_sessions_queue) == 3

    def test_borrow_single_session(self):
        """Test borrowing a single session"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        pool = SessionPool(copy(sessions), SessionPoolConfig(size=3))
        
        with pool.borrow() as session:
            assert session in sessions
            assert session in pool.busy_sessions_queue
            assert session not in pool.free_sessions_queue
            assert len(pool.busy_sessions_queue) == 1
            assert len(pool.free_sessions_queue) == 2
        
        # After context exit, session should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 3
        assert session in pool.free_sessions_queue

    def test_borrow_all_sessions(self):
        """Test borrowing all available sessions"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        pool = SessionPool(copy(sessions), SessionPoolConfig(size=2))
        
        with pool.borrow() as session1:
            with pool.borrow() as session2:
                assert {session1, session2} == sessions
                assert len(pool.busy_sessions_queue) == 2
                assert len(pool.free_sessions_queue) == 0

    def test_borrow_timeout_exceeded(self):
        """Test borrowing when timeout is exceeded"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1, wait_timeout=0.2)
        pool = SessionPool(copy(sessions), config)
        
        with pool.borrow():  # Acquire the only session
            # Try to borrow another session - should timeout
            with pytest.raises(PoolError, match="No session available in the SessionPool after waiting 0.2 seconds"):
                with pool.borrow():
                    pass

    def test_borrow_infinite_wait_with_release(self):
        """Test borrowing with infinite wait that succeeds when session becomes available"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1, wait_timeout=None)
        pool = SessionPool(copy(sessions), config)
        
        def release_session():
            time.sleep(0.1)  # Wait a bit
            # This will be triggered by the context manager exit
        
        def acquire_and_release():
            with pool.borrow():
                threading.Thread(target=release_session).start()
                time.sleep(0.2)  # Hold session briefly
        
        # Start a thread that will acquire and then release the session
        thread1 = threading.Thread(target=acquire_and_release)
        thread1.start()
        
        time.sleep(0.05)  # Ensure first thread acquires the session
        
        # This should succeed once the first thread releases the session
        start_time = time.time()
        with pool.borrow() as session:
            assert session in sessions
            elapsed = time.time() - start_time
            assert elapsed >= 0.15  # Should have waited for release
        
        thread1.join()

    def test_concurrent_borrowing(self):
        """Test concurrent borrowing from multiple threads"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username=f"user{i}", password=f"pass{i}", session_config=None, auth_options=None)
            for i in range(5)
        }
        config = SessionPoolConfig(size=5)
        pool = SessionPool(copy(sessions), config)
        results = []
        errors = []
        
        def borrow_session(thread_id):
            try:
                with pool.borrow() as session:
                    results.append((thread_id, session))
                    time.sleep(0.1)  # Simulate work
            except Exception as e:
                errors.append((thread_id, e))
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=borrow_session, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 5
        
        # All sessions should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 5

    def test_semaphore_consistency(self):
        """Test that semaphore behavior stays consistent with actual session availability"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = SessionPool(copy(sessions), config)
        
        # Test we can acquire sessions sequentially
        with pool.borrow():
            # One session borrowed - test we can still acquire one more
            acquired_second = pool.queue_count.acquire(blocking=False)
            if acquired_second:
                pool.queue_count.release()
            assert acquired_second, "Should be able to acquire second session"
            
            with pool.borrow():
                # Both sessions borrowed - test we cannot acquire more
                cannot_acquire = not pool.queue_count.acquire(blocking=False)
                assert cannot_acquire, "Should not be able to acquire third session"
        
        # All sessions returned - test we can acquire again
        acquired_after_return = pool.queue_count.acquire(blocking=False)
        if acquired_after_return:
            pool.queue_count.release()
        assert acquired_after_return, "Should be able to acquire session after return"

    def test_close_all_free_sessions(self):
        """Test closing pool with all sessions free"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=3)
        pool = SessionPool(copy(sessions), config)
        
        # Mock the close_session method for all sessions
        for session in sessions:
            session.close = Mock()
        
        pool.close()
        
        # Should close all sessions
        for session in sessions:
            session.close.assert_called_once()

    @patch('nebulagraph_python.client._session_pool.logger')
    def test_close_with_busy_sessions(self, mock_logger):
        """Test closing pool with some busy sessions"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=3)
        pool = SessionPool(copy(sessions), config)
        
        # Mock the close_session method for all sessions
        for session in sessions:
            session.close = Mock()
        
        # Manually move a session to busy state
        busy_session = list(sessions)[1]  # Get the second session
        pool.free_sessions_queue.remove(busy_session)
        pool.busy_sessions_queue.add(busy_session)
        
        pool.close()
        
        # Should close all sessions
        for session in sessions:
            session.close.assert_called_once()
        # Should log error about busy sessions
        mock_logger.error.assert_called_once()
        assert "Busy sessions remain" in mock_logger.error.call_args[0][0]

    def test_connect_success(self):
        """Test successful connection via classmethod"""
        mock_conn = Mock()
        
        config = SessionPoolConfig(size=4)
        
        # Test the connect method
        pool = SessionPool.connect(
            conn=mock_conn,
            username="test_user",
            password="test_pass",
            pool_config=config
        )
        
        # Verify the pool was created correctly
        assert len(pool.free_sessions_queue) == 4
        assert len(pool.busy_sessions_queue) == 0

    def test_connect_partial_failure(self):
        """Test connection with partial failure during setup"""
        # Create a mock connection that fails after creating some sessions
        mock_conn = Mock()
        
        config = SessionPoolConfig(size=3)
        
        # Mock Session constructor to fail on third call
        original_session = Session
        call_count = 0
        
        def mock_session_init(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                raise Exception("Auth failed")
            return original_session(*args, **kwargs)
        
        with patch('nebulagraph_python.client._session_pool.Session', side_effect=mock_session_init):
            with pytest.raises(Exception, match="Auth failed"):
                SessionPool.connect(
                    conn=mock_conn,
                    username="test_user",
                    password="test_pass",
                    pool_config=config
                )

    def test_connect_authentication_failure_first_attempt(self):
        """Test connection failure on first authentication attempt"""
        mock_conn = Mock()
        
        config = SessionPoolConfig(size=2)
        
        # Mock Session constructor to fail on first call
        with patch('nebulagraph_python.client._session_pool.Session', side_effect=Exception("Auth failed on first attempt")):
            with pytest.raises(Exception, match="Auth failed on first attempt"):
                SessionPool.connect(
                    conn=mock_conn,
                    username="test_user",
                    password="test_pass",
                    pool_config=config
                )

    def test_multiple_borrow_release_cycles(self):
        """Test multiple borrow-release cycles work correctly"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = SessionPool(copy(sessions), config)
        
        # First cycle
        with pool.borrow() as session1:
            assert session1 in pool.busy_sessions_queue
            assert len(pool.free_sessions_queue) == 1
        
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 2
        
        # Second cycle
        with pool.borrow() as session2:
            with pool.borrow() as session3:
                assert {session2, session3} == sessions
                assert len(pool.busy_sessions_queue) == 2
                assert len(pool.free_sessions_queue) == 0
        
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 2


class TestAsyncSessionPool:
    """Test cases for AsyncSessionPool (asynchronous)"""

    @pytest.mark.asyncio
    async def test_init_basic(self):
        """Test basic initialization"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=3)
        pool = AsyncSessionPool(copy(sessions), config)
        
        assert pool.free_sessions_queue == sessions
        assert pool.busy_sessions_queue == set()
        assert len(pool.free_sessions_queue) == 3
        # Test that we can borrow a session (semaphore has permits)
        async with pool.borrow() as session:
            assert session in sessions

    @pytest.mark.asyncio
    async def test_init_with_config(self):
        """Test initialization with custom config"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2, wait_timeout=10.0)
        pool = AsyncSessionPool(copy(sessions), config)
        
        assert pool.config.size == 2
        assert pool.config.wait_timeout == 10.0

    @pytest.mark.asyncio
    async def test_init_with_all_config_params(self):
        """Test initialization with all configuration parameters"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(
            size=3, 
            wait_timeout=5.0, 
        )
        pool = AsyncSessionPool(copy(sessions), config)
        
        assert pool.config.size == 3
        assert pool.config.wait_timeout == 5.0
        assert len(pool.free_sessions_queue) == 3

    @pytest.mark.asyncio
    async def test_borrow_single_session(self):
        """Test borrowing a single session"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=3)
        pool = AsyncSessionPool(copy(sessions), config)
        
        async with pool.borrow() as session:
            assert session in sessions
            assert session in pool.busy_sessions_queue
            assert session not in pool.free_sessions_queue
            assert len(pool.busy_sessions_queue) == 1
            assert len(pool.free_sessions_queue) == 2
        
        # After context exit, session should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 3
        assert session in pool.free_sessions_queue

    @pytest.mark.asyncio
    async def test_borrow_all_sessions(self):
        """Test borrowing all available sessions"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = AsyncSessionPool(copy(sessions), config)
        
        async with pool.borrow() as session1:
            async with pool.borrow() as session2:
                assert {session1, session2} == sessions
                assert len(pool.busy_sessions_queue) == 2
                assert len(pool.free_sessions_queue) == 0

    @pytest.mark.asyncio
    async def test_borrow_timeout_exceeded(self):
        """Test borrowing when timeout is exceeded"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1, wait_timeout=0.2)
        pool = AsyncSessionPool(copy(sessions), config)
        
        async with pool.borrow():  # Acquire the only session
            # Try to borrow another session - should timeout
            with pytest.raises(PoolError, match="No session available in the SessionPool after waiting 0.2 seconds"):
                async with pool.borrow():
                    pass

    @pytest.mark.asyncio
    async def test_borrow_infinite_wait_with_release(self):
        """Test borrowing with infinite wait that succeeds when session becomes available"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1, wait_timeout=None)
        pool = AsyncSessionPool(copy(sessions), config)
        
        async def acquire_and_release():
            async with pool.borrow():
                await asyncio.sleep(0.2)  # Hold session briefly
        
        # Start a task that will acquire and then release the session
        task1 = asyncio.create_task(acquire_and_release())
        
        await asyncio.sleep(0.05)  # Ensure first task acquires the session
        
        # This should succeed once the first task releases the session
        start_time = time.time()
        async with pool.borrow() as session:
            assert session in sessions
            elapsed = time.time() - start_time
            assert elapsed >= 0.15  # Should have waited for release
        
        await task1

    @pytest.mark.asyncio
    async def test_concurrent_borrowing(self):
        """Test concurrent borrowing from multiple coroutines"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username=f"user{i}", password=f"pass{i}", session_config=None, auth_options=None)
            for i in range(5)
        }
        config = SessionPoolConfig(size=5)
        pool = AsyncSessionPool(copy(sessions), config)
        results = []
        errors = []
        
        async def borrow_session(task_id):
            try:
                async with pool.borrow() as session:
                    results.append((task_id, session))
                    await asyncio.sleep(0.1)  # Simulate async work
            except Exception as e:
                errors.append((task_id, e))
        
        # Start multiple concurrent tasks
        tasks = [borrow_session(i) for i in range(5)]
        await asyncio.gather(*tasks)
        
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 5
        
        # All sessions should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 5

    @pytest.mark.asyncio
    async def test_semaphore_consistency(self):
        """Test that semaphore behavior stays consistent with actual session availability"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = AsyncSessionPool(copy(sessions), config)
        
        # Test we can acquire sessions sequentially
        async with pool.borrow():
            # One session borrowed - test we can still acquire one more
            acquired_second = False
            try:
                pool.queue_count.acquire_nowait()
                acquired_second = True
                pool.queue_count.release()
            except:
                pass
            assert acquired_second, "Should be able to acquire second session"
            
            async with pool.borrow():
                # Both sessions borrowed - test we cannot acquire more
                cannot_acquire = False
                try:
                    pool.queue_count.acquire_nowait()
                except:
                    cannot_acquire = True
                assert cannot_acquire, "Should not be able to acquire third session"
        
        # All sessions returned - test we can acquire again
        acquired_after_return = False
        try:
            pool.queue_count.acquire_nowait()
            acquired_after_return = True
            pool.queue_count.release()
        except:
            pass
        assert acquired_after_return, "Should be able to acquire session after return"

    @pytest.mark.asyncio
    async def test_close_all_free_sessions(self):
        """Test closing pool with all sessions free"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=3)
        pool = AsyncSessionPool(copy(sessions), config)
        
        # Mock the close_session method for all sessions
        for session in sessions:
            session.close = AsyncMock()
        
        await pool.close()
        
        # Should close all sessions
        for session in sessions:
            session.close.assert_called_once()

    @pytest.mark.asyncio
    @patch('nebulagraph_python.client._session_pool.logger')
    async def test_close_with_busy_sessions(self, mock_logger):
        """Test closing pool with some busy sessions"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user3", password="pass3", session_config=None, auth_options=None),
        }
        pool = AsyncSessionPool(copy(sessions), config=SessionPoolConfig(size=3))
        
        # Mock the close_session method for all sessions
        for session in sessions:
            session.close = AsyncMock()
        
        # Manually move a session to busy state
        busy_session = list(sessions)[1]  # Get the second session
        pool.free_sessions_queue.remove(busy_session)
        pool.busy_sessions_queue.add(busy_session)
        
        await pool.close()
        
        # Should close all sessions
        for session in sessions:
            session.close.assert_called_once()
        # Should log error about busy sessions
        mock_logger.error.assert_called_once()
        assert "Busy sessions remain" in mock_logger.error.call_args[0][0]

    @pytest.mark.asyncio
    async def test_connect_success(self):
        """Test successful connection via classmethod"""
        mock_conn = AsyncMock()
        
        config = SessionPoolConfig(size=4)
        
        # Test the connect method
        pool = await AsyncSessionPool.connect(
            conn=mock_conn,
            username="test_user",
            password="test_pass",
            pool_config=config
        )
        
        # Verify the pool was created correctly
        assert len(pool.free_sessions_queue) == 4
        assert len(pool.busy_sessions_queue) == 0

    @pytest.mark.asyncio
    async def test_connect_partial_failure(self):
        """Test connection with partial failure during setup"""
        mock_conn = AsyncMock()
        
        config = SessionPoolConfig(size=3)
        
        # Mock AsyncSession constructor to fail on third call
        original_session = AsyncSession
        call_count = 0
        
        def mock_session_init(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                raise Exception("Auth failed")
            return original_session(*args, **kwargs)
        
        with patch('nebulagraph_python.client._session_pool.AsyncSession', side_effect=mock_session_init):
            with pytest.raises(Exception, match="Auth failed"):
                await AsyncSessionPool.connect(
                    conn=mock_conn,
                    username="test_user",
                    password="test_pass",
                    pool_config=config
                )

    @pytest.mark.asyncio
    async def test_connect_authentication_failure_first_attempt(self):
        """Test connection failure on first authentication attempt"""
        mock_conn = AsyncMock()
        
        config = SessionPoolConfig(size=2)
        
        # Mock AsyncSession constructor to fail on first call
        with patch('nebulagraph_python.client._session_pool.AsyncSession', side_effect=Exception("Auth failed on first attempt")):
            with pytest.raises(Exception, match="Auth failed on first attempt"):
                await AsyncSessionPool.connect(
                    conn=mock_conn,
                    username="test_user",
                    password="test_pass",
                    pool_config=config
                )

    @pytest.mark.asyncio
    async def test_multiple_borrow_release_cycles(self):
        """Test multiple borrow-release cycles work correctly"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = AsyncSessionPool(copy(sessions), config)
        
        # First cycle
        async with pool.borrow() as session1:
            assert session1 in pool.busy_sessions_queue
            assert len(pool.free_sessions_queue) == 1
        
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 2
        
        # Second cycle
        async with pool.borrow() as session2:
            async with pool.borrow() as session3:
                assert {session2, session3} == sessions
                assert len(pool.busy_sessions_queue) == 2
                assert len(pool.free_sessions_queue) == 0
        
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 2


class TestSessionPoolEdgeCases:
    """Test edge cases for both sync and async session pools"""

    def test_sync_pool_exception_in_context(self):
        """Test that sessions are properly returned even when exceptions occur in sync pool"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1)
        pool = SessionPool(copy(sessions), config)
        
        with pytest.raises(ValueError):
            with pool.borrow() as session:
                assert session in pool.busy_sessions_queue
                raise ValueError("Test exception")
        
        # Session should be returned to pool even after exception
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 1

    @pytest.mark.asyncio
    async def test_async_pool_exception_in_context(self):
        """Test that sessions are properly returned even when exceptions occur in async pool"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1)
        pool = AsyncSessionPool(copy(sessions), config)
        
        with pytest.raises(ValueError):
            async with pool.borrow() as session:
                assert session in pool.busy_sessions_queue
                raise ValueError("Test exception")
        
        # Session should be returned to pool even after exception
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 1

    def test_sync_multiple_exceptions_in_context(self):
        """Test multiple exceptions in sync pool context managers"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            Session(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = SessionPool(copy(sessions), config)
        
        with pytest.raises(ValueError):
            with pool.borrow():
                with pool.borrow():
                    assert len(pool.busy_sessions_queue) == 2
                    raise ValueError("Test exception")
        
        # All sessions should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 2

    @pytest.mark.asyncio
    async def test_async_multiple_exceptions_in_context(self):
        """Test multiple exceptions in async pool context managers"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
            AsyncSession(conn=mock_conn, username="user2", password="pass2", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=2)
        pool = AsyncSessionPool(copy(sessions), config)
        
        with pytest.raises(ValueError):
            async with pool.borrow():
                async with pool.borrow():
                    assert len(pool.busy_sessions_queue) == 2
                    raise ValueError("Test exception")
        
        # All sessions should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 2

    def test_sync_empty_pool(self):
        """Test sync pool with zero sessions"""
        with pytest.raises(ValueError, match="SessionPoolConfig.size must be greater than 0, but got 0"):
            config = SessionPoolConfig(size=0)

    @pytest.mark.asyncio
    async def test_async_empty_pool(self):
        """Test async pool with zero sessions"""
        with pytest.raises(ValueError, match="SessionPoolConfig.size must be greater than 0, but got 0"):
            config = SessionPoolConfig(size=0)

    def test_sync_zero_timeout(self):
        """Test sync pool with zero timeout converts to None"""
        config = SessionPoolConfig(size=1, wait_timeout=0.0)
        # Should convert zero timeout to None
        assert config.wait_timeout is None
        
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        pool = SessionPool(copy(sessions), config)
        
        with pool.borrow():
            # Should timeout immediately since timeout is None (infinite wait) but session is busy
            pass  # This test is mainly about the config behavior

    @pytest.mark.asyncio
    async def test_async_zero_timeout(self):
        """Test async pool with zero timeout converts to None"""
        config = SessionPoolConfig(size=1, wait_timeout=0.0)
        # Should convert zero timeout to None
        assert config.wait_timeout is None
        
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        pool = AsyncSessionPool(copy(sessions), config)
        
        async with pool.borrow():
            # Should timeout immediately since timeout is None (infinite wait) but session is busy
            pass  # This test is mainly about the config behavior

    def test_sync_negative_timeout(self):
        """Test sync pool with negative timeout converts to None"""
        config = SessionPoolConfig(size=1, wait_timeout=-1.0)
        # Should convert negative timeout to None
        assert config.wait_timeout is None


class TestSessionPoolConfig:
    """Test SessionPoolConfig validation and behavior"""

    def test_config_default_values(self):
        """Test default configuration values"""
        config = SessionPoolConfig(size=5)
        
        assert config.size == 5
        assert config.wait_timeout == 60  # Default is 60 seconds, not None

    def test_config_custom_values(self):
        """Test custom configuration values"""
        config = SessionPoolConfig(
            size=10,
            wait_timeout=30.0,
        )
        
        assert config.size == 10
        assert config.wait_timeout == 30.0

    def test_config_zero_size(self):
        """Test configuration with zero pool size"""
        # SessionPoolConfig raises ValueError for size <= 0
        with pytest.raises(ValueError, match="SessionPoolConfig.size must be greater than 0"):
            config = SessionPoolConfig(size=0)

    def test_config_large_size(self):
        """Test configuration with large pool size"""
        config = SessionPoolConfig(size=1000)
        assert config.size == 1000

    def test_sync_pool_with_custom_retry_interval(self):
        """Test sync pool behavior with custom retry interval"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1, wait_timeout=0.3)
        pool = SessionPool(copy(sessions), config)
        
        with pool.borrow():
            start_time = time.time()
            with pytest.raises(PoolError):
                with pool.borrow():
                    pass
            elapsed = time.time() - start_time
            # Should have attempted multiple retries within the timeout
            assert 0.25 <= elapsed <= 0.35

    @pytest.mark.asyncio
    async def test_async_pool_with_custom_retry_interval(self):
        """Test async pool behavior with custom retry interval"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username="user1", password="pass1", session_config=None, auth_options=None),
        }
        config = SessionPoolConfig(size=1, wait_timeout=0.3)
        pool = AsyncSessionPool(copy(sessions), config)
        
        async with pool.borrow():
            start_time = time.time()
            with pytest.raises(PoolError):
                async with pool.borrow():
                    pass
            elapsed = time.time() - start_time
            # Should have attempted multiple retries within the timeout
            assert 0.25 <= elapsed <= 0.35


class TestSessionPoolStressTests:
    """Stress tests for session pools"""

    def test_sync_high_concurrency_stress(self):
        """Test sync pool under high concurrency stress"""
        mock_conn = Mock()
        sessions = {
            Session(conn=mock_conn, username=f"user{i}", password=f"pass{i}", session_config=None, auth_options=None)
            for i in range(10)
        }
        config = SessionPoolConfig(size=10)
        pool = SessionPool(copy(sessions), config)
        results = []
        errors = []
        
        def stress_worker(worker_id):
            try:
                for i in range(5):  # Each worker does 5 operations
                    with pool.borrow() as session:
                        results.append((worker_id, i, session))
                        time.sleep(0.01)  # Very short work simulation
            except Exception as e:
                errors.append((worker_id, e))
        
        # Start 20 threads (more than pool size)
        threads = []
        for i in range(20):
            thread = threading.Thread(target=stress_worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 100  # 20 workers * 5 operations each
        
        # All sessions should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 10

    @pytest.mark.asyncio
    async def test_async_high_concurrency_stress(self):
        """Test async pool under high concurrency stress"""
        mock_conn = AsyncMock()
        sessions = {
            AsyncSession(conn=mock_conn, username=f"user{i}", password=f"pass{i}", session_config=None, auth_options=None)
            for i in range(10)
        }
        config = SessionPoolConfig(size=10)
        pool = AsyncSessionPool(copy(sessions), config)
        results = []
        errors = []
        
        async def stress_worker(worker_id):
            try:
                for i in range(5):  # Each worker does 5 operations
                    async with pool.borrow() as session:
                        results.append((worker_id, i, session))
                        await asyncio.sleep(0.01)  # Very short async work simulation
            except Exception as e:
                errors.append((worker_id, e))
        
        # Start 20 tasks (more than pool size)
        tasks = [stress_worker(i) for i in range(20)]
        await asyncio.gather(*tasks)
        
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 100  # 20 workers * 5 operations each
        
        # All sessions should be returned
        assert len(pool.busy_sessions_queue) == 0
        assert len(pool.free_sessions_queue) == 10 