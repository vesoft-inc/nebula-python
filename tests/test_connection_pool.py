import asyncio
import threading
import time
from unittest.mock import AsyncMock, Mock, patch

import pytest

from nebulagraph_python.client._connection import (
    AsyncConnection,
    Connection,
    ConnectionConfig,
)
from nebulagraph_python.client._connection_pool import (
    AsyncConnectionPool,
    ConnectionPool,
)
from nebulagraph_python.data import HostAddress
from nebulagraph_python.error import ConnectingError, PoolError


class TestConnectionPool:
    """Test cases for ConnectionPool (synchronous)"""

    def test_init_single_host(self):
        """Test initialization with a single host"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host])
        pool = ConnectionPool(config)
        
        assert len(pool.addresses) == 1
        assert pool.addresses[0] == host
        assert pool.current_address == host
        assert len(pool._connections) == 1
        assert host in pool._connections

    def test_init_multiple_hosts(self):
        """Test initialization with multiple hosts"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
            HostAddress("localhost", 9671),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        assert len(pool.addresses) == 3
        assert pool.addresses == hosts
        assert pool.current_address == hosts[0]
        assert len(pool._connections) == 3
        for host in hosts:
            assert host in pool._connections

    def test_init_with_ping_enabled(self):
        """Test initialization with ping enabled"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = ConnectionPool(config)
        
        # Each connection should have ping disabled in its config
        for conn in pool._connections.values():
            assert not conn.config.ping_before_execute

    @patch('nebulagraph_python.client._connection_pool.Connection')
    def test_init_creates_connections_with_single_host_config(self, mock_connection):
        """Test that each connection is created with a config containing only its host"""
        hosts = [
            HostAddress("host1", 9669),
            HostAddress("host2", 9669),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Should create one connection per host
        assert mock_connection.call_count == 2
        
        # Each connection should be created with a config containing only one host
        for call_args in mock_connection.call_args_list:
            conn_config = call_args[0][0]
            assert len(conn_config.hosts) == 1
            assert not conn_config.ping_before_execute

    def test_next_address_round_robin(self):
        """Test that next_address implements round-robin"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
            HostAddress("localhost", 9671),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Initial address should be hosts[0]
        assert pool.current_address == hosts[0]
        
        # Should cycle through hosts
        assert pool.next_address() == hosts[1]
        assert pool.next_address() == hosts[2]
        assert pool.next_address() == hosts[0]  # Back to first
        assert pool.next_address() == hosts[1]

    def test_next_address_thread_safety(self):
        """Test that next_address is thread-safe"""
        hosts = [HostAddress(f"localhost", 9669 + i) for i in range(3)]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        results = []
        errors = []

        def get_addresses(thread_id):
            try:
                for _ in range(100):
                    addr = pool.next_address()
                    results.append((thread_id, addr))
            except Exception as e:
                errors.append((thread_id, e))

        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=get_addresses, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 500  # 5 threads * 100 calls each
        
        # Check that all addresses are valid
        for _, addr in results:
            assert addr in hosts

    @patch('nebulagraph_python.client._connection_pool.Connection')
    def test_connect_success(self, mock_connection):
        """Test successful connection to all hosts"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Mock the connections
        mock_conn1 = Mock()
        mock_conn2 = Mock()
        pool._connections[hosts[0]] = mock_conn1
        pool._connections[hosts[1]] = mock_conn2
        
        pool.connect()
        
        # Should call connect on all connections
        mock_conn1.connect.assert_called_once()
        mock_conn2.connect.assert_called_once()

    def test_get_connection_without_ping(self):
        """Test getting connection when ping is disabled"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=False)
        pool = ConnectionPool(config)
        
        # Mock the connection
        mock_conn = Mock()
        pool._connections[host] = mock_conn
        
        result = pool.get_connection(host)
        
        assert result == mock_conn
        mock_conn.ping.assert_not_called()

    def test_get_connection_with_ping_success(self):
        """Test getting connection when ping succeeds"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = ConnectionPool(config)
        
        # Mock the connection
        mock_conn = Mock()
        mock_conn.ping.return_value = True
        pool._connections[host] = mock_conn
        
        result = pool.get_connection(host)
        
        assert result == mock_conn
        mock_conn.ping.assert_called_once()
        mock_conn.reconnect.assert_not_called()

    def test_get_connection_with_ping_fail_reconnect_success(self):
        """Test getting connection when ping fails but reconnect succeeds"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = ConnectionPool(config)
        
        # Mock the connection
        mock_conn = Mock()
        mock_conn.ping.return_value = False
        pool._connections[host] = mock_conn
        
        result = pool.get_connection(host)
        
        assert result == mock_conn
        mock_conn.ping.assert_called_once()
        mock_conn.reconnect.assert_called_once()

    @patch('nebulagraph_python.client._connection_pool.logger')
    def test_get_connection_with_ping_fail_reconnect_fail(self, mock_logger):
        """Test getting connection when both ping and reconnect fail"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = ConnectionPool(config)
        
        # Mock the connection
        mock_conn = Mock()
        mock_conn.ping.return_value = False
        mock_conn.reconnect.side_effect = ConnectingError("Connection failed")
        pool._connections[host] = mock_conn
        
        result = pool.get_connection(host)
        
        assert result is None
        mock_conn.ping.assert_called_once()
        mock_conn.reconnect.assert_called_once()
        mock_logger.exception.assert_called_once()

    def test_next_connection_success(self):
        """Test getting next available connection"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Mock connections
        mock_conn1 = Mock()
        mock_conn2 = Mock()
        pool._connections[hosts[0]] = mock_conn1
        pool._connections[hosts[1]] = mock_conn2
        
        # Mock get_connection to return the connection for the current address
        with patch.object(pool, 'get_connection', side_effect=[mock_conn1]):
            addr, conn = pool.next_connection()
            
            assert addr == hosts[1]  # Should advance to next address
            assert conn == mock_conn1

    def test_next_connection_with_failures(self):
        """Test getting next connection when some hosts are unavailable"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
            HostAddress("localhost", 9671),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Mock get_connection to fail for first two hosts, succeed for third
        def mock_get_connection(host_addr):
            if host_addr in [hosts[0], hosts[1]]:
                return None
            return Mock()
        
        with patch.object(pool, 'get_connection', side_effect=mock_get_connection):
            addr, conn = pool.next_connection()
            
            assert addr == hosts[2]
            assert conn is not None

    def test_next_connection_all_fail(self):
        """Test getting next connection when all hosts are unavailable"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Mock get_connection to always return None
        with patch.object(pool, 'get_connection', return_value=None):
            with pytest.raises(PoolError, match="No connection available in the pool"):
                pool.next_connection()

    def test_close_all_connections(self):
        """Test closing all connections in the pool"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Mock connections
        mock_conn1 = Mock()
        mock_conn2 = Mock()
        pool._connections[hosts[0]] = mock_conn1
        pool._connections[hosts[1]] = mock_conn2
        
        pool.close()
        
        # Should close all connections and clear the dictionary
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        assert len(pool._connections) == 0

    def test_concurrent_next_connection(self):
        """Test concurrent access to next_connection"""
        hosts = [HostAddress(f"localhost", 9669 + i) for i in range(3)]
        config = ConnectionConfig(hosts=hosts)
        pool = ConnectionPool(config)
        
        # Mock all connections to be available
        for host in hosts:
            pool._connections[host] = Mock()
        
        results = []
        errors = []

        def get_next_connection(thread_id):
            try:
                for _ in range(10):
                    with patch.object(pool, 'get_connection', return_value=Mock()):
                        addr, conn = pool.next_connection()
                        results.append((thread_id, addr))
            except Exception as e:
                errors.append((thread_id, e))

        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=get_next_connection, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 30  # 3 threads * 10 calls each


class TestAsyncConnectionPool:
    """Test cases for AsyncConnectionPool (asynchronous)"""

    @pytest.mark.asyncio
    async def test_init_single_host(self):
        """Test initialization with a single host"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host])
        pool = AsyncConnectionPool(config)
        
        assert len(pool.addresses) == 1
        assert pool.addresses[0] == host
        assert pool.current_address == host
        assert len(pool._connections) == 1
        assert host in pool._connections

    @pytest.mark.asyncio
    async def test_init_multiple_hosts(self):
        """Test initialization with multiple hosts"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
            HostAddress("localhost", 9671),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        assert len(pool.addresses) == 3
        assert pool.addresses == hosts
        assert pool.current_address == hosts[0]
        assert len(pool._connections) == 3
        for host in hosts:
            assert host in pool._connections

    @pytest.mark.asyncio
    async def test_init_with_ping_enabled(self):
        """Test initialization with ping enabled"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = AsyncConnectionPool(config)
        
        # Each connection should have ping disabled in its config
        for conn in pool._connections.values():
            assert not conn.config.ping_before_execute

    @pytest.mark.asyncio
    @patch('nebulagraph_python.client._connection_pool.AsyncConnection')
    async def test_init_creates_connections_with_single_host_config(self, mock_connection):
        """Test that each connection is created with a config containing only its host"""
        hosts = [
            HostAddress("host1", 9669),
            HostAddress("host2", 9669),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Should create one connection per host
        assert mock_connection.call_count == 2
        
        # Each connection should be created with a config containing only one host
        for call_args in mock_connection.call_args_list:
            conn_config = call_args[0][0]
            assert len(conn_config.hosts) == 1
            assert not conn_config.ping_before_execute

    @pytest.mark.asyncio
    async def test_next_address_round_robin(self):
        """Test that next_address implements round-robin"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
            HostAddress("localhost", 9671),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Initial address should be hosts[0]
        assert pool.current_address == hosts[0]
        
        # Should cycle through hosts
        assert await pool.next_address() == hosts[1]
        assert await pool.next_address() == hosts[2]
        assert await pool.next_address() == hosts[0]  # Back to first
        assert await pool.next_address() == hosts[1]

    @pytest.mark.asyncio
    async def test_next_address_async_safety(self):
        """Test that next_address is async/coroutine-level safe"""
        hosts = [HostAddress(f"localhost", 9669 + i) for i in range(10)]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        results = []

        async def get_addresses(task_id):
            for _ in range(100):
                addr = await pool.next_address()
                results.append((task_id, addr))

        # Start multiple tasks
        tasks = []
        for i in range(5):
            task = asyncio.create_task(get_addresses(i))
            tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        assert len(results) == 500  # 5 tasks * 100 calls each
        
        # Check that all addresses are valid
        for _, addr in results:
            assert addr in hosts

    @pytest.mark.asyncio
    @patch('nebulagraph_python.client._connection_pool.AsyncConnection')
    async def test_connect_success(self, mock_connection):
        """Test successful connection to all hosts"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Mock the connections
        mock_conn1 = AsyncMock()
        mock_conn2 = AsyncMock()
        pool._connections[hosts[0]] = mock_conn1
        pool._connections[hosts[1]] = mock_conn2
        
        await pool.connect()
        
        # Should call connect on all connections
        mock_conn1.connect.assert_called_once()
        mock_conn2.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_without_ping(self):
        """Test getting connection when ping is disabled"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=False)
        pool = AsyncConnectionPool(config)
        
        # Mock the connection
        mock_conn = AsyncMock()
        pool._connections[host] = mock_conn
        
        result = await pool.get_connection(host)
        
        assert result == mock_conn
        mock_conn.ping.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_connection_with_ping_success(self):
        """Test getting connection when ping succeeds"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = AsyncConnectionPool(config)
        
        # Mock the connection
        mock_conn = AsyncMock()
        mock_conn.ping.return_value = True
        pool._connections[host] = mock_conn
        
        result = await pool.get_connection(host)
        
        assert result == mock_conn
        mock_conn.ping.assert_called_once()
        mock_conn.reconnect.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_connection_with_ping_fail_reconnect_success(self):
        """Test getting connection when ping fails but reconnect succeeds"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = AsyncConnectionPool(config)
        
        # Mock the connection
        mock_conn = AsyncMock()
        mock_conn.ping.return_value = False
        pool._connections[host] = mock_conn
        
        result = await pool.get_connection(host)
        
        assert result == mock_conn
        mock_conn.ping.assert_called_once()
        mock_conn.reconnect.assert_called_once()

    @pytest.mark.asyncio
    @patch('nebulagraph_python.client._connection_pool.logger')
    async def test_get_connection_with_ping_fail_reconnect_fail(self, mock_logger):
        """Test getting connection when both ping and reconnect fail"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = AsyncConnectionPool(config)
        
        # Mock the connection
        mock_conn = AsyncMock()
        mock_conn.ping.return_value = False
        mock_conn.reconnect.side_effect = ConnectingError("Connection failed")
        pool._connections[host] = mock_conn
        
        result = await pool.get_connection(host)
        
        assert result is None
        mock_conn.ping.assert_called_once()
        mock_conn.reconnect.assert_called_once()
        mock_logger.exception.assert_called_once()

    @pytest.mark.asyncio
    async def test_next_connection_success(self):
        """Test getting next available connection"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Mock connections
        mock_conn1 = AsyncMock()
        mock_conn2 = AsyncMock()
        pool._connections[hosts[0]] = mock_conn1
        pool._connections[hosts[1]] = mock_conn2
        
        # Mock get_connection to return the connection for the current address
        async def mock_get_connection(host_addr):
            if host_addr == hosts[1]:
                return mock_conn1
            return None
        
        with patch.object(pool, 'get_connection', side_effect=mock_get_connection):
            addr, conn = await pool.next_connection()
            
            assert addr == hosts[1]  # Should advance to next address
            assert conn == mock_conn1

    @pytest.mark.asyncio
    async def test_next_connection_with_failures(self):
        """Test getting next connection when some hosts are unavailable"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
            HostAddress("localhost", 9671),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Mock get_connection to fail for first two hosts, succeed for third
        async def mock_get_connection(host_addr):
            if host_addr in [hosts[0], hosts[1]]:
                return None
            return AsyncMock()
        
        with patch.object(pool, 'get_connection', side_effect=mock_get_connection):
            addr, conn = await pool.next_connection()
            
            assert addr == hosts[2]
            assert conn is not None

    @pytest.mark.asyncio
    async def test_next_connection_all_fail(self):
        """Test getting next connection when all hosts are unavailable"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Mock get_connection to always return None
        async def mock_get_connection(host_addr):
            return None
        
        with patch.object(pool, 'get_connection', side_effect=mock_get_connection):
            with pytest.raises(PoolError, match="No connection available in the pool"):
                await pool.next_connection()

    @pytest.mark.asyncio
    async def test_close_all_connections(self):
        """Test closing all connections in the pool"""
        hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Mock connections
        mock_conn1 = AsyncMock()
        mock_conn2 = AsyncMock()
        pool._connections[hosts[0]] = mock_conn1
        pool._connections[hosts[1]] = mock_conn2
        
        await pool.close()
        
        # Should close all connections and clear the dictionary
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        assert len(pool._connections) == 0

    @pytest.mark.asyncio
    async def test_concurrent_next_connection(self):
        """Test concurrent access to next_connection"""
        hosts = [HostAddress(f"localhost", 9669 + i) for i in range(5)]
        config = ConnectionConfig(hosts=hosts)
        pool = AsyncConnectionPool(config)
        
        # Mock all connections to be available
        for host in hosts:
            pool._connections[host] = AsyncMock()
        
        results = []

        async def get_next_connection(task_id):
            for _ in range(10):
                async def mock_get_connection(host_addr):
                    return AsyncMock()
                
                with patch.object(pool, 'get_connection', side_effect=mock_get_connection):
                    addr, conn = await pool.next_connection()
                    results.append((task_id, addr))

        # Start multiple tasks
        tasks = []
        for i in range(3):
            task = asyncio.create_task(get_next_connection(i))
            tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        assert len(results) == 30  # 3 tasks * 10 calls each


class TestConnectionPoolEdgeCases:
    """Test edge cases and error conditions for both pool types"""

    def test_pool_empty_hosts_list(self):
        """Test sync pool with empty hosts list"""
        with pytest.raises(ValueError):
            config = ConnectionConfig(hosts=[])



    def test_sync_pool_single_host_round_robin(self):
        """Test sync pool with single host always returns same address"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host])
        pool = ConnectionPool(config)
        
        for _ in range(10):
            assert pool.next_address() == host

    @pytest.mark.asyncio
    async def test_async_pool_single_host_round_robin(self):
        """Test async pool with single host always returns same address"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host])
        pool = AsyncConnectionPool(config)
        
        for _ in range(10):
            assert await pool.next_address() == host

    @patch('nebulagraph_python.client._connection_pool.logger')
    def test_sync_get_connection_exception_during_reconnect(self, mock_logger):
        """Test sync pool handling of unexpected exceptions during reconnect"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = ConnectionPool(config)
        
        # Mock the connection
        mock_conn = Mock()
        mock_conn.ping.return_value = False
        mock_conn.reconnect.side_effect = RuntimeError("Unexpected error")
        pool._connections[host] = mock_conn
        
        # Should catch the exception and return None
        result = pool.get_connection(host)
        assert result is None
        mock_logger.exception.assert_called_once()

    @pytest.mark.asyncio
    @patch('nebulagraph_python.client._connection_pool.logger')
    async def test_async_get_connection_exception_during_reconnect(self, mock_logger):
        """Test async pool handling of unexpected exceptions during reconnect"""
        host = HostAddress("localhost", 9669)
        config = ConnectionConfig(hosts=[host], ping_before_execute=True)
        pool = AsyncConnectionPool(config)
        
        # Mock the connection
        mock_conn = AsyncMock()
        mock_conn.ping.return_value = False
        mock_conn.reconnect.side_effect = RuntimeError("Unexpected error")
        pool._connections[host] = mock_conn
        
        # Should catch the exception and return None
        result = await pool.get_connection(host)
        assert result is None
        mock_logger.exception.assert_called_once()

    def test_sync_pool_config_modification_isolation(self):
        """Test that pool config modifications don't affect original config"""
        original_hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=original_hosts, ping_before_execute=True)
        pool = ConnectionPool(config)
        
        # Original config should remain unchanged
        assert config.ping_before_execute is True
        assert len(config.hosts) == 2
        
        # Each connection should have modified config
        for conn in pool._connections.values():
            assert not conn.config.ping_before_execute
            assert len(conn.config.hosts) == 1

    @pytest.mark.asyncio
    async def test_async_pool_config_modification_isolation(self):
        """Test that pool config modifications don't affect original config"""
        original_hosts = [
            HostAddress("localhost", 9669),
            HostAddress("localhost", 9670),
        ]
        config = ConnectionConfig(hosts=original_hosts, ping_before_execute=True)
        pool = AsyncConnectionPool(config)
        
        # Original config should remain unchanged
        assert config.ping_before_execute is True
        assert len(config.hosts) == 2
        
        # Each connection should have modified config
        for conn in pool._connections.values():
            assert not conn.config.ping_before_execute
            assert len(conn.config.hosts) == 1

