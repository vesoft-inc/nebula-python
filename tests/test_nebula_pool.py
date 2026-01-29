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

"""Complete tests for NebulaPool and NebulaPoolConfig"""

import time
import threading
from unittest.mock import Mock, MagicMock, patch, AsyncMock

import pytest

from nebulagraph_python.client.nebula_pool import NebulaPool, NebulaPoolConfig
from nebulagraph_python.client.nebula_client import NebulaClient
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import AuthenticatingError, ExecutingError
from nebulagraph_python.client.constants import (
    DEFAULT_MAX_CLIENT_SIZE,
    DEFAULT_MIN_CLIENT_SIZE,
    DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    DEFAULT_PING_TIMEOUT_MS,
    DEFAULT_HEALTH_CHECK_TIME_MS,
    DEFAULT_TEST_ON_BORROW,
    DEFAULT_BLOCK_WHEN_EXHAUSTED,
    DEFAULT_MAX_WAIT_MS,
    DEFAULT_IDLE_EVICT_SCHEDULE_MS,
    DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS,
    DEFAULT_STRICT_SERVER_HEALTHY,
    DEFAULT_MAX_LIFE_TIME_MS,
    DEFAULT_SCAN_PARALLEL,
    DEFAULT_ENABLE_TLS,
)


class TestNebulaPoolConfig:
    """Test cases for NebulaPoolConfig"""

    def test_config_defaults(self):
        """Test NebulaPoolConfig with default values"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass"
        )
        assert config.addresses == "127.0.0.1:9669"
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.max_client_size == DEFAULT_MAX_CLIENT_SIZE
        assert config.min_client_size == DEFAULT_MIN_CLIENT_SIZE
        assert config.max_wait_ms == DEFAULT_MAX_WAIT_MS
        assert config.block_when_exhausted == DEFAULT_BLOCK_WHEN_EXHAUSTED
        assert config.connect_timeout_ms == DEFAULT_CONNECT_TIMEOUT_MS
        assert config.request_timeout_ms == DEFAULT_REQUEST_TIMEOUT_MS
        assert config.server_ping_timeout_ms == DEFAULT_PING_TIMEOUT_MS
        assert config.health_check_time_ms == DEFAULT_HEALTH_CHECK_TIME_MS
        assert config.test_on_borrow == DEFAULT_TEST_ON_BORROW
        assert config.idle_evict_schedule_ms == DEFAULT_IDLE_EVICT_SCHEDULE_MS
        assert config.min_evictable_idle_time_ms == DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS
        assert config.strictly_server_healthy == DEFAULT_STRICT_SERVER_HEALTHY
        assert config.max_life_time_ms == DEFAULT_MAX_LIFE_TIME_MS
        assert config.scan_parallel == DEFAULT_SCAN_PARALLEL
        assert config.enable_tls == DEFAULT_ENABLE_TLS
        assert config.graph is None
        assert config.schema is None
        assert config.timezone is None
        assert config.session_configs == {}
        assert config.parameters == {}
        assert config.pre_statements == []
        assert config.ssl_param is None

    def test_config_custom_pool_settings(self):
        """Test NebulaPoolConfig with custom pool settings"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            max_client_size=20,
            min_client_size=5,
            max_wait_ms=5000,
            block_when_exhausted=True,
        )
        assert config.max_client_size == 20
        assert config.min_client_size == 5
        assert config.max_wait_ms == 5000
        assert config.block_when_exhausted is True

    def test_config_custom_timeout_settings(self):
        """Test NebulaPoolConfig with custom timeout settings"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            connect_timeout_ms=5000,
            request_timeout_ms=120000,
            server_ping_timeout_ms=2000,
        )
        assert config.connect_timeout_ms == 5000
        assert config.request_timeout_ms == 120000
        assert config.server_ping_timeout_ms == 2000

    def test_config_custom_health_check_settings(self):
        """Test NebulaPoolConfig with custom health check settings"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            health_check_time_ms=300000,
            test_on_borrow=False,
        )
        assert config.health_check_time_ms == 300000
        assert config.test_on_borrow is False

    def test_config_custom_eviction_settings(self):
        """Test NebulaPoolConfig with custom eviction settings"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            idle_evict_schedule_ms=60000,
            min_evictable_idle_time_ms=900000,
        )
        assert config.idle_evict_schedule_ms == 60000
        assert config.min_evictable_idle_time_ms == 900000

    def test_config_custom_server_settings(self):
        """Test NebulaPoolConfig with custom server settings"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            strictly_server_healthy=True,
            max_life_time_ms=3600000,
        )
        assert config.strictly_server_healthy is True
        assert config.max_life_time_ms == 3600000

    def test_config_custom_session_settings(self):
        """Test NebulaPoolConfig with custom session settings"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            graph="test_graph",
            schema="test_schema",
            timezone="UTC",
            session_configs={"key": "value"},
            parameters={"param1": "value1"},
            pre_statements=["USE test_graph"],
        )
        assert config.graph == "test_graph"
        assert config.schema == "test_schema"
        assert config.timezone == "UTC"
        assert config.session_configs == {"key": "value"}
        assert config.parameters == {"param1": "value1"}
        assert config.pre_statements == ["USE test_graph"]

    def test_config_custom_other_settings(self):
        """Test NebulaPoolConfig with custom other settings"""
        ssl_param = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            scan_parallel=20,
            enable_tls=True,
            ssl_param=ssl_param,
        )
        assert config.scan_parallel == 20
        assert config.enable_tls is True
        assert config.ssl_param == ssl_param

    def test_config_auth_options_post_init(self):
        """Test that auth_options is populated with password in __post_init__"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass"
        )
        assert config.auth_options == {"password": "test_pass"}

    def test_config_auth_options_without_password(self):
        """Test auth_options without password"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password=None
        )
        assert config.auth_options == {}

    def test_config_multiple_addresses(self):
        """Test NebulaPoolConfig with multiple addresses"""
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669,127.0.0.2:9669,127.0.0.3:9669",
            username="test_user",
            password="test_pass"
        )
        assert config.addresses == "127.0.0.1:9669,127.0.0.2:9669,127.0.0.3:9669"

    def test_config_all_parameters(self):
        """Test NebulaPoolConfig with all parameters"""
        ssl_param = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669,127.0.0.2:9669",
            username="test_user",
            password="test_pass",
            max_client_size=20,
            min_client_size=5,
            max_wait_ms=5000,
            block_when_exhausted=True,
            connect_timeout_ms=5000,
            request_timeout_ms=120000,
            server_ping_timeout_ms=2000,
            health_check_time_ms=300000,
            test_on_borrow=False,
            idle_evict_schedule_ms=60000,
            min_evictable_idle_time_ms=900000,
            strictly_server_healthy=True,
            max_life_time_ms=3600000,
            graph="test_graph",
            schema="test_schema",
            timezone="UTC",
            session_configs={"key": "value"},
            parameters={"param1": "value1"},
            pre_statements=["USE test_graph"],
            scan_parallel=20,
            enable_tls=True,
            ssl_param=ssl_param,
        )
        assert config.addresses == "127.0.0.1:9669,127.0.0.2:9669"
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.max_client_size == 20
        assert config.min_client_size == 5
        assert config.max_wait_ms == 5000
        assert config.block_when_exhausted is True
        assert config.connect_timeout_ms == 5000
        assert config.request_timeout_ms == 120000
        assert config.server_ping_timeout_ms == 2000
        assert config.health_check_time_ms == 300000
        assert config.test_on_borrow is False
        assert config.idle_evict_schedule_ms == 60000
        assert config.min_evictable_idle_time_ms == 900000
        assert config.strictly_server_healthy is True
        assert config.max_life_time_ms == 3600000
        assert config.graph == "test_graph"
        assert config.schema == "test_schema"
        assert config.timezone == "UTC"
        assert config.session_configs == {"key": "value"}
        assert config.parameters == {"param1": "value1"}
        assert config.pre_statements == ["USE test_graph"]
        assert config.scan_parallel == 20
        assert config.enable_tls is True
        assert config.ssl_param == ssl_param


class TestNebulaPool:
    """Test cases for NebulaPool"""

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_creation_with_defaults(self, mock_factory_class, mock_lb_class):
        """Test creating a NebulaPool with default configuration"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=2
        )

        pool = NebulaPool(config)

        assert pool.config == config
        assert len(pool._pool) == 2
        assert pool._closed is False
        mock_lb.check_servers.assert_called_once()

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_creation_with_custom_config(self, mock_factory_class, mock_lb_class):
        """Test creating a NebulaPool with custom configuration"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669,127.0.0.2:9669",
            username="test_user",
            password="test_pass",
            max_client_size=10,
            min_client_size=3,
            test_on_borrow=False,
        )

        pool = NebulaPool(config)

        assert len(pool._pool) == 3
        assert mock_factory.create.call_count == 3

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_creation_with_ssl(self, mock_factory_class, mock_lb_class):
        """Test creating a NebulaPool with SSL enabled"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        ssl_param = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            enable_tls=True,
            ssl_param=ssl_param,
            min_client_size=1
        )

        pool = NebulaPool(config)

        assert pool.config.enable_tls is True
        assert pool.config.ssl_param == ssl_param

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_client_success(self, mock_factory_class, mock_lb_class):
        """Test successfully getting a client from the pool"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client
        mock_factory.validate.return_value = True

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1,
            test_on_borrow=True
        )

        pool = NebulaPool(config)

        client = pool.get_client()

        assert client is not None
        assert client in pool._pool
        assert pool._in_use[client] is True

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_client_creates_new(self, mock_factory_class, mock_lb_class):
        """Test getting a client creates a new one if under max limit"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client1 = MagicMock(spec=NebulaClient)
        mock_client1.is_closed_client.return_value = False
        mock_client1.get_create_time.return_value = int(time.time() * 1000)
        mock_client2 = MagicMock(spec=NebulaClient)
        mock_client2.is_closed_client.return_value = False
        mock_client2.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.side_effect = [mock_client1, mock_client2]

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1,
            max_client_size=2
        )

        pool = NebulaPool(config)

        # Get all clients
        client1 = pool.get_client()
        client2 = pool.get_client()

        assert client1 != client2
        assert len(pool._pool) == 2
        assert pool._in_use[client1] is True
        assert pool._in_use[client2] is True

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_client_timeout(self, mock_factory_class, mock_lb_class):
        """Test getting a client times out when all are in use"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1,
            max_client_size=1,
            max_wait_ms=100,
            block_when_exhausted=True  # Enable blocking to wait for timeout
        )

        pool = NebulaPool(config)

        # Get the only client
        client1 = pool.get_client()

        # Try to get another - should timeout
        with pytest.raises(RuntimeError, match="Timeout waiting for client"):
            pool.get_client()

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_client_block_when_exhausted_false(self, mock_factory_class, mock_lb_class):
        """Test getting a client raises when block_when_exhausted is False"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1,
            max_client_size=1,
            block_when_exhausted=False
        )

        pool = NebulaPool(config)

        # Get the only client
        client1 = pool.get_client()

        # Try to get another - should raise immediately
        with pytest.raises(RuntimeError, match="No available clients in pool"):
            pool.get_client()

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_client_test_on_borrow_invalidates(self, mock_factory_class, mock_lb_class):
        """Test getting a client with test_on_borrow invalidates invalid clients"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client1 = MagicMock(spec=NebulaClient)
        mock_client1.is_closed_client.return_value = False
        mock_client1.get_create_time.return_value = int(time.time() * 1000)
        mock_client2 = MagicMock(spec=NebulaClient)
        mock_client2.is_closed_client.return_value = False
        mock_client2.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.side_effect = [mock_client1, mock_client2]
        mock_factory.validate.side_effect = [False, True]

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1,
            test_on_borrow=True
        )

        pool = NebulaPool(config)

        # First client should be invalidated, second should be returned
        client = pool.get_client()

        assert client == mock_client2
        assert mock_client1 not in pool._pool
        assert mock_client2 in pool._pool

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_return_client_success(self, mock_factory_class, mock_lb_class):
        """Test successfully returning a client to the pool"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1
        )

        pool = NebulaPool(config)

        client = pool.get_client()
        assert pool._in_use[client] is True

        pool.return_client(client)
        assert pool._in_use[client] is False

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_return_client_closed(self, mock_factory_class, mock_lb_class):
        """Test returning a closed client removes it from pool"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = True
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1
        )

        pool = NebulaPool(config)

        client = pool.get_client()
        pool.return_client(client)

        assert client not in pool._pool
        assert client not in pool._in_use
        mock_factory.destroy.assert_called_once_with(client)

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_return_client_expired(self, mock_factory_class, mock_lb_class):
        """Test returning an expired client removes it from pool"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        # Set create time to 2 hours ago
        mock_client.get_create_time.return_value = int(time.time() * 1000) - 7200000
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1,
            max_life_time_ms=3600000  # 1 hour
        )

        pool = NebulaPool(config)

        client = pool.get_client()
        pool.return_client(client)

        assert client not in pool._pool
        assert client not in pool._in_use
        mock_factory.destroy.assert_called_once_with(client)

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_close(self, mock_factory_class, mock_lb_class):
        """Test closing the pool"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client1 = MagicMock(spec=NebulaClient)
        mock_client2 = MagicMock(spec=NebulaClient)
        mock_factory.create.side_effect = [mock_client1, mock_client2]

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=2
        )

        pool = NebulaPool(config)
        pool.close()

        assert pool._closed is True
        assert len(pool._pool) == 0
        assert len(pool._in_use) == 0
        mock_factory.destroy.assert_any_call(mock_client1)
        mock_factory.destroy.assert_any_call(mock_client2)

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_client_after_close(self, mock_factory_class, mock_lb_class):
        """Test getting a client after pool is closed raises error"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1
        )

        pool = NebulaPool(config)
        pool.close()

        with pytest.raises(RuntimeError, match="Pool is closed"):
            pool.get_client()

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_active_sessions(self, mock_factory_class, mock_lb_class):
        """Test getting active session count"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client1 = MagicMock(spec=NebulaClient)
        mock_client2 = MagicMock(spec=NebulaClient)
        mock_client3 = MagicMock(spec=NebulaClient)
        mock_factory.create.side_effect = [mock_client1, mock_client2, mock_client3]

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=3
        )

        pool = NebulaPool(config)

        assert pool.get_active_sessions() == 0

        client1 = pool.get_client()
        assert pool.get_active_sessions() == 1

        client2 = pool.get_client()
        assert pool.get_active_sessions() == 2

        pool.return_client(client1)
        assert pool.get_active_sessions() == 1

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_get_idle_sessions(self, mock_factory_class, mock_lb_class):
        """Test getting idle session count"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client1 = MagicMock(spec=NebulaClient)
        mock_client2 = MagicMock(spec=NebulaClient)
        mock_client3 = MagicMock(spec=NebulaClient)
        mock_client1.is_closed_client.return_value = False
        mock_client2.is_closed_client.return_value = False
        mock_client3.is_closed_client.return_value = False
        mock_client1.get_create_time.return_value = int(time.time() * 1000)
        mock_client2.get_create_time.return_value = int(time.time() * 1000)
        mock_client3.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.side_effect = [mock_client1, mock_client2, mock_client3]

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=3
        )

        pool = NebulaPool(config)

        # Initially all 3 clients are idle
        assert pool.get_idle_sessions() == 3

        # Get client1 - now 2 idle
        client1 = pool.get_client()
        assert pool.get_idle_sessions() == 2

        # Get client2 - now 1 idle
        client2 = pool.get_client()
        assert pool.get_idle_sessions() == 1

        # Return client1 - now 2 idle
        pool.return_client(client1)
        assert pool.get_idle_sessions() == 2

        # Return client2 - now 3 idle
        pool.return_client(client2)
        assert pool.get_idle_sessions() == 3

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_context_manager(self, mock_factory_class, mock_lb_class):
        """Test pool as context manager"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1
        )

        with NebulaPool(config) as pool:
            assert pool._closed is False

        assert pool._closed is True

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_concurrent_access(self, mock_factory_class, mock_lb_class):
        """Test concurrent access to the pool"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_clients = [MagicMock(spec=NebulaClient) for _ in range(3)]
        for client in mock_clients:
            client.is_closed_client.return_value = False
            client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.side_effect = mock_clients

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            max_client_size=3,
            min_client_size=3
        )

        pool = NebulaPool(config)

        results = []
        errors = []

        def use_pool(thread_id):
            try:
                client = pool.get_client()
                results.append(thread_id)
                time.sleep(0.1)
                pool.return_client(client)
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(3):
            thread = threading.Thread(target=use_pool, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 3

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_parse_addresses(self, mock_factory_class, mock_lb_class):
        """Test address parsing"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669,127.0.0.2:9669,127.0.0.3:9669",
            username="test_user",
            password="test_pass",
            min_client_size=1
        )

        pool = NebulaPool(config)

        addresses = NebulaPool._parse_addresses(config.addresses)
        assert len(addresses) == 3
        assert addresses[0] == HostAddress("127.0.0.1", 9669)
        assert addresses[1] == HostAddress("127.0.0.2", 9669)
        assert addresses[2] == HostAddress("127.0.0.3", 9669)

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_parse_addresses_invalid(self, mock_factory_class, mock_lb_class):
        """Test parsing invalid addresses raises error"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client = MagicMock(spec=NebulaClient)
        mock_client.is_closed_client.return_value = False
        mock_client.get_create_time.return_value = int(time.time() * 1000)
        mock_factory.create.return_value = mock_client

        config = NebulaPoolConfig(
            addresses="127.0.0.1",  # Missing port
            username="test_user",
            password="test_pass",
            min_client_size=1
        )

        with pytest.raises(ValueError, match="Invalid address format"):
            NebulaPool._parse_addresses(config.addresses)

    @patch("nebulagraph_python.client.nebula_pool.RoundRobinLoadBalancer")
    @patch("nebulagraph_python.client.nebula_pool.ClientPoolFactory")
    def test_pool_init_failure_handles_gracefully(self, mock_factory_class, mock_lb_class):
        """Test pool initialization handles creation failures gracefully"""
        mock_lb = MagicMock()
        mock_lb_class.return_value = mock_lb
        mock_factory = MagicMock()
        mock_factory_class.return_value = mock_factory
        mock_client1 = MagicMock(spec=NebulaClient)
        mock_client2 = MagicMock(spec=NebulaClient)
        mock_client1.is_closed_client.return_value = False
        mock_client1.get_create_time.return_value = int(time.time() * 1000)
        mock_client2.is_closed_client.return_value = False
        mock_client2.get_create_time.return_value = int(time.time() * 1000)

        # First call succeeds, second fails, third succeeds
        mock_factory.create.side_effect = [mock_client1, Exception("Create failed"), mock_client2]

        config = NebulaPoolConfig(
            addresses="127.0.0.1:9669",
            username="test_user",
            password="test_pass",
            min_client_size=3
        )

        # Pool should still be created, but with fewer clients
        pool = NebulaPool(config)

        # Should have 2 clients (one failed)
        assert len(pool._pool) == 2