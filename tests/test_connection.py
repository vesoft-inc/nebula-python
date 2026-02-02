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

import asyncio
from unittest.mock import AsyncMock, Mock, patch, MagicMock

import grpc
import pytest

from nebulagraph_python.client._connection import (
    AsyncConnection,
    Connection,
    ConnectionConfig,
    _parse_hosts,
)
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import (
    AuthenticatingError,
    ConnectingError,
    ErrorCode,
    ExecutingError,
    InternalError,
)


class TestParseHosts:
    """Test cases for _parse_hosts function"""

    def test_parse_single_string_host(self):
        """Test parsing a single host string"""
        hosts = _parse_hosts("127.0.0.1:9669")
        assert len(hosts) == 1
        assert hosts[0].host == "127.0.0.1"
        assert hosts[0].port == 9669

    def test_parse_multiple_string_hosts(self):
        """Test parsing multiple host strings"""
        hosts = _parse_hosts("127.0.0.1:9669,127.0.0.2:9669")
        assert len(hosts) == 2
        assert hosts[0].host == "127.0.0.1"
        assert hosts[0].port == 9669
        assert hosts[1].host == "127.0.0.2"
        assert hosts[1].port == 9669

    def test_parse_host_address_objects(self):
        """Test parsing HostAddress objects"""
        hosts = _parse_hosts([HostAddress("127.0.0.1", 9669), HostAddress("127.0.0.2", 9670)])
        assert len(hosts) == 2
        assert hosts[0].host == "127.0.0.1"
        assert hosts[0].port == 9669
        assert hosts[1].host == "127.0.0.2"
        assert hosts[1].port == 9670

    def test_parse_mixed_hosts(self):
        """Test parsing mixed host formats"""
        hosts = _parse_hosts(["127.0.0.1:9669", HostAddress("127.0.0.2", 9670)])
        assert len(hosts) == 2
        assert hosts[0].host == "127.0.0.1"
        assert hosts[0].port == 9669
        assert hosts[1].host == "127.0.0.2"
        assert hosts[1].port == 9670


class TestConnectionConfig:
    """Test cases for ConnectionConfig"""

    def test_from_defaults_basic(self):
        """Test creating ConnectionConfig from defaults"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        assert len(config.hosts) == 1
        assert config.hosts[0].host == "127.0.0.1"
        assert config.hosts[0].port == 9669
        assert config.ssl_param is None
        assert config.connect_timeout is not None
        assert config.request_timeout is not None

    def test_from_defaults_with_ssl_true(self):
        """Test creating ConnectionConfig with SSL enabled"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669", ssl_param=True)
        assert config.ssl_param is not None
        assert isinstance(config.ssl_param, SSLParam)

    def test_from_defaults_with_ssl_param(self):
        """Test creating ConnectionConfig with custom SSLParam"""
        ssl = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        config = ConnectionConfig.from_defaults("127.0.0.1:9669", ssl_param=ssl)
        assert config.ssl_param == ssl

    def test_from_defaults_multiple_hosts(self):
        """Test creating ConnectionConfig with multiple hosts"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669,127.0.0.2:9669")
        assert len(config.hosts) == 2

    def test_from_defaults_with_timeouts(self):
        """Test creating ConnectionConfig with custom timeouts"""
        config = ConnectionConfig.from_defaults(
            "127.0.0.1:9669", connect_timeout=10.0, request_timeout=30.0
        )
        assert config.connect_timeout == 10.0
        assert config.request_timeout == 30.0

    def test_connection_config_empty_hosts_raises_error(self):
        """Test that empty hosts raises ValueError"""
        with pytest.raises(ValueError, match="hosts cannot be empty"):
            ConnectionConfig(hosts=[])


class TestConnection:
    """Test cases for synchronous Connection"""

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_connect_success(self, mock_stub_class, mock_channel_class, mock_future):
        """Test successful connection"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        assert conn._stub is not None
        assert conn._channel is not None
        assert conn.connected is not None
        assert conn.connected.host == "127.0.0.1"
        assert conn.connected.port == 9669

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_connect_with_timeout(self, mock_stub_class, mock_channel_class, mock_future):
        """Test connection with timeout"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669", connect_timeout=5.0)
        conn = Connection(config)

        assert conn._stub is not None

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_connect_failover_to_second_host(self, mock_stub_class, mock_channel_class, mock_future):
        """Test connection failover to second host"""
        call_count = [0]

        def create_channel(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("First host failed")
            mock_channel = MagicMock()
            return mock_channel

        mock_channel_class.side_effect = create_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result

        config = ConnectionConfig.from_defaults("127.0.0.1:9669,127.0.0.2:9669")
        conn = Connection(config)

        assert conn._stub is not None
        assert conn.connected.host == "127.0.0.2"

    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    def test_connect_all_hosts_fail(self, mock_channel_class):
        """Test connection failure when all hosts fail"""
        mock_channel_class.side_effect = Exception("Connection failed")

        config = ConnectionConfig.from_defaults("127.0.0.1:9669,127.0.0.2:9669")
        with pytest.raises(ConnectingError, match="Failed to connect to any"):
            Connection(config)

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_close(self, mock_stub_class, mock_channel_class, mock_future):
        """Test closing connection"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)
        conn.close()

        assert conn._channel is None
        assert conn._stub is None
        assert conn.connected is None

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_ping_success(self, mock_stub_class, mock_channel_class, mock_future):
        """Test successful ping"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        assert conn.ping() is True

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_ping_failure(self, mock_stub_class, mock_channel_class, mock_future):
        """Test ping failure"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub.Execute.side_effect = Exception("Ping failed")
        mock_stub_class.return_value = mock_stub
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        assert conn.ping() is False

    def test_ping_no_stub(self):
        """Test ping when stub is not initialized"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)
        conn._stub = None

        assert conn.ping() is False

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_execute_success(self, mock_stub_class, mock_channel_class, mock_future):
        """Test successful execute"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_response = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Execute.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        # Mock authenticate to set session_id
        with patch.object(conn, "authenticate", return_value=1):
            result = conn.execute(1, "RETURN 1")

            assert result is not None
            mock_stub.Execute.assert_called_once()

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_execute_with_timeout(self, mock_stub_class, mock_channel_class, mock_future):
        """Test execute with custom timeout"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_response = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Execute.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        with patch.object(conn, "authenticate", return_value=1):
            conn.execute(1, "RETURN 1", timeout=5.0)

            # Verify timeout was passed
            call_args = mock_stub.Execute.call_args
            assert call_args[1]["timeout"] == 5.0

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_execute_rpc_error(self, mock_stub_class, mock_channel_class, mock_future):
        """Test execute with RPC error"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Execute.side_effect = grpc.RpcError("RPC error")

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        with patch.object(conn, "authenticate", return_value=1):
            with pytest.raises(ExecutingError, match="RPC error"):
                conn.execute(1, "RETURN 1")

    def test_execute_no_stub(self):
        """Test execute when stub is not initialized"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)
        conn._stub = None

        with pytest.raises(InternalError, match="Connection not established"):
            conn.execute(1, "RETURN 1")

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_authenticate_success(self, mock_stub_class, mock_channel_class, mock_future):
        """Test successful authentication"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_response = MagicMock()
        mock_response.status.code = b"00000"
        mock_response.session_id = 12345
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        with patch("nebulagraph_python.client.client.init_session"):
            session_id = conn.authenticate("user", "pass")

            assert session_id == 12345
            mock_stub.Authenticate.assert_called_once()

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_authenticate_with_auth_options(self, mock_stub_class, mock_channel_class, mock_future):
        """Test authentication with auth options"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_response = MagicMock()
        mock_response.status.code = b"00000"
        mock_response.session_id = 12345
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        with patch("nebulagraph_python.client.client.init_session"):
            session_id = conn.authenticate(
                "user", "pass", auth_options={"option1": "value1"}
            )

            assert session_id == 12345

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_authenticate_failure(self, mock_stub_class, mock_channel_class, mock_future):
        """Test authentication failure"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_response = MagicMock()
        mock_response.status.code = b"E_AUTH_FAILURE"
        mock_response.status.message = b"Authentication failed"
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        with pytest.raises(Exception):
            conn.authenticate("user", "wrong_pass")

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_authenticate_rpc_error(self, mock_stub_class, mock_channel_class, mock_future):
        """Test authentication with RPC error"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.side_effect = grpc.RpcError("RPC error")

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)

        with pytest.raises(AuthenticatingError, match="RPC error"):
            conn.authenticate("user", "pass")

    def test_authenticate_no_stub(self):
        """Test authenticate when stub is not initialized"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = Connection(config)
        conn._stub = None

        with pytest.raises(InternalError, match="Connection not established"):
            conn.authenticate("user", "pass")

    @patch("nebulagraph_python.client._connection.grpc.channel_ready_future")
    @patch("nebulagraph_python.client._connection.grpc.secure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    def test_connect_with_ssl(self, mock_stub_class, mock_channel_class, mock_future):
        """Test connection with SSL"""
        mock_future_result = MagicMock()
        mock_future_result.result.return_value = None
        mock_future.return_value = mock_future_result
        mock_channel = MagicMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        ssl = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        config = ConnectionConfig.from_defaults("127.0.0.1:9669", ssl_param=ssl)
        conn = Connection(config)

        assert conn._stub is not None
        assert conn._channel is not None


class TestAsyncConnection:
    """Test cases for asynchronous AsyncConnection"""

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_connect_success(self, mock_stub_class, mock_channel_class):
        """Test successful async connection"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        assert conn._stub is not None
        assert conn._channel is not None
        assert conn.connected is not None

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_connect_with_timeout(self, mock_stub_class, mock_channel_class):
        """Test async connection with timeout"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669", connect_timeout=5.0)
        conn = AsyncConnection(config)
        await conn.connect()

        assert conn._stub is not None

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_connect_failover_to_second_host(self, mock_stub_class, mock_channel_class):
        """Test async connection failover to second host"""
        call_count = [0]

        async def create_channel(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("First host failed")
            mock_channel = AsyncMock()
            return mock_channel

        mock_channel_class.side_effect = create_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669,127.0.0.2:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        assert conn._stub is not None
        assert conn.connected.host == "127.0.0.2"

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    async def test_connect_all_hosts_fail(self, mock_channel_class):
        """Test async connection failure when all hosts fail"""
        mock_channel_class.side_effect = Exception("Connection failed")

        config = ConnectionConfig.from_defaults("127.0.0.1:9669,127.0.0.2:9669")
        conn = AsyncConnection(config)

        with pytest.raises(ConnectingError, match="Failed to connect asynchronously"):
            await conn.connect()

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_close(self, mock_stub_class, mock_channel_class):
        """Test closing async connection"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()
        await conn.close()

        assert conn._channel is None
        assert conn._stub is None
        assert conn.connected is None

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_ping_success(self, mock_stub_class, mock_channel_class):
        """Test successful async ping"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        assert await conn.ping() is True

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_ping_failure(self, mock_stub_class, mock_channel_class):
        """Test async ping failure"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub.Execute.side_effect = Exception("Ping failed")
        mock_stub_class.return_value = mock_stub
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        assert await conn.ping() is False

    @pytest.mark.asyncio
    async def test_ping_no_stub(self):
        """Test async ping when stub is not initialized"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        conn._stub = None

        assert await conn.ping() is False

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_execute_success(self, mock_stub_class, mock_channel_class):
        """Test successful async execute"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_response = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Execute.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        with patch.object(conn, "authenticate", return_value=1):
            result = await conn.execute(1, "RETURN 1")

            assert result is not None
            mock_stub.Execute.assert_called_once()

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_execute_with_timeout(self, mock_stub_class, mock_channel_class):
        """Test async execute with custom timeout"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_response = MagicMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Execute.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        with patch.object(conn, "authenticate", return_value=1):
            await conn.execute(1, "RETURN 1", timeout=5.0)

            call_args = mock_stub.Execute.call_args
            assert call_args[1]["timeout"] == 5.0

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_execute_rpc_error(self, mock_stub_class, mock_channel_class):
        """Test async execute with RPC error"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Execute.side_effect = grpc.aio.AioRpcError("RPC error")

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        with patch.object(conn, "authenticate", return_value=1):
            with pytest.raises(ExecutingError, match="RPC error"):
                await conn.execute(1, "RETURN 1")

    @pytest.mark.asyncio
    async def test_execute_no_stub(self):
        """Test async execute when stub is not initialized"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        conn._stub = None

        with pytest.raises(InternalError, match="Async connection not established"):
            await conn.execute(1, "RETURN 1")

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_authenticate_success(self, mock_stub_class, mock_channel_class):
        """Test successful async authentication"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_response = MagicMock()
        mock_response.status.code = b"00000"
        mock_response.session_id = 12345
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        with patch("nebulagraph_python.client._connection.ainit_session"):
            session_id = await conn.authenticate("user", "pass")

            assert session_id == 12345
            mock_stub.Authenticate.assert_called_once()

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_authenticate_failure(self, mock_stub_class, mock_channel_class):
        """Test async authentication failure"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_response = MagicMock()
        mock_response.status.code = b"E_AUTH_FAILURE"
        mock_response.status.message = b"Authentication failed"
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.return_value = mock_response

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        with pytest.raises(Exception):
            await conn.authenticate("user", "wrong_pass")

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.insecure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_authenticate_rpc_error(self, mock_stub_class, mock_channel_class):
        """Test async authentication with RPC error"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub
        mock_stub.Authenticate.side_effect = grpc.aio.AioRpcError("RPC error")

        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        await conn.connect()

        with pytest.raises(AuthenticatingError, match="RPC error"):
            await conn.authenticate("user", "pass")

    @pytest.mark.asyncio
    async def test_authenticate_no_stub(self):
        """Test async authenticate when stub is not initialized"""
        config = ConnectionConfig.from_defaults("127.0.0.1:9669")
        conn = AsyncConnection(config)
        conn._stub = None

        with pytest.raises(InternalError, match="Async connection not established"):
            await conn.authenticate("user", "pass")

    @pytest.mark.asyncio
    @patch("nebulagraph_python.client._connection.grpc.aio.secure_channel")
    @patch("nebulagraph_python.client._connection.graph_pb2_grpc.GraphServiceStub")
    async def test_connect_with_ssl(self, mock_stub_class, mock_channel_class):
        """Test async connection with SSL"""
        mock_channel = AsyncMock()
        mock_channel_class.return_value = mock_channel
        mock_stub = AsyncMock()
        mock_stub_class.return_value = mock_stub

        ssl = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        config = ConnectionConfig.from_defaults("127.0.0.1:9669", ssl_param=ssl)
        conn = AsyncConnection(config)
        await conn.connect()

        assert conn._stub is not None
        assert conn._channel is not None