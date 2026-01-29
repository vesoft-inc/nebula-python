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

"""Complete tests for NebulaClient"""

import time
from unittest.mock import Mock, MagicMock, patch, call

import pytest

from nebulagraph_python.client.nebula_client import NebulaClient
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import AuthenticatingError, ExecutingError
from nebulagraph_python.client.constants import (
    DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    DEFAULT_PING_TIMEOUT_MS,
    DEFAULT_SCAN_PARALLEL,
    DEFAULT_ENABLE_TLS,
    DEFAULT_MAX_TIMEOUT_MS,
)
from nebulagraph_python.proto import graph_pb2


class TestNebulaClient:
    """Test cases for NebulaClient"""

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_creation_with_defaults(self, mock_connection_class):
        """Test creating a NebulaClient with default values"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass"
        )

        assert client.servers == [HostAddress("127.0.0.1", 9669)]
        assert client.user_name == "test_user"
        assert client.password == "test_pass"
        assert client.connect_timeout_mills == DEFAULT_CONNECT_TIMEOUT_MS
        assert client.request_timeout_mills == DEFAULT_REQUEST_TIMEOUT_MS
        assert client.server_ping_timeout_mills == DEFAULT_PING_TIMEOUT_MS
        assert client.scan_parallel == DEFAULT_SCAN_PARALLEL
        assert client.enable_tls == DEFAULT_ENABLE_TLS
        assert client.session_id == 12345
        assert client.version == "v5.0.0"
        assert client.is_closed is False
        mock_connection.open.assert_called_once()
        mock_connection.authenticate.assert_called_once()

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_creation_with_custom_timeouts(self, mock_connection_class):
        """Test creating a NebulaClient with custom timeouts"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
            connect_timeout_ms=5000,
            request_timeout_ms=120000,
            server_ping_timeout_ms=2000,
        )

        assert client.connect_timeout_mills == 5000
        assert client.request_timeout_mills == 120000
        assert client.server_ping_timeout_mills == 2000

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_creation_with_scan_parallel(self, mock_connection_class):
        """Test creating a NebulaClient with custom scan_parallel"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
            scan_parallel=20,
        )

        assert client.scan_parallel == 20

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_creation_with_tls(self, mock_connection_class):
        """Test creating a NebulaClient with TLS enabled"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        ssl_param = SSLParam(ca_crt=b"ca", private_key=b"key", cert=b"cert")
        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
            enable_tls=True,
            ssl_param=ssl_param,
        )

        assert client.enable_tls is True
        assert client.ssl_param == ssl_param

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_creation_with_auth_options(self, mock_connection_class):
        """Test creating a NebulaClient with auth options"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        auth_options = {"password": "test_pass", "custom_option": "value"}
        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            auth_options=auth_options,
        )

        assert client.auth_options == auth_options

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_creation_multiple_addresses(self, mock_connection_class):
        """Test creating a NebulaClient with multiple addresses"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669,127.0.0.2:9669,127.0.0.3:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert len(client.servers) == 3
        assert HostAddress("127.0.0.1", 9669) in client.servers
        assert HostAddress("127.0.0.2", 9669) in client.servers
        assert HostAddress("127.0.0.3", 9669) in client.servers

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_execute(self, mock_connection_class):
        """Test executing a query"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        # Create a proper ExecuteResponse object
        mock_execute_response = graph_pb2.ExecuteResponse()
        mock_execute_response.status.code = b"00000"
        mock_execute_response.status.message = b"Success"
        # Initialize summary field by creating a Summary object
        from nebulagraph_python.proto.graph_pb2 import Summary
        mock_execute_response.summary.CopyFrom(Summary())
        mock_connection.execute_default_timeout.return_value = mock_execute_response

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        result = client.execute("RETURN 1")

        assert result is not None
        mock_connection.execute_default_timeout.assert_called_once_with(12345, "RETURN 1")

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_execute_with_timeout(self, mock_connection_class):
        """Test executing a query with custom timeout"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        # Create a proper ExecuteResponse object
        mock_execute_response = graph_pb2.ExecuteResponse()
        mock_execute_response.status.code = b"00000"
        mock_execute_response.status.message = b"Success"
        # Initialize summary field by creating a Summary object
        from nebulagraph_python.proto.graph_pb2 import Summary
        mock_execute_response.summary.CopyFrom(Summary())
        mock_connection.execute_default_timeout.return_value = mock_execute_response

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        result = client.execute_with_timeout("RETURN 1", 5000)

        assert result is not None
        mock_connection.execute_default_timeout.assert_called_once_with(12345, "RETURN 1")

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_session_id(self, mock_connection_class):
        """Test getting session ID"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.get_session_id() == 12345

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_version(self, mock_connection_class):
        """Test getting version"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.get_version() == "v5.0.0"

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_create_time(self, mock_connection_class):
        """Test getting create time"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        create_time = client.get_create_time()
        assert create_time > 0
        assert create_time <= int(time.time() * 1000)

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_host(self, mock_connection_class):
        """Test getting host address"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result
        mock_connection.get_server_address.return_value = HostAddress("127.0.0.1", 9669)

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.get_host() == "127.0.0.1:9669"

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_connect_timeout_mills(self, mock_connection_class):
        """Test getting connect timeout"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
            connect_timeout_ms=5000,
        )

        assert client.get_connect_timeout_mills() == 5000

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_request_timeout_mills(self, mock_connection_class):
        """Test getting request timeout"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
            request_timeout_ms=120000,
        )

        assert client.get_request_timeout_mills() == 120000

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_get_scan_parallel(self, mock_connection_class):
        """Test getting scan parallel"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
            scan_parallel=20,
        )

        assert client.get_scan_parallel() == 20

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_ping_success(self, mock_connection_class):
        """Test successful ping"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result
        mock_connection.ping.return_value = True

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.ping() is True

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_ping_failure(self, mock_connection_class):
        """Test ping failure"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result
        mock_connection.ping.side_effect = ExecutingError("Ping failed")
        mock_connection.get_server_address.return_value = HostAddress("127.0.0.1", 9669)

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.ping() is False

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_close(self, mock_connection_class):
        """Test closing the client"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        client.close()

        assert client.is_closed is True
        mock_connection.execute.assert_called_once_with(12345, "SESSION CLOSE", 1000)
        mock_connection.close.assert_called_once()

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_close_exception_handling(self, mock_connection_class):
        """Test closing client handles exceptions gracefully"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result
        mock_connection.execute.side_effect = Exception("Close failed")

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        # Should not raise exception
        client.close()

        assert client.is_closed is True

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_is_closed_client(self, mock_connection_class):
        """Test checking if client is closed"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.is_closed_client() is False

        client.close()

        assert client.is_closed_client() is True

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_execute_after_close_raises_error(self, mock_connection_class):
        """Test executing after close raises error"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        client.close()

        with pytest.raises(RuntimeError, match="The NebulaClient already closed"):
            client.execute("RETURN 1")

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_ping_after_close_raises_error(self, mock_connection_class):
        """Test ping after close raises error"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669",
            user_name="test_user",
            password="test_pass",
        )

        client.close()

        with pytest.raises(RuntimeError, match="The NebulaClient already closed"):
            client.ping()

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_validate_address_valid(self, mock_connection_class):
        """Test validating valid address"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        addresses = NebulaClient._validate_address("127.0.0.1:9669")

        assert addresses == [HostAddress("127.0.0.1", 9669)]

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_validate_multiple_addresses(self, mock_connection_class):
        """Test validating multiple addresses"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        addresses = NebulaClient._validate_address("127.0.0.1:9669,127.0.0.2:9669,127.0.0.3:9669")

        assert len(addresses) == 3
        assert addresses[0] == HostAddress("127.0.0.1", 9669)
        assert addresses[1] == HostAddress("127.0.0.2", 9669)
        assert addresses[2] == HostAddress("127.0.0.3", 9669)

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_validate_address_invalid(self, mock_connection_class):
        """Test validating invalid address raises error"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"
        mock_connection.authenticate.return_value = mock_auth_result

        with pytest.raises(ValueError, match="Invalid address format"):
            NebulaClient._validate_address("127.0.0.1")

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_init_retries_on_failure(self, mock_connection_class):
        """Test client initialization retries on connection failure"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_auth_result = MagicMock()
        mock_auth_result.get_session_id.return_value = 12345
        mock_auth_result.get_version.return_value = "v5.0.0"

        # First two attempts fail, third succeeds
        mock_connection.open.side_effect = [
            Exception("Connection failed"),
            Exception("Connection failed"),
            None,
        ]
        mock_connection.authenticate.return_value = mock_auth_result

        client = NebulaClient(
            addresses="127.0.0.1:9669,127.0.0.2:9669,127.0.0.3:9669",
            user_name="test_user",
            password="test_pass",
        )

        assert client.session_id == 12345
        assert mock_connection.open.call_count == 3

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_init_auth_failure_raises_error(self, mock_connection_class):
        """Test client initialization raises error on auth failure"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_connection.authenticate.side_effect = AuthenticatingError("Auth failed")

        with pytest.raises(AuthenticatingError, match="Auth failed"):
            NebulaClient(
                addresses="127.0.0.1:9669",
                user_name="test_user",
                password="test_pass",
            )

    @patch("nebulagraph_python.client.nebula_client.GrpcConnection")
    def test_client_init_all_servers_fail_raises_error(self, mock_connection_class):
        """Test client initialization raises error when all servers fail"""
        mock_connection = MagicMock()
        mock_connection_class.return_value = mock_connection
        mock_connection.open.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            NebulaClient(
                addresses="127.0.0.1:9669",
                user_name="test_user",
                password="test_pass",
            )