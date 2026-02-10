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
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

import grpc
import grpc.aio
from nebulagraph_python.proto import (
    common_pb2,
    graph_pb2,
    graph_pb2_grpc,
)
from nebulagraph_python.client.address_utils import parse_hosts
from nebulagraph_python.client.auth_result import AuthResult
from nebulagraph_python.client.constants import DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import (
    AuthenticatingError,
    ErrorCode,
    ExecutingError,
)

if TYPE_CHECKING:
    from nebulagraph_python.client.nebula_client import NebulaClient

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """Configuration for connections (backward compatibility)"""
    hosts: List[HostAddress] = field(default_factory=list)
    ssl_param: Optional[SSLParam] = None
    connect_timeout: Optional[float] = 3.0
    request_timeout: Optional[float] = 60.0
    ping_before_execute: bool = False

    @classmethod
    def from_defaults(
        cls,
        hosts: Union[str, List[str], List[HostAddress]],
        ssl_param: Union[SSLParam, Literal[True], None] = None,
        connect_timeout: Optional[float] = 3.0,
        request_timeout: Optional[float] = 60.0,
    ):
        if ssl_param is True:
            ssl_param = SSLParam()
        return cls(
            hosts=parse_hosts(hosts),
            ssl_param=ssl_param,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
        )

    def __post_init__(self):
        if len(self.hosts) == 0:
            raise ValueError("hosts cannot be empty")


class Connection(ABC):
    """Abstract base class for connections, matching Java Connection"""

    server_addr: Optional[HostAddress] = None

    def get_server_address(self) -> Optional[HostAddress]:
        """Get the server address"""
        return self.server_addr

    @abstractmethod
    def open(self, address: HostAddress, builder: "NebulaClient.Builder") -> None:
        """Open connection to the server"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection"""
        pass

    @abstractmethod
    def ping(self, session_id: int, timeout_ms: int) -> bool:
        """Ping the server"""
        pass


class GrpcConnection(Connection):
    """gRPC connection implementation, matching Java GrpcConnection"""

    def __init__(self):
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[graph_pb2_grpc.GraphServiceStub] = None
        self.connect_timeout: int = 0
        self.request_timeout: int = 0

    def open(self, address: HostAddress, client: "NebulaClient") -> None:
        """Open gRPC connection to the server"""
        self.server_addr = address
        self.connect_timeout = client.connect_timeout_mills
        self.request_timeout = client.request_timeout_mills

        formatted_host = address.host
        if ":" in formatted_host and not formatted_host.startswith("["):
            formatted_host = f"[{formatted_host}]"

        channel_options = [
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ]

        if client.enable_tls:
            ssl_param = client.ssl_param
            if ssl_param is None:
                ssl_param = SSLParam()

            self.channel = grpc.secure_channel(
                f"{formatted_host}:{address.port}",
                credentials=grpc.ssl_channel_credentials(
                    root_certificates=ssl_param.ca_crt,
                    private_key=ssl_param.private_key,
                    certificate_chain=ssl_param.cert,
                ),
                options=channel_options,
            )
        else:
            self.channel = grpc.insecure_channel(
                f"{formatted_host}:{address.port}",
                options=channel_options,
            )

        # Wait for channel to be ready
        if self.connect_timeout > 0:
            try:
                grpc.channel_ready_future(self.channel).result(
                    timeout=self.connect_timeout / 1000.0
                )
            except grpc.FutureTimeoutError:
                raise ExecutingError(
                    f"Connection timeout after {self.connect_timeout}ms to {address}"
                )

        self.stub = graph_pb2_grpc.GraphServiceStub(self.channel)

    def close(self) -> None:
        """Close the gRPC connection"""
        if self.channel is not None:
            self.channel.close()
            self.channel = None
        self.stub = None

    def ping(self, session_id: int, timeout_ms: int) -> bool:
        """Ping the server"""
        response = self.execute(session_id, "RETURN 1", timeout_ms)
        return (
            response.status.code == b"00000"
            if hasattr(response, "status")
            else True
        )

    def authenticate(
        self, user: str, auth_options: Dict[str, object]
    ) -> AuthResult:
        """Authenticate with the server"""
        if self.stub is None:
            raise ExecutingError("Connection not established")

        client_info = common_pb2.ClientInfo(
            lang=common_pb2.ClientInfo.PYTHON,
            protocol_version=b"5.0.0",
        )

        user_bytes = user.encode("utf-8") if user else b""
        auth_info_bytes = json.dumps(auth_options).encode("utf-8")

        request = graph_pb2.AuthRequest(
            username=user_bytes,
            auth_info=auth_info_bytes,
            client_info=client_info,
        )

        try:
            response = self.stub.Authenticate(
                request, timeout=self.connect_timeout / 1000.0
            )
        except grpc.RpcError as e:
            self.close()
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise AuthenticatingError(
                    f"authenticate to {self.server_addr} timeout after {self.connect_timeout}ms"
                )
            raise AuthenticatingError(f"RPC error: {e.details()}")

        if response.status.code != b"00000":
            self.close()
            raise AuthenticatingError(
                response.status.message.decode("utf-8")
            )

        return AuthResult(
            session_id=int(response.session_id),
            version=response.version.decode("utf-8"),
        )

    def execute(
        self, session_id: int, stmt: str, timeout: int
    ) -> graph_pb2.ExecuteResponse:
        """Execute a statement"""
        if stmt is None:
            raise ValueError("statement is null")

        if self.stub is None:
            raise ExecutingError("Connection not established")

        request = graph_pb2.ExecuteRequest(
            session_id=session_id, stmt=stmt.encode("utf-8")
        )

        try:
            return self.stub.Execute(request, timeout=timeout / 1000.0)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise ExecutingError(
                    f"request to {self.server_addr} timeout after {timeout}ms"
                )
            raise ExecutingError(f"RPC error: {e.details()}")

    def execute_default_timeout(
        self, session_id: int, stmt: str
    ) -> graph_pb2.ExecuteResponse:
        """Execute a statement with default timeout"""
        return self.execute(session_id, stmt, self.request_timeout)


class AsyncConnection:
    """Async gRPC connection for backward compatibility"""

    def __init__(self, config):
        self.config = config
        self.server_addr: Optional[HostAddress] = None
        self._stub: Optional[graph_pb2_grpc.GraphServiceStub] = None
        self._channel: Optional[grpc.aio.Channel] = None
        self.connect_timeout: float = config.connect_timeout
        self.request_timeout: float = config.request_timeout

    async def connect(self, address: HostAddress) -> None:
        """Open async gRPC connection to the server"""
        self.server_addr = address

        formatted_host = address.host
        if ":" in formatted_host and not formatted_host.startswith("["):
            formatted_host = f"[{formatted_host}]"

        channel_options = [
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ]

        if self.config.ssl_param:
            ssl_param = self.config.ssl_param
            self._channel = grpc.aio.secure_channel(
                f"{formatted_host}:{address.port}",
                credentials=grpc.ssl_channel_credentials(
                    root_certificates=ssl_param.ca_crt,
                    private_key=ssl_param.private_key,
                    certificate_chain=ssl_param.cert,
                ),
                options=channel_options,
            )
        else:
            self._channel = grpc.aio.insecure_channel(
                f"{formatted_host}:{address.port}",
                options=channel_options,
            )

        # Wait for channel to be ready
        if self.connect_timeout > 0:
            try:
                await asyncio.wait_for(
                    self._channel.channel_ready(),
                    timeout=self.connect_timeout
                )
            except asyncio.TimeoutError:
                await self.close()
                raise ExecutingError(
                    f"Connection timeout after {self.connect_timeout}s to {address}"
                )

        self._stub = graph_pb2_grpc.GraphServiceStub(self._channel)

    async def close(self) -> None:
        """Close the async gRPC connection"""
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
        self._stub = None

    async def ping(self, session_id: int, timeout_ms: int) -> bool:
        """Ping the server"""
        response = await self.execute(session_id, "RETURN 1", timeout_ms)
        return (
            response.status.code == b"00000"
            if hasattr(response, "status")
            else True
        )

    async def execute(
        self, session_id: int, stmt: str, timeout: int
    ) -> graph_pb2.ExecuteResponse:
        """Execute a statement"""
        if stmt is None:
            raise ValueError("statement is null")

        if self._stub is None:
            raise ExecutingError("Connection not established")

        request = graph_pb2.ExecuteRequest(
            session_id=session_id, stmt=stmt.encode("utf-8")
        )

        try:
            return await self._stub.Execute(
                request,
                timeout=timeout / 1000.0
            )
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise ExecutingError(
                    f"request to {self.server_addr} timeout after {timeout}ms"
                )
            raise ExecutingError(f"RPC error: {e.details()}")

    async def execute_default_timeout(
        self, session_id: int, stmt: str
    ) -> graph_pb2.ExecuteResponse:
        """Execute a statement with default timeout"""
        return await self.execute(session_id, stmt, int(self.request_timeout * 1000))

    async def authenticate(
        self, user: str, auth_options: Dict[str, object]
    ) -> AuthResult:
        """Authenticate with the server"""
        if self._stub is None:
            raise ExecutingError("Connection not established")

        client_info = common_pb2.ClientInfo(
            lang=common_pb2.ClientInfo.PYTHON,
            protocol_version=b"5.0.0",
        )

        user_bytes = user.encode("utf-8") if user else b""
        auth_info_bytes = json.dumps(auth_options).encode("utf-8")

        request = graph_pb2.AuthRequest(
            username=user_bytes,
            auth_info=auth_info_bytes,
            client_info=client_info,
        )

        try:
            response = await self._stub.Authenticate(
                request,
                timeout=self.connect_timeout
            )
        except grpc.aio.AioRpcError as e:
            await self.close()
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise AuthenticatingError(
                    f"authenticate to {self.server_addr} timeout after {self.connect_timeout}s"
                )
            raise AuthenticatingError(f"RPC error: {e.details()}")

        if response.status.code != b"00000":
            await self.close()
            raise AuthenticatingError(
                response.status.message.decode("utf-8")
            )

        return AuthResult(
            session_id=int(response.session_id),
            version=response.version.decode("utf-8"),
        )
