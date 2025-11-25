import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import anyio
import grpc
import grpc.aio

from nebulagraph_python.client import constants
from nebulagraph_python.client.logger import logger
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import (
    AuthenticatingError,
    ConnectingError,
    ErrorCode,
    ExecutingError,
    InternalError,
    NebulaGraphRemoteError,
)
from nebulagraph_python.proto import (
    common_pb2,
    graph_pb2,
    graph_pb2_grpc,
)
from nebulagraph_python.result_set import ResultSet

if TYPE_CHECKING:
    from nebulagraph_python.client._session import SessionConfig


def _parse_hosts(hosts: Union[str, List[str], List[HostAddress]]) -> List[HostAddress]:
    """Convert various host formats to list of HostAddress objects"""
    if isinstance(hosts, str):
        hosts = hosts.split(",")

    addresses = []
    for host in hosts:
        if isinstance(host, HostAddress):
            addresses.append(host)
        else:
            addr, port = host.split(":")
            addresses.append(HostAddress(addr, int(port)))
    return addresses


@dataclass
class ConnectionConfig:
    hosts: List[HostAddress] = field(default_factory=list)
    ssl_param: Optional[SSLParam] = None
    connect_timeout: Optional[float] = constants.DEFAULT_CONNECT_TIMEOUT
    request_timeout: Optional[float] = constants.DEFAULT_REQUEST_TIMEOUT
    ping_before_execute: bool = False

    @classmethod
    def from_defults(
        cls,
        hosts: Union[str, List[str], List[HostAddress]],
        ssl_param: Union[SSLParam, Literal[True], None] = None,
        connect_timeout: Optional[float] = constants.DEFAULT_CONNECT_TIMEOUT,
        request_timeout: Optional[float] = constants.DEFAULT_REQUEST_TIMEOUT,
    ):
        if ssl_param is True:
            ssl_param = SSLParam()
        return cls(
            hosts=_parse_hosts(hosts),
            ssl_param=ssl_param,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
        )

    def __post_init__(self):
        if len(self.hosts) == 0:
            raise ValueError("hosts cannot be empty")


@dataclass
class Connection:
    """Represents a connection to a NebulaGraph server. It is built upon grpc and is thread-safe.

    Required to explicitly call `close()` to release all resources.
    """

    # Config
    config: ConnectionConfig

    # Owned Resources
    _stub: Optional[graph_pb2_grpc.GraphServiceStub] = field(default=None, init=False)
    _channel: Optional[grpc.Channel] = field(default=None, init=False)

    def __post_init__(self):
        self.connect()

    def connect(self):
        """Establish connection to NebulaGraph"""
        last_error: Optional[Exception] = None

        # Try each address until one succeeds
        for host_addr in self.config.hosts:
            try:
                channel_options = [
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                    ("grpc.enable_deadline_checking", 1),
                ]

                if self.config.ssl_param:
                    self._channel = grpc.secure_channel(
                        f"{host_addr.host}:{host_addr.port}",
                        credentials=grpc.ssl_channel_credentials(
                            root_certificates=self.config.ssl_param.ca_crt,
                            private_key=self.config.ssl_param.private_key,
                            certificate_chain=self.config.ssl_param.cert,
                        ),
                        options=channel_options,
                    )
                else:
                    self._channel = grpc.insecure_channel(
                        f"{host_addr.host}:{host_addr.port}",
                        options=channel_options,
                    )

                # Wait for channel to be ready with timeout
                if self.config.connect_timeout is not None:
                    try:
                        grpc.channel_ready_future(self._channel).result(
                            timeout=self.config.connect_timeout
                        )
                    except grpc.FutureTimeoutError as e:
                        raise ConnectingError(
                            f"Connection timeout after {self.config.connect_timeout} seconds to {host_addr.host}:{host_addr.port}"
                        ) from e
                else:
                    grpc.channel_ready_future(
                        self._channel
                    ).result()  # Wait indefinitely if no timeout

                self._stub = graph_pb2_grpc.GraphServiceStub(self._channel)
                logger.info(
                    f"Successfully connected to {host_addr.host}:{host_addr.port}."
                )
                return
            except Exception as e:
                logger.warning(
                    f"Failed to connect to {(host_addr.host, host_addr.port) if host_addr else 'No Available Addr'}: {e}",
                )
                last_error = e
                self.close()
            else:
                return

        # If we get here, all connection attempts failed
        raise ConnectingError(
            f"Failed to connect to any of the provided hosts. Last error: {last_error}",
        )

    def close(self):
        """Close the connection. No Exception will be raised but an error will be logged."""
        try:
            if self._channel:
                self._channel.close()
                self._channel = None
            self._stub = None
        except Exception:
            logger.exception("Failed to close connection")

    def reconnect(self):
        self.close()
        self.connect()

    def ping(self) -> bool:
        """Ping the connection to check if it's healthy.

        Returns:
            True if the connection is healthy, False otherwise.
        """
        if not self._stub:
            return False
        try:
            request = graph_pb2.ExecuteRequest(
                session_id=-1,
                stmt="RETURN 1".encode("utf-8"),
            )
            _response = self._stub.Execute(request, timeout=self.config.connect_timeout)
            return True
        except Exception:
            return False

    def execute(
        self,
        session_id: int,
        statement: str,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        # Retry connection if ping fails for only one time
        if (self.config.ping_before_execute or do_ping) and not self.ping():
            self.close()
            self.connect()
        if not self._stub:
            raise InternalError("Connection not established")

        logger.debug(f"Executing in Hosts: {self.config.hosts}")
        logger.debug(f"Executing statement: {statement}")

        try:
            request = graph_pb2.ExecuteRequest(
                session_id=session_id,
                stmt=statement.encode("utf-8"),
            )
            logger.debug(f"Request: {request}")
            # Use request_timeout as default if timeout is not specified
            effective_timeout = (
                timeout if timeout is not None else self.config.request_timeout
            )
            response = self._stub.Execute(request, timeout=effective_timeout)
            logger.debug(f"Response: {response}")
        except grpc.RpcError as e:
            logger.error(f"RPC error during execute: {e.code()} {e.details()}")
            raise ExecutingError(f"RPC error: {e.details()}") from e
        except Exception as e:
            logger.error(f"Unexpected error during execute: {e}")
            raise ExecutingError("Unexpected error during execute") from e

        return ResultSet(response)

    def authenticate(
        self,
        username: str,
        password: Optional[str] = None,
        *,
        auth_options: Optional[Dict[str, Any]] = None,
        session_config: Optional["SessionConfig"] = None,
    ) -> int:
        """Authenticate with NebulaGraph and return session ID. May raise Exception when authentication failed."""
        from nebulagraph_python.client._session import SessionConfig, init_session

        if not self._stub:
            raise InternalError("Connection not established")

        _auth_options = auth_options or {}
        _session_config = session_config or SessionConfig()

        client_info = common_pb2.ClientInfo(
            lang=common_pb2.ClientInfo.PYTHON,
            protocol_version=b"5.0.0",
        )

        auth_info_dict = (
            {"password": password, **_auth_options} if password else _auth_options
        )
        auth_info_bytes = json.dumps(auth_info_dict).encode("utf-8")

        request = graph_pb2.AuthRequest(
            username=username.encode("utf-8"),
            auth_info=auth_info_bytes,
            client_info=client_info,
        )

        try:
            response = self._stub.Authenticate(
                request, timeout=self.config.request_timeout
            )
        except grpc.RpcError as e:
            logger.error(f"RPC error during authenticate: {e.code()} {e.details()}")
            raise AuthenticatingError(
                f"RPC error during authentication: {e.details()}"
            ) from e
        except Exception as e:
            logger.error(f"Unexpected error during authenticate: {e}")
            raise AuthenticatingError("Unexpected error during authentication") from e

        if response.status.code != b"00000":
            raise NebulaGraphRemoteError(
                code=ErrorCode(response.status.code.decode("utf-8")),
                message=response.status.message.decode("utf-8"),
            )

        # Initialize session and return session ID
        init_session(self, int(response.session_id), _session_config)
        return int(response.session_id)


@dataclass
class AsyncConnection:
    """Represents a connection to a NebulaGraph server. It is built upon grpc.aio and is async/coroutine-level safe but not thread-safe.

    Required to explicitly call `close()` to release all resources.
    """

    config: ConnectionConfig
    _stub: Optional[graph_pb2_grpc.GraphServiceStub] = field(default=None, init=False)
    _channel: Optional[grpc.aio.Channel] = field(
        default=None, init=False
    )  # Use grpc.aio.Channel

    # Note: __post_init__ cannot be async.
    # An async factory method (e.g., AsyncConnection.create(...)) or an explicit await self.connect()
    # after __init__ would be needed. For now, connect will be called separately.

    async def connect(self):
        last_error: Optional[Exception] = None

        for host_addr in self.config.hosts:
            try:
                channel_options = [
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                    ("grpc.enable_deadline_checking", 1),  # Deadline checking is good
                ]

                if self.config.ssl_param:
                    self._channel = grpc.aio.secure_channel(
                        f"{host_addr.host}:{host_addr.port}",
                        credentials=grpc.ssl_channel_credentials(
                            root_certificates=self.config.ssl_param.ca_crt,
                            private_key=self.config.ssl_param.private_key,
                            certificate_chain=self.config.ssl_param.cert,
                        ),
                        options=channel_options,
                    )
                else:
                    self._channel = grpc.aio.insecure_channel(
                        f"{host_addr.host}:{host_addr.port}",
                        options=channel_options,
                    )

                if self.config.connect_timeout is not None:
                    try:
                        with anyio.fail_after(
                            self.config.connect_timeout,
                        ):
                            await self._channel.channel_ready()
                    except TimeoutError as e:
                        raise ConnectingError(
                            f"Connection timeout after {self.config.connect_timeout} seconds to {host_addr.host}:{host_addr.port}"
                        ) from e
                else:
                    await (
                        self._channel.channel_ready()
                    )  # Wait indefinitely if no timeout

                self._stub = graph_pb2_grpc.GraphServiceStub(self._channel)
                logger.info(
                    f"Successfully connected to {host_addr.host}:{host_addr.port} asynchronously."
                )
                return
            except Exception as e:
                logger.warning(
                    f"Failed to connect asynchronously to {(host_addr.host, host_addr.port) if host_addr else 'No Available Addr'}: {e}",
                )
                last_error = e
                if self._channel:  # Ensure channel is closed on partial failure before trying next host
                    await self._channel.close()
                    self._channel = None
                self._stub = None  # Also clear stub

        # If we get here, all connection attempts failed
        raise ConnectingError(
            f"Failed to connect asynchronously to any of the provided hosts. Last error: {last_error}",
        )

    async def close(self):
        try:
            if self._channel:
                await self._channel.close()
                self._channel = None
            self._stub = None
        except BaseException:
            logger.exception("Failed to close async connection")

    async def reconnect(self):
        await self.close()
        await self.connect()

    async def ping(self) -> bool:
        """Ping the connection to check if it's healthy.

        Returns:
            True if the connection is healthy, False otherwise.
        """
        if not self._stub:
            return False
        try:
            request = graph_pb2.ExecuteRequest(
                session_id=-1,
                stmt="RETURN 1".encode("utf-8"),
            )
            _response = await self._stub.Execute(
                request, timeout=self.config.connect_timeout
            )
            return True
        except Exception:
            return False

    async def execute(
        self,
        session_id: int,
        statement: str,
        *,
        timeout: Optional[float] = None,
        do_ping: bool = False,
    ) -> ResultSet:
        # Retry connection if ping fails for only one time
        if (self.config.ping_before_execute or do_ping) and not await self.ping():
            await self.close()
            await self.connect()
        if not self._stub:
            raise InternalError("Async connection not established or stub is missing.")

        logger.debug(f"Executing in Hosts: {self.config.hosts}")
        logger.debug(f"Async executing statement: {statement}")

        try:
            request = graph_pb2.ExecuteRequest(
                session_id=session_id,
                stmt=statement.encode("utf-8"),
            )
            logger.debug(f"Async request: {request}")
            effective_timeout = (
                timeout if timeout is not None else self.config.request_timeout
            )
            # The stub call itself is now awaitable
            response = await self._stub.Execute(request, timeout=effective_timeout)  # type: ignore
            logger.debug(f"Async response: {response}")
        except grpc.aio.AioRpcError as e:  # Catch async gRPC errors
            # TODO: Map to specific Nebula errors like ExecutingError, AuthenticatingError
            logger.error(f"Async RPC error during execute: {e.code()} {e.details()}")
            raise ExecutingError(f"RPC error: {e.details()}") from e
        except Exception as e:
            logger.error(f"Unexpected error during async execute: {e}")
            raise ExecutingError("Unexpected error during async execute") from e

        return ResultSet(response)  # ResultSet creation should be the same

    async def authenticate(
        self,
        username: str,
        password: Optional[str] = None,
        *,
        auth_options: Optional[Dict[str, Any]] = None,
        session_config: Optional["SessionConfig"] = None,  # Re-use SessionConfig
    ) -> int:
        from nebulagraph_python.client._session import SessionConfig, ainit_session

        if not self._stub:
            raise InternalError("Async connection not established or stub is missing.")

        _auth_options = auth_options or {}
        _session_config = session_config or SessionConfig()

        client_info = common_pb2.ClientInfo(
            lang=common_pb2.ClientInfo.PYTHON,
            protocol_version=b"5.0.0",  # Ensure this is up-to-date or configurable
        )

        auth_info_dict = (
            {"password": password, **_auth_options} if password else _auth_options
        )
        auth_info_bytes = json.dumps(auth_info_dict).encode("utf-8")

        request = graph_pb2.AuthRequest(
            username=username.encode("utf-8"),
            auth_info=auth_info_bytes,
            client_info=client_info,
        )

        try:
            # Use request_timeout as default if timeout is not specified for authenticate
            response = await self._stub.Authenticate(
                request, timeout=self.config.request_timeout
            )
        except grpc.aio.AioRpcError as e:
            logger.error(
                f"Async RPC error during authenticate: {e.code()} {e.details()}"
            )
            raise AuthenticatingError(
                f"RPC error during authentication: {e.details()}"
            ) from e
        except Exception as e:  # Catch other potential errors
            logger.error(f"Unexpected error during async authenticate: {e}")
            raise AuthenticatingError(
                "Unexpected error during async authentication"
            ) from e

        if response.status.code != b"00000":
            raise NebulaGraphRemoteError(
                code=ErrorCode(response.status.code.decode("utf-8")),
                message=response.status.message.decode("utf-8"),
            )

        # Create and return an AsyncSession instance
        # The AsyncSession class will need to be defined.
        # For now, let's create it and initialize its async parts (like setting session params).
        await ainit_session(self, int(response.session_id), _session_config)
        return int(response.session_id)
