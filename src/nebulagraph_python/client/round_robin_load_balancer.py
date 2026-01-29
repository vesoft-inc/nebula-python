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


import logging
from typing import TYPE_CHECKING, Dict, List

from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.error import AuthenticatingError, ExecutingError

if TYPE_CHECKING:
    from nebulagraph_python.client.nebula_pool import NebulaPool

logger = logging.getLogger(__name__)


class RoundRobinLoadBalancer:
    """Round-robin load balancer for NebulaGraph servers"""

    def __init__(self, builder: "NebulaPool.Builder"):
        """Initialize the load balancer"""
        self.addresses: List[HostAddress] = list(builder.address)
        self.strictly_server_healthy: bool = builder.strictly_server_healthy
        self.user_name: str = builder.user_name
        self.auth_options: Dict[str, object] = builder.auth_options
        self.connection_timeout: int = builder.connect_timeout_mills
        self.enable_tls: bool = builder.enable_tls
        self.disable_verify_server_cert: bool = builder.disable_verify_server_cert
        self.tls_ca: Optional[str] = builder.tls_ca
        self.tls_cert: Optional[str] = builder.tls_cert
        self.tls_key: Optional[str] = builder.tls_key

        from nebulagraph_python.client.nebula_client import NebulaClient

        self._nebula_client_class = NebulaClient
        self._pos: int = 0

    def address_size(self) -> int:
        """Get the number of addresses"""
        return len(self.addresses)

    def get_address(self) -> HostAddress:
        """Get the next address using round-robin"""
        if self._pos >= 2**31 - 1:
            self._pos = 0
        new_pos = self._pos % len(self.addresses)
        self._pos += 1
        return self.addresses[new_pos]

    def ping(self, addr: HostAddress) -> bool:
        """Ping a server address"""
        from nebulagraph_python.client.nebula_client import NebulaClient

        ssl_param = None
        if self.enable_tls:
            ssl_param = SSLParam(
                ca_crt=self.tls_ca.encode() if self.tls_ca else None,
                private_key=self.tls_key.encode() if self.tls_key else None,
                cert=self.tls_cert.encode() if self.tls_cert else None,
            )

        client = NebulaClient(
            f"{addr.host}:{addr.port}",
            self.user_name,
            auth_options=self.auth_options,
            connect_timeout_ms=self.connection_timeout,
            enable_tls=self.enable_tls,
            ssl_param=ssl_param,
        )
        client.close()
        return True

    def check_servers(self) -> None:
        """Check if servers are healthy"""
        last_auth_e: AuthenticatingError = None
        last_io_e: ExecutingError = None
        good_address: int = 0

        for host_address in self.addresses:
            try:
                self.ping(host_address)
                good_address += 1
            except AuthenticatingError as e:
                last_auth_e = e
            except ExecutingError as e:
                last_io_e = e

        if self.strictly_server_healthy:
            if good_address == self.address_size():
                return
        else:
            if good_address >= 1:
                return

        if last_auth_e is not None:
            raise last_auth_e
        if last_io_e is not None:
            raise last_io_e
