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

"""ClientPoolFactory matching Java implementation"""

import logging
import time
from typing import TYPE_CHECKING

from nebulagraph_python.data import SSLParam
from nebulagraph_python.error import AuthenticatingError, ExecutingError

if TYPE_CHECKING:
    from nebulagraph_python.client.nebula_client import NebulaClient
    from nebulagraph_python.client.nebula_pool import NebulaPool
    from nebulagraph_python.client.round_robin_load_balancer import RoundRobinLoadBalancer

logger = logging.getLogger(__name__)


class ClientPoolFactory:
    """Factory for creating NebulaClient instances for the pool"""

    def __init__(
        self,
        load_balancer: "RoundRobinLoadBalancer",
        builder: "NebulaPool.Builder",
    ):
        """Initialize the factory"""
        self.load_balancer = load_balancer
        self.builder = builder

    def create(self) -> "NebulaClient":
        """Create a new NebulaClient instance"""
        try_create = 0
        io_exception = None
        auth_exception = None

        while try_create < self.load_balancer.address_size():
            try:
                return self._create_client()
            except ExecutingError as e:
                io_exception = e
            except AuthenticatingError as e:
                auth_exception = e
            try_create += 1

        if auth_exception is not None:
            raise auth_exception
        if io_exception is not None:
            raise io_exception

        raise ExecutingError(
            "No servers host is available, please check your servers is up and network between client and server is connected."
        )

    def _create_client(self) -> "NebulaClient":
        """Create and configure a NebulaClient"""
        from nebulagraph_python.client.nebula_client import NebulaClient

        address = self.load_balancer.get_address()

        ssl_param = None
        if self.builder.enable_tls:
            ssl_param = SSLParam(
                ca_crt=self.builder.tls_ca.encode() if self.builder.tls_ca else None,
                private_key=self.builder.tls_key.encode() if self.builder.tls_key else None,
                cert=self.builder.tls_cert.encode() if self.builder.tls_cert else None,
            )

        client = NebulaClient(
            f"{address.host}:{address.port}",
            self.builder.user_name,
            auth_options=self.builder.auth_options,
            connect_timeout_ms=self.builder.connect_timeout_mills,
            request_timeout_ms=self.builder.request_timeout_mills,
            scan_parallel=self.builder.scan_parallel,
            enable_tls=self.builder.enable_tls,
            ssl_param=ssl_param,
        )

        # Set home schema, home graph and time zone for session
        try:
            if self.builder.schema and self.builder.schema.strip():
                stmt = f'SESSION SET SCHEMA `{self.builder.schema}`'
                result_set = client.execute(stmt)
                if not result_set.is_succeeded:
                    raise RuntimeError(
                        f"{stmt} failed for {result_set.status_message}"
                    )

            if self.builder.graph and self.builder.graph.strip():
                stmt = f'SESSION SET GRAPH "{self.builder.graph}"'
                result_set = client.execute(stmt)
                if not result_set.is_succeeded:
                    raise RuntimeError(
                        f"{stmt} failed for {result_set.status_message}"
                    )

            if self.builder.timezone and self.builder.timezone.strip():
                stmt = f'SESSION SET timezone="{self.builder.timezone}"'
                result_set = client.execute(stmt)
                if not result_set.is_succeeded:
                    raise RuntimeError(
                        f"{stmt} failed for {result_set.status_message}"
                    )

            for key, value in self.builder.session_configs.items():
                stmt = f"SESSION SET {key}={value}"
                result_set = client.execute(stmt)
                if not result_set.is_succeeded:
                    raise RuntimeError(
                        f"{stmt} failed for {result_set.status_message}"
                    )

            if self.builder.parameters:
                parameters_set_statement = "SESSION SET VALUE "
                for param_key, param_value in self.builder.parameters.items():
                    parameters_set_statement += (
                        f"${param_key}={param_value},"
                    )
                parameters_set_statement = parameters_set_statement[:-1]
                if parameters_set_statement:
                    result = client.execute(parameters_set_statement)
                    if not result.is_succeeded:
                        raise RuntimeError(
                            f"{parameters_set_statement} failed for {result.status_message}"
                        )

            for pre_stmt in self.builder.pre_statements:
                res = client.execute(pre_stmt)
                if not res.is_succeeded:
                    raise RuntimeError(
                        f"{pre_stmt} failed for {res.status_message}"
                    )

        except ExecutingError as e:
            client.close()
            raise e

        return client

    def destroy(self, client: "NebulaClient") -> None:
        """Destroy a NebulaClient instance"""
        try:
            client.close()
        except Exception as e:
            logger.warn(f"session release failed: {e}")

    def validate(self, client: "NebulaClient") -> bool:
        """Validate if a NebulaClient is still valid"""
        is_alive = (
            time.time() * 1000 - client.get_create_time()
        ) < self.builder.max_life_time_ms
        return client.ping(self.builder.server_ping_timeout_mills) and is_alive
