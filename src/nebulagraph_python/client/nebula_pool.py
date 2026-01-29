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

"""NebulaPool implementation using Python configuration style"""

import logging
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING

from nebulagraph_python.client.client_pool_factory import ClientPoolFactory
from nebulagraph_python.client.constants import (
    DEFAULT_BLOCK_WHEN_EXHAUSTED,
    DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_ENABLE_TLS,
    DEFAULT_HEALTH_CHECK_TIME_MS,
    DEFAULT_IDLE_EVICT_SCHEDULE_MS,
    DEFAULT_MAX_CLIENT_SIZE,
    DEFAULT_MAX_LIFE_TIME_MS,
    DEFAULT_MAX_WAIT_MS,
    DEFAULT_MIN_CLIENT_SIZE,
    DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS,
    DEFAULT_PING_TIMEOUT_MS,
    DEFAULT_REQUEST_TIMEOUT_MS,
    DEFAULT_SCAN_PARALLEL,
    DEFAULT_STRICT_SERVER_HEALTHY,
    DEFAULT_TEST_ON_BORROW,
)
from nebulagraph_python.data import HostAddress, SSLParam
from nebulagraph_python.client.nebula_client import NebulaClient
from nebulagraph_python.client.round_robin_load_balancer import RoundRobinLoadBalancer

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class NebulaPoolConfig:
    """Configuration for NebulaPool using Python dataclass"""

    # Connection settings
    addresses: str
    user_name: str
    password: Optional[str] = None

    # Pool settings
    max_client_size: int = DEFAULT_MAX_CLIENT_SIZE
    min_client_size: int = DEFAULT_MIN_CLIENT_SIZE
    max_wait_ms: int = DEFAULT_MAX_WAIT_MS
    block_when_exhausted: bool = DEFAULT_BLOCK_WHEN_EXHAUSTED

    # Timeout settings
    connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS
    request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS
    server_ping_timeout_ms: int = DEFAULT_PING_TIMEOUT_MS

    # Health check settings
    health_check_time_ms: int = DEFAULT_HEALTH_CHECK_TIME_MS
    test_on_borrow: bool = DEFAULT_TEST_ON_BORROW

    # Eviction settings
    idle_evict_schedule_ms: int = DEFAULT_IDLE_EVICT_SCHEDULE_MS
    min_evictable_idle_time_ms: int = DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS

    # Server health settings
    strictly_server_healthy: bool = DEFAULT_STRICT_SERVER_HEALTHY

    # Life time settings
    max_life_time_ms: int = DEFAULT_MAX_LIFE_TIME_MS

    # Session settings
    graph: Optional[str] = None
    schema: Optional[str] = None
    timezone: Optional[str] = None
    session_configs: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, str] = field(default_factory=dict)
    pre_statements: List[str] = field(default_factory=list)

    # Other settings
    scan_parallel: int = DEFAULT_SCAN_PARALLEL
    enable_tls: bool = DEFAULT_ENABLE_TLS
    ssl_param: Optional[SSLParam] = None
    auth_options: Dict[str, object] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize auth_options with password if provided"""
        if self.password:
            self.auth_options["password"] = self.password


class NebulaPool:
    """NebulaGraph connection pool using Python configuration style"""

    def __init__(self, config: NebulaPoolConfig):
        """Initialize the NebulaPool with configuration"""
        self.config = config
        self._load_balancer: Optional[RoundRobinLoadBalancer] = None
        self._factory: Optional[ClientPoolFactory] = None
        self._pool: List[NebulaClient] = []
        self._in_use: Dict[NebulaClient, bool] = {}
        self._lock = threading.Lock()
        self._closed = False

        self._init_pool()

    def _init_pool(self) -> None:
        """Initialize the connection pool"""
        # Parse addresses
        addresses = self._parse_addresses(self.config.addresses)

        # Create load balancer config
        class LoadBalancerConfig:
            def __init__(self, pool_config: NebulaPoolConfig, addrs: List[HostAddress]):
                self.address = addrs
                self.strictly_server_healthy = pool_config.strictly_server_healthy
                self.user_name = pool_config.user_name
                self.auth_options = pool_config.auth_options
                self.connect_timeout_mills = pool_config.connect_timeout_ms
                self.request_timeout_mills = pool_config.request_timeout_ms
                self.server_ping_timeout_mills = pool_config.server_ping_timeout_ms
                self.scan_parallel = pool_config.scan_parallel
                self.enable_tls = pool_config.enable_tls
                self.disable_verify_server_cert = False
                self.tls_ca = (
                    pool_config.ssl_param.ca_crt.decode()
                    if pool_config.ssl_param and pool_config.ssl_param.ca_crt
                    else None
                )
                self.tls_cert = (
                    pool_config.ssl_param.cert.decode()
                    if pool_config.ssl_param and pool_config.ssl_param.cert
                    else None
                )
                self.tls_key = (
                    pool_config.ssl_param.private_key.decode()
                    if pool_config.ssl_param and pool_config.ssl_param.private_key
                    else None
                )
                # Session settings
                self.graph = pool_config.graph
                self.schema = pool_config.schema
                self.timezone = pool_config.timezone
                self.session_configs = pool_config.session_configs
                self.parameters = pool_config.parameters
                self.pre_statements = pool_config.pre_statements
                self.max_life_time_ms = pool_config.max_life_time_ms

        lb_config = LoadBalancerConfig(self.config, addresses)
        self._load_balancer = RoundRobinLoadBalancer(lb_config)
        self._factory = ClientPoolFactory(self._load_balancer, lb_config)

        # Check server health
        self._load_balancer.check_servers()

        # Initialize minimum number of clients
        for _ in range(self.config.min_client_size):
            try:
                client = self._factory.create()
                self._pool.append(client)
                self._in_use[client] = False
            except Exception as e:
                logger.warning(f"Failed to create initial client: {e}")

    @staticmethod
    def _parse_addresses(addresses: str) -> List[HostAddress]:
        """Parse address string to HostAddress list"""
        result = []
        for addr in addresses.split(","):
            addr = addr.strip()
            if ":" in addr:
                host, port = addr.rsplit(":", 1)
                result.append(HostAddress(host, int(port)))
            else:
                raise ValueError(f"Invalid address format: {addr}")
        return result

    def get_client(self) -> NebulaClient:
        """Get a client from the pool"""
        if self._closed:
            raise RuntimeError("Pool is closed")

        start_time = time.time()
        while time.time() - start_time < self.config.max_wait_ms / 1000.0:
            with self._lock:
                # Try to find an available client
                for client in self._pool:
                    if not self._in_use.get(client, False):
                        if self.config.test_on_borrow:
                            if not self._factory.validate(client):
                                self._pool.remove(client)
                                del self._in_use[client]
                                self._factory.destroy(client)
                                continue
                        self._in_use[client] = True
                        return client

                # Try to create a new client if under max limit
                if len(self._pool) < self.config.max_client_size:
                    try:
                        client = self._factory.create()
                        self._pool.append(client)
                        self._in_use[client] = True
                        return client
                    except Exception as e:
                        logger.error(f"Failed to create new client: {e}")

                # If block_when_exhausted is False, raise exception
                if not self.config.block_when_exhausted:
                    raise RuntimeError("No available clients in pool")

                # Wait a bit before retrying
            time.sleep(0.01)

        raise RuntimeError(f"Timeout waiting for client after {self.config.max_wait_ms}ms")

    def return_client(self, client: NebulaClient) -> None:
        """Return a client to the pool"""
        if self._closed:
            return

        with self._lock:
            if client in self._in_use:
                # Check if client should be invalidated
                if (
                    client.is_closed_client()
                    or (time.time() * 1000 - client.get_create_time())
                    >= self.config.max_life_time_ms
                ):
                    self._pool.remove(client)
                    del self._in_use[client]
                    self._factory.destroy(client)
                else:
                    self._in_use[client] = False

    def close(self) -> None:
        """Close the pool and all clients"""
        with self._lock:
            if not self._closed:
                self._closed = True
                for client in self._pool:
                    try:
                        self._factory.destroy(client)
                    except Exception as e:
                        logger.warning(f"Failed to close client: {e}")
                self._pool.clear()
                self._in_use.clear()

    def get_active_sessions(self) -> int:
        """Get the number of active sessions"""
        with self._lock:
            return sum(1 for in_use in self._in_use.values() if in_use)

    def get_idle_sessions(self) -> int:
        """Get the number of idle sessions"""
        with self._lock:
            return sum(1 for in_use in self._in_use.values() if not in_use)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
