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


DEFAULT_MAX_CLIENT_SIZE: int = 10
DEFAULT_MIN_CLIENT_SIZE: int = 1
DEFAULT_CONNECT_TIMEOUT_MS: int = 3 * 1000  # 3 seconds
DEFAULT_REQUEST_TIMEOUT_MS: int = 60 * 1000  # 1 minute
DEFAULT_MAX_TIMEOUT_MS: int = 2**31 - 1  # about 25 days
DEFAULT_MAX_PING_TIMEOUT_MS: int = 10 * 60 * 1000
DEFAULT_PING_TIMEOUT_MS: int = 1000
DEFAULT_HEALTH_CHECK_TIME_MS: int = 5 * 60 * 1000
DEFAULT_TEST_ON_BORROW: bool = True
DEFAULT_BLOCK_WHEN_EXHAUSTED: bool = False
DEFAULT_MAX_WAIT_MS: int = 2**63 - 1 // 1000
DEFAULT_IDLE_EVICT_SCHEDULE_MS: int = -1
DEFAULT_MIN_EVICTABLE_IDLE_TIME_MS: int = 30 * 60 * 1000
DEFAULT_STRICT_SERVER_HEALTHY: bool = False
DEFAULT_MAX_LIFE_TIME_MS: int = 2**63 - 1
DEFAULT_BATCH_SIZE: int = 1000
DEFAULT_SCAN_PARALLEL: int = 10
DEFAULT_ENABLE_TLS: bool = False
DEFAULT_DISABLE_VERIFY_SERVER_CERT: bool = False
DEFAULT_TLS_PEER_NAME_VERIFY: bool = True

# old default config
DEFAULT_SESSION_POOL_SIZE: int = 10
DEFAULT_SESSION_POOL_WAIT_TIMEOUT: float = 0.0
DEFAULT_MAX_CLIENT_SIZE_OLD: int = 10
DEFAULT_MIN_CLIENT_SIZE_OLD: int = 1
DEFAULT_TEST_ON_BORROW_OLD: bool = True
DEFAULT_STRICTLY_SERVER_HEALTHY: bool = False
DEFAULT_MAX_WAIT: float = 5.0
DEFAULT_CONNECT_TIMEOUT: float = 3.0
DEFAULT_REQUEST_TIMEOUT: float = 60.0
