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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from nebulagraph_python.client._connection import AsyncConnection, Connection

import uuid

from nebulagraph_python._error_code import ErrorCode
from nebulagraph_python.client.logger import logger
from nebulagraph_python.error import ExecutingError


@dataclass
class SessionConfig:
    schema: Optional[str] = None
    graph: Optional[str] = None
    timezone: Optional[str] = None
    values: Dict[str, str] = field(default_factory=dict)
    configs: Dict[str, str] = field(default_factory=dict)


@dataclass(kw_only=True)
class SessionBase:
    username: str
    password: str | None
    session_config: SessionConfig | None
    auth_options: Dict[str, str] | None

    _session: int = -1
    _hash: int = field(default_factory=lambda: uuid.uuid4().int)


@dataclass
class Session(SessionBase):
    conn: "Connection"

    def execute(
        self, statement: str, *, timeout: Optional[float] = None, do_ping: bool = False
    ):
        res = self.conn.execute(
            self._session, statement, timeout=timeout, do_ping=do_ping
        )
        # Retry for only one time
        if res.status_code == ErrorCode.SESSION_NOT_FOUND.value:
            self._session = self.conn.authenticate(
                self.username,
                self.password,
                session_config=self.session_config,
                auth_options=self.auth_options,
            )
            res = self.conn.execute(
                self._session, statement, timeout=timeout, do_ping=do_ping
            )
        return res

    def close(self):
        """Close session"""
        try:
            self.conn.execute(self._session, "SESSION CLOSE")
        except Exception:
            logger.exception("Failed to close session")

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._hash == other._hash


@dataclass
class AsyncSession(SessionBase):
    conn: "AsyncConnection"

    async def execute(
        self, statement: str, *, timeout: Optional[float] = None, do_ping: bool = False
    ):
        res = await self.conn.execute(
            self._session, statement, timeout=timeout, do_ping=do_ping
        )
        # Retry for only one time
        if res.status_code == ErrorCode.SESSION_NOT_FOUND.value:
            self._session = await self.conn.authenticate(
                self.username,
                self.password,
                session_config=self.session_config,
                auth_options=self.auth_options,
            )
            res = await self.conn.execute(
                self._session, statement, timeout=timeout, do_ping=do_ping
            )
        return res

    async def close(self):
        try:
            await self.conn.execute(self._session, "SESSION CLOSE")
        except Exception:
            logger.exception("Failed to close async session")

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._hash == other._hash


async def ainit_session(conn: "AsyncConnection", sid: int, config: SessionConfig):
    # All execute calls here must be awaited and then checked
    try:
        if config.schema is not None:
            result = await conn.execute(sid, f"SESSION SET SCHEMA `{config.schema}`")
            result.raise_on_error()
        if config.graph is not None:
            result = await conn.execute(sid, f"SESSION SET GRAPH `{config.graph}`")
            result.raise_on_error()
        if config.timezone is not None:
            result = await conn.execute(
                sid, f"SESSION SET TIME ZONE `{config.timezone}`"
            )
            result.raise_on_error()
        if config.values:
            result = await conn.execute(
                sid,
                f"SESSION SET VALUE {','.join(f'${k_}={v_}' for k_, v_ in config.values.items())}",
            )
            result.raise_on_error()
        if config.configs:
            for k, v in config.configs.items():
                result = await conn.execute(sid, f"SESSION SET {k}={v}")
                result.raise_on_error()
    except ExecutingError as e:
        logger.error(f"Error during async session post-init: {e}")
        raise


def init_session(conn: "Connection", sid: int, config: SessionConfig):
    """Initialize session with configuration settings"""
    try:
        if config.schema is not None:
            result = conn.execute(sid, f"SESSION SET SCHEMA `{config.schema}`")
            result.raise_on_error()
        if config.graph is not None:
            result = conn.execute(sid, f"SESSION SET GRAPH `{config.graph}`")
            result.raise_on_error()
        if config.timezone is not None:
            result = conn.execute(sid, f"SESSION SET TIME ZONE `{config.timezone}`")
            result.raise_on_error()
        if config.values:
            result = conn.execute(
                sid,
                f"SESSION SET VALUE {','.join(f'${k_}={v_}' for k_, v_ in config.values.items())}",
            )
            result.raise_on_error()
        if config.configs:
            for k, v in config.configs.items():
                result = conn.execute(sid, f"SESSION SET {k}={v}")
                result.raise_on_error()
    except ExecutingError as e:
        logger.error(f"Error during session post-init: {e}")
        raise
