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

from typing import TYPE_CHECKING, Optional

from nebulagraph_python._error_code import ErrorCode

if TYPE_CHECKING:
    from nebulagraph_python.result_set import ResultSet


class NebulaGraphRemoteError(Exception):
    code: ErrorCode
    message: str
    result: Optional["ResultSet"]

    def __init__(
        self, code: ErrorCode, message: str, result: Optional["ResultSet"] = None
    ):
        self.code = code
        self.message = message
        self.result = result
        super().__init__(f"{code.value}: {message}")


class NebulaGraphClientError(Exception):
    pass


class InternalError(NebulaGraphClientError):
    """General internal error that cannot be recovered due to unexpected inputs or bugs"""

    pass


class ConnectingError(NebulaGraphClientError):
    """Error raised when connecting to the server"""

    pass


class AuthenticatingError(NebulaGraphClientError):
    """Error raised when authenticating failed"""

    pass


class ExecutingError(NebulaGraphClientError):
    """Error raised when executing a statement failed"""

    pass


class PoolError(NebulaGraphClientError):
    """Error raised when pool is not healthy"""

    pass
