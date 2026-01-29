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

"""AuthResult class matching Java implementation"""


from dataclasses import dataclass


@dataclass(frozen=True)
class AuthResult:
    """Result of authentication, matching Java AuthResult class"""

    session_id: int
    version: str

    def get_session_id(self) -> int:
        """Get the session ID"""
        return self.session_id

    def get_version(self) -> str:
        """Get the server version"""
        return self.version