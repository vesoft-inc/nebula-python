from abc import abstractmethod
from typing import Dict, Any, Optional
from nebula3.data.ResultSet import ResultSet


class BaseExecutor:
    @abstractmethod
    def execute_parameter(
        self, stmt: str, params: Optional[Dict[str, Any]]
    ) -> ResultSet:
        pass

    @abstractmethod
    def execute_json_with_parameter(
        self, stmt: str, params: Optional[Dict[str, Any]]
    ) -> bytes:
        pass

    def execute(self, stmt: str) -> ResultSet:
        return self.execute_parameter(stmt, None)

    def execute_json(self, stmt: str) -> bytes:
        return self.execute_json_with_parameter(stmt, None)
