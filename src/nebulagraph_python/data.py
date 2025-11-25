from dataclasses import dataclass, field
from typing import List, Optional

from nebulagraph_python.proto.graph_pb2 import PlanInfo


@dataclass
class HostAddress:
    """Represents a NebulaGraph service address"""

    host: str
    port: int

    def __str__(self):
        return f"{self.host}:{self.port}"

    def __hash__(self):
        return hash((self.host, self.port))

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


@dataclass()
class SSLParam:
    """SSL parameters for secure connections"""

    ca_crt: Optional[bytes] = field(default=None)
    private_key: Optional[bytes] = field(default=None)
    cert: Optional[bytes] = field(default=None)

    @classmethod
    def from_files(
        cls,
        ca_crt_file_path: Optional[str] = None,
        crt_file_path: Optional[str] = None,
        key_file_path: Optional[str] = None,
    ) -> "SSLParam":
        """
        Create SSLParam instance for CA-signed certificates

        Args:
            ca_crt_file_path: Path to the CA certificate file
            crt_file_path: Path to the certificate file
            key_file_path: Path to the private key file

        Returns:
            SSLParam instance configured for CA-signed certificates
        """
        if ca_crt_file_path:
            with open(ca_crt_file_path, "rb") as f:
                ca_crt = f.read()
        else:
            ca_crt = None

        if crt_file_path:
            with open(crt_file_path, "rb") as f:
                cert = f.read()
        else:
            cert = None

        if key_file_path:
            with open(key_file_path, "rb") as f:
                private_key = f.read()
        else:
            private_key = None

        return cls(
            ca_crt=ca_crt,
            private_key=private_key,
            cert=cert,
        )


class PlanInfoNode:
    def __init__(self, plan_info: PlanInfo):
        self.plan_info = plan_info
        self.id = plan_info.id.decode()
        self.name = plan_info.name.decode()
        self.details = plan_info.details.decode()
        self.time_ms = plan_info.time_ms
        self.rows = plan_info.rows
        self.memory_kib = plan_info.memory_kib
        self.blocked_ms = plan_info.blocked_ms
        self.queued_ms = plan_info.queued_ms
        self.consume_ms = plan_info.consume_ms
        self.produce_ms = plan_info.produce_ms
        self.finish_ms = plan_info.finish_ms
        self.batches = plan_info.batches
        self.concurrency = plan_info.concurrency
        self.other_stats_json = plan_info.other_stats_json.decode()
        self.children = [PlanInfoNode(plan) for plan in plan_info.children]

    def get_plan_id(self) -> str:
        return self.id

    def get_id(self) -> str:
        return self.id

    def get_name(self) -> str:
        return self.name

    def get_details(self) -> str:
        return self.details

    def get_time_ms(self) -> float:
        return self.time_ms

    def get_rows(self) -> int:
        return self.rows

    def get_memory_kib(self) -> float:
        return self.memory_kib

    def get_blocked_ms(self) -> float:
        return self.blocked_ms

    def get_children(self) -> List["PlanInfoNode"]:
        return self.children


@dataclass
class ExtraInfo:
    """Class that maintains additional information for execution result."""

    cursor: Optional[str] = None
    affected_nodes: int = 0
    affected_edges: int = 0
    total_server_time_us: int = 0
    build_time_us: int = 0
    optimize_time_us: int = 0
    serialize_time_us: int = 0

    def __str__(self) -> str:
        return (
            f"ExtraInfo{{cursor='{self.cursor}', "
            f"affectedNodes={self.affected_nodes}, "
            f"affectedEdges={self.affected_edges}, "
            f"totalServerTimeUs={self.total_server_time_us}, "
            f"buildTimeUs={self.build_time_us}, "
            f"optimizeTimeUs={self.optimize_time_us}, "
            f"serializeTimeUs={self.serialize_time_us}}}"
        )
