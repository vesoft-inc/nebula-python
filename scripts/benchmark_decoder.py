#!/usr/bin/env python3
"""Micro-benchmark for decoder hot paths.

This benchmark avoids importing `nebulagraph_python.__init__` (which pulls optional runtime
client dependencies like grpc) and only loads decoder modules from `src/`.
"""

from __future__ import annotations

import os
import sys
import time
import types
from dataclasses import dataclass


def _bootstrap_local_package() -> None:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_root = os.path.join(repo_root, "src")
    pkg_root = os.path.join(src_root, "nebulagraph_python")

    if src_root not in sys.path:
        sys.path.insert(0, src_root)

    # Avoid executing nebulagraph_python/__init__.py (imports grpc client stack).
    if "nebulagraph_python" not in sys.modules:
        pkg = types.ModuleType("nebulagraph_python")
        pkg.__path__ = [pkg_root]
        sys.modules["nebulagraph_python"] = pkg


_bootstrap_local_package()

from nebulagraph_python.decoder.data_types import (  # noqa: E402
    BasicType,
    ByteOrder,
    ColumnType,
    ResultGraphSchemas,
)
from nebulagraph_python.decoder.value_parser import ValueParser  # noqa: E402


@dataclass
class FakeNestedVector:
    vector_data: bytes
    nested_vectors: list


class FakeVectorWrapper:
    def __init__(self, vector_data: bytes, nested_vectors: list | None = None):
        nested_vectors = nested_vectors or []
        self.vector = FakeNestedVector(vector_data=vector_data, nested_vectors=nested_vectors)
        self._vector_data = vector_data

    def get_vector_data(self) -> bytes:
        return self._vector_data


def _bench_int32(parser: ValueParser, rows: int, rounds: int) -> tuple[float, int]:
    vector_data = bytearray(rows * 4)
    for i in range(rows):
        vector_data[i * 4 : i * 4 + 4] = int(i).to_bytes(4, "little", signed=True)

    vec = FakeVectorWrapper(bytes(vector_data))
    data_type = BasicType(ColumnType.INT32)

    checksum = 0
    start = time.perf_counter()
    for _ in range(rounds):
        for i in range(rows):
            checksum += parser._decode_flat_value(vec, data_type, i)
    elapsed = time.perf_counter() - start
    return elapsed, checksum


def _bench_string(parser: ValueParser, rows: int, rounds: int) -> tuple[float, int]:
    # Build inline strings only (length <= 12) to focus on string-header parsing.
    row_size = 16
    vector_data = bytearray(rows * row_size)
    for i in range(rows):
        s = f"v{i % 1000}".encode("utf-8")
        base = i * row_size
        vector_data[base : base + 4] = len(s).to_bytes(4, "little", signed=True)
        vector_data[base + 4 : base + 4 + len(s)] = s

    vec = FakeVectorWrapper(bytes(vector_data))
    data_type = BasicType(ColumnType.STRING)

    checksum = 0
    start = time.perf_counter()
    for _ in range(rounds):
        for i in range(rows):
            checksum += len(parser._decode_flat_value(vec, data_type, i))
    elapsed = time.perf_counter() - start
    return elapsed, checksum


def main() -> None:
    parser = ValueParser(ResultGraphSchemas(), 0, ByteOrder.LITTLE_ENDIAN)
    rows = 20_000
    rounds = 10

    int_elapsed, int_checksum = _bench_int32(parser, rows, rounds)
    str_elapsed, str_checksum = _bench_string(parser, rows, rounds)

    print(f"int32 decode:   {int_elapsed:.6f}s (checksum={int_checksum})")
    print(f"string decode:  {str_elapsed:.6f}s (checksum={str_checksum})")


if __name__ == "__main__":
    main()

