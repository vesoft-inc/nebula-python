# nebula3/hash.py
from __future__ import annotations

_M: int = 0xC6A4A7935BD1E995
_R: int = 47
_MASK64: int = (1 << 64) - 1


def _read_u64_le(buf: bytes) -> int:
    """ Convert little-endian bytes of up to 8 bytes to an unsigned integer. """
    return int.from_bytes(buf, byteorder="little", signed=False)


def hash(data: bytes | str, seed: int = 0xC70F6907) -> int:
    """MurmurHash2 64-bit variant:
    :Param data: supports str (utf-8 encoding), bytes, bytearray
    :Param seed: defaults to 0xC70F6907
    :return: Python int, in the range of signed 64-bit
    """
    if isinstance(data, str):
        data_as_bytes = data.encode("utf-8")
    elif isinstance(data, (bytes, bytearray)):
        data_as_bytes = bytes(data)
    else:
        raise TypeError("Input must be str, bytes, or bytearray")

    h = (seed ^ (_M * len(data_as_bytes) & _MASK64)) & _MASK64
    off = len(data_as_bytes) // 8 * 8
    for i in range(0, off, 8):
        k = _read_u64_le(data_as_bytes[i: i + 8])
        k = (k * _M) & _MASK64
        k ^= (k >> _R)
        k = (k * _M) & _MASK64
        h ^= k
        h = (h * _M) & _MASK64

    tail = data_as_bytes[off:]
    if tail:
        t = _read_u64_le(tail)
        h ^= t
        h = (h * _M) & _MASK64

    h ^= (h >> _R)
    h = (h * _M) & _MASK64
    h ^= (h >> _R)

    if h & (1 << 63):
        h -= 1 << 64
    return h
