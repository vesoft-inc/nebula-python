import struct

from nebulagraph_python.decoder.data_types import ByteOrder, charset
from nebulagraph_python.decoder.size_constant import ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE
from nebulagraph_python.proto.vector_pb2 import NestedVector


def bytes_to_int8(data: bytes) -> int:
    """Match Java's DecodeUtils.bytesToInt8"""
    # Java: return data.byteAt(0);
    return int.from_bytes([data[0]], byteorder="big", signed=True)


def bytes_to_uint8(data: bytes) -> int:
    """Match Java's DecodeUtils.bytesToUInt8"""
    # Java: return data.byteAt(0) & 0xFF;
    return data[0] & 0xFF


def bytes_to_int16(data: bytes, byte_order: ByteOrder) -> int:
    """Match Java's DecodeUtils.bytesToInt16"""
    # Java: ByteBuffer buffer = ByteBuffer.wrap(data.toByteArray());
    # return buffer.order(order).getShort();
    return int.from_bytes(data, byteorder=byte_order.value, signed=True)


def bytes_to_uint16(data: bytes, byte_order: ByteOrder) -> int:
    """Match Java's DecodeUtils.bytesToUInt16"""
    # Java: return bytesToInt16(data, order) & 0xFFFF;
    return bytes_to_int16(data, byte_order) & 0xFFFF


def bytes_to_int32(data: bytes, byte_order: ByteOrder) -> int:
    """Match Java's DecodeUtils.bytesToInt32"""
    # Java: ByteBuffer buffer = ByteBuffer.wrap(data.toByteArray());
    # return buffer.order(order).getInt();
    return int.from_bytes(data, byteorder=byte_order.value, signed=True)


def bytes_to_uint32(data: bytes, byte_order: ByteOrder) -> int:
    """Match Java's DecodeUtils.bytesToUInt32"""
    # Java: return Integer.toUnsignedLong(bytesToInt32(data, order));
    return int.from_bytes(data, byteorder=byte_order.value, signed=False)


def bytes_to_int64(data: bytes, byte_order: ByteOrder) -> int:
    """Match Java's DecodeUtils.bytesToInt64"""
    # Java: ByteBuffer buffer = ByteBuffer.wrap(data.toByteArray());
    # return buffer.order(order).getLong();
    return int.from_bytes(data, byteorder=byte_order.value, signed=True)


def bytes_to_float(data: bytes, byte_order: ByteOrder) -> float:
    """Match Java's DecodeUtils.bytesToFloat"""
    # Java: ByteBuffer buffer = ByteBuffer.wrap(data.toByteArray());
    # return buffer.order(order).getFloat();
    fmt = "<f" if byte_order == ByteOrder.LITTLE_ENDIAN else ">f"
    return struct.unpack(fmt, data)[0]


def bytes_to_double(data: bytes, byte_order: ByteOrder) -> float:
    """Match Java's DecodeUtils.bytesToDouble"""
    # Java: ByteBuffer buffer = ByteBuffer.wrap(data.toByteArray());
    # return buffer.order(order).getDouble();
    fmt = "<d" if byte_order == ByteOrder.LITTLE_ENDIAN else ">d"
    return struct.unpack(fmt, data)[0]


def bytes_to_bool(data: bytes) -> bool:
    """Match Java's DecodeUtils.bytesToBool"""
    # Java: return data.byteAt(0) == 0x01;
    return data[0] == 0x01


def is_null_bit_map_all_set(vector: NestedVector) -> bool:
    """Match Java's DecodeUtils.isNullBitMapAllSet"""
    content_type = vector.common_meta_data.vector_content_type
    return (content_type & 0x00000100) != 0


def bytes_to_sized_string(data: bytes, start_pos: int, byte_order: ByteOrder) -> str:
    """Match Java's DecodeUtils.bytesToSizedString"""
    length = bytes_to_int16(
        data[start_pos : start_pos + ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE],
        byte_order,
    )
    start_pos += ELEMENT_NUMBER_SIZE_FOR_ANY_VALUE

    # Use charset-based decoding instead of character by character
    str_bytes = data[start_pos : start_pos + length]
    return str_bytes.decode(charset)
