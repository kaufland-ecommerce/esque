import struct
from typing import Dict, Optional, Any

# noinspection PyDictCreation
PRIMITIVE_STRUCTS: Dict[str, struct.Struct] = {}

# Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true
# respectively. When reading a boolean value, any non-zero value is considered true.
PRIMITIVE_STRUCTS["BOOLEAN"] = struct.Struct("?")


def encode_boolean(value: bool) -> bytes:
    return PRIMITIVE_STRUCTS["BOOLEAN"].pack(value)


def decode_boolean(buffer: bytes) -> bool:
    return PRIMITIVE_STRUCTS["BOOLEAN"].unpack(buffer)[0]


# Represents an integer between -1**7 and 1**7-1 inclusive.
PRIMITIVE_STRUCTS["INT8"] = struct.Struct(">b")


def encode_int8(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT8"].pack(value)


def decode_int8(buffer: bytes) -> int:
    return PRIMITIVE_STRUCTS["INT8"].unpack(buffer)[0]


# Represents an integer between -1**15 and 1**15-1 inclusive. The values are encoded using two bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["INT16"] = struct.Struct(">h")


def encode_int16(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT16"].pack(value)


def decode_int16(buffer: bytes) -> int:
    return PRIMITIVE_STRUCTS["INT16"].unpack(buffer)[0]


# Represents an integer between -1**31 and 1**31-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["INT32"] = struct.Struct(">i")


def encode_int32(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT32"].pack(value)


def decode_int32(buffer: bytes) -> int:
    return PRIMITIVE_STRUCTS["INT32"].unpack(buffer)[0]


# Represents an integer between -1**63 and 1**63-1 inclusive. The values are encoded using eight bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["INT64"] = struct.Struct(">q")


def encode_int64(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT64"].pack(value)


def decode_int64(buffer: bytes) -> int:
    return PRIMITIVE_STRUCTS["INT64"].unpack(buffer)[0]


# Represents an integer between 0 and 1**32-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["UINT32"] = struct.Struct(">I")


def encode_uint32(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["UINT32"].pack(value)


def decode_uint32(buffer: bytes) -> int:
    return PRIMITIVE_STRUCTS["UINT32"].unpack(buffer)[0]


# Represents an integer between -2**31 and 2**31-1 inclusive. Encoding follows the variable-length zig-zag
# encoding from Google Protocol Buffers.
def encode_varint(value: int) -> bytes:
    return encode_uint32((value << 1) ^ (value >> 31))


def decode_varint(buffer: bytes) -> int:
    value = decode_uint32(buffer)
    value = (value // 2) ^ (-1 * (value & 1))
    return value


# Represents an integer between -1**63 and 1**63-1 inclusive. Encoding follows the variable-length zig-zag
# encoding from Google Protocol Buffers.
PRIMITIVE_STRUCTS["UINT64"] = struct.Struct(">Q")


def encode_varlong(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["UINT64"].pack((value << 1) ^ (value >> 63))


def decode_varlong(buffer: bytes) -> int:
    value = PRIMITIVE_STRUCTS["UINT64"].unpack(buffer)[0]
    value = (value // 2) ^ (-1 * (value & 1))
    return value


# Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow
# which are the UTF-8 encoding of the character sequence. Length must not be negative.
def encode_string(value: str) -> bytes:
    len_ = len(value)
    return encode_int16(len_) + struct.pack(f"{len_}s", value.encode("utf-8"))


def decode_string(buffer: bytes) -> str:
    len_ = decode_int16(buffer[:2])
    return struct.unpack(f"{len_}s", buffer[2:])[0].decode("utf-8")


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an
# INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is
# encoded with length of -1 and there are no following bytes.
def encode_nullable_string(value: Optional[str]) -> bytes:
    if value is None:
        return encode_int16(-1)
    return encode_string(value)


def decode_nullable_string(buffer: bytes) -> Optional[str]:
    len_ = decode_int16(buffer[:2])
    if len_ == -1:
        return None
    return struct.unpack(f"{len_}s", buffer[2:])[0].decode("utf-8")


# Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
def encode_bytes(value: bytes) -> bytes:
    len_ = len(value)
    return encode_int32(len_) + struct.pack(f"{len_}s", value)


def decode_bytes(buffer: bytes) -> bytes:
    len_ = decode_int32(buffer[:4])
    return struct.unpack(f"{len_}s", buffer[4:])[0]


# Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an
# INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following
# bytes.
def encode_nullable_bytes(value: Optional[bytes]) -> bytes:
    if value is None:
        return encode_int32(-1)
    return encode_bytes(value)


def decode_nullable_bytes(buffer: bytes) -> Optional[bytes]:
    len_ = decode_int32(buffer[:4])
    if len_ == -1:
        return None
    return struct.unpack(f"{len_}s", buffer[4:])[0]


# Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see
# Message Sets.
def encode_records(value: Optional[bytes]) -> bytes:
    return encode_nullable_bytes(value)


def decode_records(buffer: bytes) -> Optional[bytes]:
    return decode_nullable_bytes(buffer)


# Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g.
# STRING) or a structure. First, the length N is given as an INT32. Then N instances of type T follow.
# A null array is represented with a length of -1. In protocol documentation an array of T instances
# is referred to as [T].
def encode_array(type_: str, value: list) -> bytes:
    len_ = len(value)
    return encode_int32(len_) + b"".join((encode_type(type_, elem) for elem in value))


def decode_array(type_: str, buffer: bytes) -> list:
    len_ = decode_int32(buffer[:4])
    return [decode_type(type_, buffer[4:]) for _ in range(len_)]  # TODO this is bullshit, doesn't work


def decode_type(type_: str, buffer: bytes) -> Any:
    if type_ == "BOOLEAN":
        return decode_boolean(buffer)

    if type_ == "INT8":
        return decode_int8(buffer)

    if type_ == "INT16":
        return decode_int16(buffer)

    if type_ == "INT32":
        return decode_int32(buffer)

    if type_ == "INT64":
        return decode_int64(buffer)

    if type_ == "UINT32":
        return decode_uint32(buffer)

    if type_ == "VARINT":
        return decode_varint(buffer)

    if type_ == "VARLONG":
        return decode_varlong(buffer)

    if type_ == "STRING":
        return decode_string(buffer)

    if type_ == "NULLABLE_STRING":
        return decode_nullable_string(buffer)

    if type_ == "BYTES":
        return decode_bytes(buffer)

    if type_ == "NULLABLE_BYTES":
        return decode_nullable_bytes(buffer)

    if type_ == "RECORDS":
        return decode_records(buffer)

    if type_.startswith("ARRAY"):
        return decode_array(strip_array(type_), buffer)


def encode_type(type_: str, value: Any) -> bytes:
    if type_ == "BOOLEAN":
        return encode_boolean(value)

    if type_ == "INT8":
        return encode_int8(value)

    if type_ == "INT16":
        return encode_int16(value)

    if type_ == "INT32":
        return encode_int32(value)

    if type_ == "INT64":
        return encode_int64(value)

    if type_ == "UINT32":
        return encode_uint32(value)

    if type_ == "VARINT":
        return encode_varint(value)

    if type_ == "VARLONG":
        return encode_varlong(value)

    if type_ == "STRING":
        return encode_string(value)

    if type_ == "NULLABLE_STRING":
        return encode_nullable_string(value)

    if type_ == "BYTES":
        return encode_bytes(value)

    if type_ == "NULLABLE_BYTES":
        return encode_nullable_bytes(value)

    if type_ == "RECORDS":
        return encode_records(value)

    if type_.startswith("ARRAY"):
        return encode_array(strip_array(type_), value)


def strip_array(type_: str) -> str:
    return type_[len('ARRAY['):-1]
