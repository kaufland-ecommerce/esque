import struct
from typing import Dict

# noinspection PyDictCreation
PRIMITIVE_STRUCTS: Dict[str, struct.Struct] = {}

# Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true
# respectively. When reading a boolean value, any non-zero value is considered true.

PRIMITIVE_STRUCTS["BOOLEAN"] = struct.Struct("?")


def encode_boolean(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["BOOLEAN"].pack(value)


def decode_boolean(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["BOOLEAN"].unpack(buffer)


# Represents an integer between -27 and 27-1 inclusive.

PRIMITIVE_STRUCTS["INT8"] = struct.Struct(">b")


def encode_int8(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["INT8"].pack(value)


def decode_int8(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["INT8"].unpack(buffer)


# Represents an integer between -215 and 215-1 inclusive. The values are encoded using two bytes in
# network byte order (big-endian).

PRIMITIVE_STRUCTS["INT16"] = struct.Struct(">h")


def encode_int16(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["INT16"].pack(value)


def decode_int16(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["INT16"].unpack(buffer)


# Represents an integer between -231 and 231-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).

PRIMITIVE_STRUCTS["INT32"] = struct.Struct(">i")


def encode_int32(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["INT32"].pack(value)


def decode_int32(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["INT32"].unpack(buffer)


# Represents an integer between -263 and 263-1 inclusive. The values are encoded using eight bytes in
# network byte order (big-endian).

PRIMITIVE_STRUCTS["INT64"] = struct.Struct(">q")


def encode_int64(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["INT64"].pack(value)


def decode_int64(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["INT64"].unpack(buffer)


# Represents an integer between 0 and 232-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).

PRIMITIVE_STRUCTS["UINT32"] = struct.Struct(">I")


def encode_uint32(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["UINT32"].pack(value)


def decode_uint32(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["UINT32"].unpack(buffer)


# Represents an integer between -231 and 231-1 inclusive. Encoding follows the variable-length zig-zag
# encoding from Google Protocol Buffers.

PRIMITIVE_STRUCTS["VARINT"] = struct.Struct("")


def encode_varint(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["VARINT"].pack(value)


def decode_varint(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["VARINT"].unpack(buffer)


# Represents an integer between -263 and 263-1 inclusive. Encoding follows the variable-length zig-zag
# encoding from Google Protocol Buffers.

PRIMITIVE_STRUCTS["VARLONG"] = struct.Struct("")


def encode_varlong(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["VARLONG"].pack(value)


def decode_varlong(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["VARLONG"].unpack(buffer)


# Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow
# which are the UTF-8 encoding of the character sequence. Length must not be negative.

PRIMITIVE_STRUCTS["STRING"] = struct.Struct("")


def encode_string(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["STRING"].pack(value)


def decode_string(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["STRING"].unpack(buffer)


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an
# INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is
# encoded with length of -1 and there are no following bytes.

PRIMITIVE_STRUCTS["NULLABLE_STRING"] = struct.Struct("")


def encode_nullable_string(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["NULLABLE_STRING"].pack(value)


def decode_nullable_string(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["NULLABLE_STRING"].unpack(buffer)


# Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.

PRIMITIVE_STRUCTS["BYTES"] = struct.Struct("")


def encode_bytes(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["BYTES"].pack(value)


def decode_bytes(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["BYTES"].unpack(buffer)


# Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an
# INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following
# bytes.

PRIMITIVE_STRUCTS["NULLABLE_BYTES"] = struct.Struct("")


def encode_nullable_bytes(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["NULLABLE_BYTES"].pack(value)


def decode_nullable_bytes(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["NULLABLE_BYTES"].unpack(buffer)


# Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see
# Message Sets.

PRIMITIVE_STRUCTS["RECORDS"] = struct.Struct("")


def encode_records(value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["RECORDS"].pack(value)


def decode_records(buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["RECORDS"].unpack(buffer)


# Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g.
# STRING) or a structure. First, the length N is given as an INT32. Then N instances of type T follow.
# A null array is represented with a length of -1. In protocol documentation an array of T instances
# is referred to as [T].

PRIMITIVE_STRUCTS["ARRAY"] = struct.Struct("")


def encode_array(type_:str, value: ...) -> bytes:
    return PRIMITIVE_STRUCTS["ARRAY"].pack(value)


def decode_array(type_:str, buffer: bytes) -> ...:
    return PRIMITIVE_STRUCTS["ARRAY"].unpack(buffer)


def decode_type(type:str, buffer: bytes) -> ...:
    pass

def encode_type(type:str, value: ...) -> bytes:
    pass