import struct
from typing import Any, Dict, Optional

from typing import BinaryIO

# noinspection PyDictCreation
PRIMITIVE_STRUCTS: Dict[str, struct.Struct] = {}

# Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true
# respectively. When reading a boolean value, any non-zero value is considered true.
PRIMITIVE_STRUCTS["BOOLEAN"] = struct.Struct("?")


def encode_boolean(value: bool) -> bytes:
    return PRIMITIVE_STRUCTS["BOOLEAN"].pack(value)


def decode_boolean(buffer: BinaryIO) -> bool:
    return unpack_primitive("BOOLEAN", buffer)


def unpack_primitive(primitive: str, buffer: BinaryIO) -> Any:
    primitive_struct = PRIMITIVE_STRUCTS[primitive]
    return primitive_struct.unpack(buffer.read(primitive_struct.size))[0]


# Represents an integer between -1**7 and 1**7-1 inclusive.
PRIMITIVE_STRUCTS["INT8"] = struct.Struct(">b")


def encode_int8(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT8"].pack(value)


def decode_int8(buffer: BinaryIO) -> int:
    return unpack_primitive("INT8", buffer)


# Represents an integer between -1**15 and 1**15-1 inclusive. The values are encoded using two bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["INT16"] = struct.Struct(">h")


def encode_int16(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT16"].pack(value)


def decode_int16(buffer: BinaryIO) -> int:
    return unpack_primitive("INT16", buffer)


# Represents an integer between -1**31 and 1**31-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["INT32"] = struct.Struct(">i")


def encode_int32(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT32"].pack(value)


def decode_int32(buffer: BinaryIO) -> int:
    return unpack_primitive("INT32", buffer)


# Represents an integer between -1**63 and 1**63-1 inclusive. The values are encoded using eight bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["INT64"] = struct.Struct(">q")


def encode_int64(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["INT64"].pack(value)


def decode_int64(buffer: BinaryIO) -> int:
    return unpack_primitive("INT64", buffer)


# Represents an integer between 0 and 1**32-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).
PRIMITIVE_STRUCTS["UINT32"] = struct.Struct(">I")


def encode_uint32(value: int) -> bytes:
    return PRIMITIVE_STRUCTS["UINT32"].pack(value)


def decode_uint32(buffer: BinaryIO) -> int:
    return unpack_primitive("UINT32", buffer)


# Represents an integer between -2**31 and 2**31-1 inclusive. Encoding follows the variable-length zig-zag
# encoding from Google Protocol Buffers.
def encode_varint(value: int) -> bytes:
    assert (
        -2 ** 31 <= value <= (2 ** 31 - 1)
    ), f"Number not in range! {-2**31}(min) <= {value}(value) <= {(2**31-1)}(max)"
    value = (value << 1) ^ (value >> 31)
    return varlen_encode(value)


def varlen_encode(value: int) -> bytes:
    # see how many bytes we need, only 7 bit are available per byte
    # the msb is used to indicate whether another byte is following
    len_, rest = divmod(value.bit_length(), 7)
    if rest:
        len_ += 1

    if len_ == 0:
        return b"\x00"

    arr = bytearray(len_)
    for i in range(len_):
        # put last 7 bits into current position (we start at the end of the bytes array)
        arr[-i - 1] = value & 0x7F
        # shift our value by the seven bits we've just serialized
        value >>= 7

        # if this is not the first iteration, i.e. not the last byte, we set msb here to 1 to indicate that another
        # byte is following
        if i:
            arr[-i - 1] |= 0x80

    return bytes(arr)


def decode_varint(buffer: BinaryIO) -> int:
    zigzag = varlen_decode(buffer)

    # now turn ZigZag encoding
    value = (zigzag // 2) ^ (-1 * (zigzag & 1))
    return value


def varlen_decode(buffer) -> int:
    value = 0
    while True:
        # shift the value we've computed so far by 7 bits to make space for the next 7
        value <<= 7

        # read one byte
        byte = decode_int8(buffer)

        # append this byte's 7 data bits
        value ^= byte & 0x7F

        # check msb to see if we need to continue reading
        if not (byte & 0x80):
            break
    return value


# Represents an integer between -2**63 and 2**63-1 inclusive. Encoding follows the variable-length zig-zag
# encoding from Google Protocol Buffers.
PRIMITIVE_STRUCTS["UINT64"] = struct.Struct(">Q")


def encode_varlong(value: int) -> bytes:
    assert (
        -2 ** 63 <= value <= (2 ** 63 - 1)
    ), f"Number not in range! {-2 ** 63}(min) <= {value}(value) <= {(2 ** 63 - 1)}(max)"

    value = (value << 1) ^ (value >> 63)
    return varlen_encode(value)


def decode_varlong(buffer: BinaryIO) -> int:
    return decode_varint(buffer)


# Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow
# which are the UTF-8 encoding of the character sequence. Length must not be negative.
def encode_string(value: str) -> bytes:
    len_ = len(value)
    return encode_int16(len_) + struct.pack(f"{len_}s", value.encode("utf-8"))


def decode_string(buffer: BinaryIO) -> str:
    len_ = decode_int16(buffer)
    return buffer.read(len_).decode("utf-8")


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an
# INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is
# encoded with length of -1 and there are no following bytes.
def encode_nullable_string(value: Optional[str]) -> bytes:
    if value is None:
        return encode_int16(-1)
    return encode_string(value)


def decode_nullable_string(buffer: BinaryIO) -> Optional[str]:
    len_ = decode_int16(buffer)
    if len_ == -1:
        return None
    return buffer.read(len_).decode("utf-8")


# Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
def encode_bytes(value: bytes) -> bytes:
    len_ = len(value)
    return encode_int32(len_) + struct.pack(f"{len_}s", value)


def decode_bytes(buffer: BinaryIO) -> bytes:
    len_ = decode_int32(buffer)
    return buffer.read(len_)


# Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an
# INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following
# bytes.
def encode_nullable_bytes(value: Optional[bytes]) -> bytes:
    if value is None:
        return encode_int32(-1)
    return encode_bytes(value)


def decode_nullable_bytes(buffer: BinaryIO) -> Optional[bytes]:
    len_ = decode_int32(buffer)
    if len_ == -1:
        return None
    return buffer.read(len_)


# Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see
# Message Sets.
def encode_records(value: Optional[bytes]) -> bytes:
    return encode_nullable_bytes(value)


def decode_records(buffer: BinaryIO) -> Optional[bytes]:
    return decode_nullable_bytes(buffer)


# Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g.
# STRING) or a structure. First, the length N is given as an INT32. Then N instances of type T follow.
# A null array is represented with a length of -1. In protocol documentation an array of T instances
# is referred to as [T].
def encode_array(type_: str, value: list) -> bytes:
    len_ = len(value)
    return encode_int32(len_) + b"".join((encode_type(type_, elem) for elem in value))


def decode_array(type_: str, buffer: BinaryIO) -> list:
    len_ = decode_int32(buffer)
    return [decode_type(type_, buffer) for _ in range(len_)]


def decode_type(type_: str, buffer: BinaryIO) -> Any:
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
    return type_[len("ARRAY[") : -1]
