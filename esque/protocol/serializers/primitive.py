import struct
from abc import ABCMeta
from enum import Enum
from typing import Any, BinaryIO, Callable, Dict, Generic, List, Optional, Tuple, TypeVar

T = TypeVar("T")
Schema = List[Tuple[Optional[str], "BaseSerializer"]]


class PrimitiveType(Enum):
    Boolean = 1
    Int8 = 2
    Int16 = 3
    Int32 = 4
    UInt32 = 5
    Int64 = 6
    VarInt = 7
    VarLong = 8
    String = 9
    NullableString = 10
    Bytes = 11
    NullableBytes = 12
    Records = 13


class BaseSerializer(Generic[T], metaclass=ABCMeta):
    def write(self, buffer: BinaryIO, value: T):
        buffer.write(self.encode(value))

    def encode(self, value: T) -> bytes:
        raise NotImplementedError()

    def read(self, buffer: BinaryIO) -> T:
        raise NotImplementedError()


class PrimitiveSerializer(Generic[T], BaseSerializer):
    def __init__(self, format_: str):
        self._struct = struct.Struct(format_)

    def encode(self, value: T) -> bytes:
        return self._struct.pack(value)

    def read(self, buffer: BinaryIO) -> T:
        return self._struct.unpack(buffer.read(self._struct.size))[0]


class GenericSerializer(Generic[T], BaseSerializer):
    def __init__(self, encoder: Callable[[T], bytes], reader: Callable[[BinaryIO], T]):
        self._encoder: Callable[[Any], bytes] = encoder
        self._reader: Callable[[BinaryIO], Any] = reader

    def encode(self, value: T) -> bytes:
        return self._encoder(value)

    def read(self, buffer: BinaryIO) -> T:
        return self._reader(buffer)


class VarlenZigZagSerializer(BaseSerializer):
    def __init__(self, bits: int):
        self.bits = bits - 1

    def encode(self, value: int) -> bytes:
        assert (
            -2 ** self.bits <= value <= (2 ** self.bits - 1)
        ), f"Number not in range! {-2 ** self.bits}(min) <= {value}(value) <= {(2 ** self.bits - 1)}(max)"

        value = (value << 1) ^ (value >> self.bits)
        return self.varlen_encode(value)

    def read(self, buffer: BinaryIO) -> int:
        zigzag = self.varlen_decode(buffer)

        # now turn ZigZag encoding
        value = (zigzag // 2) ^ (-1 * (zigzag & 1))
        return value

    @staticmethod
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

    @staticmethod
    def varlen_decode(buffer) -> int:
        value = 0
        while True:
            # shift the value we've computed so far by 7 bits to make space for the next 7
            value <<= 7

            # read one byte
            byte = int8Serializer.read(buffer)

            # append this byte's 7 data bits
            value ^= byte & 0x7F

            # check msb to see if we need to continue reading
            if not (byte & 0x80):
                break
        return value


# Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
def encode_bytes(value: bytes) -> bytes:
    assert value is not None, "Value cannot be None!"
    len_ = len(value)
    return int32Serializer.encode(len_) + value


def read_bytes(buffer: BinaryIO) -> bytes:
    len_ = int32Serializer.read(buffer)
    return buffer.read(len_)


# Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an
# INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following
# bytes.
def encode_nullable_bytes(value: Optional[bytes]) -> bytes:
    if value is None:
        return int32Serializer.encode(-1)
    return encode_bytes(value)


def read_nullable_bytes(buffer: BinaryIO) -> Optional[bytes]:
    len_ = int32Serializer.read(buffer)
    if len_ == -1:
        return None
    return buffer.read(len_)


# Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow
# which are the UTF-8 encoding of the character sequence. Length must not be negative.
def encode_string(value: str) -> bytes:
    assert value is not None, "Value cannot be None!"
    len_ = len(value)
    return int16Serializer.encode(len_) + value.encode("utf-8")


def read_string(buffer: BinaryIO) -> str:
    len_ = int16Serializer.read(buffer)
    return buffer.read(len_).decode("utf-8")


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an
# INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is
# encoded with length of -1 and there are no following bytes.
def encode_nullable_string(value: Optional[str]) -> bytes:
    if value is None:
        return int16Serializer.encode(-1)
    return encode_string(value)


def read_nullable_string(buffer: BinaryIO) -> Optional[str]:
    len_ = int16Serializer.read(buffer)
    if len_ == -1:
        return None
    return buffer.read(len_).decode("utf-8")


booleanSerializer: BaseSerializer[bool] = PrimitiveSerializer("?")
int8Serializer: BaseSerializer[int] = PrimitiveSerializer(">b")
int16Serializer: BaseSerializer[int] = PrimitiveSerializer(">h")
int32Serializer: BaseSerializer[int] = PrimitiveSerializer(">i")
int64Serializer: BaseSerializer[int] = PrimitiveSerializer(">q")
uint32Serializer: BaseSerializer[int] = PrimitiveSerializer(">I")
varIntSerializer: BaseSerializer[int] = VarlenZigZagSerializer(32)
varLongSerializer: BaseSerializer[int] = VarlenZigZagSerializer(64)
nullableStringSerializer: BaseSerializer[Optional[str]] = GenericSerializer(
    encode_nullable_string, read_nullable_string
)
stringSerializer: BaseSerializer[str] = GenericSerializer(encode_string, read_string)
nullableBytesSerializer: BaseSerializer[Optional[bytes]] = GenericSerializer(
    encode_nullable_bytes, read_nullable_bytes
)
bytesSerializer: BaseSerializer[bytes] = GenericSerializer(encode_bytes, read_bytes)

# Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see
# Message Sets.
recordsSerializer: BaseSerializer[bytes] = nullableBytesSerializer

SERIALIZER_MAP: Dict[PrimitiveType, BaseSerializer] = {
    PrimitiveType.Boolean: booleanSerializer,
    PrimitiveType.Int8: int8Serializer,
    PrimitiveType.Int16: int16Serializer,
    PrimitiveType.Int32: int32Serializer,
    PrimitiveType.UInt32: uint32Serializer,
    PrimitiveType.Int64: int64Serializer,
    PrimitiveType.VarInt: varIntSerializer,
    PrimitiveType.VarLong: varLongSerializer,
    PrimitiveType.NullableString: nullableStringSerializer,
    PrimitiveType.String: stringSerializer,
    PrimitiveType.NullableBytes: nullableBytesSerializer,
    PrimitiveType.Bytes: bytesSerializer,
    PrimitiveType.Records: recordsSerializer,
}

get_serializer: Callable[[PrimitiveType], BaseSerializer] = SERIALIZER_MAP.get
