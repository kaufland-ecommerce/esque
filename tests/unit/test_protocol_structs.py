from typing import Dict, Generic, List, Optional, TypeVar

import pytest

from esque.protocol import structs

T = TypeVar("T")


class Sample(Generic[T]):
    def __init__(self, encoded_value: bytes, decoded_value: T):
        self.encoded_value: bytes = encoded_value
        self.decoded_value: T = decoded_value


class ArraySample(Generic[T]):
    def __init__(self, encoded_value: bytes, decoded_value: T, type_: str):
        self.encoded_value: bytes = encoded_value
        self.decoded_value: T = decoded_value
        self.type_: str = type_


SAMPLES: Dict[str, List[Sample]] = {
    "BOOLEAN": [
        Sample[bool](encoded_value=b"\00", decoded_value=False),
        Sample[bool](encoded_value=b"\01", decoded_value=True),
    ],
    "INT8": [
        Sample[int](encoded_value=b"\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x7f", decoded_value=127),
        Sample[int](encoded_value=b"\x80", decoded_value=-128),
    ],
    "INT16": [
        Sample[int](encoded_value=b"\x00\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x7f\xff", decoded_value=32767),
        Sample[int](encoded_value=b"\x80\x00", decoded_value=-32768),
    ],
    "INT32": [
        Sample[int](encoded_value=b"\x00\x00\x00\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x7f\xff\xff\xff", decoded_value=2147483647),
        Sample[int](encoded_value=b"\x80\x00\x00\x00", decoded_value=-2147483648),
    ],
    "INT64": [
        Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x7f\xff\xff\xff\xff\xff\xff\xff", decoded_value=9223372036854775807),
        Sample[int](encoded_value=b"\x80\x00\x00\x00\x00\x00\x00\x00", decoded_value=-9223372036854775808),
    ],
    "UINT32": [
        Sample[int](encoded_value=b"\x00\x00\x00\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x7f\xff\xff\xff", decoded_value=2147483647),
        Sample[int](encoded_value=b"\x80\x00\x00\x00", decoded_value=2147483648),
    ],
    "VARINT": [
        Sample[int](encoded_value=b"\x00\x00\x00\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x00\x00\x00\x01", decoded_value=-1),
        Sample[int](encoded_value=b"\x00\x00\x00\x02", decoded_value=1),
        Sample[int](encoded_value=b"\x00\x00\x00\x03", decoded_value=-2),
        Sample[int](encoded_value=b"\xff\xff\xff\xfe", decoded_value=2147483647),
        Sample[int](encoded_value=b"\xff\xff\xff\xff", decoded_value=-2147483648),
    ],
    "VARLONG": [
        Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x00", decoded_value=0),
        Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x01", decoded_value=-1),
        Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x02", decoded_value=1),
        Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x03", decoded_value=-2),
        Sample[int](encoded_value=b"\xff\xff\xff\xff\xff\xff\xff\xfe", decoded_value=9223372036854775807),
        Sample[int](encoded_value=b"\xff\xff\xff\xff\xff\xff\xff\xff", decoded_value=-9223372036854775808),
    ],
    "STRING": [
        Sample[str](encoded_value=b"\x00\x0812345678", decoded_value="12345678"),
        Sample[str](encoded_value=b"\x00\x00", decoded_value=""),
    ],
    "NULLABLE_STRING": [
        Sample[Optional[str]](encoded_value=b"\x00\x0812345678", decoded_value="12345678"),
        Sample[Optional[str]](encoded_value=b"\x00\x00", decoded_value=""),
        Sample[Optional[str]](encoded_value=b"\xff\xff", decoded_value=None),
    ],
    "BYTES": [
        Sample[bytes](encoded_value=b"\x00\x00\x00\x041234", decoded_value=b"1234"),
        Sample[bytes](encoded_value=b"\x00\x00\x00\x00", decoded_value=b""),
    ],
    "NULLABLE_BYTES": [
        Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x041234", decoded_value=b"1234"),
        Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x00", decoded_value=b""),
        Sample[Optional[bytes]](encoded_value=b"\xff\xff\xff\xff", decoded_value=None),
    ],
    "RECORDS": [
        # TODO we're pretending this is the same as NULLABLE_BYTES, don't know if that's true though...
        Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x041234", decoded_value=b"1234"),
        Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x00", decoded_value=b""),
        Sample[Optional[bytes]](encoded_value=b"\xff\xff\xff\xff", decoded_value=None),
    ],
}

# Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true
# respectively. When reading a boolean value, any non-zero value is considered true.
class TestBoolean:
    @pytest.fixture()
    def sample(self):
        yield Sample[bool](encoded_value=b"\00", decoded_value=False)
        yield Sample[bool](encoded_value=b"\01", decoded_value=True)

    def test_encode_boolean(self, sample) -> None:
        actual_encoded_value = structs.encode_boolean(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_boolean(self, sample) -> None:
        actual_decoded_value = structs.decode_boolean(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_boolean(self, sample) -> None:
        recreated_original_value = structs.decode_boolean(structs.encode_boolean(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**7 and 2**7-1 inclusive.
class TestInt8:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x7f", decoded_value=127)
        yield Sample[int](encoded_value=b"\x80", decoded_value=-128)

    def test_encode_int8(self, sample) -> None:
        actual_encoded_value = structs.encode_int8(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_int8(self, sample) -> None:
        actual_decoded_value = structs.decode_int8(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_int8(self, sample) -> None:
        recreated_original_value = structs.decode_int8(structs.encode_int8(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**15 and 2**15-1 inclusive. The values are encoded using two bytes
# in network byte order (big-endian).
class TestInt16:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x7f\xff", decoded_value=32767)
        yield Sample[int](encoded_value=b"\x80\x00", decoded_value=-32768)

    def test_encode_int16(self, sample) -> None:
        actual_encoded_value = structs.encode_int16(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_int16(self, sample) -> None:
        actual_decoded_value = structs.decode_int16(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_int16(self, sample) -> None:
        recreated_original_value = structs.decode_int16(structs.encode_int16(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**31 and 2**31-1 inclusive. The values are encoded using four bytes
# in network byte order (big-endian).
class TestInt32:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x7f\xff\xff\xff", decoded_value=2147483647)
        yield Sample[int](encoded_value=b"\x80\x00\x00\x00", decoded_value=-2147483648)

    def test_encode_int32(self, sample) -> None:
        actual_encoded_value = structs.encode_int32(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_int32(self, sample) -> None:
        actual_decoded_value = structs.decode_int32(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_int32(self, sample) -> None:
        recreated_original_value = structs.decode_int32(structs.encode_int32(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**63 and 2**63-1 inclusive. The values are encoded using eight bytes
# in network byte order (big-endian).
class TestInt64:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x7f\xff\xff\xff\xff\xff\xff\xff", decoded_value=9223372036854775807)
        yield Sample[int](encoded_value=b"\x80\x00\x00\x00\x00\x00\x00\x00", decoded_value=-9223372036854775808)

    def test_encode_int64(self, sample) -> None:
        actual_encoded_value = structs.encode_int64(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_int64(self, sample) -> None:
        actual_decoded_value = structs.decode_int64(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_int64(self, sample) -> None:
        recreated_original_value = structs.decode_int64(structs.encode_int64(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between 0 and 2**32-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).
class TestUint32:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x7f\xff\xff\xff", decoded_value=2147483647)
        yield Sample[int](encoded_value=b"\x80\x00\x00\x00", decoded_value=2147483648)

    def test_encode_uint32(self, sample) -> None:
        actual_encoded_value = structs.encode_uint32(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_uint32(self, sample) -> None:
        actual_decoded_value = structs.decode_uint32(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_uint32(self, sample) -> None:
        recreated_original_value = structs.decode_uint32(structs.encode_uint32(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**31 and 2**31-1 inclusive. Encoding follows the variable-length
# zig-zag encoding from Google Protocol Buffers.
class TestVarint:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x00\x00\x00\x01", decoded_value=-1)
        yield Sample[int](encoded_value=b"\x00\x00\x00\x02", decoded_value=1)
        yield Sample[int](encoded_value=b"\x00\x00\x00\x03", decoded_value=-2)
        yield Sample[int](encoded_value=b"\xff\xff\xff\xfe", decoded_value=2147483647)
        yield Sample[int](encoded_value=b"\xff\xff\xff\xff", decoded_value=-2147483648)

    def test_encode_varint(self, sample) -> None:
        actual_encoded_value = structs.encode_varint(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_varint(self, sample) -> None:
        actual_decoded_value = structs.decode_varint(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_varint(self, sample) -> None:
        recreated_original_value = structs.decode_varint(structs.encode_varint(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**63 and 2**63-1 inclusive. Encoding follows the variable-length
# zig-zag encoding from Google Protocol Buffers.
class TestVarlong:
    @pytest.fixture()
    def sample(self):
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x00", decoded_value=0)
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x01", decoded_value=-1)
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x02", decoded_value=1)
        yield Sample[int](encoded_value=b"\x00\x00\x00\x00\x00\x00\x00\x03", decoded_value=-2)
        yield Sample[int](encoded_value=b"\xff\xff\xff\xff\xff\xff\xff\xfe", decoded_value=9223372036854775807)
        yield Sample[int](encoded_value=b"\xff\xff\xff\xff\xff\xff\xff\xff", decoded_value=-9223372036854775808)

    def test_encode_varlong(self, sample) -> None:
        actual_encoded_value = structs.encode_varlong(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_varlong(self, sample) -> None:
        actual_decoded_value = structs.decode_varlong(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_varlong(self, sample) -> None:
        recreated_original_value = structs.decode_varlong(structs.encode_varlong(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow
# which are the UTF-8 encoding of the character sequence. Length must not be negative.
class TestString:
    @pytest.fixture()
    def sample(self):
        yield Sample[str](encoded_value=b"\x00\x0812345678", decoded_value="12345678")
        yield Sample[str](encoded_value=b"\x00\x00", decoded_value="")

    def test_encode_string(self, sample) -> None:
        actual_encoded_value = structs.encode_string(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_string(self, sample) -> None:
        actual_decoded_value = structs.decode_string(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_string(self, sample) -> None:
        recreated_original_value = structs.decode_string(structs.encode_string(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an
# INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is
# encoded with length of -1 and there are no following bytes.
class TestNullableString:
    @pytest.fixture()
    def sample(self):
        yield Sample[Optional[str]](encoded_value=b"\x00\x0812345678", decoded_value="12345678")
        yield Sample[Optional[str]](encoded_value=b"\x00\x00", decoded_value="")
        yield Sample[Optional[str]](encoded_value=b"\xff\xff", decoded_value=None)

    def test_encode_nullable_string(self, sample) -> None:
        actual_encoded_value = structs.encode_nullable_string(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_nullable_string(self, sample) -> None:
        actual_decoded_value = structs.decode_nullable_string(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_nullable_string(self, sample) -> None:
        recreated_original_value = structs.decode_nullable_string(structs.encode_nullable_string(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
class TestBytes:
    @pytest.fixture()
    def sample(self):
        yield Sample[bytes](encoded_value=b"\x00\x00\x00\x041234", decoded_value=b"1234")
        yield Sample[bytes](encoded_value=b"\x00\x00\x00\x00", decoded_value=b"")

    def test_encode_bytes(self, sample) -> None:
        actual_encoded_value = structs.encode_bytes(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_bytes(self, sample) -> None:
        actual_decoded_value = structs.decode_bytes(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_bytes(self, sample) -> None:
        recreated_original_value = structs.decode_bytes(structs.encode_bytes(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an
# INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following
# bytes.
class TestNullableBytes:
    @pytest.fixture()
    def sample(self):
        yield Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x041234", decoded_value=b"1234")
        yield Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x00", decoded_value=b"")
        yield Sample[Optional[bytes]](encoded_value=b"\xff\xff\xff\xff", decoded_value=None)

    def test_encode_nullable_bytes(self, sample) -> None:
        actual_encoded_value = structs.encode_nullable_bytes(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_nullable_bytes(self, sample) -> None:
        actual_decoded_value = structs.decode_nullable_bytes(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_nullable_bytes(self, sample) -> None:
        recreated_original_value = structs.decode_nullable_bytes(structs.encode_nullable_bytes(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see
# Message Sets.
class TestRecords:
    @pytest.fixture()
    def sample(self):
        # TODO we're pretending this is the same as NULLABLE_BYTES, don't know if that's true though...
        yield Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x041234", decoded_value=b"1234")
        yield Sample[Optional[bytes]](encoded_value=b"\x00\x00\x00\x00", decoded_value=b"")
        yield Sample[Optional[bytes]](encoded_value=b"\xff\xff\xff\xff", decoded_value=None)

    def test_encode_records(self, sample) -> None:
        actual_encoded_value = structs.encode_records(sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_records(self, sample) -> None:
        actual_decoded_value = structs.decode_records(sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_records(self, sample) -> None:
        recreated_original_value = structs.decode_records(structs.encode_records(sample.decoded_value))

        assert sample.decoded_value == recreated_original_value


# Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g.
# STRING) or a structure. First, the length N is given as an INT32. Then N instances of type T follow.
# A null array is represented with a length of -1. In protocol documentation an array of T instances
# is referred to as [T].
class TestArray:
    @pytest.fixture()
    def sample(self):
        yield ArraySample(type_=..., encoded_value=..., decoded_value=...)

    def test_encode_array(self, sample) -> None:
        actual_encoded_value = structs.encode_array(sample.type_, sample.decoded_value)

        assert actual_encoded_value == sample.encoded_value

    def test_decode_array(self, sample) -> None:
        actual_decoded_value = structs.decode_array(sample.type_, sample.encoded_value)

        assert actual_decoded_value == sample.decoded_value

    def test_serde_array(self, sample) -> None:
        recreated_original_value = structs.decode_array(
            sample.type_, structs.encode_array(sample.type_, sample.decoded_value)
        )

        assert sample.decoded_value == recreated_original_value
