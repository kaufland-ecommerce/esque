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
@pytest.mark.parametrize("sample", SAMPLES["BOOLEAN"])
def test_encode_boolean(sample: Sample[bool]) -> None:
    actual_encoded_value = structs.encode_boolean(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["BOOLEAN"])
def test_decode_boolean(sample: Sample[bool]) -> None:
    actual_decoded_value = structs.decode_boolean(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["BOOLEAN"])
def test_serde_boolean(sample: Sample[bool]) -> None:

    recreated_original_value = structs.decode_boolean(structs.encode_boolean(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**7 and 2**7-1 inclusive.
@pytest.mark.parametrize("sample", SAMPLES["INT8"])
def test_encode_int8(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_int8(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT8"])
def test_decode_int8(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_int8(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT8"])
def test_serde_int8(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_int8(structs.encode_int8(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**15 and 2**15-1 inclusive. The values are encoded using two bytes
# in network byte order (big-endian).
@pytest.mark.parametrize("sample", SAMPLES["INT16"])
def test_encode_int16(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_int16(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT16"])
def test_decode_int16(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_int16(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT16"])
def test_serde_int16(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_int16(structs.encode_int16(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**31 and 2**31-1 inclusive. The values are encoded using four bytes
# in network byte order (big-endian).
@pytest.mark.parametrize("sample", SAMPLES["INT32"])
def test_encode_int32(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_int32(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT32"])
def test_decode_int32(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_int32(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT32"])
def test_serde_int32(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_int32(structs.encode_int32(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**63 and 2**63-1 inclusive. The values are encoded using eight bytes
# in network byte order (big-endian).
@pytest.mark.parametrize("sample", SAMPLES["INT64"])
def test_encode_int64(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_int64(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT64"])
def test_decode_int64(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_int64(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["INT64"])
def test_serde_int64(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_int64(structs.encode_int64(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between 0 and 2**32-1 inclusive. The values are encoded using four bytes in
# network byte order (big-endian).
@pytest.mark.parametrize("sample", SAMPLES["UINT32"])
def test_encode_uint32(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_uint32(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["UINT32"])
def test_decode_uint32(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_uint32(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["UINT32"])
def test_serde_uint32(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_uint32(structs.encode_uint32(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**31 and 2**31-1 inclusive. Encoding follows the variable-length
# zig-zag encoding from Google Protocol Buffers.
@pytest.mark.parametrize("sample", SAMPLES["VARINT"])
def test_encode_varint(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_varint(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["VARINT"])
def test_decode_varint(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_varint(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["VARINT"])
def test_serde_varint(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_varint(structs.encode_varint(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents an integer between -2**63 and 2**63-1 inclusive. Encoding follows the variable-length
# zig-zag encoding from Google Protocol Buffers.
@pytest.mark.parametrize("sample", SAMPLES["VARLONG"])
def test_encode_varlong(sample: Sample[int]) -> None:
    actual_encoded_value = structs.encode_varlong(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["VARLONG"])
def test_decode_varlong(sample: Sample[int]) -> None:
    actual_decoded_value = structs.decode_varlong(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["VARLONG"])
def test_serde_varlong(sample: Sample[int]) -> None:

    recreated_original_value = structs.decode_varlong(structs.encode_varlong(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow
# which are the UTF-8 encoding of the character sequence. Length must not be negative.
@pytest.mark.parametrize("sample", SAMPLES["STRING"])
def test_encode_string(sample: Sample[str]) -> None:
    actual_encoded_value = structs.encode_string(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["STRING"])
def test_decode_string(sample: Sample[str]) -> None:
    actual_decoded_value = structs.decode_string(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["STRING"])
def test_serde_string(sample: Sample[str]) -> None:

    recreated_original_value = structs.decode_string(structs.encode_string(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an
# INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. A null value is
# encoded with length of -1 and there are no following bytes.
@pytest.mark.parametrize("sample", SAMPLES["NULLABLE_STRING"])
def test_encode_nullable_string(sample: Sample[Optional[str]]) -> None:
    actual_encoded_value = structs.encode_nullable_string(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["NULLABLE_STRING"])
def test_decode_nullable_string(sample: Sample[Optional[str]]) -> None:
    actual_decoded_value = structs.decode_nullable_string(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["NULLABLE_STRING"])
def test_serde_nullable_string(sample: Sample[Optional[str]]) -> None:

    recreated_original_value = structs.decode_nullable_string(structs.encode_nullable_string(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
@pytest.mark.parametrize("sample", SAMPLES["BYTES"])
def test_encode_bytes(sample: Sample[bytes]) -> None:
    actual_encoded_value = structs.encode_bytes(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["BYTES"])
def test_decode_bytes(sample: Sample[bytes]) -> None:
    actual_decoded_value = structs.decode_bytes(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["BYTES"])
def test_serde_bytes(sample: Sample[bytes]) -> None:

    recreated_original_value = structs.decode_bytes(structs.encode_bytes(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an
# INT32. Then N bytes follow. A null value is encoded with length of -1 and there are no following
# bytes.
@pytest.mark.parametrize("sample", SAMPLES["NULLABLE_BYTES"])
def test_encode_nullable_bytes(sample: Sample[Optional[bytes]]) -> None:
    actual_encoded_value = structs.encode_nullable_bytes(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["NULLABLE_BYTES"])
def test_decode_nullable_bytes(sample: Sample[Optional[bytes]]) -> None:
    actual_decoded_value = structs.decode_nullable_bytes(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["NULLABLE_BYTES"])
def test_serde_nullable_bytes(sample: Sample[Optional[bytes]]) -> None:

    recreated_original_value = structs.decode_nullable_bytes(structs.encode_nullable_bytes(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


# Represents a sequence of Kafka records as NULLABLE_BYTES. For a detailed description of records see
# Message Sets.
@pytest.mark.parametrize("sample", SAMPLES["RECORDS"])
def test_encode_records(sample: Sample[Optional[bytes]]) -> None:
    actual_encoded_value = structs.encode_records(sample.decoded_value)

    assert actual_encoded_value == sample.encoded_value


@pytest.mark.parametrize("sample", SAMPLES["RECORDS"])
def test_decode_records(sample: Sample[Optional[bytes]]) -> None:
    actual_decoded_value = structs.decode_records(sample.encoded_value)

    assert actual_decoded_value == sample.decoded_value


@pytest.mark.parametrize("sample", SAMPLES["RECORDS"])
def test_serde_records(sample: Sample[Optional[bytes]]) -> None:

    recreated_original_value = structs.decode_records(structs.encode_records(sample.decoded_value))

    assert sample.decoded_value == recreated_original_value


@pytest.mark.parametrize(["type_", "samples"], SAMPLES.items())
def test_serde_array(type_: str, samples: List[Sample]) -> None:
    original_value = [sample.decoded_value for sample in samples]
    recreated_original_value = structs.decode_array(type_, structs.encode_array(type_, original_value))

    assert original_value == recreated_original_value
