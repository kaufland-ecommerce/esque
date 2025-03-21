import datetime
import struct

import pytest

from esque.io.data_types import UnknownDataType
from esque.io.messages import Data
from esque.io.serializers.struct import StructSerializer, StructSerializerConfig

CET = datetime.timezone(datetime.timedelta(seconds=3600), "CET")


def test_struct_serializer_with_valid_format():
    serializer = StructSerializer(
        StructSerializerConfig(scheme="struct", serializer_format="<I", deserializer_format="<I")
    )
    num = 123
    expected = struct.pack("<I", num)
    serialized_result = serializer.serialize(Data(num, UnknownDataType()))
    assert serialized_result == expected
    deserialized_result = serializer.deserialize(serialized_result)
    assert deserialized_result.payload == num


def test_struct_deserializer_with_invalid_format():
    serializer = StructSerializer(StructSerializerConfig(scheme="struct"))
    with pytest.raises(ValueError, match="serializer struct format cannot be None"):
        serializer.serialize(Data(payload=[], data_type=UnknownDataType()))
    with pytest.raises(ValueError, match="deserializer struct format cannot be None"):
        serializer.deserialize(b"1")
