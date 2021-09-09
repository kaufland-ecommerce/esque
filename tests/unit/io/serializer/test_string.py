from typing import List

import pytest

from esque.io.messages import Data
from esque.io.serializers.string import StringSerializer, StringSerializerConfig


@pytest.fixture(params=["latin1", "utf-8"])
def serializer_config(request) -> StringSerializerConfig:
    return StringSerializerConfig("str", encoding=request.param)


def test_string_serializer(serializer_config):
    string_serializer: StringSerializer = StringSerializer(config=serializer_config)
    raw_message: bytes = "Übung".encode(encoding=serializer_config.encoding)
    expected_deserialized_message: Data = Data(payload="Übung", data_type=string_serializer.data_type)
    deserialized_message: Data = string_serializer.deserialize(raw_message)
    assert deserialized_message == expected_deserialized_message
    serializer_message: bytes = string_serializer.serialize(deserialized_message)
    assert serializer_message == raw_message


def test_string_serializer_many(serializer_config):
    string_serializer: StringSerializer = StringSerializer(config=serializer_config)
    raw_messages: List[bytes] = [
        "Änderung".encode(encoding=serializer_config.encoding),
        "Übung".encode(encoding=serializer_config.encoding),
    ]
    expected_deserialized_messages: List[Data] = [
        Data(payload="Änderung", data_type=string_serializer.data_type),
        Data(payload="Übung", data_type=string_serializer.data_type),
    ]
    deserialized_messages: List[Data] = list(string_serializer.deserialize_many(raw_messages))
    assert deserialized_messages == expected_deserialized_messages
    serializer_messages: List[bytes] = list(string_serializer.serialize_many(deserialized_messages))
    assert serializer_messages == raw_messages
