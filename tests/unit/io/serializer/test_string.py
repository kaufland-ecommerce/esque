from typing import List

from esque.io.serializers.string import StringSerializer


def test_string_latin1_serializer():
    string_serializer: StringSerializer = StringSerializer(encoding="latin1")
    raw_message: bytes = "Übung".encode(encoding="latin1")
    expected_deserialized_message: str = "Übung"
    deserialized_message: str = string_serializer.deserialize(raw_message)
    assert deserialized_message == expected_deserialized_message
    serializer_message: bytes = string_serializer.serialize(deserialized_message)
    assert serializer_message == raw_message


def test_string_latin1_serializer_many():
    string_serializer: StringSerializer = StringSerializer(encoding="latin1")
    raw_messages: List[bytes] = ["Änderung".encode(encoding="latin1"), "Übung".encode(encoding="latin1")]
    expected_deserialized_messages: List[str] = ["Änderung", "Übung"]
    deserialized_messages: List[str] = list(string_serializer.deserialize_many(raw_messages))
    assert deserialized_messages == expected_deserialized_messages
    serializer_messages: List[bytes] = list(string_serializer.serialize_many(deserialized_messages))
    assert serializer_messages == raw_messages
