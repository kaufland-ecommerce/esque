from typing import List

from esque.io.messages import BinaryMessage, Message
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer


def test_message_serializer():
    string_serializer: StringSerializer = StringSerializer()
    serializer: MessageSerializer = MessageSerializer(key_serializer=string_serializer)
    raw_message: BinaryMessage = BinaryMessage(key=b"key", value=b"value", offset=5, partition=3)
    expected_deserialized_message: Message = Message(key="key", value="value", offset=5, partition=3)
    deserialized_message: Message = serializer.deserialize(raw_message)
    assert deserialized_message == expected_deserialized_message
    serializer_message: BinaryMessage = serializer.serialize(deserialized_message)
    assert serializer_message == raw_message


def test_message_serializer_many():
    string_serializer: StringSerializer = StringSerializer()
    serializer: MessageSerializer = MessageSerializer(key_serializer=string_serializer)
    raw_messages: List[BinaryMessage] = [
        BinaryMessage(key=b"key", value=b"value", offset=5, partition=3),
        BinaryMessage(key=b"key2", value=b"value2", offset=6, partition=4),
    ]
    expected_deserialized_messages: List[Message] = [
        Message(key="key", value="value", offset=5, partition=3),
        Message(key="key2", value="value2", offset=6, partition=4),
    ]
    deserialized_messages: List[Message] = list(serializer.deserialize_many(raw_messages))
    assert deserialized_messages == expected_deserialized_messages
    serializer_messages: List[BinaryMessage] = list(serializer.serialize_many(deserialized_messages))
    assert serializer_messages == raw_messages
