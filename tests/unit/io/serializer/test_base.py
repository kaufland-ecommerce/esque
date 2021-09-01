from typing import List

from esque.io.messages import BinaryMessage, Message
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer


def test_message_serializer(
    binary_messages: List[BinaryMessage], string_messages: List[Message], string_serializer: StringSerializer
):
    serializer: MessageSerializer = MessageSerializer(key_serializer=string_serializer)
    deserialized_message: Message = serializer.deserialize(binary_messages[0])
    assert deserialized_message == string_messages[0]
    serialized_message: BinaryMessage = serializer.serialize(string_messages[0])
    assert serialized_message == binary_messages[0]


def test_message_serializer_many(
    binary_messages: List[BinaryMessage], string_messages: List[Message], string_serializer: StringSerializer
):
    serializer: MessageSerializer = MessageSerializer(key_serializer=string_serializer)
    deserialized_messages: List[Message] = list(serializer.deserialize_many(binary_messages))
    assert deserialized_messages == string_messages
    serialized_messages: List[BinaryMessage] = list(serializer.serialize_many(string_messages))
    assert serialized_messages == binary_messages
