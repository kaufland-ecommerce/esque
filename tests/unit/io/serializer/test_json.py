from esque.io.messages import BinaryMessage, Message


def test_json_serializer(json_serialized_message: BinaryMessage,
                         deserialized_message: Message):
    serializer: JsonSerializer = JsonSerializer()
    actual_message: BinaryMessage = serializer.serialize(deserialized_message)
    assert actual_message == json_serialized_message
