from confluent_kafka.cimpl import Message


class DecodedMessage:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value)


