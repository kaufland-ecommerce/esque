import pickle
from typing import BinaryIO

from confluent_kafka.cimpl import Message


class DecodedMessage:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


class Serializer:

    def serialize(self, message: Message, file: BinaryIO):
        pass


class JsonSerializer:

    def serialize(self, message: Message, file: BinaryIO):
        decoded_message = decode_message(message)
        serializable_message = {"key": decoded_message.key, "value": decoded_message.value}
        pickle.dump(serializable_message, file)


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value)
