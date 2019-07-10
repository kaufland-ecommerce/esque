import pickle
from typing import BinaryIO, Iterable

from confluent_kafka.cimpl import Message


class DecodedMessage:
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


class KafkaMessage:
    def __init__(self, key: str, value: str, key_schema=None, value_schema=None):
        self.key = key
        self.value = value
        self.key_schema = key_schema
        self.value_schema = value_schema


class FileWriter(object):
    def write_message_to_file(self, message: Message, file: BinaryIO):
        pass


class FileReader(object):
    def read_from_file(self, file: BinaryIO) -> Iterable[KafkaMessage]:
        pass


class PlainTextFileWriter(FileWriter):
    def write_message_to_file(self, message: Message, file: BinaryIO):
        decoded_message = decode_message(message)
        serializable_message = {"key": decoded_message.key, "value": decoded_message.value}
        pickle.dump(serializable_message, file)


class PlainTextFileReader(FileReader):
    def read_from_file(self, file: BinaryIO) -> Iterable[KafkaMessage]:
        while True:
            try:
                record = pickle.load(file)
            except EOFError:
                return

            yield KafkaMessage(record["key"], record["value"])


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value)
