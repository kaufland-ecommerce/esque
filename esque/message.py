import pathlib
import pickle
from typing import Iterable

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


class IOHandler:
    def __init__(self, directory: pathlib.Path):
        self.directory = directory
        self.file_name = "data"
        self.open_mode = "w+"

    def __enter__(self):
        self.file = (self.directory / self.file_name).open(self.open_mode)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


class FileWriter(IOHandler):
    def write_message_to_file(self, message: Message):
        pass


class FileReader(IOHandler):
    def read_from_file(self) -> Iterable[KafkaMessage]:
        pass


class PlainTextFileWriter(FileWriter):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "w+"

    def write_message_to_file(self, message: Message):
        decoded_message = decode_message(message)
        serializable_message = {"key": decoded_message.key, "value": decoded_message.value}
        pickle.dump(serializable_message, self.file)


class PlainTextFileReader(FileReader):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "r"

    def read_from_file(self) -> Iterable[KafkaMessage]:
        while True:
            try:
                record = pickle.load(self.file)
            except EOFError:
                return

            yield KafkaMessage(record["key"], record["value"])


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value)
