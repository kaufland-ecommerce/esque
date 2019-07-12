import pathlib
import pickle
from typing import BinaryIO, Iterable, IO

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
    def __init__(self, file_path: pathlib.Path):
        self.working_dir = file_path

    def write_message_to_file(self, message: Message):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FileReader(object):
    def __init__(self, file_path: pathlib.Path):
        self.working_dir = file_path

    def read_from_file(self, file: BinaryIO) -> Iterable[KafkaMessage]:
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class PlainTextFileWriter(FileWriter):
    def write_message_to_file(self, message: Message):
        decoded_message = decode_message(message)
        serializable_message = {"key": decoded_message.key, "value": decoded_message.value}
        print(serializable_message)
        pickle.dump(serializable_message, self.file)

    def __enter__(self):
        self.file = (self.working_dir / "data").open("w+")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


class PlainTextFileReader(FileReader):
    def read_from_file(self, file: BinaryIO) -> Iterable[KafkaMessage]:
        while True:
            try:
                record = pickle.load(file)
            except EOFError:
                return

            yield KafkaMessage(record["key"], record["value"])

    def __enter__(self):
        self.file = (self.working_dir / "data").open("r")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value)
