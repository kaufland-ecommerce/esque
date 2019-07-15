import json
import pathlib
from typing import Iterable, NamedTuple

from confluent_kafka.cimpl import Message


class DecodedMessage(NamedTuple):
    key: str
    value: str


class KafkaMessage(NamedTuple):
    key: str
    value: str
    key_schema: str = None
    value_schema: str = None


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
        self.file.write(json.dumps(serializable_message) + "\n")


class PlainTextFileReader(FileReader):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "r"

    def read_from_file(self) -> Iterable[KafkaMessage]:
        for line in self.file:
            try:
                record = json.loads(line)
            except EOFError:
                return

            yield KafkaMessage(record["key"], record["value"])


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value)
