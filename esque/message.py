import json
import pathlib
from typing import Iterable, NamedTuple, Any

from avro.schema import RecordSchema
from confluent_kafka.cimpl import Message


class DecodedMessage(NamedTuple):
    key: str
    value: str
    partition: int


class KafkaMessage(NamedTuple):
    key: Any
    value: Any
    partition: int
    key_schema: RecordSchema = None
    value_schema: RecordSchema = None


class IOHandler:
    def __init__(self, directory: pathlib.Path):
        self.directory = directory
        self.file_name = "data"
        self.open_mode = "w+"

    def __enter__(self):
        if not self.directory.exists():
            self.directory.mkdir()
        self.file = (self.directory / self.file_name).open(self.open_mode)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()


class FileWriter(IOHandler):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "w+"

    def write_message_to_file(self, message: Message):
        pass


class FileReader(IOHandler):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "r"

    def read_from_file(self) -> Iterable[KafkaMessage]:
        pass


class PlainTextFileWriter(FileWriter):
    def write_message_to_file(self, message: Message):
        decoded_message = decode_message(message)
        serializable_message = {
            "key": decoded_message.key,
            "value": decoded_message.value,
            "partition": decoded_message.partition,
        }
        self.file.write(json.dumps(serializable_message) + "\n")


class PlainTextFileReader(FileReader):
    def read_from_file(self) -> Iterable[KafkaMessage]:
        for line in self.file:
            try:
                record = json.loads(line)
            except EOFError:
                return

            yield KafkaMessage(record["key"], record["value"], record["partition"])


def decode_message(message: Message) -> DecodedMessage:
    decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value, message.partition())
