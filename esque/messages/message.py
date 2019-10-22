import json
import pathlib
from typing import Any, Iterable, NamedTuple

from avro.schema import RecordSchema
from confluent_kafka.cimpl import Message

from esque.errors import translate_third_party_exceptions


class DecodedMessage(NamedTuple):
    key: str
    value: str
    partition: int
    offset: int
    timestamp: str


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

    def read_message_from_file(self) -> Iterable[KafkaMessage]:
        pass


class PlainTextFileWriter(FileWriter):
    @translate_third_party_exceptions
    def write_message_to_file(self, message: Message):
        self.file.write(serialize_message(message) + "\n")


class PlainTextFileReader(FileReader):
    @translate_third_party_exceptions
    def read_message_from_file(self) -> Iterable[KafkaMessage]:
        for line in self.file:
            yield deserialize_message(line)


def decode_message(message: Message) -> DecodedMessage:
    if message.key() is None:
        decoded_key = None
    else:
        decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")

    return DecodedMessage(decoded_key, decoded_value, message.partition(), message.offset(), str(message.timestamp()))


def serialize_message(message: Message):
    decoded_message = decode_message(message)
    serializable_message = {
        "key": decoded_message.key,
        "value": decoded_message.value,
        "partition": decoded_message.partition,
    }
    return json.dumps(serializable_message)


def deserialize_message(message_line: str) -> KafkaMessage:
    json_record = json.loads(message_line)
    key = None if "key" not in json_record or not json_record["key"] else json_record["key"]
    value = json_record["value"]
    partition = json_record.get("partition", 0)
    key_schema = json_record["key_schema"] if "key_schema" in json_record else None
    value_schema = json_record["value_schema"] if "value_schema" in json_record else None
    return KafkaMessage(key=key, value=value, partition=partition, key_schema=key_schema, value_schema=value_schema)
