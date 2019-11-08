import json
import pathlib
from abc import abstractmethod
from typing import Any, Iterable, List, NamedTuple, Optional

import click
from avro.schema import RecordSchema
from confluent_kafka.cimpl import Message


class MessageHeader(NamedTuple):
    key: str
    value: Optional[str]


class DecodedMessage(NamedTuple):
    key: str
    value: str
    partition: int
    offset: int
    timestamp: str
    headers: List[MessageHeader] = []


class KafkaMessage(NamedTuple):
    key: Any
    value: Any
    partition: int
    key_schema: RecordSchema = None
    value_schema: RecordSchema = None
    headers: List[MessageHeader] = []


class GenericWriter:
    @abstractmethod
    def write_message(self, message: Message):
        pass


class FileHandler:
    def __init__(self, directory: pathlib.Path):
        self.directory = directory
        self.file_name = "data"
        self.open_mode = "w+"
        self.file = None

    def __enter__(self):
        self.init_destination_directory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def init_destination_directory(self):
        if not self.directory.exists() and "w" in self.open_mode:
            self.directory.mkdir()
        self.file = (self.directory / self.file_name).open(self.open_mode)


class StdOutWriter(GenericWriter):
    def write_message(self, message: Message):
        click.echo(serialize_message(message))


class FileWriter(GenericWriter, FileHandler):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "w+"

    def write_message(self, message: Message):
        pass


class FileReader(FileHandler):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "r"

    def read_message_from_file(self) -> Iterable[KafkaMessage]:
        pass


class PlainTextFileWriter(FileWriter):
    def write_message(self, message: Message):
        self.file.write(serialize_message(message) + "\n")


class PlainTextFileReader(FileReader):
    def read_message_from_file(self) -> Iterable[KafkaMessage]:
        for line in self.file:
            yield deserialize_message(line)


def decode_message(message: Message) -> DecodedMessage:
    if message.key() is None:
        decoded_key = None
    else:
        decoded_key = message.key().decode("utf-8")
    decoded_value = message.value().decode("utf-8")
    headers = []
    if message.headers():
        for header_key, header_value in message.headers():
            headers.append(MessageHeader(key=header_key, value=header_value.decode("utf-8") if header_value else None))
    return DecodedMessage(
        key=decoded_key,
        value=decoded_value,
        partition=message.partition(),
        offset=message.offset(),
        timestamp=str(message.timestamp()),
        headers=headers,
    )


def serialize_message(message: Message):
    decoded_message = decode_message(message)
    serializable_message = {
        "key": decoded_message.key,
        "value": decoded_message.value,
        "partition": decoded_message.partition,
        "offset": decoded_message.offset,
        "headers": [
            {"key": header_element.key, "value": header_element.value} for header_element in decoded_message.headers
        ],
    }
    return json.dumps(serializable_message)


def deserialize_message(message_line: str) -> KafkaMessage:
    json_record = json.loads(message_line)
    key = None if "key" not in json_record or not json_record["key"] else json_record["key"]
    value = json_record["value"]
    partition = json_record.get("partition", 0)
    key_schema = json_record["key_schema"] if "key_schema" in json_record else None
    value_schema = json_record["value_schema"] if "value_schema" in json_record else None
    headers = []
    if json_record["headers"]:
        for header_item in json_record["headers"]:
            header_key = header_item["key"]
            header_value = header_item["value"] if "value" in header_item else None
            if header_key:
                headers.append(MessageHeader(header_key, header_value if header_value else None))
    return KafkaMessage(
        key=key, value=value, partition=partition, key_schema=key_schema, value_schema=value_schema, headers=headers
    )
