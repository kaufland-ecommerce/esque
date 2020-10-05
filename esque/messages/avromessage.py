import base64
import itertools as it
import json
import pathlib
import pickle
import struct
from datetime import date, datetime
from io import BytesIO
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Tuple

import click
import fastavro
from avro.schema import RecordSchema
from confluent_kafka.avro import loads as load_schema
from confluent_kafka.cimpl import Message

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.messages.message import FileReader, FileWriter, KafkaMessage, MessageHeader, StdOutWriter

KEY_SCHEMA_FOLDER_NAME = "key_schema"
VALUE_SCHEMA_FOLDER_NAME = "value_schema"


class DecodedAvroMessage(NamedTuple):
    key: Any
    value: Any
    partition: int
    key_schema_id: int
    value_schema_id: int
    headers: List[MessageHeader] = []


class AvroFileWriter(FileWriter):
    def __init__(
        self, schema_registry_client: SchemaRegistryClient, directory: pathlib.Path, partition: Optional[str] = None
    ):
        super().__init__(directory, partition)
        self.directory = directory
        self.schema_registry_client = schema_registry_client
        self.current_key_schema_id = None
        self.current_value_schema_id = None
        self.schema_dir_name = None
        self.schema_version = it.count(1)
        self.open_mode = "wb+"
        self.avro_decoder = AvroMessageDecoder(self.schema_registry_client)

    def write_message(self, message: Message):
        (
            key_schema_id,
            value_schema_id,
            decoded_message,
            serializable_message,
        ) = self.avro_decoder.decode_message_from_avro(message)

        self._dump_schema(value_schema_id, VALUE_SCHEMA_FOLDER_NAME)
        self._dump_schema(key_schema_id, KEY_SCHEMA_FOLDER_NAME)
        pickle.dump(serializable_message, self.file)

    def _dump_schema(self, schema_id: int, schema_folder_name: str):
        if schema_id == -1:
            return
        schema_file_path = get_schema_file_path(self.directory, schema_id, schema_folder_name)
        folder_path = schema_file_path.parent
        if not folder_path.exists():
            folder_path.mkdir(exist_ok=True)
        schema_file_path.write_text(
            json.dumps(self.schema_registry_client.get_schema_from_id(schema_id).original_schema), encoding="utf-8"
        )


class StdOutAvroWriter(StdOutWriter):
    def __init__(self, schema_registry_client):
        self.avro_decoder = AvroMessageDecoder(schema_registry_client=schema_registry_client)

    def write_message(self, message: Message):
        _, _, _, plaintext_message = self.avro_decoder.decode_message_from_avro(message)
        click.echo(json.dumps(plaintext_message, default=StdOutAvroWriter.deserializer))

    @staticmethod
    def deserializer(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, bytes):
            return base64.b64encode(value).decode("ascii")


class AvroFileReader(FileReader):
    def __init__(self, directory: pathlib.Path, partition: Optional[str] = None):
        super().__init__(directory, partition)
        self.open_mode = "rb"

    def read_message_from_file(self) -> Iterable[KafkaMessage]:
        while True:
            try:
                record = pickle.load(self.file)
            except EOFError:
                return

            key_schema, value_schema = get_schemata(self.directory, record["key_schema_id"], record["value_schema_id"])
            yield KafkaMessage(
                json.dumps(record["key"]) if record["key"] is not None else None,
                json.dumps(record["value"]) if record["value"] is not None else None,
                record["partition"],
                key_schema,
                value_schema,
            )


class AvroMessageDecoder:
    def __init__(self, schema_registry_client: SchemaRegistryClient):
        self.schema_registry_client = schema_registry_client

    def decode_bytes(self, raw_data: Optional[bytes]) -> Tuple[int, Optional[Dict]]:
        if raw_data is None:
            return -1, None

        with BytesIO(raw_data) as fake_stream:
            schema_id = extract_schema_id(fake_stream.read(5))
            parsed_schema = self.schema_registry_client.get_schema_from_id(schema_id).parsed_schema
            record = fastavro.schemaless_reader(fake_stream, parsed_schema)
        return schema_id, record

    def decode_message_from_avro(self, message: Message) -> Tuple[int, int, DecodedAvroMessage, Dict]:
        key_schema_id, decoded_key = self.decode_bytes(message.key())
        value_schema_id, decoded_value = self.decode_bytes(message.value())

        headers = []
        if message.headers():
            for header_key, header_value in message.headers():
                headers.append(
                    MessageHeader(key=header_key, value=header_value.decode("utf-8") if header_value else None)
                )
        decoded_message = DecodedAvroMessage(
            decoded_key, decoded_value, message.partition(), key_schema_id, value_schema_id, headers=headers
        )

        serializable_message = {
            "key": decoded_message.key,
            "value": decoded_message.value,
            "partition": decoded_message.partition,
            "key_schema_id": key_schema_id if key_schema_id != -1 else None,
            "value_schema_id": value_schema_id if value_schema_id != -1 else None,
        }

        return key_schema_id, value_schema_id, decoded_message, serializable_message


def extract_schema_id(message: bytes) -> int:
    magic_byte, schema_id = struct.unpack(">bI", message[:5])
    assert magic_byte == 0, f"Wrong magic byte ({magic_byte}), no AVRO message."
    return schema_id


def get_schemata(
    base_directory: pathlib.Path, key_schema_id: Optional[int], value_schema_id: Optional[int]
) -> Tuple[Optional[RecordSchema], Optional[RecordSchema]]:
    return (
        (None if key_schema_id is None else get_schema(base_directory, key_schema_id, KEY_SCHEMA_FOLDER_NAME)),
        (None if value_schema_id is None else get_schema(base_directory, value_schema_id, VALUE_SCHEMA_FOLDER_NAME)),
    )


def get_schema_file_path(base_directory: pathlib.Path, schema_id: int, schema_folder_name: str) -> pathlib.Path:
    return pathlib.Path(base_directory / schema_folder_name / ("schema_id_" + str(schema_id) + ".avsc"))


def get_schema(base_directory: pathlib.Path, schema_id: int, schema_folder_name: str) -> RecordSchema:
    schema_file_path = get_schema_file_path(base_directory, schema_id, schema_folder_name)
    return load_schema(schema_file_path.read_text(encoding="utf-8"))
