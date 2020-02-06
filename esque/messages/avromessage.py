import itertools as it
import json
import pathlib
import pickle
import struct
from io import BytesIO
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Tuple

import click
import fastavro
from confluent_kafka.avro import loads as load_schema
from confluent_kafka.cimpl import Message

from esque.clients.schemaregistry import SchemaRegistryClient
from esque.messages.message import FileReader, FileWriter, KafkaMessage, MessageHeader, StdOutWriter


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
        key_schema_id, value_schema_id, decoded_message, serializable_message = self.avro_decoder.decode_message_from_avro(
            message
        )

        if value_schema_id != -1:
            value_schema_dir_name = f"value_schema_{value_schema_id}"
            self._dump_value_schema(value_schema_id, value_schema_dir_name)

        key_schema_dir_name = f"key_schema_{key_schema_id}"
        self._dump_key_schema(key_schema_id, key_schema_dir_name)

        pickle.dump(serializable_message, self.file)

    def _dump_key_schema(self, key_schema_id, schema_dir_name: str):
        directory = self.directory / schema_dir_name
        if directory.exists():
            return
        directory.mkdir()
        (directory / "key_schema.avsc").write_text(
            json.dumps(self.schema_registry_client.get_schema_from_id(key_schema_id).original_schema), encoding="utf-8"
        )

    def _dump_value_schema(self, value_schema_id, schema_dir_name: str):
        directory = self.directory / schema_dir_name
        if directory.exists():
            return
        directory.mkdir()
        (directory / "value_schema.avsc").write_text(
            json.dumps(self.schema_registry_client.get_schema_from_id(value_schema_id).original_schema),
            encoding="utf-8",
        )


class StdOutAvroWriter(StdOutWriter):
    def __init__(self, schema_registry_client):
        self.avro_decoder = AvroMessageDecoder(schema_registry_client=schema_registry_client)

    def write_message(self, message: Message):
        _, _, _, plaintext_message = self.avro_decoder.decode_message_from_avro(message)
        click.echo(json.dumps(plaintext_message))


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
            key_schema_directory = self.directory / record["key_schema_directory_name"]
            key_schema = load_schema((key_schema_directory / "key_schema.avsc").read_text(encoding="utf-8"))

            value_schema = None
            if record["value_schema_directory_name"] is not None:
                value_schema_directory = self.directory / record["value_schema_directory_name"]
                value_schema = load_schema((value_schema_directory / "value_schema.avsc").read_text(encoding="utf-8"))

            yield KafkaMessage(
                json.dumps(record["key"]),
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

    def decode_message_from_avro(self, message: Message):
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
            "key_schema_directory_name": f"key_schema_{key_schema_id}",
            "value_schema_directory_name": f"value_schema_{value_schema_id}" if value_schema_id != -1 else None,
        }

        return key_schema_id, value_schema_id, decoded_message, serializable_message


def extract_schema_id(message: bytes) -> int:
    magic_byte, schema_id = struct.unpack(">bI", message[:5])
    assert magic_byte == 0, f"Wrong magic byte ({magic_byte}), no AVRO message."
    return schema_id
