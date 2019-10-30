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
    def __init__(self, directory: pathlib.Path, schema_registry_client: SchemaRegistryClient):
        super().__init__(directory)
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

        if self.schema_changed(decoded_message) or self.schema_dir_name is None:
            self.schema_dir_name = f"{next(self.schema_version):04}_{key_schema_id}_{value_schema_id}"
            self.current_key_schema_id = key_schema_id
            self.current_value_schema_id = value_schema_id
            self._dump_schemata(key_schema_id, value_schema_id)

        pickle.dump(serializable_message, self.file)

    def _dump_schemata(self, key_schema_id, value_schema_id):
        directory = self.directory / self.schema_dir_name
        directory.mkdir()
        (directory / "key_schema.avsc").write_text(
            json.dumps(self.schema_registry_client.get_schema_from_id(key_schema_id).original_schema), encoding="utf-8"
        )
        (directory / "value_schema.avsc").write_text(
            json.dumps(self.schema_registry_client.get_schema_from_id(value_schema_id).original_schema),
            encoding="utf-8",
        )

    def schema_changed(self, decoded_message: DecodedAvroMessage) -> bool:
        return (
            self.current_value_schema_id != decoded_message.value_schema_id and decoded_message.value is not None
        ) or self.current_key_schema_id != decoded_message.key_schema_id


class StdOutAvroWriter(StdOutWriter):
    def __init__(self, schema_registry_client):
        self.avro_decoder = AvroMessageDecoder(schema_registry_client=schema_registry_client)

    def write_message(self, message: Message):
        _, _, _, plaintext_message = self.avro_decoder.decode_message_from_avro(message)
        click.echo(json.dumps(plaintext_message))


class AvroFileReader(FileReader):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "rb"

    def read_message_from_file(self) -> Iterable[KafkaMessage]:
        while True:
            try:
                record = pickle.load(self.file)
            except EOFError:
                return
            schema_directory = self.directory / record["schema_directory_name"]

            key_schema = load_schema((schema_directory / "key_schema.avsc").read_text(encoding="utf-8"))
            value_schema = load_schema((schema_directory / "value_schema.avsc").read_text(encoding="utf-8"))

            yield KafkaMessage(
                json.dumps(record["key"]), json.dumps(record["value"]), record["partition"], key_schema, value_schema
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
        schema_version = it.count(1)
        serializable_message = {
            "key": decoded_message.key,
            "value": decoded_message.value,
            "partition": decoded_message.partition,
            "schema_directory_name": f"{next(schema_version):04}_{key_schema_id}_{value_schema_id}",
        }
        return key_schema_id, value_schema_id, decoded_message, serializable_message


def extract_schema_id(message: bytes) -> int:
    magic_byte, schema_id = struct.unpack(">bI", message[:5])
    assert magic_byte == 0, f"Wrong magic byte ({magic_byte}), no AVRO message."
    return schema_id
