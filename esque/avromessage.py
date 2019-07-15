import json
import pathlib
import pickle
import struct
from io import BytesIO
from typing import Optional, Tuple, Dict, Iterable
import itertools as it

import fastavro
from confluent_kafka.cimpl import Message
from confluent_kafka.avro import loads as load_schema

from esque.message import FileWriter, FileReader, KafkaMessage
from esque.schemaregistry import SchemaRegistryClient


class DecodedAvroMessage:
    def __init__(self, key: Optional[Dict], value: Optional[Dict], key_schema_id: int, value_schema_id: int):
        self.key = key
        self.value = value
        self.key_schema_id = key_schema_id
        self.value_schema_id = value_schema_id


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

    def write_message_to_file(self, message: Message):
        key_schema_id, decoded_key = self.decode_bytes(message.key())
        value_schema_id, decoded_value = self.decode_bytes(message.value())
        decoded_message = DecodedAvroMessage(decoded_key, decoded_value, key_schema_id, value_schema_id)

        if self.schema_changed(decoded_message) or self.schema_dir_name is None:
            self.schema_dir_name = f"{next(self.schema_version):04}_{key_schema_id}_{value_schema_id}"
            self.current_key_schema_id = key_schema_id
            self.current_value_schema_id = value_schema_id
            self._dump_schemata(key_schema_id, value_schema_id)

        serializable_message = {
            "key": decoded_key,
            "value": decoded_value,
            "schema_directory_name": self.schema_dir_name,
        }
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

    def decode_bytes(self, raw_data: Optional[bytes]) -> Tuple[int, Optional[Dict]]:
        if raw_data is None:
            return -1, None

        with BytesIO(raw_data) as fake_stream:
            schema_id = extract_schema_id(fake_stream.read(5))
            parsed_schema = self.schema_registry_client.get_schema_from_id(schema_id).parsed_schema
            record = fastavro.schemaless_reader(fake_stream, parsed_schema)
        return schema_id, record

    def schema_changed(self, decoded_message: DecodedAvroMessage) -> bool:
        return (
            self.current_value_schema_id != decoded_message.value_schema_id and decoded_message.value is not None
        ) or self.current_key_schema_id != decoded_message.key_schema_id


class AvroFileReader(FileReader):
    def __init__(self, directory: pathlib.Path):
        super().__init__(directory)
        self.open_mode = "rb"

    def read_from_file(self) -> Iterable[KafkaMessage]:
        while True:
            try:
                record = pickle.load(self.file)
            except EOFError:
                return

            schema_directory = self.directory / record["schema_directory_name"]

            key_schema = load_schema((schema_directory / "key_schema.avsc").read_text(encoding="utf-8"))
            value_schema = load_schema((schema_directory / "value_schema.avsc").read_text(encoding="utf-8"))

            yield KafkaMessage(json.dumps(record["key"]), json.dumps(record["value"]), key_schema, value_schema)


def extract_schema_id(message: bytes) -> int:
    magic_byte, schema_id = struct.unpack(">bI", message[:5])
    assert magic_byte == 0, f"Wrong magic byte ({magic_byte}), no AVRO message."
    return schema_id
