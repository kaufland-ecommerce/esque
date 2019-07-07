import json
import pathlib
import pickle
import struct
from io import BytesIO
from typing import Optional, Tuple, Dict, BinaryIO
import itertools as it

import fastavro
from confluent_kafka.cimpl import Message

from esque.message import Serializer
from esque.schemaregistry import SchemaRegistryClient


class DecodedAvroMessage:
    def __init__(self, key: Optional[Dict], value: Optional[Dict], key_schema_id: int, value_schema_id: int):
        self.key = key
        self.value = value
        self.key_schema_id = key_schema_id
        self.value_schema_id = value_schema_id


class AvroSerializer(Serializer):

    def __init__(self, working_dir: pathlib.Path, schema_registry_client: SchemaRegistryClient):
        super().__init__(working_dir)
        self.schema_registry_client = schema_registry_client
        self.current_key_schema_id = None
        self.current_value_schema_id = None
        self.schema_version = it.count(1)

    def serialize(self, message: Message, file: BinaryIO):
        key_schema_id, decoded_key = self.decode_bytes(message.key())
        value_schema_id, decoded_value = self.decode_bytes(message.value())
        decoded_message = DecodedAvroMessage(decoded_key, decoded_value, key_schema_id, value_schema_id)

        if self.schema_changed(decoded_message):
            schema_dir_name = f"{next(self.schema_version):04}_{key_schema_id}_{value_schema_id}"
            directory = self.working_dir / schema_dir_name
            directory.mkdir()

            (directory / "key_schema.avsc").write_text(
                json.dumps(self.schema_registry_client.get_schema_from_id(key_schema_id).original_schema)
            )

            (directory / "value_schema.avsc").write_text(
                json.dumps(self.schema_registry_client.get_schema_from_id(value_schema_id).original_schema)
            )

        serializable_message = {"key": decoded_key, "value": decoded_value}
        pickle.dump(serializable_message, file)

    def decode_bytes(self, raw_data: Optional[bytes]) -> Tuple[int, Optional[Dict]]:
        if raw_data is None:
            return -1, None

        with BytesIO(raw_data) as fake_stream:
            schema_id = extract_schema_id(fake_stream.read(5))
            parsed_schema = self.schema_registry_client.get_schema_from_id(schema_id).parsed_schema
            record = fastavro.schemaless_reader(fake_stream, parsed_schema)
        return schema_id, record

    def schema_changed(self, decoded_message: DecodedAvroMessage) -> bool:
        return (self.current_value_schema_id != decoded_message.value_schema_id and decoded_message.value is not None) \
               or self.current_key_schema_id != decoded_message.key_schema_id


def extract_schema_id(message: bytes) -> int:
    _, schema_id = struct.unpack(">bI", message[:5])
    return schema_id
