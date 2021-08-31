from typing import Any, Dict
from unittest import mock

import pytest

from esque.io.serializers.avro import (
    AvroSerializer,
    AvroSerializerConfig,
    InMemorySchemaRegistryClient,
    SchemaRegistryClient,
    SCHEMA_REGISTRY_CLIENT_SCHEME_MAP,
)


@pytest.fixture
def schema_registry_client() -> SchemaRegistryClient:
    return InMemorySchemaRegistryClient()


@pytest.fixture
def avro_schema() -> Dict:
    return {
        "type": "record",
        "namespace": "com.example",
        "name": "Identifier",
        "fields": [{"name": "id", "type": "string"}],
    }


@pytest.fixture
def deserialized_data() -> Dict:
    return {"id": "asdf"}


@pytest.fixture
def avro_serialized_data() -> bytes:
    return b"\x08asdf"


def test_avro_deserialize(
    avro_serialized_data: bytes, schema_registry_client: SchemaRegistryClient, deserialized_data: Any
):

    deserializer: AvroSerializer = AvroSerializer(schema_registry_client)
    actual_message: Any = deserializer.deserialize(avro_serialized_data)
    assert actual_message == deserialized_data


def test_from_config_not_implemented():
    class SchemaRegistryClientSubclass(SchemaRegistryClient):
        def get_schema_by_id(self, id: int) -> Dict:
            return {}

        def get_or_create_id_for_schema(self, schema: Dict) -> int:
            return 42

    with mock.patch.dict(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, {"dummy": SchemaRegistryClientSubclass}):
        with pytest.raises(AssertionError):
            config: AvroSerializerConfig = AvroSerializerConfig(
                scheme="avro", schema_registry_uri="dummy://local.test"
            )
            SchemaRegistryClient.from_config(config)


def test_from_config_implemented():
    class SchemaRegistryClientSubclass(SchemaRegistryClient):
        def get_schema_by_id(self, id: int) -> Dict:
            return {}

        def get_or_create_id_for_schema(self, schema: Dict) -> int:
            return 42

        @classmethod
        def from_config(cls, config: "AvroSerializerConfig") -> "SchemaRegistryClient":
            return cls()

    with mock.patch.dict(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, {"dummy": SchemaRegistryClientSubclass}):
        config: AvroSerializerConfig = AvroSerializerConfig(scheme="avro", schema_registry_uri="dummy://local.test")
        client = SchemaRegistryClient.from_config(config)
        assert isinstance(client, SchemaRegistryClientSubclass)
