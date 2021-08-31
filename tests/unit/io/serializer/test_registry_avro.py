import random
from string import ascii_lowercase
from typing import Any, Dict
from unittest import mock

import pytest

from esque.io.serializers.registry_avro import (
    SCHEMA_REGISTRY_CLIENT_SCHEME_MAP,
    InMemorySchemaRegistryClient,
    RegistryAvroSerializer,
    RegistryAvroSerializerConfig,
    SchemaRegistryClient,
    create_schema_id_prefix,
)


@pytest.fixture
def registry_uri() -> str:
    hostname = "".join(random.choices(ascii_lowercase, k=5))
    return f"memory://{hostname}"


@pytest.fixture
def registry_avro_config(registry_uri: str) -> RegistryAvroSerializerConfig:
    return RegistryAvroSerializerConfig(scheme="registry_avro", schema_registry_uri=registry_uri)


@pytest.fixture
def schema_registry_client(registry_avro_config: RegistryAvroSerializerConfig) -> InMemorySchemaRegistryClient:
    return InMemorySchemaRegistryClient.from_config(registry_avro_config)


@pytest.fixture
def schema_id(avro_schema: Dict, schema_registry_client: SchemaRegistryClient) -> int:
    return schema_registry_client.get_or_create_id_for_schema(avro_schema)


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
def avro_serialized_data(schema_id: int) -> bytes:
    return create_schema_id_prefix(schema_id) + b"\x08asdf"


@pytest.fixture
def registry_avro_serializer(registry_avro_config: RegistryAvroSerializerConfig) -> RegistryAvroSerializer:
    return RegistryAvroSerializer(registry_avro_config)


def test_avro_deserialize(
    avro_serialized_data: bytes, registry_avro_serializer: RegistryAvroSerializer, deserialized_data: Any
):
    actual_message: Any = registry_avro_serializer.deserialize(avro_serialized_data)
    assert actual_message == deserialized_data


def test_avro_serialize(
    avro_serialized_data: bytes, registry_avro_serializer: RegistryAvroSerializer, deserialized_data: Any
):
    actual_serialized_message: Any = registry_avro_serializer.serialize(deserialized_data)
    assert actual_serialized_message == avro_serialized_data


def test_from_config_not_implemented():
    class SchemaRegistryClientSubclass(SchemaRegistryClient):
        def get_schema_by_id(self, id: int) -> Dict:
            return {}

        def get_or_create_id_for_schema(self, schema: Dict) -> int:
            return 42

    with mock.patch.dict(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, {"dummy": SchemaRegistryClientSubclass}):
        with pytest.raises(AssertionError):
            config: RegistryAvroSerializerConfig = RegistryAvroSerializerConfig(
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
        def from_config(cls, config: "RegistryAvroSerializerConfig") -> "SchemaRegistryClient":
            return cls()

    with mock.patch.dict(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, {"dummy": SchemaRegistryClientSubclass}):
        config: RegistryAvroSerializerConfig = RegistryAvroSerializerConfig(
            scheme="avro", schema_registry_uri="dummy://local.test"
        )
        client = SchemaRegistryClient.from_config(config)
        assert isinstance(client, SchemaRegistryClientSubclass)
