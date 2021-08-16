from typing import Any, Dict

import pytest

from esque.io.serializers.avro import AvroSerializer, InMemorySchemaRegistryClient, SchemaRegistryClient


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
