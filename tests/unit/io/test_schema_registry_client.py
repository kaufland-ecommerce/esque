import pytest

from esque.io.exceptions import EsqueIONoSuchSchemaException
from esque.io.serializers.registry_avro import AvroType, InMemorySchemaRegistryClient, SchemaRegistryClient


@pytest.fixture
def schema_registry_client() -> SchemaRegistryClient:
    return InMemorySchemaRegistryClient()


def test_get_unknown_schema(schema_registry_client: SchemaRegistryClient):
    with pytest.raises(EsqueIONoSuchSchemaException):
        schema_registry_client.get_avro_type_by_id(1337)


def test_schema_persistence(schema_registry_client: SchemaRegistryClient):
    avro_type = AvroType({"type": "string"})
    schema_id = schema_registry_client.get_or_create_id_for_avro_type(avro_type)
    assert isinstance(schema_id, int)

    recovered_schema = schema_registry_client.get_avro_type_by_id(schema_id)
    assert avro_type == recovered_schema


def test_posting_existing_schema(schema_registry_client: SchemaRegistryClient):
    schema1 = {"type": "record", "name": "myrecord", "fields": [{"name": "f1", "type": "string"}]}
    schema2 = {"name": "myrecord", "type": "record", "fields": [{"name": "f1", "type": "string"}]}

    avro_type1 = AvroType(schema1)
    avro_type2 = AvroType(schema2)
    schema_id1 = schema_registry_client.get_or_create_id_for_avro_type(avro_type1)
    schema_id2 = schema_registry_client.get_or_create_id_for_avro_type(avro_type2)

    assert isinstance(schema_id1, int)
    assert isinstance(schema_id2, int)
    assert schema_id1 == schema_id2
