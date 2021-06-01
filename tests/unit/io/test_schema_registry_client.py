import pytest


@pytest.fixture
def schema_registry_client() -> SchemaRegistryClient:
    raise NotImplementedError


def test_get_schema(schema_registry_client: SchemaRegistryClient):
    pass


def test_post_schema(schema_registry_client: SchemaRegistryClient):
    pass


def test_posting_existing_schema(schema_registry_client: SchemaRegistryClient):
    pass
