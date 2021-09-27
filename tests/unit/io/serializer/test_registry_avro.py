import pathlib
import random
from string import ascii_lowercase
from typing import Any
from unittest import mock

import pytest
from pytest_cases import fixture, parametrize_with_cases

from esque.config import Config
from esque.io.messages import Data
from esque.io.serializers.registry_avro import (
    SCHEMA_REGISTRY_CLIENT_SCHEME_MAP,
    AvroType,
    RegistryAvroSerializer,
    RegistryAvroSerializerConfig,
    SchemaRegistryClient,
    create_schema_id_prefix,
)


@fixture
def registry_uri() -> str:
    hostname = "".join(random.choices(ascii_lowercase, k=5))
    return f"memory://{hostname}"


def case_in_memory_registry_config() -> RegistryAvroSerializerConfig:
    memory_key = "".join(random.choices(ascii_lowercase, k=5))
    return RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=f"memory://{memory_key}")


def case_path_registry_config(tmpdir: pathlib.Path) -> RegistryAvroSerializerConfig:
    return RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri=f"path:///{tmpdir}")


@pytest.mark.integration
def case_confluent_registry_config(topic_id: str, unittest_config: Config) -> RegistryAvroSerializerConfig:
    return RegistryAvroSerializerConfig(
        scheme="reg-avro", schema_registry_uri=unittest_config.schema_registry
    ).with_key_subject_for_topic(topic_id)


@fixture
@parametrize_with_cases("config", cases=".")
def registry_avro_config(config: RegistryAvroSerializerConfig) -> RegistryAvroSerializerConfig:
    return config


@fixture
def schema_registry_client(registry_avro_config: RegistryAvroSerializerConfig) -> SchemaRegistryClient:
    return SchemaRegistryClient.from_config(registry_avro_config)


@fixture
def schema_id(avro_type: AvroType, schema_registry_client: SchemaRegistryClient) -> int:
    return schema_registry_client.get_or_create_id_for_avro_type(avro_type)


@fixture
def avro_type() -> AvroType:
    return AvroType(
        {
            "type": "record",
            "namespace": "com.example",
            "name": "Identifier",
            "fields": [{"name": "id", "type": "string"}],
        }
    )


@fixture
def deserialized_data(avro_type) -> Data:
    return Data(payload={"id": "asdf"}, data_type=avro_type)


@fixture
def avro_serialized_data(schema_id: int) -> bytes:
    return create_schema_id_prefix(schema_id) + b"\x08asdf"


@fixture
def registry_avro_serializer(registry_avro_config: RegistryAvroSerializerConfig) -> RegistryAvroSerializer:
    return RegistryAvroSerializer(registry_avro_config)


def test_registry_client_same_schema_same_id(registry_avro_config: RegistryAvroSerializerConfig, avro_type: Data):
    client1 = SchemaRegistryClient.from_config(registry_avro_config)
    schema_id1 = client1.get_or_create_id_for_avro_type(avro_type)

    client2 = SchemaRegistryClient.from_config(registry_avro_config)
    schema_id2 = client2.get_or_create_id_for_avro_type(avro_type)

    assert schema_id1 == schema_id2


def test_registry_client_schema_retrieval(schema_registry_client: SchemaRegistryClient, avro_type: Data):
    schema_id = schema_registry_client.get_or_create_id_for_avro_type(avro_type)
    actual_type = schema_registry_client.get_avro_type_by_id(schema_id)

    assert avro_type == actual_type


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
    class SchemaRegistryClientSubclassA(SchemaRegistryClient):
        def get_avro_type_by_id(self, id: int) -> "AvroType":
            return AvroType({})

        def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
            return 42

    with mock.patch.dict(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP, {"dummy": SchemaRegistryClientSubclassA}):
        with pytest.raises(AssertionError):
            config: RegistryAvroSerializerConfig = RegistryAvroSerializerConfig(
                scheme="avro", schema_registry_uri="dummy://local.test"
            )
            SchemaRegistryClient.from_config(config)


def test_from_config_implemented():
    class SchemaRegistryClientSubclass(SchemaRegistryClient):
        def get_avro_type_by_id(self, id: int) -> "AvroType":
            return AvroType({})

        def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
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
