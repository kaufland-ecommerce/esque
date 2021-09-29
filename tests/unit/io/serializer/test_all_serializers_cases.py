from esque.io.serializers import (
    DataSerializer,
    JsonSerializer,
    RawSerializer,
    RegistryAvroSerializer,
    StringSerializer,
)
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.raw import RawSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig


def case_json_serializer() -> DataSerializer:
    return JsonSerializer(JsonSerializerConfig(scheme="json"))


def case_raw_serializer() -> DataSerializer:
    return RawSerializer(RawSerializerConfig(scheme="raw"))


def case_registry_avro_serializer() -> DataSerializer:
    return RegistryAvroSerializer(
        RegistryAvroSerializerConfig(scheme="reg-avro", schema_registry_uri="memory://foobar")
    )


def case_string_serializer() -> DataSerializer:
    return StringSerializer(StringSerializerConfig(scheme="str"))
