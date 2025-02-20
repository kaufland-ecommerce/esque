from pathlib import Path

from esque.io.serializers import (
    DataSerializer,
    JsonSerializer,
    ProtoSerializer,
    RawSerializer,
    RegistryAvroSerializer,
    StringSerializer,
)
from esque.io.serializers.json import JsonSerializerConfig
from esque.io.serializers.proto import ProtoSerializerConfig
from esque.io.serializers.raw import RawSerializerConfig
from esque.io.serializers.registry_avro import RegistryAvroSerializerConfig
from esque.io.serializers.string import StringSerializerConfig
from esque.io.serializers.struct import StructSerializer


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


def case_proto_serializer() -> DataSerializer:
    return ProtoSerializer(
        ProtoSerializerConfig(
            scheme="proto",
            protoc_py_path=f"{Path(__file__).parent}/pb",
            module_name="hi_pb2",
            class_name="HelloWorldResponse",
        )
    )


def case_struct() -> DataSerializer:
    return StructSerializer(StringSerializerConfig(scheme="struct"))
