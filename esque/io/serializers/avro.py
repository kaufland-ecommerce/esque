import dataclasses
import itertools
import json
import urllib.parse
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Type
from urllib.parse import ParseResult

from esque.io.exceptions import EsqueIONoSuchSchemaException
from esque.io.serializers.base import DataSerializer, SerializerConfig

SCHEMA_REGISTRY_CLIENT_SCHEME_MAP: Dict[str, Type["SchemaRegistryClient"]] = {}


class SchemaRegistryClient(ABC):
    @abstractmethod
    def get_schema_by_id(self, id: int) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def get_or_create_id_for_schema(self, schema: Dict) -> int:
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: "AvroSerializerConfig") -> "SchemaRegistryClient":
        scheme: str = config.parsed_uri().scheme
        schema_registry_client_cls = SCHEMA_REGISTRY_CLIENT_SCHEME_MAP[scheme]
        assert cls == SchemaRegistryClient, f"Make sure you implement from_config on {cls.__name__}"
        return schema_registry_client_cls.from_config(config)


class InMemorySchemaRegistryClient(SchemaRegistryClient):
    def __init__(self):
        self._schemas_by_id: Dict[int, Dict] = {}
        self._ids_by_schema: Dict[str, int] = {}
        self._id_counter: Iterator[int] = itertools.count()

    def get_schema_by_id(self, id: int) -> Dict:
        if id not in self._schemas_by_id:
            raise EsqueIONoSuchSchemaException(f"Unknown schema ID {id}")

        return self._schemas_by_id[id]

    def get_or_create_id_for_schema(self, schema: Dict) -> int:
        schema_str = json.dumps(schema, sort_keys=True)
        if schema_str in self._ids_by_schema:
            return self._ids_by_schema[schema_str]
        else:
            schema_id = next(self._id_counter)
            self._ids_by_schema[schema_str] = schema_id
            self._schemas_by_id[schema_id] = json.loads(schema_str)
            return schema_id


SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["memory"] = InMemorySchemaRegistryClient


@dataclasses.dataclass(frozen=True)
class AvroSerializerConfig(SerializerConfig):
    schema_registry_uri: str

    def parsed_uri(self) -> ParseResult:
        return urllib.parse.urlparse(url=self.schema_registry_uri)

    def _validate_fields(self):
        problems = super()._validate_fields()
        if not self.schema_registry_uri:
            problems.append("uri cannot be None")
        try:
            parsed_uri_result: ParseResult = self.parsed_uri()
        except Exception as e:  # noqa
            problems.append(f"exception of type {type(e).__name__} occurred during uri parsing: {e.args}")
        else:
            if parsed_uri_result.scheme not in SCHEMA_REGISTRY_CLIENT_SCHEME_MAP:
                problems.append(
                    f"unknown scheme for schema registry client: {parsed_uri_result.scheme}. "
                    f"Supported client schemes: {','.join(SCHEMA_REGISTRY_CLIENT_SCHEME_MAP.keys())}"
                )

        return problems


class AvroSerializer(DataSerializer):
    config_cls = AvroSerializerConfig

    def serialize(self, data: Any) -> bytes:
        pass

    def deserialize(self, raw_data: bytes) -> Any:
        pass
