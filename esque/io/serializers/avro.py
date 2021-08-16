import itertools
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator

from esque.io.exceptions import EsqueIONoSuchSchemaException
from esque.io.serializers.base import BaseSerializer, SerializerConfig


class SchemaRegistryClient(ABC):
    @abstractmethod
    def get_schema_by_id(self, id: int) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def get_or_create_id_for_schema(self, schema: Dict) -> int:
        raise NotImplementedError


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


class AvroSerializerConfig(SerializerConfig):
    # TODO continue here
    pass


class AvroSerializer(BaseSerializer):
    config_cls = AvroSerializerConfig

    def serialize(self, data: Any) -> bytes:
        pass

    def deserialize(self, raw_data: bytes) -> Any:
        pass
