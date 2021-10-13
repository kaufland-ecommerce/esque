import dataclasses
import functools
import hashlib
import io
import itertools
import json
import pathlib
import urllib.parse
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Iterator, Optional, Type
from urllib.parse import ParseResult

import fastavro
import requests

from esque.io.data_types import CustomDataType, DataType, NoData, UnknownDataType
from esque.io.exceptions import EsqueIONoSuchSchemaException, EsqueIOSerializerConfigException
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig

SCHEMA_REGISTRY_CLIENT_SCHEME_MAP: Dict[str, Type["SchemaRegistryClient"]] = {}
MAGIC_BYTE = b"\x00"


def create_schema_id_prefix(schema_id: int) -> bytes:
    return MAGIC_BYTE + schema_id.to_bytes(length=4, byteorder="big")


def get_schema_id_from_prefix(prefix: bytes) -> int:
    assert prefix[:1] == MAGIC_BYTE
    return int.from_bytes(prefix[1:5], byteorder="big")


class SchemaRegistryClient(ABC):
    @abstractmethod
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        raise NotImplementedError

    @abstractmethod
    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "SchemaRegistryClient":
        scheme: str = config.parsed_uri().scheme
        schema_registry_client_cls = SCHEMA_REGISTRY_CLIENT_SCHEME_MAP[scheme]
        assert cls == SchemaRegistryClient, f"Make sure you implement from_config on {cls.__name__}"
        return schema_registry_client_cls.from_config(config)


class InMemorySchemaRegistryClient(SchemaRegistryClient):
    _IN_MEMORY_REGISTRIES: ClassVar[Dict[str, "SchemaRegistryClient"]] = {}

    def __init__(self):
        self._avro_types_by_id: Dict[int, AvroType] = {}
        self._ids_by_avro_type: Dict[AvroType, int] = {}
        self._id_counter: Iterator[int] = itertools.count()

    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        if schema_id not in self._avro_types_by_id:
            raise EsqueIONoSuchSchemaException(f"Unknown schema ID {schema_id}")

        return self._avro_types_by_id[schema_id]

    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        if avro_type in self._ids_by_avro_type:
            return self._ids_by_avro_type[avro_type]
        else:
            schema_id = next(self._id_counter)
            self._ids_by_avro_type[avro_type] = schema_id
            self._avro_types_by_id[schema_id] = avro_type
            return schema_id

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "InMemorySchemaRegistryClient":
        hostname = config.parsed_uri().hostname
        if hostname not in cls._IN_MEMORY_REGISTRIES:
            cls._IN_MEMORY_REGISTRIES[hostname] = cls()
        return cls._IN_MEMORY_REGISTRIES[hostname]


SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["memory"] = InMemorySchemaRegistryClient


class RestSchemaRegistryClient(SchemaRegistryClient):
    def __init__(self, registry_url: str, subject: str):
        self._base_url = registry_url
        self._subject = subject
        self._request_session = requests.Session()

    @functools.lru_cache(maxsize=512)
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        url = f"{self._base_url}/schemas/ids/{schema_id}"
        response = self._request_session.get(url)
        response.raise_for_status()
        schema: Dict = json.loads(response.json()["schema"])
        return AvroType(avro_schema=schema)

    @functools.lru_cache(maxsize=512)
    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        self._assert_subject_valid()
        schema_id = self._try_get_existing_schema_id_from_subject(avro_type)

        if schema_id is None:
            schema_id = self._register_new_version_on_subject_and_get_schema_id(avro_type)

        return schema_id

    def _assert_subject_valid(self):
        if not self._subject:
            raise EsqueIOSerializerConfigException(
                "Need to provide a key or value schema subject! I.e. topic suffixed with '-key' or '-value'."
            )
        elif not (self._subject.endswith("key") or self._subject.endswith("value")):
            raise EsqueIOSerializerConfigException("Need to provide a specific key or value subject.")

    def _try_get_existing_schema_id_from_subject(self, avro_type: "AvroType") -> Optional[int]:
        url = f"{self._base_url}/subjects/{self._subject}"
        response = self._request_session.post(url, json={"schema": json.dumps(avro_type.avro_schema)})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["id"]

    def _register_new_version_on_subject_and_get_schema_id(self, avro_type: "AvroType") -> int:
        url = f"{self._base_url}/subjects/{self._subject}/versions"
        response = self._request_session.post(url, json={"schema": json.dumps(avro_type.avro_schema)})
        response.raise_for_status()
        return response.json()["id"]

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "RestSchemaRegistryClient":
        return cls(registry_url=config.schema_registry_uri, subject=config.schema_subject)


SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["http"] = RestSchemaRegistryClient
SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["https"] = RestSchemaRegistryClient


# hash to schema id
IndexData = Dict[str, int]


class PathSchemaRegistryClient(SchemaRegistryClient):
    def __init__(self, schema_registry_uri: str):
        # skip the leading slash to allow for relative paths
        path = urllib.parse.urlparse(schema_registry_uri).path[1:]
        self._base_path = pathlib.Path(path)

    @functools.lru_cache
    def get_avro_type_by_id(self, schema_id: int) -> "AvroType":
        path = self._path_for_id(schema_id)
        if not path.exists():
            raise EsqueIONoSuchSchemaException(f"Unknown schema ID {schema_id}")
        with path.open("r") as o:
            data = json.load(o)
        return AvroType(data)

    def _path_for_id(self, schema_id: int) -> pathlib.Path:
        return self._base_path / f"schema_{schema_id:03}.avsc"

    def get_or_create_id_for_avro_type(self, avro_type: "AvroType") -> int:
        # JSON doesn't support int keys, so we need to make them strings here
        type_hash = str(hash(avro_type))
        if type_hash in self._index_data:
            return self._index_data[type_hash]

        schema_id = max(self._index_data.values(), default=-1) + 1
        self._write_schema_file(avro_type, schema_id)

        self._index_data[type_hash] = schema_id
        self._update_index_file()
        return schema_id

    def _write_schema_file(self, avro_type: "AvroType", schema_id: int):
        schema_file = self._path_for_id(schema_id)
        schema_file.parent.mkdir(exist_ok=True)
        with schema_file.open("w") as o:
            json.dump(avro_type.avro_schema, o)

    def _get_index_path(self) -> pathlib.Path:
        return self._base_path / "schema_index.json"

    @functools.cached_property
    def _index_data(self) -> IndexData:
        index_path = self._get_index_path()
        if not index_path.exists():
            return {}
        with index_path.open("r") as o:
            return json.load(o)

    def _update_index_file(self):
        with self._get_index_path().open("w") as o:
            json.dump(self._index_data, o)

    @classmethod
    def from_config(cls, config: "RegistryAvroSerializerConfig") -> "PathSchemaRegistryClient":
        return cls(config.schema_registry_uri)


SCHEMA_REGISTRY_CLIENT_SCHEME_MAP["path"] = PathSchemaRegistryClient


@dataclasses.dataclass(frozen=True)
class RegistryAvroSerializerConfig(SerializerConfig):
    schema_registry_uri: str
    schema_subject: str = ""

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

    def with_key_subject_for_topic(self, topic: str) -> "RegistryAvroSerializerConfig":
        return dataclasses.replace(self, schema_subject=f"{topic}-key")

    def with_value_subject_for_topic(self, topic: str) -> "RegistryAvroSerializerConfig":
        return dataclasses.replace(self, schema_subject=f"{topic}-value")


class RegistryAvroSerializer(DataSerializer):
    config_cls = RegistryAvroSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def __init__(self, config: RegistryAvroSerializerConfig):
        super().__init__(config)
        self._registry_client = SchemaRegistryClient.from_config(config)

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        avro_type = ensure_avro_type(data.data_type)
        schema_id = self._registry_client.get_or_create_id_for_avro_type(avro_type)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, avro_type.fastavro_schema, data.payload)
        return create_schema_id_prefix(schema_id) + buffer.getvalue()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA

        with io.BytesIO(raw_data) as fake_stream:
            schema_id = get_schema_id_from_prefix(fake_stream.read(5))
            avro_type = self._registry_client.get_avro_type_by_id(schema_id)
            record = fastavro.schemaless_reader(fake_stream, avro_type.fastavro_schema)
            return Data(payload=record, data_type=avro_type)


@dataclasses.dataclass
class AvroType(CustomDataType):
    avro_schema: Dict

    def __hash__(self) -> int:
        data_bytes: bytes = json.dumps(self.avro_schema, sort_keys=True).encode(encoding="utf-8")
        digest: bytes = hashlib.md5(data_bytes).digest()
        return int.from_bytes(digest, byteorder="big")

    @functools.cached_property
    def fastavro_schema(self) -> Any:
        return fastavro.parse_schema(schema=self.avro_schema)


def ensure_avro_type(data_type: DataType) -> AvroType:
    if isinstance(data_type, AvroType):
        # everything fine, return as is
        return data_type

    if isinstance(data_type, CustomDataType):
        # It's another custom data type, we'll have to do lossy conversion
        # by converting it to a general esque data type first
        # TODO cache this somehow?
        data_type = data_type.to_esque_data_type()

    return AvroType.from_esque_data_type(data_type)
