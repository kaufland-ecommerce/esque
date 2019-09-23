import json
from collections import namedtuple
from functools import lru_cache
from typing import Dict

import fastavro
import requests

SchemaPair = namedtuple("SchemaPair", ["original_schema", "parsed_schema"])


class SchemaRegistryClient:
    def __init__(self, schema_registry_uri: str):
        self.schema_registry_uri = schema_registry_uri

    @lru_cache(maxsize=100)
    def get_schema_from_id(self, schema_id: int) -> SchemaPair:
        url = f"{self.schema_registry_uri}/schemas/ids/{schema_id}"
        response = requests.get(url)
        response.raise_for_status()
        schema: Dict = json.loads(response.json()["schema"])
        return SchemaPair(schema, fastavro.schema.parse_schema(schema))
