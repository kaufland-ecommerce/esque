import base64
import dataclasses
import datetime as dt
import json
from typing import Any, Optional

from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer


def field_serializer(data: Any) -> str:
    if isinstance(data, (dt.datetime, dt.date, dt.time)):
        return data.isoformat()
    if isinstance(data, bytes):
        return base64.b64encode(data).decode("utf-8")
    raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")


@dataclasses.dataclass(frozen=True)
class JsonSerializerConfig(SerializerConfig):
    indent: Optional[int]


class JsonSerializer(DataSerializer[JsonSerializerConfig]):
    def serialize(self, data: Data) -> bytes:
        return json.dumps(data, indent=self.config.indent, default=field_serializer).encode(encoding="UTF-8")

    def deserialize(self, raw_data: bytes) -> Data:
        return json.loads(raw_data.decode("UTF-8"))
