import base64
import dataclasses
import datetime as dt
import json
from typing import Any, Optional

from esque.io.data_types import UnknownDataType
from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass(frozen=True)
class JsonSerializerConfig(SerializerConfig):
    indent: Optional[int]
    encoding: str = "UTF-8"


# TODO: implement solution to handle data types when they are known
class JsonSerializer(DataSerializer[JsonSerializerConfig]):
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: Data) -> bytes:
        return json.dumps(data, indent=self.config.indent, default=self.field_serializer).encode(
            encoding=self.config.encoding
        )

    def deserialize(self, raw_data: bytes) -> Data:
        return Data(payload=json.loads(raw_data.decode(self.config.encoding)), data_type=self.unknown_data_type)

    def field_serializer(self, data: Any) -> str:
        if isinstance(data, (dt.datetime, dt.date, dt.time)):
            return data.isoformat()
        if isinstance(data, bytes):
            return base64.b64encode(data).decode(self.config.encoding)
        raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")
