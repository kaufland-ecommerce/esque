import base64
import dataclasses
import datetime as dt
import json
from typing import Any, Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass(frozen=True)
class JsonSerializerConfig(SerializerConfig):
    indent: Optional[str] = None
    encoding: str = "UTF-8"


# TODO: implement solution to handle data types when they are known
class JsonSerializer(DataSerializer[JsonSerializerConfig]):
    config_cls = JsonSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        indent = None
        if self.config.indent is not None:
            indent = int(self.config.indent)
        return json.dumps(data.payload, indent=indent, default=self.field_serializer).encode(
            encoding=self.config.encoding
        )

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA
        return Data(payload=json.loads(raw_data.decode(self.config.encoding)), data_type=self.unknown_data_type)

    def field_serializer(self, data: Any) -> str:
        if isinstance(data, (dt.datetime, dt.date, dt.time)):
            return data.isoformat()
        if isinstance(data, bytes):
            return base64.b64encode(data).decode(self.config.encoding)
        raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")
