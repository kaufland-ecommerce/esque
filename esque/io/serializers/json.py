import base64
import datetime as dt
import json
from typing import Any, Optional

from esque.io.serializers.base import BaseSerializer


def field_serializer(data: Any) -> str:
    if isinstance(data, (dt.datetime, dt.date, dt.time)):
        return data.isoformat()
    if isinstance(data, bytes):
        return base64.b64encode(data).decode("utf-8")
    raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")


class JsonSerializer(BaseSerializer):
    def __init__(self, indent: Optional[int] = None):
        self._indent = indent

    def serialize(self, data: Any) -> bytes:
        return json.dumps(data, indent=self._indent, default=field_serializer).encode(encoding="UTF-8")

    def deserialize(self, raw_data: bytes) -> Any:
        return json.loads(raw_data.decode("UTF-8"))
