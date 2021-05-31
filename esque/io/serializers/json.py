import json
from typing import Any, Optional

from esque.io.serializers.base import BaseSerializer


class JsonSerializer(BaseSerializer):
    def __init__(self, indent: Optional[int] = None):
        self._indent = indent

    def serialize(self, message: Any) -> bytes:
        return json.dumps(message, indent=self._indent).encode(encoding="UTF-8")

    def deserialize(self, binary_message: bytes) -> Any:
        return json.loads(binary_message.decode("UTF-8"))
