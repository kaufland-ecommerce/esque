from typing import Any

from esque.io.serializers.base import BaseSerializer


class AvroSerializer(BaseSerializer):
    def serialize(self, message: Any) -> bytes:
        pass

    def deserialize(self, binary_message: bytes) -> Any:
        pass
