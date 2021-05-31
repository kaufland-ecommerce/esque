from typing import Any

from esque.io.serializers.base import BaseSerializer


class AvroSerializer(BaseSerializer):
    def serialize(self, data: Any) -> bytes:
        pass

    def deserialize(self, raw_data: bytes) -> Any:
        pass
