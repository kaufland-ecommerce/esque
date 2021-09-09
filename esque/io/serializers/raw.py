import dataclasses

from esque.io.data_types import UnknownDataType
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass(frozen=True)
class RawSerializerConfig(SerializerConfig):
    pass


class RawSerializer(DataSerializer):
    config_cls = RawSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: bytes) -> Data:
        return Data(payload=raw_data, data_type=self.unknown_data_type)

    def serialize(self, data: Data) -> bytes:
        if not isinstance(data.payload, bytes):
            raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return data.payload
