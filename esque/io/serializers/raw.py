import dataclasses
from typing import Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass(frozen=True)
class RawSerializerConfig(SerializerConfig):
    pass


class RawSerializer(DataSerializer):
    config_cls = RawSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA
        return Data(payload=raw_data, data_type=self.unknown_data_type)

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        if not isinstance(data.payload, bytes):
            raise TypeError(f"Data payload has to be bytes, not {type(data.payload).__name__}!")
        return data.payload
