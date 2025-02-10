import dataclasses
from struct import pack, unpack
from typing import Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass()
class StructSerializerConfig:
    deserializer_struct_format: str = None
    serializer_struct_format: str = None


class StructSerializer(DataSerializer):
    config_cls = StructSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA
        return unpack(self.config.deserializer_struct_format, raw_data)[0]

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        if not isinstance(data, bytes):
            raise TypeError(f"Data payload must be bytes or bytearray, not {type(data).__name__}!")
        return pack(self.config.serializer_struct_format, data)
