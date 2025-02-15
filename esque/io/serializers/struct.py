import dataclasses
from struct import pack, unpack
from typing import Optional

from esque.io.data_types import NoData, UnknownDataType
from esque.io.messages import Data
from esque.io.serializers import SerializerConfig
from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass
class StructSerializerConfig(SerializerConfig):
    deserializer_format: Optional[str] = None
    serializer_format: Optional[str] = None


def _validate_format(format_type: str, format_value: Optional[str]) -> str:
    if format_value is None:
        raise ValueError(f"{format_type} struct format cannot be None")
    return format_value


class StructSerializer(DataSerializer):
    config_cls = StructSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA

        deserializer_format = _validate_format("deserializer", self.config.deserializer_struct_format)
        return Data(payload=unpack(deserializer_format, raw_data)[0], data_type=UnknownDataType())

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None

        serializer_format = _validate_format("serializer", self.config.serializer_struct_format)
        return pack(serializer_format, data.payload)
