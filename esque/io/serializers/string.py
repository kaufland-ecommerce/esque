import dataclasses

from esque.io.data_types import String
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass(frozen=True)
class StringSerializerConfig(SerializerConfig):
    encoding: str = "UTF-8"


class StringSerializer(DataSerializer[StringSerializerConfig]):
    def serialize(self, data: Data) -> bytes:
        return data.payload.encode(encoding=self.config.encoding)

    def deserialize(self, raw_data: bytes) -> Data:
        return Data(raw_data.decode(encoding=self.config.encoding), String())
