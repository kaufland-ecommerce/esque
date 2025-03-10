import dataclasses
from typing import Optional

from esque.io.data_types import NoData, String
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass
class StringSerializerConfig(SerializerConfig):
    encoding: str = "UTF-8"


class StringSerializer(DataSerializer[StringSerializerConfig]):
    data_type: String = String()

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        try:
            return data.payload.encode(encoding=self.config.encoding)
        except Exception as e:
            raise ValueError(f"unable to serialize to string. try binary. error:{e}")

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA
        try:
            return Data(raw_data.decode(encoding=self.config.encoding), self.data_type)
        except Exception as e:
            raise ValueError(f"unable to de-serialize to string. try binary. error:{e}")
