from esque.io.serializers.base import DataSerializer


class StringSerializer(DataSerializer):
    def __init__(self, encoding: str = "UTF-8"):
        self._encoding = encoding

    def serialize(self, data: str) -> bytes:
        return data.encode(encoding=self._encoding)

    def deserialize(self, raw_data: bytes) -> str:
        return raw_data.decode(encoding=self._encoding)
