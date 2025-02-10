import dataclasses
import importlib
import sys
from typing import Optional, Type, Union

from esque.io.data_types import UnknownDataType
from esque.io.messages import Data
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from esque.io.serializers.base import DataSerializer


@dataclasses.dataclass
class ProtoSerializerConfig:
    protoc_py_path: str
    module_name: str
    class_name: str

    def __post_init__(self):
        self._message_class = self._load_message_class()

    def _load_message_class(self) -> Type[Message]:
        sys.path.append(self.protoc_py_path)
        module = importlib.import_module(self.module_name)
        return getattr(module, self.class_name)

    def get_message_class(self) -> Type[Message]:
        return self._message_class


class ProtoSerializer(DataSerializer):
    config_cls = ProtoSerializerConfig
    unknown_data_type: UnknownDataType = UnknownDataType()

    def serialize(self, data: Data) -> Optional[bytes]:
        if not isinstance(data, dict):
            raise ValueError(f"Protobuf serialization requires a dictionary, got {type(data)}")

        message = self.config.get_message_class()()
        ParseDict(data, message)
        return message.SerializeToString()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA

        message = self.config.get_message_class()()
        message.ParseFromString(raw_data)

        return Data(
            payload=MessageToDict(message,
                                  preserving_proto_field_name=True,
                                  always_print_fields_with_no_presence=True),
            data_type=self.unknown_data_type)
