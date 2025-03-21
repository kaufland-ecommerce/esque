import dataclasses
import importlib
import sys
from typing import Optional, Type

from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from esque.io.data_types import Dict, NoData
from esque.io.messages import Data
from esque.io.serializers.base import DataSerializer, SerializerConfig


@dataclasses.dataclass
class ProtoSerializerConfig(SerializerConfig):
    protoc_py_path: str
    module_name: str
    class_name: str

    def __post_init__(self):
        self._message_class = self._load_message_class()

    def _load_message_class(self) -> Type[Message]:
        sys.path.append(self.protoc_py_path)
        grpc_module = importlib.import_module(self.module_name)
        return getattr(grpc_module, self.class_name)

    def get_message_class(self) -> Type[Message]:
        return self._message_class


class ProtoSerializer(DataSerializer):
    config_cls = ProtoSerializerConfig
    dict_data_type: Dict = Dict()

    def serialize(self, data: Data) -> Optional[bytes]:
        if isinstance(data.data_type, NoData):
            return None
        if not isinstance(data.payload, dict):
            raise ValueError(f"Protobuf serialization requires a dictionary, got {type(data)}")

        message = self.config.get_message_class()()
        ParseDict(data.payload, message)
        return message.SerializeToString()

    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        if raw_data is None:
            return Data.NO_DATA

        message = self.config.get_message_class()()
        message.ParseFromString(raw_data)

        return Data(
            payload=MessageToDict(
                message, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
            ),
            data_type=self.dict_data_type,
        )
