import dataclasses
from abc import ABC, abstractmethod
from typing import ClassVar, Generic, Iterable, List, Optional, Type, TypeVar, Union

from esque.io.exceptions import EsqueIOSerializerConfigException
from esque.io.messages import BinaryMessage, Data, Message
from esque.io.stream_events import StreamEvent

SC = TypeVar("SC", bound="SerializerConfig")


@dataclasses.dataclass(frozen=True)
class SerializerConfig:
    scheme: str

    def __post_init__(self):
        self._validate()

    def copy(self: SC) -> SC:
        return dataclasses.replace(self)

    def _validate(self):
        problems: List[str] = self._validate_fields()
        if problems:
            raise EsqueIOSerializerConfigException("Serializer config validation failed: \n" + "\n".join(problems))

    def _validate_fields(self) -> List[str]:
        problems = []
        if not self.scheme:
            problems.append("scheme cannot be None")

        return problems


class DataSerializer(ABC, Generic[SC]):
    config_cls: ClassVar[Type[SC]] = SerializerConfig
    config: SC

    def __init__(self, config: SC):
        self.config = config

    @abstractmethod
    def serialize(self, data: Data) -> Optional[bytes]:
        raise NotImplementedError

    def serialize_many(self, data_list: Iterable[Data]) -> Iterable[Optional[bytes]]:
        return (self.serialize(message) for message in data_list)

    @abstractmethod
    def deserialize(self, raw_data: Optional[bytes]) -> Data:
        raise NotImplementedError

    def deserialize_many(self, raw_data_stream: Iterable[Optional[bytes]]) -> Iterable[Data]:
        return (self.deserialize(raw_data) for raw_data in raw_data_stream)


class MessageSerializer:
    def __init__(self, key_serializer: DataSerializer, value_serializer: Optional[DataSerializer] = None):
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer if value_serializer else key_serializer

    def serialize(self, message: Union[Message, StreamEvent]) -> Union[BinaryMessage, StreamEvent]:
        if isinstance(message, StreamEvent):
            return message
        key_data = self._key_serializer.serialize(message.key)
        value_data = self._value_serializer.serialize(message.value)
        return BinaryMessage(
            key=key_data,
            value=value_data,
            offset=message.offset,
            partition=message.partition,
            timestamp=message.timestamp,
            headers=message.headers.copy(),
        )

    def serialize_many(
        self, messages: Iterable[Union[Message, StreamEvent]]
    ) -> Iterable[Union[BinaryMessage, StreamEvent]]:
        return (self.serialize(message) for message in messages)

    def deserialize(self, binary_message: Union[BinaryMessage, StreamEvent]) -> Union[Message, StreamEvent]:
        if isinstance(binary_message, StreamEvent):
            return binary_message

        key_data = self._key_serializer.deserialize(binary_message.key)
        value_data = self._value_serializer.deserialize(binary_message.value)
        return Message(
            key=key_data,
            value=value_data,
            offset=binary_message.offset,
            partition=binary_message.partition,
            timestamp=binary_message.timestamp,
            headers=binary_message.headers.copy(),
        )

    def deserialize_many(
        self, binary_message_stream: Iterable[Union[BinaryMessage, StreamEvent]]
    ) -> Iterable[Union[Message, StreamEvent]]:
        return (self.deserialize(binary_message) for binary_message in binary_message_stream)
