import dataclasses
import random
from string import ascii_letters
from typing import Any, Dict, List, Optional, Tuple, Union

import pytest

from esque.io.handlers.base import BaseHandler, HandlerConfig
from esque.io.messages import BinaryMessage, Message
from esque.io.pipeline import HandlerSerializerMessageReader, HandlerSerializerMessageWriter
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer, StringSerializerConfig
from esque.io.stream_events import PermanentEndOfStream, StreamEvent, TemporaryEndOfPartition


@dataclasses.dataclass(frozen=True)
class DummyHandlerConfig(HandlerConfig):
    pass


class DummyHandler(BaseHandler):
    config_cls = DummyHandlerConfig

    def __init__(self, config: DummyHandlerConfig):
        super().__init__(config=config)
        self._messages: List[Optional[BinaryMessage]] = []
        self._serializer_configs: Tuple[Dict[str, Any], Dict[str, Any]] = ({}, {})

    def get_serializer_configs(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return self._serializer_configs

    def put_serializer_configs(self, configs: Tuple[Dict[str, Any], Dict[str, Any]]) -> None:
        self._serializer_configs = configs

    def write_message(self, binary_message: BinaryMessage) -> None:
        self._messages.append(binary_message)

    def read_message(self) -> Union[BinaryMessage, StreamEvent]:
        if self._messages:
            elem = self._messages.pop(0)
            if elem is None:
                return TemporaryEndOfPartition("Temporary end of stream")
            return elem
        else:
            return PermanentEndOfStream("No messages left in memory")

    def get_messages(self) -> List[BinaryMessage]:
        return self._messages.copy()

    def set_messages(self, messages: List[BinaryMessage]):
        self._messages = messages.copy()

    def insert_temporary_end_of_stream(self, position: int):
        self._messages.insert(position, None)

    @classmethod
    def create_default(cls) -> "DummyHandler":
        return cls(config=DummyHandlerConfig(host="", path="", scheme="dummy"))


@pytest.fixture
def topic_id() -> str:
    return "".join(random.choices(ascii_letters, k=5))


@pytest.fixture
def dummy_handler() -> DummyHandler:
    return DummyHandler.create_default()


@pytest.fixture()
def binary_messages() -> List[BinaryMessage]:
    return [
        BinaryMessage(key=b"foo1", value=b"bar1", partition=0, offset=0),
        BinaryMessage(key=b"foo2", value=b"bar2", partition=0, offset=1),
        BinaryMessage(key=b"foo3", value=b"bar3", partition=1, offset=2),
        BinaryMessage(key=b"foo4", value=b"bar4", partition=1, offset=3),
    ]


@pytest.fixture()
def string_messages(
    binary_messages: List[BinaryMessage], string_message_serializer: MessageSerializer
) -> List[Message]:
    return list(string_message_serializer.deserialize_many(binary_messages))


@pytest.fixture()
def string_serializer() -> StringSerializer:
    return StringSerializer(StringSerializerConfig(scheme="str"))


@pytest.fixture()
def string_message_serializer(string_serializer: StringSerializer) -> MessageSerializer:
    return MessageSerializer(string_serializer)


class DummyMessageReader(HandlerSerializerMessageReader):
    _handler: DummyHandler

    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig(host="", path="", scheme="")),
            message_serializer=MessageSerializer(StringSerializer(StringSerializerConfig(scheme="str"))),
        )

    def set_messages(self, messages: List[BinaryMessage]) -> None:
        self._handler.set_messages(messages)


@pytest.fixture
def dummy_message_reader() -> DummyMessageReader:
    return DummyMessageReader()


class DummyMessageWriter(HandlerSerializerMessageWriter):
    _handler: DummyHandler

    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig(host="", path="", scheme="")),
            message_serializer=MessageSerializer(StringSerializer(StringSerializerConfig(scheme="str"))),
        )

    def get_written_messages(self) -> List[BinaryMessage]:
        return self._handler.get_messages()


@pytest.fixture
def dummy_message_writer() -> DummyMessageWriter:
    return DummyMessageWriter()
