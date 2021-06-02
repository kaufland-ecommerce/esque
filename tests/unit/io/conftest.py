import dataclasses
from typing import Any, Dict, List, Optional

import pytest

from esque.io.exceptions import EsqueIONoMessageLeft
from esque.io.handlers.base import BaseHandler, HandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.pipeline import MessageReader, MessageWriter
from esque.io.serializers.base import MessageSerializer
from esque.io.serializers.string import StringSerializer


@dataclasses.dataclass(frozen=True)
class DummyHandlerConfig(HandlerConfig):
    pass


class DummyHandler(BaseHandler):
    config_cls = DummyHandlerConfig

    def __init__(self, config: DummyHandlerConfig):
        super().__init__(config=config)
        self._messages: List[BinaryMessage] = []
        self._serializer_settings: Optional[Dict[str, Any]] = None

    def get_serializer_settings(self) -> Dict[str, Any]:
        return self._serializer_settings

    def put_serializer_settings(self, settings: Dict[str, Any]) -> None:
        self._serializer_settings = settings

    def write_message(self, binary_message: BinaryMessage) -> None:
        self._messages.append(binary_message)

    def read_message(self) -> Optional[BinaryMessage]:
        if self._messages:
            return self._messages.pop(0)
        else:
            raise EsqueIONoMessageLeft("No messages left in memory")

    def get_messages(self) -> List[BinaryMessage]:
        return self._messages.copy()

    def set_messages(self, messages: List[BinaryMessage]):
        self._messages = messages.copy()


@pytest.fixture
def dummy_handler() -> DummyHandler:
    return DummyHandler(config=DummyHandlerConfig(host="", path=""))


@pytest.fixture()
def binary_messages() -> List[BinaryMessage]:
    return [
        BinaryMessage(key=b"foo1", value=b"bar1", partition=0, offset=0),
        BinaryMessage(key=b"foo2", value=b"bar2", partition=0, offset=1),
        BinaryMessage(key=b"foo3", value=b"bar3", partition=1, offset=2),
        BinaryMessage(key=b"foo4", value=b"bar4", partition=1, offset=3),
    ]


@pytest.fixture()
def string_message_serializer() -> MessageSerializer:
    string_serializer = StringSerializer()
    return MessageSerializer(string_serializer)


class DummyReader(MessageReader):
    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig(host="", path="")), message_serializer=StringSerializer()
        )

    def set_messages(self, messages: List[BinaryMessage]) -> None:
        self._handler.set_messages(messages)


class DummyWriter(MessageWriter):
    def __init__(self):
        super().__init__(
            handler=DummyHandler(config=DummyHandlerConfig(host="", path="")), message_serializer=StringSerializer()
        )

    def get_written_messages(self, messages: List[BinaryMessage]) -> None:
        return self._handler.get_messages(messages)
