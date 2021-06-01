import dataclasses
from typing import Any, Dict, List, Optional

from esque.io.exceptions import EsqueIOHandlerConfigException, EsqueIONoMessageLeft
from esque.io.handlers.base import BaseHandler, HandlerSettings
from esque.io.messages import BinaryMessage


@dataclasses.dataclass
class DummyHandlerSettings(HandlerSettings):
    pass


class DummyHandler(BaseHandler):
    settings_cls = DummyHandlerSettings

    def __init__(self, settings: DummyHandlerSettings):
        super().__init__(settings=settings)
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


def test_validation_all_fields_missing():
    try:
        DummyHandler(settings=DummyHandlerSettings(host=None, path=None))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" in actual_exception.message
    assert "path" in actual_exception.message
    assert len(actual_exception.message.splitlines()) == 3


def test_validation_one_field_missing():
    try:
        DummyHandler(settings=DummyHandlerSettings(host=None, path="pathval"))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" in actual_exception.message
    assert "path" not in actual_exception.message
    assert len(actual_exception.message.splitlines()) == 2


def test_validation_wrong_settings_class_type():
    try:
        DummyHandler(settings=HandlerSettings(host="hostval", path="pathval"))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" not in actual_exception.message
    assert "path" not in actual_exception.message
    assert "HandlerSettings" in actual_exception.message
    assert "DummyHandlerSettings" in actual_exception.message
