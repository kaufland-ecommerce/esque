from typing import Any, Dict, List, Optional, Tuple

from esque.io.exceptions import EsqueIOHandlerConfigException
from esque.io.handlers.base import BaseHandler
from esque.io.messages import BinaryMessage


class DummyHandler(BaseHandler):
    def _get_required_field_specs(self) -> List[Tuple[str, type]]:
        return [("test_int", int), ("test_str", str), ("test_tuple", tuple)]

    def get_serializer_settings(self) -> Dict[str, Any]:
        pass

    def put_serializer_settings(self, settings: Dict[str, Any]) -> None:
        pass

    def write_message(self, binary_message: BinaryMessage) -> None:
        pass

    def read_message(self) -> Optional[BinaryMessage]:
        pass


def test_validation_all_fields_missing():
    try:
        DummyHandler({})
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "test_int" in actual_exception.message
    assert "test_str" in actual_exception.message
    assert "test_tuple" in actual_exception.message


def test_validation_one_field_missing_one_field_wrong_type():
    try:
        DummyHandler({"test_int": 1, "test_tuple": "a"})
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    header_msg, first_msg, second_msg = actual_exception.message.splitlines()
    assert "DummyHandler" in header_msg

    assert "test_str" in first_msg

    assert "test_tuple" in second_msg
    assert "tuple" in second_msg.replace("test_tuple", "")
    assert "str" in second_msg
