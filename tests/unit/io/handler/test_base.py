from esque.io.exceptions import EsqueIOHandlerConfigException
from esque.io.handlers.base import HandlerConfig
from tests.unit.io.conftest import DummyHandler, DummyHandlerConfig


def test_validation_all_fields_missing():
    try:
        DummyHandler(config=DummyHandlerConfig(host=None, path=None))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" in actual_exception.message
    assert "path" in actual_exception.message
    assert len(actual_exception.message.splitlines()) == 3


def test_validation_one_field_missing():
    try:
        DummyHandler(config=DummyHandlerConfig(host=None, path="pathval"))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" in actual_exception.message
    assert "path" not in actual_exception.message
    assert len(actual_exception.message.splitlines()) == 2


def test_validation_wrong_settings_class_type():
    try:
        DummyHandler(config=HandlerConfig(host="hostval", path="pathval"))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" not in actual_exception.message
    assert "path" not in actual_exception.message
    assert "HandlerSettings" in actual_exception.message
    assert "DummyHandlerSettings" in actual_exception.message
