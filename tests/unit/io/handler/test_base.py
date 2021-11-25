from pytest_cases import parametrize

from esque.io.exceptions import EsqueIOHandlerConfigException
from esque.io.handlers.base import HandlerConfig
from tests.unit.io.conftest import DummyHandler, DummyHandlerConfig


def test_validation_all_fields_missing():
    try:
        DummyHandler(config=DummyHandlerConfig(host=None, path=None, scheme=None))  # noqa
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" in actual_exception.message
    assert "path" in actual_exception.message
    assert "scheme" in actual_exception.message
    assert len(actual_exception.message.splitlines()) == 4


@parametrize(
    argnames="host,path,scheme",
    argvalues=[[None, "pathval", "pipe+str"], ["localhost", None, "pipe+str"], ["localhost", "pathval", None]],
    ids=["host", "path", "scheme"],
)
def test_validation_one_field_missing(host, path, scheme):
    try:
        DummyHandler(config=DummyHandlerConfig(host=host, path=path, scheme=scheme))
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert ("host" in actual_exception.message) == (host is None)
    assert ("path" in actual_exception.message) == (path is None)
    assert ("scheme" in actual_exception.message) == (scheme is None)
    assert len(actual_exception.message.splitlines()) == 2


def test_validation_wrong_config_class_type():
    try:
        DummyHandler(config=HandlerConfig(host="hostval", path="pathval", scheme="pipe+str"))  # noqa
    except EsqueIOHandlerConfigException as e:
        actual_exception = e

    assert "host" not in actual_exception.message
    assert "path" not in actual_exception.message
    assert "HandlerConfig" in actual_exception.message
    assert "DummyHandlerConfig" in actual_exception.message


# TODO create a test for case where serializer config is supported but not persisted
