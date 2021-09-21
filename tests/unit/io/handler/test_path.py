from typing import Callable, List

import pytest
from pytest_cases import fixture

from esque.io.exceptions import EsqueIOHandlerWriteException
from esque.io.handlers import PathHandler
from esque.io.messages import BinaryMessage


@fixture
def path_handler(path_handler_factory) -> PathHandler:
    return path_handler_factory()


def test_non_empty_directory_raises_exception_upon_write(
    path_handler_factory: Callable[[], PathHandler], binary_messages: List[BinaryMessage]
):
    path_handler = path_handler_factory()
    path_handler.write_message(binary_messages[0])
    path_handler.close()

    path_handler = path_handler_factory()
    with pytest.raises(EsqueIOHandlerWriteException):
        path_handler.write_message(binary_messages[0])


def test_handler_doesnt_allow_read_after_write(
    path_handler_factory: Callable[[], PathHandler], binary_messages: List[BinaryMessage]
):
    path_handler = path_handler_factory()
    path_handler.write_many_messages(binary_messages)
    with pytest.raises(EsqueIOHandlerWriteException):
        path_handler.read_message()


def test_handler_doesnt_allow_write_after_read(
    path_handler_factory: Callable[[], PathHandler], binary_messages: List[BinaryMessage]
):
    path_handler = path_handler_factory()
    path_handler.write_many_messages(binary_messages)
    path_handler.close()

    path_handler = path_handler_factory()
    path_handler.read_message()
    with pytest.raises(EsqueIOHandlerWriteException):
        path_handler.write_message(binary_messages[0])
