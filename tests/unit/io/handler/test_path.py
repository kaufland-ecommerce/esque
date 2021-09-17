import pathlib
from typing import List

from esque.io.handlers.path import PathHandler, PathHandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import skip_stream_events


def mk_pipe_handler(path: pathlib.Path) -> PathHandler:
    handler = PathHandler(PathHandlerConfig(scheme="path", host="", path=str(path)))
    return handler


def test_write_read_message(tmpdir: pathlib.Path, binary_messages: List[BinaryMessage]):
    binary_message = binary_messages[0]

    output_handler = mk_pipe_handler(tmpdir)
    output_handler.write_message(binary_message)

    input_handler = mk_pipe_handler(tmpdir)
    actual_message = input_handler.read_message()

    assert binary_message == actual_message


def test_write_read_many_messages(tmpdir: pathlib.Path, binary_messages: List[BinaryMessage]):
    output_handler = mk_pipe_handler(tmpdir)
    output_handler.write_many_messages(binary_messages)

    input_handler = mk_pipe_handler(tmpdir)
    actual_messages = list(skip_stream_events(input_handler.binary_message_stream()))

    assert binary_messages == actual_messages
