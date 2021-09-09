from io import StringIO
from typing import List

import pytest

from esque.io.exceptions import EsqueIOHandlerReadException
from esque.io.handlers.pipe import PipeHandler, PipeHandlerConfig
from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import skip_stream_events

# TODO create tests for these additional scenarios:
# - broken pipe
# - the stream is still open, but no new data is coming in (temporary end)


def mk_pipe_handler(stream: StringIO) -> PipeHandler:
    handler = PipeHandler(PipeHandlerConfig(scheme="pipe", host="stdin", path=""))
    handler._stream = stream
    return handler


def test_write_read_message(binary_messages: List[BinaryMessage]):
    binary_message = binary_messages[0]

    stream = StringIO()
    output_handler = mk_pipe_handler(stream)
    output_handler.write_message(binary_message)

    stream.seek(0)
    input_handler = mk_pipe_handler(stream)
    actual_message = input_handler.read_message()

    assert binary_message == actual_message


def test_write_read_many_messages(binary_messages: List[BinaryMessage]):
    stream = StringIO()
    output_handler = mk_pipe_handler(stream)
    output_handler.write_many_messages(binary_messages)

    stream.seek(0)
    input_handler = mk_pipe_handler(stream)
    actual_messages = list(skip_stream_events(input_handler.binary_message_stream()))

    assert binary_messages == actual_messages


def test_write_read_last_message_incomplete():
    stream = StringIO("incomplete message\n")

    stream.seek(0)
    input_handler = mk_pipe_handler(stream)
    with pytest.raises(EsqueIOHandlerReadException):
        _ = input_handler.read_message()
