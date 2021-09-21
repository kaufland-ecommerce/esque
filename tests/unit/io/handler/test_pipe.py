from io import StringIO
from typing import Callable

import pytest
from pytest_cases import fixture

from esque.io.exceptions import EsqueIOHandlerReadException
from esque.io.handlers.pipe import PipeHandler

# TODO create tests for these additional scenarios:
# - broken pipe
# - the stream is still open, but no new data is coming in (temporary end)


@fixture
def pipe_handler(pipe_handler_factory: Callable[[], PipeHandler]) -> PipeHandler:
    return pipe_handler_factory()


def test_write_read_last_message_incomplete(pipe_handler: PipeHandler, pipe_handler_stream: StringIO):
    pipe_handler_stream.write("incomplete message\n")
    pipe_handler_stream.seek(0)
    with pytest.raises(EsqueIOHandlerReadException):
        _ = pipe_handler.read_message()
