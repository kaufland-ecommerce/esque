import functools
import pathlib
from io import StringIO
from typing import Callable

from pytest_cases import fixture

from esque.io.handlers import PathHandler, PipeHandler
from esque.io.handlers.path import PathHandlerConfig
from esque.io.handlers.pipe import PipeHandlerConfig


@fixture
def pipe_handler_stream() -> StringIO:
    return StringIO()


@fixture
def pipe_handler_factory(pipe_handler_stream: StringIO) -> Callable[[], PipeHandler]:
    def _pipe_handler_factory() -> PipeHandler:
        handler = PipeHandler(PipeHandlerConfig(scheme="pipe", host="stdin", path=""))
        handler._stream = pipe_handler_stream
        handler.close = functools.partial(pipe_handler_stream.seek, 0)
        return handler

    return _pipe_handler_factory


@fixture
def path_handler_factory(tmpdir: pathlib.Path) -> Callable[[], PathHandler]:
    def _path_handler_factory() -> PathHandler:
        handler = PathHandler(PathHandlerConfig(scheme="path", host="", path=str(tmpdir)))
        return handler

    return _path_handler_factory
