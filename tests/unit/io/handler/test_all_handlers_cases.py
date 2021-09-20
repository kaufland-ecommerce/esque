import pathlib
from typing import Callable, NamedTuple, Tuple

import pytest

from esque.io.handlers import BaseHandler
from esque.io.handlers.kafka import KafkaHandler, KafkaHandlerConfig
from esque.io.handlers.path import PathHandler, PathHandlerConfig


class HandlerPair(NamedTuple):
    """
    Pair of connected handlers.
    Writing to output_handler will allow reading back the same messages from input_handler.
    """

    input_handler: BaseHandler
    output_handler: BaseHandler


def case_path_handler(tmpdir: pathlib.Path):
    config = PathHandlerConfig(scheme="path", host="", path=str(tmpdir))
    return HandlerPair(PathHandler(config), PathHandler(config))


def case_pipe_handler(pipe_handler_factory):
    return HandlerPair(pipe_handler_factory(), pipe_handler_factory())


@pytest.mark.integration
def case_kafka_handler(topic_id: str, topic_factory: Callable[[int, str], Tuple[str, int]], partition_count: int):
    topic_factory(partition_count, topic_id)
    config = KafkaHandlerConfig(scheme="kafka", host="", path=topic_id, send_timestamp="1")
    return HandlerPair(KafkaHandler(config), KafkaHandler(config))
