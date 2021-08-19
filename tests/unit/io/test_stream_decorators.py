from typing import List

import pytest

from esque.io.messages import BinaryMessage, Message
from esque.io.stream_decorators import stop_at_permanent_end_of_stream, stop_at_temporary_end_of_stream
from tests.unit.io.conftest import DummyHandler


def test_stop_at_temporary_end_of_stream_with_temporary_decorator(
    binary_messages: List[BinaryMessage], dummy_handler: DummyHandler
):
    dummy_handler.set_messages(messages=binary_messages)
    temporarily_ended_stream = stop_at_temporary_end_of_stream(dummy_handler.message_stream())
    dummy_handler.insert_temporary_end_of_stream(2)
    assert list(temporarily_ended_stream) == binary_messages[:2]


def test_stop_at_permanent_end_of_stream_with_temporary_decorator(
    binary_messages: List[BinaryMessage], dummy_handler: DummyHandler
):
    dummy_handler.set_messages(messages=binary_messages)
    temporarily_ended_stream = stop_at_temporary_end_of_stream(dummy_handler.message_stream())
    assert list(temporarily_ended_stream) == binary_messages


def test_stop_at_permanent_end_of_stream_with_permanent_decorator(
    binary_messages: List[BinaryMessage], dummy_handler: DummyHandler
):
    dummy_handler.set_messages(messages=binary_messages)
    permanently_ended_stream = stop_at_permanent_end_of_stream(dummy_handler.message_stream())
    assert list(permanently_ended_stream) == binary_messages


def test_not_stopping_at_temporary_end_of_stream_with_permanent_decorator(
        binary_messages: List[BinaryMessage], dummy_handler: DummyHandler
):
    dummy_handler.set_messages(messages=binary_messages)
    permanently_ended_stream = stop_at_permanent_end_of_stream(dummy_handler.message_stream())
    dummy_handler.insert_temporary_end_of_stream(2)
    assert list(permanently_ended_stream) == binary_messages
#
# def test_reading_until_timeout():
#     assert False
#
#
# def test_reading_until_count_reached():
#     assert False
