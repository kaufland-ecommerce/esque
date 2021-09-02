from typing import List

from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import skip_stream_events, stop_after_nth_message, stop_at_temporary_end_of_stream
from tests.unit.io.conftest import DummyHandler


def test_stop_at_temporary_end_of_stream_with_temporary_end(
    binary_messages: List[BinaryMessage], dummy_handler: DummyHandler
):
    dummy_handler.set_messages(messages=binary_messages)
    temporarily_ended_stream = stop_at_temporary_end_of_stream(dummy_handler.binary_message_stream())
    dummy_handler.insert_temporary_end_of_stream(2)
    assert list(skip_stream_events(temporarily_ended_stream)) == binary_messages[:2]


def test_stop_at_temporary_end_of_stream_with_permanent_end(
    binary_messages: List[BinaryMessage], dummy_handler: DummyHandler
):
    dummy_handler.set_messages(messages=binary_messages)
    temporarily_ended_stream = stop_at_temporary_end_of_stream(dummy_handler.binary_message_stream())
    assert list(skip_stream_events(temporarily_ended_stream)) == binary_messages


def test_reading_until_count_reached(binary_messages: List[BinaryMessage], dummy_handler: DummyHandler):
    dummy_handler.set_messages(messages=binary_messages)
    dummy_handler.insert_temporary_end_of_stream(1)
    limit_ended_stream = stop_after_nth_message(dummy_handler.binary_message_stream(), 2)
    assert list(skip_stream_events(limit_ended_stream)) == binary_messages[:2]
