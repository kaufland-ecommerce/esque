from typing import List

from pytest_cases import parametrize_with_cases

from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import (
    MessageStream,
    skip_messages_with_offset_below,
    skip_stream_events,
    stop_after_nth_message,
    stop_at_temporary_end_of_stream,
    yield_messages_sorted_by_timestamp,
)
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
    limit_ended_stream = stop_after_nth_message(2)(dummy_handler.binary_message_stream())
    assert list(skip_stream_events(limit_ended_stream)) == binary_messages[:2]


def test_skip_messages_with_offset_below(binary_messages: List[BinaryMessage], dummy_handler: DummyHandler):
    dummy_handler.set_messages(messages=binary_messages)
    stream_with_skipped_messages = skip_messages_with_offset_below(2)(dummy_handler.binary_message_stream())
    assert list(skip_stream_events(stream_with_skipped_messages)) == [
        msg for msg in binary_messages if msg.offset >= 2
    ]


@parametrize_with_cases("partition_count, input_stream, expected_output", cases=".message_sort_cases")
def test_yield_messages_sorted_by_timestamp(
    partition_count: int, input_stream: MessageStream, expected_output: MessageStream
):
    actual_output = yield_messages_sorted_by_timestamp(partition_count)(input_stream)

    assert list(actual_output) == list(expected_output)
