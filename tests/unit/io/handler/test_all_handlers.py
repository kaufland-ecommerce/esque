from operator import attrgetter
from typing import List

from pytest_cases import parametrize_with_cases

from esque.io.handlers import BaseHandler
from esque.io.messages import BinaryMessage
from esque.io.stream_decorators import skip_stream_events, stop_at_temporary_end_of_all_stream_partitions
from esque.io.stream_events import StreamEvent, TemporaryEndOfPartition


@parametrize_with_cases("input_handler, output_handler")
def test_write_read_message(
    binary_messages: List[BinaryMessage], input_handler: BaseHandler, output_handler: BaseHandler
):
    for msg in binary_messages[:2]:
        output_handler.write_message(msg)
    output_handler.close()

    messages_retrieved: List[BinaryMessage] = []
    for _ in range(2):
        while True:
            actual_message = input_handler.read_message()
            if isinstance(actual_message, BinaryMessage):
                break
        messages_retrieved.append(actual_message)

    messages_retrieved.sort(key=attrgetter("timestamp"))
    assert messages_retrieved == binary_messages[:2]


@parametrize_with_cases("input_handler, output_handler")
def test_write_read_many_messages(
    binary_messages: List[BinaryMessage], input_handler: BaseHandler, output_handler: BaseHandler
):
    output_handler.write_many_messages(binary_messages)
    output_handler.close()
    actual_messages = list(
        skip_stream_events(stop_at_temporary_end_of_all_stream_partitions(input_handler.binary_message_stream()))
    )
    actual_messages.sort(key=attrgetter("timestamp"))
    input_handler.close()
    assert binary_messages == actual_messages


@parametrize_with_cases("_, output_handler")
def test_write_single_stream_event(binary_messages: List[BinaryMessage], output_handler: BaseHandler, _):
    output_handler.write_message(TemporaryEndOfPartition("test", StreamEvent.ALL_PARTITIONS))


@parametrize_with_cases("_, output_handler")
def test_write_many_stream_events(binary_messages: List[BinaryMessage], output_handler: BaseHandler, _):
    output_handler.write_many_messages([TemporaryEndOfPartition("test", StreamEvent.ALL_PARTITIONS)])


@parametrize_with_cases("input_handler, output_handler")
def test_seek(binary_messages: List[BinaryMessage], input_handler: BaseHandler, output_handler: BaseHandler):
    seek_offset = 2
    output_handler.write_many_messages(binary_messages)
    output_handler.close()

    input_handler.seek(seek_offset)
    actual_messages = list(
        skip_stream_events(stop_at_temporary_end_of_all_stream_partitions(input_handler.binary_message_stream()))
    )
    input_handler.close()

    actual_messages.sort(key=attrgetter("timestamp"))
    assert actual_messages == [msg for msg in binary_messages if msg.offset >= seek_offset]
