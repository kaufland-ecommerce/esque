from typing import List

from esque.io.messages import BinaryMessage


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
    actual_messages = list(input_handler.message_stream())

    assert binary_messages == actual_messages
