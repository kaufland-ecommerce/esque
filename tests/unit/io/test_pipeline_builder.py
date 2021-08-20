from typing import List

from esque.io.messages import BinaryMessage, Message
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers.base import MessageSerializer
from esque.io.stream_decorators import skip_stream_events
from tests.unit.io.conftest import DummyHandler


def test_create_empty_builder():
    pipeline_builder: PipelineBuilder = PipelineBuilder()
    pipeline_builder.build()


def test_create_pipeline_with_handler_and_serializer_input(
    dummy_handler: DummyHandler,
    string_message_serializer: MessageSerializer,
    binary_messages: List[BinaryMessage],
):
    builder = PipelineBuilder()
    builder.with_input_handler(dummy_handler)
    builder.with_input_message_serializer(string_message_serializer)
    builder.with_stream_decorator(skip_stream_events)
    pipeline = builder.build()

    dummy_handler.set_messages(binary_messages)

    assert list(pipeline.decorated_message_stream()) == binary_messages


def test_create_pipeline_with_handler_and_serializer_output(
    dummy_handler: DummyHandler,
    string_message_serializer: MessageSerializer,
    binary_messages: List[BinaryMessage],
    string_messages: List[Message],
):
    builder = PipelineBuilder()
    builder.with_output_handler(dummy_handler)
    builder.with_output_message_serializer(string_message_serializer)
    pipeline = builder.build()

    pipeline.write_many_messages(string_messages)
    assert dummy_handler.get_messages() == binary_messages
