from typing import List

import pytest
from pytest_cases import fixture

from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState
from esque.io.messages import BinaryMessage, Message
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers.base import MessageSerializer
from esque.io.stream_decorators import skip_stream_events
from tests.unit.io.conftest import DummyHandler, DummyMessageReader, DummyMessageWriter


@fixture
def output_uri() -> str:
    return "pipe+str://stdout"


@fixture
def input_uri() -> str:
    return "pipe+str://stdin"


def test_create_empty_builder():
    pipeline_builder: PipelineBuilder = PipelineBuilder()
    pipeline_builder.build()


def test_create_pipeline_with_handler_and_serializer_input(
    dummy_handler: DummyHandler,
    string_message_serializer: MessageSerializer,
    binary_messages: List[BinaryMessage],
    string_messages: List[Message],
):
    builder = PipelineBuilder()
    builder.with_input_handler(dummy_handler)
    builder.with_input_message_serializer(string_message_serializer)
    builder.with_stream_decorator(skip_stream_events)
    pipeline = builder.build()

    dummy_handler.set_messages(binary_messages)

    assert list(pipeline.decorated_message_stream()) == string_messages


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


def test_create_pipeline_with_message_reader(
    dummy_message_reader: DummyMessageReader, binary_messages: List[BinaryMessage], string_messages: List[Message]
):
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_stream_decorator(skip_stream_events)
    pipeline = builder.build()

    dummy_message_reader.set_messages(binary_messages)
    assert list(pipeline.decorated_message_stream()) == string_messages


def test_create_pipeline_with_message_writer(
    dummy_message_writer, binary_messages: List[BinaryMessage], string_messages: List[Message]
):
    builder = PipelineBuilder()
    builder.with_message_writer(dummy_message_writer)
    pipeline = builder.build()

    pipeline.write_many_messages(string_messages)

    assert dummy_message_writer.get_written_messages() == binary_messages


def test_build_fails_with_only_handler(dummy_handler):
    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_input_handler(dummy_handler).build()

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_output_handler(dummy_handler).build()


def test_build_fails_with_only_serializer(string_message_serializer: MessageSerializer):
    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_input_message_serializer(string_message_serializer).build()

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_output_message_serializer(string_message_serializer).build()


def test_build_fails_with_message_reader_and_serializer(
    dummy_message_reader: DummyMessageReader, string_message_serializer: MessageSerializer
):
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_input_message_serializer(string_message_serializer)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_reader_and_handler(
    dummy_message_reader: DummyMessageReader, dummy_handler: DummyHandler
):
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_input_handler(dummy_handler)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_reader_serializer_and_handler(
    dummy_message_reader: DummyMessageReader, string_message_serializer: MessageSerializer, dummy_handler: DummyHandler
):
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_input_message_serializer(string_message_serializer)
    builder.with_input_handler(dummy_handler)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_reader_and_uri(
    dummy_message_reader: DummyMessageReader,
    string_message_serializer: MessageSerializer,
    dummy_handler: DummyHandler,
    input_uri: str,
):
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    builder.with_input_from_uri(input_uri)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_writer_and_serializer(
    dummy_message_writer: DummyMessageWriter, string_message_serializer: MessageSerializer
):
    builder = PipelineBuilder()
    builder.with_message_writer(dummy_message_writer)
    builder.with_input_message_serializer(string_message_serializer)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_writer_and_handler(
    dummy_message_writer: DummyMessageWriter, dummy_handler: DummyHandler
):
    builder = PipelineBuilder()
    builder.with_message_writer(dummy_message_writer)
    builder.with_input_handler(dummy_handler)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_writer_serializer_and_handler(
    dummy_message_writer: DummyMessageWriter, string_message_serializer: MessageSerializer, dummy_handler: DummyHandler
):
    builder = PipelineBuilder()
    builder.with_message_writer(dummy_message_writer)
    builder.with_output_message_serializer(string_message_serializer)
    builder.with_output_handler(dummy_handler)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()


def test_build_fails_with_message_writer_and_uri(
    dummy_message_writer: DummyMessageWriter,
    string_message_serializer: MessageSerializer,
    dummy_handler: DummyHandler,
    output_uri: str,
):
    builder = PipelineBuilder()
    builder.with_message_writer(dummy_message_writer)
    builder.with_output_from_uri(output_uri)

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        builder.build()
