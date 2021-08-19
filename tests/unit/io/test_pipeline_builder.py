from typing import List

import pytest

from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState
from esque.io.messages import BinaryMessage, Message
from esque.io.pipeline import PipelineBuilder
from esque.io.serializers.base import MessageSerializer
from tests.unit.io.conftest import DummyHandler, DummyMessageReader


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
    pipeline = builder.build()

    dummy_handler.set_messages(binary_messages)
    #TODO continue here
    assert list(stop_at_end_of_stream(pipeline.message_stream())) == string_messages


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
    dummy_message_reader: DummyMessageReader,
    binary_messages: List[BinaryMessage],
    string_messages: List[Message],
):
    builder = PipelineBuilder()
    builder.with_message_reader(dummy_message_reader)
    pipeline = builder.build()

    dummy_message_reader.set_messages(binary_messages)
    assert list(pipeline.message_stream()) == string_messages


def test_create_pipeline_with_message_writer(dummy_message_writer):
    builder = PipelineBuilder()
    builder.with_message_writer(dummy_message_writer)
    builder.build()


def test_build_fails_with_only_handler(dummy_handler):
    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_input_handler(dummy_handler).build()

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_output_handler(dummy_handler).build()


def test_build_fails_with_only_serializer(string_message_serializer):
    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_input_message_serializer(string_message_serializer).build()

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_output_message_serializer(string_message_serializer).build()
