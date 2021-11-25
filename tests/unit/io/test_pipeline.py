from typing import List

import pytest

from esque.io.exceptions import ExqueIOInvalidURIException
from esque.io.messages import BinaryMessage
from esque.io.pipeline import Pipeline, PipelineBuilder, UriConfig
from tests.unit.io.conftest import DummyMessageWriter


def test_uri_config_correct_configs_extracted_without_parameters():
    uri_config = UriConfig("pipe+str://stdin")

    assert uri_config.handler_config == {"scheme": "pipe", "host": "stdin", "path": ""}
    assert uri_config.key_serializer_config == {"scheme": "str"}
    assert uri_config.value_serializer_config == {"scheme": "str"}


def test_uri_config_correct_configs_extracted_with_parameters():
    query_str = "&".join(
        [
            "k__keyparam=keyparamvalue",
            "v__valueparam=valueparamvalue",
            "kv__keyvalueparam=keyvalueparamvalue",
            "h__handlerparam=handlerparamvalue",
        ]
    )
    uri_config = UriConfig(f"pipe+str://stdin/somepath?{query_str}")

    assert uri_config.handler_config == {
        "scheme": "pipe",
        "host": "stdin",
        "path": "somepath",
        "handlerparam": "handlerparamvalue",
    }
    assert uri_config.key_serializer_config == {
        "scheme": "str",
        "keyparam": "keyparamvalue",
        "keyvalueparam": "keyvalueparamvalue",
    }
    assert uri_config.value_serializer_config == {
        "scheme": "str",
        "valueparam": "valueparamvalue",
        "keyvalueparam": "keyvalueparamvalue",
    }


def test_uri_config_with_different_serializer_schemes():
    uri_config = UriConfig("pipe+str+avro://stdin")

    assert uri_config.handler_config == {"scheme": "pipe", "host": "stdin", "path": ""}
    assert uri_config.key_serializer_config == {"scheme": "str"}
    assert uri_config.value_serializer_config == {"scheme": "avro"}


def test_uri_config_throws_exception_without_serializer_scheme():
    with pytest.raises(ExqueIOInvalidURIException):
        UriConfig("pipe://stdin")


def test_uri_config_throws_exception_for_duplicate_parameters():
    with pytest.raises(ExqueIOInvalidURIException):
        UriConfig("pipe+str://stdin?h__param=value&h__param=value")

    with pytest.raises(ExqueIOInvalidURIException):
        UriConfig("pipe+str://stdin?k__param=value&k__param=value")

    with pytest.raises(ExqueIOInvalidURIException):
        UriConfig("pipe+str://stdin?v__param=value&v__param=value")

    with pytest.raises(ExqueIOInvalidURIException):
        UriConfig("pipe+str://stdin?kv__param=value&kv__param=value")


def test_pipeline_without_special_decorators_runs_successfully(
    dummy_message_writer: DummyMessageWriter, binary_messages: List[BinaryMessage], prepared_builder: PipelineBuilder
):
    pipeline = prepared_builder.build()

    assert isinstance(pipeline, Pipeline)
    pipeline.run_pipeline()
    assert dummy_message_writer.get_written_messages() == binary_messages


def test_limited_read_with_absolute_offset(
    dummy_message_writer: DummyMessageWriter, binary_messages: List[BinaryMessage], prepared_builder: PipelineBuilder
):
    prepared_builder.with_range(start=1, limit=1)
    pipeline = prepared_builder.build()

    assert isinstance(pipeline, Pipeline)
    pipeline.run_pipeline()
    assert len(dummy_message_writer.get_written_messages()) == 1
    assert dummy_message_writer.get_written_messages()[0] in [msg for msg in binary_messages if msg.offset >= 1]


@pytest.mark.xfail(reason="Not yet implemented")
def test_limited_read_with_relative_offset_from_end(
    dummy_message_writer: DummyMessageWriter, binary_messages: List[BinaryMessage], prepared_builder: PipelineBuilder
):
    prepared_builder.with_range(start=-2, limit=1)
    pipeline = prepared_builder.build()

    assert isinstance(pipeline, Pipeline)
    pipeline.run_pipeline()
    assert dummy_message_writer.get_written_messages() == binary_messages[-2:-1]
