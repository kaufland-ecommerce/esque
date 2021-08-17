import pytest

from esque.io.exceptions import EsqueIOInvalidPipelineBuilderState
from esque.io.pipeline import PipelineBuilder


def test_create_empty_builder():
    pipeline_builder: PipelineBuilder = PipelineBuilder()
    pipeline_builder.build()


def test_create_pipeline_with_avro_reader():
    pipeline_builder: PipelineBuilder = PipelineBuilder()
    pipeline_builder = pipeline_builder.with_input_handler()


def test_build_fails_with_only_handler(dummy_handler):
    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_input_handler(dummy_handler).build()

    with pytest.raises(EsqueIOInvalidPipelineBuilderState):
        PipelineBuilder().with_output_handler(dummy_handler).build()


def test_build_fails_with_only_serializer():
    assert False
