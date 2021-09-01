from esque.config import Config
from esque.io.handlers.pipe import PipeHandler
from esque.io.pipeline import MessageReader, MessageWriter, Pipeline
from esque.io.serializers.string import StringSerializer
from tests.unit.io.conftest import DummyMessageWriter


def test_create_reader_from_uri(unittest_config):
    reader = MessageReader.from_url(unittest_config, "pipe+str://stdin")

    assert isinstance(reader._message_serializer._key_serializer, StringSerializer)
    assert isinstance(reader._message_serializer._value_serializer, StringSerializer)
    assert isinstance(reader._handler, PipeHandler)


def test_create_writer_from_url(unittest_config):
    writer = MessageWriter.from_url(unittest_config, "pipe+str://stdout")

    assert isinstance(writer._message_serializer._key_serializer, StringSerializer)
    assert isinstance(writer._message_serializer._value_serializer, StringSerializer)
    assert isinstance(writer._handler, PipeHandler)


def test_create_pipeline(dummy_reader, dummy_writer, unittest_config: Config, binary_messages):
    dummy_reader.set_messages(binary_messages)

    pipeline = dummy_reader >> dummy_writer
    assert isinstance(pipeline, Pipeline)
    pipeline.run()
    assert dummy_writer.get_written_messages() == binary_messages


def test_message_limit():
    pass


def test_read_first():
    pass


def test_read_last():
    pass


def test_serializer_scheme_present_in_config(dummy_writer: DummyMessageWriter):
    dummy_writer.write_serializer_configs()

    key_serializer_config, value_serializer_config = dummy_writer.handler.get_serializer_configs()
    assert "scheme" in key_serializer_config
    assert "scheme" in value_serializer_config
