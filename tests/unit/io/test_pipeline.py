from esque.config import Config
from esque.io.handlers.pipe import PipeHandler
from esque.io.messages import Message
from esque.io.serializers.string import StringSerializer


def test_create_reader_from_url(unittest_config):
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


def test_message_filter(dummy_reader, dummy_writer, string_message_serializer, binary_messages):
    dummy_reader.set_messages(binary_messages)

    def filter_func(msg: Message) -> bool:
        return msg.partition == 0

    filter = MessageFilter(filter_func)
    filtered_reader = dummy_reader >> filter
    assert isinstance(filtered_reader, MessageReader)

    pipeline = filtered_reader >> dummy_writer
    assert isinstance(pipeline, Pipeline)
    pipeline.run()

    assert dummy_writer.get_written_messages() == [msg for msg in binary_messages if filter.matches(msg)]


def test_message_limit():
    pass


def test_read_first():
    pass


def test_read_last():
    pass
