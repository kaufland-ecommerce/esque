import pytest

from esque.clients import FileConsumer, FileProducer
from esque.message import KafkaMessage


@pytest.mark.integration
def test_consume_to_file(mocker, file_consumer: FileConsumer):
    file_writer = mocker.patch("esque.message.FileWriter")
    file_writer.__enter__ = mocker.Mock(return_value=None)
    file_writer.__exit__ = mocker.Mock(return_value=None)
    file_writer.write_message_to_file = mocker.Mock()
    file_consumer.file_writer = file_writer
    number_of_consumer_messages = file_consumer.consume(10)

    assert number_of_consumer_messages == 10
    assert file_writer.write_message_to_file.call_count == 10


@pytest.mark.integration
def test_produce_from_file(mocker, file_producer: FileProducer, topic: str):
    file_reader = mocker.patch("esque.message.FileReader")
    file_reader.__enter__ = mocker.Mock(return_value=None)
    file_reader.__exit__ = mocker.Mock(return_value=None)
    file_reader.read_from_file = mocker.Mock()
    file_reader.read_from_file.return_value = [KafkaMessage("key", "value") for _ in range(10)]
    file_producer.file_reader = file_reader

    producer = mocker.Mock()
    producer.produce_message = mocker.Mock()
    file_producer._producer = producer

    file_producer.produce(topic)

    assert producer.produce_message.call_count == 10
