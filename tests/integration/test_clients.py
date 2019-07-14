import pathlib

import pytest

from esque.avromessage import AvroFileReader
from esque.clients import FileConsumer, AvroFileConsumer, FileProducer, AvroFileProducer
from esque.message import PlainTextFileReader
from esque.topic import Topic


@pytest.mark.integration
def test_plain_text_consume_to_file(consumer_group, filled_topic: Topic, working_dir: pathlib.Path):
    file_consumer = FileConsumer(consumer_group, filled_topic.name, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    messages = []
    file_reader = PlainTextFileReader(working_dir)
    with file_reader:
        for message in file_reader.read_from_file():
            messages.append({"key": message.key, "value": message.value})

    assert number_of_consumer_messages == 10
    assert len(messages) == 10


@pytest.mark.integration
def test_avro_consume_to_file(consumer_group, filled_avro_topic: Topic, working_dir: pathlib.Path):
    file_consumer = AvroFileConsumer(consumer_group, filled_avro_topic.name, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    messages = []
    file_reader = AvroFileReader(working_dir)
    with file_reader:
        for message in file_reader.read_from_file():
            messages.append({"key": message.key, "value": message.value})

    assert number_of_consumer_messages == 10
    assert len(messages) == 10


@pytest.mark.integration
def test_plain_text_consume_and_produce(consumer_group, filled_topic: Topic, topic: str, working_dir: pathlib.Path):
    file_consumer = FileConsumer(consumer_group, filled_topic.name, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    producer = FileProducer(working_dir)
    number_of_produced_messages = producer.produce(topic)

    assert number_of_consumer_messages == 10
    assert number_of_produced_messages == 10


@pytest.mark.integration
def test_avro_consume_and_produce(consumer_group, filled_avro_topic: Topic, topic: str, working_dir: pathlib.Path):
    file_consumer = AvroFileConsumer(consumer_group, filled_avro_topic.name, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    producer = AvroFileProducer(working_dir)
    number_of_produced_messages = producer.produce(topic)

    assert number_of_consumer_messages == 10
    assert number_of_produced_messages == 10
