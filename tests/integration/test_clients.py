import json
import random
from typing import Iterable
from string import ascii_letters

import pytest
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.avromessage import AvroFileReader
from esque.clients import FileConsumer, AvroFileConsumer, FileProducer, AvroFileProducer
from esque.message import PlainTextFileReader, KafkaMessage
from confluent_kafka.avro import loads as load_schema, AvroProducer


@pytest.mark.integration
def test_plain_text_consume_to_file(consumer_group, producer: ConfluenceProducer, source_topic: str, tmpdir_factory):
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = FileConsumer(consumer_group, source_topic, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    consumed_messages = []
    file_reader = PlainTextFileReader(working_dir)
    with file_reader:
        for message in file_reader.read_from_file():
            consumed_messages.append(message)

    assert number_of_consumer_messages == 10
    assert len(consumed_messages) == 10
    assert all(
        [
            produced_message.key == consumed_message.key and produced_message.value == consumed_message.value
            for produced_message, consumed_message in zip(produced_messages, consumed_messages)
        ]
    )


@pytest.mark.integration
def test_avro_consume_to_file(consumer_group, avro_producer: AvroProducer, source_topic: str, tmpdir_factory):
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)
    file_consumer = AvroFileConsumer(consumer_group, source_topic, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    consumed_messages = []
    file_reader = AvroFileReader(working_dir)
    with file_reader:
        for message in file_reader.read_from_file():
            consumed_messages.append(message)

    assert number_of_consumer_messages == 10
    assert len(consumed_messages) == 10
    assert all(
        [
            produced_message.key == consumed_message.key and produced_message.value == consumed_message.value
            for produced_message, consumed_message in zip(produced_messages, consumed_messages)
        ]
    )


@pytest.mark.integration
def test_plain_text_consume_and_produce(
    consumer_group, producer: ConfluenceProducer, source_topic: str, target_topic: str, tmpdir_factory
):
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = FileConsumer(consumer_group, source_topic, working_dir, False)
    file_consumer.consume(10)

    producer = FileProducer(working_dir)
    producer.produce(target_topic)

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = FileConsumer((consumer_group + "assertion_check"), target_topic, assertion_check_directory, False)
    file_consumer.consume(10)

    consumed_messages = []
    file_reader = PlainTextFileReader(assertion_check_directory)
    with file_reader:
        for message in file_reader.read_from_file():
            consumed_messages.append(message)

    assert all(
        [
            produced_message.key == consumed_message.key and produced_message.value == consumed_message.value
            for produced_message, consumed_message in zip(produced_messages, consumed_messages)
        ]
    )


@pytest.mark.integration
def test_avro_consume_and_produce(
    consumer_group, avro_producer: AvroProducer, source_topic: str, target_topic: str, tmpdir_factory
):
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)
    file_consumer = AvroFileConsumer(consumer_group, source_topic, working_dir, False)
    file_consumer.consume(10)

    producer = AvroFileProducer(working_dir)
    producer.produce(target_topic)

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = AvroFileConsumer(
        (consumer_group + "assertion_check"), target_topic, assertion_check_directory, False
    )
    file_consumer.consume(10)

    consumed_messages = []
    file_reader = AvroFileReader(assertion_check_directory)
    with file_reader:
        for message in file_reader.read_from_file():
            consumed_messages.append(message)

    assert all(
        [
            produced_message.key == consumed_message.key and produced_message.value == consumed_message.value
            for produced_message, consumed_message in zip(produced_messages, consumed_messages)
        ]
    )


def produce_test_messages(producer: ConfluenceProducer, topic: str) -> Iterable[KafkaMessage]:
    messages = []
    for i in range(10):
        random_value = "".join(random.choices(ascii_letters, k=5))
        message = KafkaMessage(str(i), random_value)
        messages.append(message)
        producer.produce(topic=topic, key=message.key, value=message.value)
        producer.flush()
    return messages


def produce_test_messages_with_avro(avro_producer: AvroProducer, topic: str) -> Iterable[KafkaMessage]:
    with open("tests/test_samples/key_schema.avsc", "r") as file:
        key_schema = load_schema(file.read())
    with open("tests/test_samples/value_schema.avsc", "r") as file:
        value_schema = load_schema(file.read())
    messages = []
    for i in range(10):
        key = {"id": str(i)}
        value = {"first": "Firstname", "last": "Lastname"}
        messages.append(KafkaMessage(json.dumps(key), json.dumps(value), key_schema, value_schema))
        avro_producer.produce(topic=topic, key=key, value=value, key_schema=key_schema, value_schema=value_schema)
        avro_producer.flush()
    return messages
