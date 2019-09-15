import json
import pathlib
import random
from contextlib import ExitStack
from glob import glob
from typing import Iterable, List, Tuple
from string import ascii_letters

import pytest
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.avromessage import AvroFileReader
from esque.clients import FileConsumer, AvroFileConsumer, FileProducer, AvroFileProducer
from esque.message import PlainTextFileReader, KafkaMessage
from confluent_kafka.avro import loads as load_schema, AvroProducer


@pytest.mark.integration
def test_plain_text_consume_to_file(
    random_id, producer: ConfluenceProducer, source_topic: Tuple[str, int], tmpdir_factory
):
    source_topic_id, _ = source_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = FileConsumer(random_id, source_topic_id, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    consumed_messages = get_consumed_messages(working_dir, False)

    assert number_of_consumer_messages == 10
    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_avro_consume_to_file(random_id, avro_producer: AvroProducer, source_topic: Tuple[str, int], tmpdir_factory):
    source_topic_id, _ = source_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)
    file_consumer = AvroFileConsumer(random_id, source_topic_id, working_dir, False)
    number_of_consumer_messages = file_consumer.consume(10)

    consumed_messages = get_consumed_messages(working_dir, True)

    assert number_of_consumer_messages == 10
    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_plain_text_consume_and_produce(
    random_id,
    producer: ConfluenceProducer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    tmpdir_factory,
):
    source_topic_id, _ = source_topic
    target_topic_id, _ = target_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = FileConsumer(random_id, source_topic_id, working_dir, False)
    file_consumer.consume(10)

    producer = FileProducer(working_dir)
    producer.produce(target_topic_id)

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = FileConsumer((random_id + "assertion_check"), target_topic_id, assertion_check_directory, False)
    file_consumer.consume(10)

    consumed_messages = get_consumed_messages(assertion_check_directory, False)

    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_avro_consume_and_produce(
    random_id,
    avro_producer: AvroProducer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    tmpdir_factory,
):
    source_topic_id, _ = source_topic
    target_topic_id, _ = target_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)
    file_consumer = AvroFileConsumer(random_id, source_topic_id, working_dir, False)
    file_consumer.consume(10)

    producer = AvroFileProducer(working_dir)
    producer.produce(target_topic_id)

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = AvroFileConsumer(
        (random_id + "assertion_check"), target_topic_id, assertion_check_directory, False
    )
    file_consumer.consume(10)

    consumed_messages = get_consumed_messages(assertion_check_directory, True)

    assert produced_messages == consumed_messages


def produce_test_messages(producer: ConfluenceProducer, topic: Tuple[str, int]) -> Iterable[KafkaMessage]:
    topic_name, num_partitions = topic
    messages = []
    for i in range(10):
        partition = random.randrange(0, num_partitions)
        random_value = "".join(random.choices(ascii_letters, k=5))
        message = KafkaMessage(str(i), random_value, partition)
        messages.append(message)
        producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)
        producer.flush()
    return messages


def produce_test_messages_with_avro(avro_producer: AvroProducer, topic: Tuple[str, int]) -> Iterable[KafkaMessage]:
    topic_name, num_partitions = topic
    with open("tests/test_samples/key_schema.avsc", "r") as file:
        key_schema = load_schema(file.read())
    with open("tests/test_samples/value_schema.avsc", "r") as file:
        value_schema = load_schema(file.read())
    messages = []
    for i in range(10):
        partition = random.randrange(0, num_partitions)
        key = {"id": str(i)}
        value = {"first": "Firstname", "last": "Lastname"}
        messages.append(KafkaMessage(json.dumps(key), json.dumps(value), partition, key_schema, value_schema))
        avro_producer.produce(
            topic=topic_name,
            key=key,
            value=value,
            key_schema=key_schema,
            value_schema=value_schema,
            partition=partition,
        )
        avro_producer.flush()
    return messages


def get_consumed_messages(directory, avro: bool) -> List[KafkaMessage]:
    consumed_messages = []
    path_list = glob(str(directory / "partition_*"))
    with ExitStack() as stack:
        for partition_path in path_list:
            if avro:
                file_reader = AvroFileReader(pathlib.Path(partition_path))
            else:
                file_reader = PlainTextFileReader(pathlib.Path(partition_path))
            stack.enter_context(file_reader)
            for message in file_reader.read_from_file():
                consumed_messages.append(message)
    return sorted(consumed_messages, key=(lambda msg: msg.key))
