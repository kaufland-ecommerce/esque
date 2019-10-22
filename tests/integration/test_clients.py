import json
import pathlib
import random
import time
from contextlib import ExitStack
from glob import glob
from string import ascii_letters
from typing import Iterable
from typing import List
from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import loads as load_schema
from confluent_kafka.cimpl import Producer as ConfluenceProducer
from esque.cli.commands import produce

from esque.cli.commands import consume

from esque.cli.commands import _consume_to_file_ordered
from esque.clients.consumer import ConsumerFactory
from esque.clients.producer import ProducerFactory
from esque.messages.avromessage import AvroFileReader
from esque.messages.message import KafkaMessage
from esque.messages.message import PlainTextFileReader


@pytest.mark.integration
def test_plain_text_consume_to_file(
    consumer_group, producer: ConfluenceProducer, source_topic: Tuple[str, int], tmpdir_factory
):
    source_topic_id, _ = source_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = ConsumerFactory().create_consumer(consumer_group, source_topic_id, working_dir, False, avro=False)
    number_of_consumer_messages = file_consumer.consume(10)

    consumed_messages = get_consumed_messages(working_dir, False)

    assert number_of_consumer_messages == 10
    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_avro_consume_to_file(
    consumer_group, avro_producer: AvroProducer, source_topic: Tuple[str, int], tmpdir_factory
):
    source_topic_id, _ = source_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)
    file_consumer = ConsumerFactory().create_consumer(consumer_group, source_topic_id, working_dir, False, avro=True)
    number_of_consumer_messages = file_consumer.consume(10)

    consumed_messages = get_consumed_messages(working_dir, True)

    assert number_of_consumer_messages == 10
    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_plain_text_consume_and_produce(
    consumer_group,
    producer: ConfluenceProducer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    tmpdir_factory,
):
    source_topic_id, _ = source_topic
    target_topic_id, _ = target_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = ConsumerFactory().create_consumer(consumer_group, source_topic_id, working_dir, False, avro=False)
    file_consumer.consume(10)

    producer = ProducerFactory().create_producer(target_topic_id, working_dir, avro=False)
    producer.produce()

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = ConsumerFactory().create_consumer(
        (consumer_group + "assertion_check"), target_topic_id, assertion_check_directory, False, avro=False
    )
    file_consumer.consume(10)

    consumed_messages = get_consumed_messages(assertion_check_directory, False)

    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_avro_consume_and_produce(
    consumer_group,
    avro_producer: AvroProducer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    tmpdir_factory,
):
    source_topic_id, _ = source_topic
    target_topic_id, _ = target_topic
    working_dir = tmpdir_factory.mktemp("working_directory")
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)

    file_consumer = ConsumerFactory().create_consumer(consumer_group, source_topic_id, working_dir, False, avro=True)
    file_consumer.consume(10)

    producer = ProducerFactory().create_producer(topic_name=target_topic_id, working_dir=working_dir, avro=True)
    producer.produce()

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group + "assertion_check", target_topic_id, assertion_check_directory, False, avro=True
    )
    file_consumer.consume(10)

    consumed_messages = get_consumed_messages(assertion_check_directory, True)

    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_plain_text_message_ordering(
    producer: ConfluenceProducer, topic_multiple_partitions: str, tmpdir_factory, cli_runner: CliRunner
):
    working_dir = tmpdir_factory.mktemp("working_directory")
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0),
        KafkaMessage(key="i", value="v2", partition=1),
        KafkaMessage(key="h", value="v3", partition=2),
        KafkaMessage(key="g", value="v4", partition=3),
        KafkaMessage(key="f", value="v5", partition=2),
        KafkaMessage(key="e", value="v6", partition=1),
        KafkaMessage(key="d", value="v7", partition=0),
        KafkaMessage(key="c", value="v8", partition=2),
        KafkaMessage(key="b", value="v9", partition=3),
        KafkaMessage(key="a", value="v10", partition=1),
    ]
    for message in ordered_messages:
        producer.produce(
            topic=topic_multiple_partitions, key=message.key, value=message.value, partition=message.partition
        )
        producer.flush()
        time.sleep(0.5)

    _consume_to_file_ordered(
        working_dir,
        topic_multiple_partitions,
        "group",
        list(range(0, 10)),
        10,
        False,
        match=None,
        last=False,
        write_to_stdout=False,
    )
    # Check assertions:
    consumed_messages = get_consumed_messages(working_dir, False, sort=False)
    assert consumed_messages[0].key == "j"
    assert consumed_messages[3].key == "g"
    assert consumed_messages[8].key == "b"


@pytest.mark.integration
def test_plain_text_message_ordering_with_filtering(
    producer: ConfluenceProducer, topic_multiple_partitions: str, tmpdir_factory, cli_runner: CliRunner
):
    working_dir = tmpdir_factory.mktemp("working_directory")
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0),
        KafkaMessage(key="i", value="v2", partition=1),
        KafkaMessage(key="h", value="v3", partition=2),
        KafkaMessage(key="g", value="v4", partition=3),
        KafkaMessage(key="f", value="v5", partition=2),
        KafkaMessage(key="e", value="v6", partition=1),
        KafkaMessage(key="d", value="v7", partition=0),
        KafkaMessage(key="c", value="v8", partition=2),
        KafkaMessage(key="b", value="v9", partition=3),
        KafkaMessage(key="a", value="v10", partition=1),
    ]
    for message in ordered_messages:
        producer.produce(
            topic=topic_multiple_partitions, key=message.key, value=message.value, partition=message.partition
        )
        time.sleep(0.5)
        producer.flush()

    _consume_to_file_ordered(
        working_dir,
        topic_multiple_partitions,
        "group",
        list(range(0, 10)),
        10,
        False,
        match="message.partition == 1",
        last=False,
        write_to_stdout=False,
    )
    # Check assertions:
    consumed_messages = get_consumed_messages(working_dir, False, sort=False)
    assert consumed_messages[0].key == "i"
    assert consumed_messages[1].key == "e"
    assert consumed_messages[2].key == "a"


@pytest.mark.integration
def test_plain_text_message_cli_pipe(
    producer: ConfluenceProducer, topic: str, tmpdir_factory, non_interactive_cli_runner: CliRunner
):
    working_dir = tmpdir_factory.mktemp("working_directory")
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0),
        KafkaMessage(key="i", value="v2", partition=0),
        KafkaMessage(key="h", value="v3", partition=0),
        KafkaMessage(key="g", value="v4", partition=0),
        KafkaMessage(key="f", value="v5", partition=0),
        KafkaMessage(key="e", value="v6", partition=0),
        KafkaMessage(key="d", value="v7", partition=0),
        KafkaMessage(key="c", value="v8", partition=0),
        KafkaMessage(key="b", value="v9", partition=0),
        KafkaMessage(key="a", value="v10", partition=0),
    ]
    for message in ordered_messages:
        producer.produce(
            topic=topic, key=message.key, value=message.value, partition=message.partition
        )
        producer.flush()
        time.sleep(0.5)

    result1=non_interactive_cli_runner.invoke(consume, args=["--stdout", "--numbers", "10", topic])
    result2=non_interactive_cli_runner.invoke(produce, args=["--stdin", topic], input=result1.output)
    # Check assertions:
    assert "10" in result2.output
    assert result2.exit_code == 0

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


def get_consumed_messages(directory, avro: bool, sort: bool = True) -> List[KafkaMessage]:
    consumed_messages = []
    path_list = glob(str(directory / "partition_*"))
    with ExitStack() as stack:
        for partition_path in path_list:
            if avro:
                file_reader = AvroFileReader(pathlib.Path(partition_path))
            else:
                file_reader = PlainTextFileReader(pathlib.Path(partition_path))
            stack.enter_context(file_reader)
            for message in file_reader.read_message_from_file():
                consumed_messages.append(message)
    return sorted(consumed_messages, key=(lambda msg: msg.key)) if sort else consumed_messages
