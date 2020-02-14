import json
import pathlib
import random
from contextlib import ExitStack
from string import ascii_letters
from typing import List, Tuple

import pytest
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import loads as load_schema
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.cli.commands import consume_to_file_ordered
from esque.clients.consumer import ConsumerFactory
from esque.clients.producer import ProducerFactory
from esque.messages.avromessage import AvroFileReader
from esque.messages.message import KafkaMessage, PlainTextFileReader, get_partitions_in_path


@pytest.mark.integration
def test_plain_text_consume_to_file(
    consumer_group, producer: ConfluenceProducer, source_topic: Tuple[str, int], tmpdir_factory
):
    source_topic_id, _ = source_topic
    output_directory = tmpdir_factory.mktemp("output_directory")
    produced_messages = produce_test_messages(producer, source_topic) + produce_delete_tombstones_messages(
        producer, source_topic
    )
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group, source_topic_id, output_directory, False, avro=False
    )
    number_of_consumer_messages = file_consumer.consume(12)

    consumed_messages = get_consumed_messages(output_directory, False)

    assert number_of_consumer_messages == 12
    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_avro_consume_to_file(
    consumer_group, avro_producer: AvroProducer, source_topic: Tuple[str, int], tmpdir_factory
):
    source_topic_id, _ = source_topic
    output_directory = tmpdir_factory.mktemp("output_directory")
    produced_messages = produce_test_messages_with_avro(
        avro_producer, source_topic
    ) + produce_delete_tombstone_messages_with_avro(avro_producer, source_topic)
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group, source_topic_id, output_directory, False, avro=True
    )
    number_of_consumer_messages = file_consumer.consume(12)

    consumed_messages = get_consumed_messages(output_directory, True)

    assert number_of_consumer_messages == 12
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
    output_directory = tmpdir_factory.mktemp("output_directory")
    produced_messages = produce_test_messages(producer, source_topic) + produce_delete_tombstones_messages(
        producer, source_topic
    )
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group, source_topic_id, output_directory, False, avro=False
    )
    file_consumer.consume(12)

    producer = ProducerFactory().create_producer(target_topic_id, output_directory, avro=False)
    producer.produce()

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = ConsumerFactory().create_consumer(
        (consumer_group + "assertion_check"), target_topic_id, assertion_check_directory, False, avro=False
    )
    file_consumer.consume(12)

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
    output_directory = tmpdir_factory.mktemp("output_directory")
    produced_messages = produce_test_messages_with_avro(
        avro_producer, source_topic
    ) + produce_delete_tombstone_messages_with_avro(avro_producer, source_topic)
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group, source_topic_id, output_directory, False, avro=True
    )
    file_consumer.consume(12)

    producer = ProducerFactory().create_producer(
        topic_name=target_topic_id, input_directory=output_directory, avro=True
    )
    producer.produce()

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group + "assertion_check", target_topic_id, assertion_check_directory, False, avro=True
    )
    file_consumer.consume(12)

    consumed_messages = get_consumed_messages(assertion_check_directory, True)

    assert produced_messages == consumed_messages


@pytest.mark.integration
def test_plain_text_message_ordering(
    producer: ConfluenceProducer,
    topic_multiple_partitions: str,
    tmpdir_factory,
    produced_messages_different_partitions,
):
    produced_messages_different_partitions(topic_multiple_partitions, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory,
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
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert consumed_messages[0].key == "j"
    assert consumed_messages[3].key == "g"
    assert consumed_messages[8].key == "b"


@pytest.mark.integration
def test_plain_text_message_ordering_with_header_filtering(
    producer: ConfluenceProducer,
    topic_multiple_partitions: str,
    tmpdir_factory,
    produced_messages_different_partitions_with_headers,
):
    produced_messages_different_partitions_with_headers(topic_multiple_partitions, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory,
        topic_multiple_partitions,
        "group",
        list(range(0, 10)),
        10,
        False,
        match="message.partition == 1 and message.header.hk6 == hv6",
        last=False,
        write_to_stdout=False,
    )
    # Check assertions:
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert len(consumed_messages) == 1
    assert consumed_messages[0].key == "e"


@pytest.mark.integration
def test_plain_text_message_ordering_with_header_filtering_nonmatching(
    producer: ConfluenceProducer,
    topic_multiple_partitions: str,
    tmpdir_factory,
    produced_messages_different_partitions_with_headers,
):
    produced_messages_different_partitions_with_headers(topic_multiple_partitions, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory,
        topic_multiple_partitions,
        "group",
        list(range(0, 10)),
        10,
        False,
        match="message.partition == 1 and message.header.hk2 == hv7",
        last=False,
        write_to_stdout=False,
    )
    # Check assertions:
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert len(consumed_messages) == 0


@pytest.mark.integration
def test_plain_text_message_ordering_with_filtering(
    producer: ConfluenceProducer,
    topic_multiple_partitions: str,
    tmpdir_factory,
    produced_messages_different_partitions,
):
    produced_messages_different_partitions(topic_multiple_partitions, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory,
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
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert consumed_messages[0].key == "i"
    assert consumed_messages[1].key == "e"
    assert consumed_messages[2].key == "a"


@pytest.mark.integration
def test_plain_text_message_ordering_with_filtering_by_message_offset(
    producer: ConfluenceProducer, topic: str, tmpdir_factory, produced_messages_same_partition
):
    produced_messages_same_partition(topic, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory, topic, "group", [0], 10, False, match="message.offset > 7", last=False, write_to_stdout=False
    )
    # Check assertions:
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert len(consumed_messages) == 2
    assert consumed_messages[0].key == "b"
    assert consumed_messages[1].key == "a"


@pytest.mark.integration
def test_plaintext_consume_produce_messages_with_header(
    consumer_group,
    producer: ConfluenceProducer,
    topic_multiple_partitions: str,
    target_topic: Tuple[str, int],
    produced_messages_same_partition_with_headers,
    tmpdir_factory,
):
    produced_messages_same_partition_with_headers(topic_multiple_partitions, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory,
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
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert consumed_messages[0].headers[0].key == "hk1"
    assert consumed_messages[9].headers[0].value == "hv10"


@pytest.mark.integration
def test_avro_consume_produce_messages_with_header(
    consumer_group,
    producer: AvroProducer,
    topic_multiple_partitions: str,
    target_topic: Tuple[str, int],
    produced_avro_messages_with_headers,
    tmpdir_factory,
):
    produced_avro_messages_with_headers(topic_multiple_partitions, producer)
    output_directory = tmpdir_factory.mktemp("output_directory")

    consume_to_file_ordered(
        output_directory,
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
    consumed_messages = get_consumed_messages(output_directory, False, sort=False)
    assert consumed_messages[0].headers[0].key == "hk1"
    assert consumed_messages[9].headers[0].value == "hv10"


def produce_test_messages(
    producer: ConfluenceProducer, topic: Tuple[str, int], amount: int = 10
) -> List[KafkaMessage]:
    topic_name, num_partitions = topic
    messages = []
    for i in range(amount):
        partition = random.randrange(0, num_partitions)
        random_value = "".join(random.choices(ascii_letters, k=5))
        message = KafkaMessage(str(i), random_value, partition)
        messages.append(message)
        producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)
        producer.flush()
    return messages


def produce_delete_tombstones_messages(
    producer: ConfluenceProducer, topic: Tuple[str, int], amount: int = 2
) -> List[KafkaMessage]:
    topic_name, num_partitions = topic
    messages = []
    for i in range(amount):
        partition = random.randrange(0, num_partitions)
        message = KafkaMessage("Delete_Tombstone_" + str(i), None, partition)
        messages.append(message)
        producer.produce(topic=topic_name, key=message.key, partition=message.partition)
        producer.flush()
    return messages


def produce_test_messages_with_avro(
    avro_producer: AvroProducer, topic: Tuple[str, int], amount: int = 10
) -> List[KafkaMessage]:
    topic_name, num_partitions = topic
    with open("tests/test_samples/key_schema.avsc", "r") as file:
        key_schema = load_schema(file.read())
    with open("tests/test_samples/value_schema.avsc", "r") as file:
        value_schema = load_schema(file.read())
    messages = []
    for i in range(amount):
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


def produce_delete_tombstone_messages_with_avro(
    avro_producer: AvroProducer, topic: Tuple[str, int], amount: int = 2
) -> List[KafkaMessage]:
    topic_name, num_partitions = topic
    with open("tests/test_samples/key_schema.avsc", "r") as file:
        key_schema = load_schema(file.read())
    messages = []
    for i in range(amount):
        partition = random.randrange(0, num_partitions)
        key = {"id": "Delete_Tombstone_" + str(i)}
        messages.append(KafkaMessage(json.dumps(key), None, partition, key_schema, None))
        avro_producer.produce(
            topic=topic_name, key=key, value=None, key_schema=key_schema, value_schema=None, partition=partition
        )
        avro_producer.flush()
    return messages


def get_consumed_messages(directory, avro: bool, sort: bool = True) -> List[KafkaMessage]:
    consumed_messages = []
    partitions = get_partitions_in_path(directory)
    with ExitStack() as stack:
        for partition in partitions:
            if avro:
                file_reader = AvroFileReader(pathlib.Path(directory), partition)
            else:
                file_reader = PlainTextFileReader(pathlib.Path(directory), partition)
            stack.enter_context(file_reader)
            for message in file_reader.read_message_from_file():
                consumed_messages.append(message)
    return sorted(consumed_messages, key=(lambda msg: msg.key)) if sort else consumed_messages
