import base64
import random
from typing import List, Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Consumer
from confluent_kafka.cimpl import Producer as ConfluentProducer
from confluent_kafka.cimpl import TopicPartition

from esque import config
from esque.cli.commands import consume, produce
from esque.config import Config
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ConsumerGroupDoesNotExistException
from esque.messages.message import KafkaMessage
from tests.integration.test_clients import produce_test_messages_with_avro


@pytest.mark.integration
def test_plain_text_message_cli_pipe(
    producer: ConfluentProducer, topic: str, non_interactive_cli_runner: CliRunner, produced_messages_same_partition
):
    produced_messages_same_partition(topic_name=topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(consume, args=["--stdout", "--number", "10", topic])
    result2 = non_interactive_cli_runner.invoke(produce, args=["--stdin", topic], input=result1.output)
    # Check assertions:
    assert "10" in result2.output
    assert result2.exit_code == 0


@pytest.mark.integration
def test_plain_text_message_with_headers_cli_pipe(
    producer: ConfluentProducer,
    topic: str,
    non_interactive_cli_runner: CliRunner,
    produced_messages_same_partition_with_headers,
):
    produced_messages_same_partition_with_headers(topic_name=topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(consume, args=["--stdout", "--number", "10", topic])
    result2 = non_interactive_cli_runner.invoke(produce, args=["--stdin", topic], input=result1.output)
    # Check assertions:
    assert "10" in result2.output
    assert result2.exit_code == 0


@pytest.mark.integration
def test_avro_consume_to_stdout(
    avro_producer: AvroProducer, source_topic: Tuple[str, int], non_interactive_cli_runner: CliRunner
):
    source_topic_id, _ = source_topic
    produce_test_messages_with_avro(avro_producer, source_topic)

    message_text = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--number", "10", "--avro", source_topic_id]
    )
    # Check assertions:
    separate_messages = message_text.output.split("\n")
    assert "Firstname" in separate_messages[0] and "Lastname" in separate_messages[0]
    assert "Firstname" in separate_messages[4] and "Lastname" in separate_messages[4]
    assert "Firstname" in separate_messages[7] and "Lastname" in separate_messages[7]


@pytest.mark.integration
def test_offset_not_committed(
    avro_producer: AvroProducer,
    source_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
    consumergroup_controller: ConsumerGroupController,
):
    source_topic_id, _ = source_topic
    produce_test_messages_with_avro(avro_producer, source_topic)

    non_interactive_cli_runner.invoke(consume, args=["--stdout", "--numbers", "10", "--avro", source_topic_id])

    # cannot use pytest.raises(ConsumerGroupDoesNotExistException) because other tests may have committed offsets
    # for this group
    try:
        data = consumergroup_controller.get_consumer_group(config.ESQUE_GROUP_ID).describe(verbose=True)
        assert source_topic_id.encode() not in data["offsets"]
    except ConsumerGroupDoesNotExistException:
        pass


@pytest.mark.integration
def test_binary_consume_to_stdout(
    producer: ConfluentProducer, source_topic: Tuple[str, int], non_interactive_cli_runner: CliRunner
):
    source_topic_id, _ = source_topic
    messages = produce_binary_test_messages(producer, source_topic)

    message_text = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--number", "10", "--binary", source_topic_id]
    )
    # Check assertions:
    separate_messages = message_text.output.split("\n")
    for expected_message, actual_message_line in zip(messages, separate_messages):
        assert base64.b64encode(expected_message.key).decode() in actual_message_line
        assert base64.b64encode(expected_message.value).decode() in actual_message_line


@pytest.mark.integration
def test_binary_message_cli_pipe(
    producer: ConfluentProducer,
    unittest_config: Config,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
):
    expected_messages = produce_binary_test_messages(topic=source_topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--binary", "--number", "10", source_topic[0]]
    )
    non_interactive_cli_runner.invoke(produce, args=["--stdin", "--binary", target_topic[0]], input=result1.output)

    consumer = Consumer(unittest_config.create_confluent_config())
    consumer.assign([TopicPartition(target_topic[0], i) for i in range(target_topic[1])])

    actual_messages = {(msg.key(), msg.value(), msg.partition()) for msg in consumer.consume(10, timeout=10)}
    expected_messages = {(msg.key, msg.value(), msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


def random_bytes(length: int = 16) -> bytes:
    return random.getrandbits(length * 8).to_bytes(length, "big")


def produce_binary_test_messages(
    producer: ConfluentProducer, topic: Tuple[str, int], amount: int = 10
) -> List[KafkaMessage]:
    topic_name, num_partitions = topic
    messages = []
    for i in range(amount):
        partition = random.randrange(0, num_partitions)
        key = random_bytes()
        value = random_bytes()
        messages.append(KafkaMessage(key, value, partition))
        producer.produce(topic=topic_name, key=key, value=value, partition=partition)
    producer.flush()
    return messages
