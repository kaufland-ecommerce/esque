from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.cimpl import Consumer
from confluent_kafka.cimpl import Producer as ConfluentProducer
from confluent_kafka.cimpl import TopicPartition

from esque.cli.commands.consume import consume
from esque.cli.commands.produce import produce
from esque.config import Config
from esque.messages.message import MessageHeader
from tests.integration.commands.conftest import (
    produce_binary_test_messages,
    produce_text_test_messages,
    produce_text_test_messages_with_headers,
)


@pytest.fixture
def target_topic_consumer(unittest_config: Config, target_topic: Tuple[str, int]) -> Consumer:
    consumer = Consumer(
        {
            "group.id": "asdf",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            **unittest_config.create_confluent_config(),
        }
    )
    consumer.assign([TopicPartition(topic=target_topic[0], partition=i, offset=0) for i in range(target_topic[1])])
    yield consumer
    consumer.close()


@pytest.mark.integration
def test_transfer_plain_text_message_using_cli_pipe(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
):
    expected_messages = produce_text_test_messages(topic=source_topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        produce, args=["--stdin", target_topic[0]], input=result1.output, catch_exceptions=False
    )

    actual_messages = {
        (msg.key().decode(), msg.value().decode(), msg.partition())
        for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_plain_text_message_with_headers_using_cli_pipe(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
):
    expected_messages = produce_text_test_messages_with_headers(topic=source_topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        produce, args=["--stdin", target_topic[0]], input=result1.output, catch_exceptions=False
    )

    actual_messages = {
        (
            msg.key().decode(),
            msg.value().decode(),
            msg.partition(),
            tuple(MessageHeader(k, (v.decode() if v is not None else None)) for k, v in (msg.headers() or [])),
        )
        for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition, tuple(msg.headers)) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_binary_message_using_cli_pipe(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner,
):
    expected_messages = produce_binary_test_messages(topic=source_topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--binary", "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        produce, args=["--stdin", "--binary", target_topic[0]], input=result1.output, catch_exceptions=False
    )

    actual_messages = {
        (msg.key(), msg.value(), msg.partition()) for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_plain_text_message_using_file(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
    tmpdir_factory,
):
    output_directory = tmpdir_factory.mktemp("output_directory")
    expected_messages = produce_text_test_messages(topic=source_topic, producer=producer)

    non_interactive_cli_runner.invoke(
        consume, args=["-d", str(output_directory), "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        produce, args=["-d", str(output_directory), target_topic[0]], catch_exceptions=False
    )

    actual_messages = {
        (msg.key().decode(), msg.value().decode(), msg.partition())
        for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_plain_text_message_with_headers_using_file(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
    tmpdir_factory,
):
    output_directory = tmpdir_factory.mktemp("output_directory")
    expected_messages = produce_text_test_messages_with_headers(topic=source_topic, producer=producer)

    non_interactive_cli_runner.invoke(
        consume, args=["-d", str(output_directory), "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        produce, args=["-d", str(output_directory), target_topic[0]], catch_exceptions=False
    )

    actual_messages = {
        (
            msg.key().decode(),
            msg.value().decode(),
            msg.partition(),
            tuple(MessageHeader(k, (v.decode() if v is not None else None)) for k, v in (msg.headers() or [])),
        )
        for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition, tuple(msg.headers)) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_binary_message_using_file(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
    tmpdir_factory,
):
    output_directory = tmpdir_factory.mktemp("output_directory")
    expected_messages = produce_binary_test_messages(topic=source_topic, producer=producer)

    non_interactive_cli_runner.invoke(
        consume,
        args=["-d", str(output_directory), "--binary", "--number", "10", source_topic[0]],
        catch_exceptions=False,
    )
    non_interactive_cli_runner.invoke(
        produce, args=["-d", str(output_directory), "--binary", target_topic[0]], catch_exceptions=False
    )

    actual_messages = {
        (msg.key(), msg.value(), msg.partition()) for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages
