import time
from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.cimpl import Consumer
from confluent_kafka.cimpl import Producer as ConfluentProducer
from confluent_kafka.cimpl import TopicPartition
from pytest_cases import fixture

from esque.cli.commands import esque
from esque.config import Config
from tests.utils import (
    produce_avro_test_messages,
    produce_binary_test_messages,
    produce_text_test_messages,
    produce_text_test_messages_with_headers,
)


@fixture
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


@fixture
def target_topic_avro_consumer(unittest_config: Config, target_topic: Tuple[str, int]) -> AvroConsumer:
    consumer = AvroConsumer(
        {
            "group.id": "asdf",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            **unittest_config.create_confluent_config(include_schema_registry=True),
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
    expected_messages = produce_text_test_messages(topic_name=source_topic[0], producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        esque, args=["consume", "--stdout", "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "--stdin", target_topic[0]], input=result1.output, catch_exceptions=False
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
    expected_messages = produce_text_test_messages_with_headers(topic_name=source_topic[0], producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        esque, args=["consume", "--stdout", "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "--stdin", target_topic[0]], input=result1.output, catch_exceptions=False
    )

    actual_messages = {
        (msg.key().decode(), msg.value().decode(), msg.partition(), tuple(msg.headers() or []))
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
    expected_messages = produce_binary_test_messages(topic_name=source_topic[0], producer=producer)

    result1 = non_interactive_cli_runner.invoke(
        esque, args=["consume", "--stdout", "--binary", "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "--stdin", "--binary", target_topic[0]], input=result1.output, catch_exceptions=False
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
    expected_messages = produce_text_test_messages(topic_name=source_topic[0], producer=producer)

    non_interactive_cli_runner.invoke(
        esque, args=["consume", "-d", str(output_directory), "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "-d", str(output_directory), target_topic[0]], catch_exceptions=False
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
    expected_messages = produce_text_test_messages_with_headers(topic_name=source_topic[0], producer=producer)

    non_interactive_cli_runner.invoke(
        esque, args=["consume", "-d", str(output_directory), "--number", "10", source_topic[0]], catch_exceptions=False
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "-d", str(output_directory), target_topic[0]], catch_exceptions=False
    )

    actual_messages = {
        (msg.key().decode(), msg.value().decode(), msg.partition(), tuple((msg.headers() or [])))
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
    expected_messages = produce_binary_test_messages(topic_name=source_topic[0], producer=producer)

    non_interactive_cli_runner.invoke(
        esque,
        args=["consume", "-d", str(output_directory), "--binary", "--number", "10", source_topic[0]],
        catch_exceptions=False,
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "-d", str(output_directory), "--binary", target_topic[0]], catch_exceptions=False
    )

    actual_messages = {
        (msg.key(), msg.value(), msg.partition()) for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_avro_message_using_file(
    avro_producer: AvroProducer,
    target_topic_avro_consumer: AvroConsumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
    tmpdir_factory,
):
    output_directory = tmpdir_factory.mktemp("output_directory")
    expected_messages = produce_avro_test_messages(topic_name=source_topic[0], avro_producer=avro_producer)

    non_interactive_cli_runner.invoke(
        esque,
        args=["consume", "-d", str(output_directory), "--avro", "--number", "10", source_topic[0]],
        catch_exceptions=False,
    )
    non_interactive_cli_runner.invoke(
        esque, args=["produce", "-d", str(output_directory), "--avro", target_topic[0]], catch_exceptions=False
    )

    actual_messages = set()
    start = time.monotonic()
    while len(actual_messages) < 10:
        msg = target_topic_avro_consumer.poll(timeout=2)
        if msg is not None:
            actual_messages.add((msg.key()["key"], msg.value()["value"], msg.partition()))
        elif time.monotonic() - start >= 20:
            raise TimeoutError("Timeout reading data from topic")

    expected_messages = {(msg.key["key"], msg.value["value"], msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_avro_with_single_command(
    avro_producer: AvroProducer,
    target_topic_avro_consumer: AvroConsumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
):
    expected_messages = produce_avro_test_messages(topic_name=source_topic[0], avro_producer=avro_producer)
    non_interactive_cli_runner.invoke(
        esque,
        args=[
            "transfer",
            "--from-topic",
            source_topic[0],
            "--to-topic",
            target_topic[0],
            "--avro",
            "--number",
            "10",
            "--first",
        ],
        catch_exceptions=False,
    )

    actual_messages = set()
    start = time.monotonic()
    while len(actual_messages) < 10:
        msg = target_topic_avro_consumer.poll(timeout=2)
        if msg is not None:
            actual_messages.add((msg.key()["key"], msg.value()["value"], msg.partition()))
        elif time.monotonic() - start >= 20:
            raise TimeoutError("Timeout reading data from topic")

    expected_messages = {(msg.key["key"], msg.value["value"], msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_binary_with_single_command(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
):
    expected_messages = produce_binary_test_messages(topic_name=source_topic[0], producer=producer)

    non_interactive_cli_runner.invoke(
        esque,
        args=[
            "transfer",
            "--from-topic",
            source_topic[0],
            "--to-topic",
            target_topic[0],
            "--binary",
            "--number",
            "10",
            "--first",
        ],
        catch_exceptions=False,
    )

    actual_messages = {
        (msg.key(), msg.value(), msg.partition()) for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_transfer_plain_with_single_command(
    producer: ConfluentProducer,
    target_topic_consumer: Consumer,
    source_topic: Tuple[str, int],
    target_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
):
    expected_messages = produce_text_test_messages_with_headers(topic_name=source_topic[0], producer=producer)

    non_interactive_cli_runner.invoke(
        esque,
        args=["transfer", "--from-topic", source_topic[0], "--to-topic", target_topic[0], "--number", "10", "--first"],
        catch_exceptions=False,
    )

    actual_messages = {
        (msg.key().decode(), msg.value().decode(), msg.partition())
        for msg in target_topic_consumer.consume(10, timeout=20)
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages
