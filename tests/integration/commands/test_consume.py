from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque import config
from esque.cli.commands import consume, produce
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ConsumerGroupDoesNotExistException
from tests.integration.test_clients import produce_test_messages_with_avro


@pytest.mark.integration
def test_plain_text_message_cli_pipe(
    producer: ConfluenceProducer, topic: str, non_interactive_cli_runner: CliRunner, produced_messages_same_partition
):
    produced_messages_same_partition(topic_name=topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(consume, args=["--stdout", "--numbers", "10", topic])
    result2 = non_interactive_cli_runner.invoke(produce, args=["--stdin", topic], input=result1.output)
    # Check assertions:
    assert "10" in result2.output
    assert result2.exit_code == 0


@pytest.mark.integration
def test_plain_text_message_with_headers_cli_pipe(
    producer: ConfluenceProducer,
    topic: str,
    non_interactive_cli_runner: CliRunner,
    produced_messages_same_partition_with_headers,
):
    produced_messages_same_partition_with_headers(topic_name=topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(consume, args=["--stdout", "--numbers", "10", topic])
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
        consume, args=["--stdout", "--numbers", "10", "--avro", source_topic_id]
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
        data = consumergroup_controller.get_consumergroup(config.ESQUE_GROUP_ID).describe(verbose=True)
        assert source_topic not in data["offsets"]
    except ConsumerGroupDoesNotExistException:
        pass
