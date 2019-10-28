from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.cli.commands import consume, produce
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
def test_avro_consume_to_stdout(
    consumer_group, avro_producer: AvroProducer, source_topic: Tuple[str, int], non_interactive_cli_runner: CliRunner
):
    source_topic_id, _ = source_topic
    produced_messages = produce_test_messages_with_avro(avro_producer, source_topic)

    message_text = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--numbers", "10", "--avro", source_topic_id]
    )
    # Check assertions:
    separate_messages = message_text.output.split("\n")
    assert "Firstname" in separate_messages[0] and "Lastname" in separate_messages[0]
    assert "Firstname" in separate_messages[4] and "Lastname" in separate_messages[4]
    assert "Firstname" in separate_messages[7] and "Lastname" in separate_messages[7]
