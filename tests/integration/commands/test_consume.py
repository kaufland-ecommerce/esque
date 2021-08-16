import base64
import json
from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer as ConfluentProducer

from esque import config
from esque.cli.commands import consume
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ConsumerGroupDoesNotExistException
from tests.integration.commands.conftest import produce_binary_test_messages
from tests.integration.test_clients import produce_test_messages_with_avro


@pytest.mark.integration
def test_avro_consume_to_stdout(
    avro_producer: AvroProducer, source_topic: Tuple[str, int], non_interactive_cli_runner: CliRunner
):
    source_topic_id, _ = source_topic
    produce_test_messages_with_avro(avro_producer, source_topic)

    message_text = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--number", "10", "--avro", source_topic_id], catch_exceptions=False
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

    non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--numbers", "10", "--avro", source_topic_id], catch_exceptions=False
    )

    # cannot use pytest.raises(ConsumerGroupDoesNotExistException) because other tests may have committed offsets
    # for this group
    try:
        data = consumergroup_controller.get_consumer_group(config.ESQUE_GROUP_ID).describe(partitions=True)
        assert source_topic_id.encode() not in data["offsets"]
    except ConsumerGroupDoesNotExistException:
        pass


@pytest.mark.integration
def test_binary_consume_to_stdout(
    producer: ConfluentProducer, source_topic: Tuple[str, int], non_interactive_cli_runner: CliRunner
):
    source_topic_id, _ = source_topic
    expected_messages = produce_binary_test_messages(producer, source_topic)

    message_text = non_interactive_cli_runner.invoke(
        consume, args=["--stdout", "--number", "10", "--binary", source_topic_id], catch_exceptions=False
    )
    # Check assertions:
    actual_messages = {
        (base64.b64decode(msg["key"].encode()), base64.b64decode(msg["value"].encode()), msg["partition"])
        for msg in map(json.loads, message_text.output.splitlines())
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_binary_and_avro_fails(non_interactive_cli_runner: CliRunner):
    with pytest.raises(ValueError):
        non_interactive_cli_runner.invoke(consume, args=["--binary", "--avro", "thetopic"], catch_exceptions=False)
