import base64
import json
from operator import attrgetter, itemgetter
from typing import Tuple

import pytest
from click.testing import CliRunner
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer as ConfluentProducer

from esque import config
from esque.cli.commands import esque
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ConsumerGroupDoesNotExistException
from tests.utils import produce_avro_test_messages, produce_binary_test_messages


@pytest.mark.integration
def test_avro_consume_to_stdout(
    avro_producer: AvroProducer, source_topic: Tuple[str, int], non_interactive_cli_runner: CliRunner
):
    source_topic_id, _ = source_topic
    expected_messages = produce_avro_test_messages(avro_producer, topic_name=source_topic_id, amount=10)

    message_text = non_interactive_cli_runner.invoke(
        esque,
        args=["consume", "--number", "10", "-s", "avro", "-k", "avro", source_topic_id],
        catch_exceptions=False,
    )
    # Check assertions:
    actual_messages = sorted(map(json.loads, message_text.output.split("\n")[:10]), key=itemgetter("partition"))
    expected_messages.sort(key=attrgetter("partition"))
    for expected_message, actual_message in zip(expected_messages, actual_messages):
        assert expected_message.key["key"] == actual_message["key"]["key"]
        assert expected_message.value["value"] == actual_message["value"]["value"]


@pytest.mark.integration
def test_offset_not_committed(
    avro_producer: AvroProducer,
    source_topic: Tuple[str, int],
    non_interactive_cli_runner: CliRunner,
    consumergroup_controller: ConsumerGroupController,
):
    source_topic_id, _ = source_topic
    produce_avro_test_messages(avro_producer, topic_name=source_topic_id)

    non_interactive_cli_runner.invoke(
        esque,
        args=["consume", "--numbers", "10", "-s", "avro", "-k", "avro", source_topic_id],
        catch_exceptions=False,
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
    expected_messages = produce_binary_test_messages(producer, topic_name=source_topic_id)

    message_text = non_interactive_cli_runner.invoke(
        esque,
        args=["consume", "--number", "10", "-s", "binary", "-k", "binary", source_topic_id],
        catch_exceptions=False,
    )
    # Check assertions:
    actual_messages = {
        (base64.b64decode(msg["key"].encode()), base64.b64decode(msg["value"].encode()), msg["partition"])
        for msg in map(json.loads, message_text.output.splitlines())
    }
    expected_messages = {(msg.key, msg.value, msg.partition) for msg in expected_messages}
    assert expected_messages == actual_messages


@pytest.mark.integration
def test_invalid_serialization_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(
        esque, args=["consume", "-s", "none", "-k", "none", "thetopic"], catch_exceptions=False
    )
    assert result.exit_code == 2
