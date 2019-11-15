from typing import Tuple

import pytest
from _pytest.tmpdir import TempdirFactory
from click.testing import CliRunner
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.cli.commands import produce
from esque.clients.consumer import ConsumerFactory
from tests.integration.test_clients import produce_test_messages, get_consumed_messages


@pytest.mark.integration
def test_plain_text_consume_and_produce_newly_created_topic(
    consumer_group: str,
    interactive_cli_runner: CliRunner,
    producer: ConfluenceProducer,
    source_topic: Tuple[str, int],
    topic_id: str,
    tmpdir_factory: TempdirFactory,
):
    source_topic_id, _ = source_topic
    target_topic_id = topic_id
    output_directory = tmpdir_factory.mktemp("output_directory")
    produced_messages = produce_test_messages(producer, source_topic)
    file_consumer = ConsumerFactory().create_consumer(
        consumer_group, source_topic_id, output_directory, last=False, avro=False
    )
    file_consumer.consume(10)

    result = interactive_cli_runner.invoke(
        produce, args=["-d", output_directory, target_topic_id], input="y\n", catch_exceptions=False
    )
    assert result.exit_code == 0

    # Check assertions:
    assertion_check_directory = tmpdir_factory.mktemp("assertion_check_directory")
    file_consumer = ConsumerFactory().create_consumer(
        (consumer_group + "assertion_check"), target_topic_id, assertion_check_directory, last=False, avro=False
    )
    file_consumer.consume(10)

    consumed_messages = get_consumed_messages(assertion_check_directory, False)

    assert produced_messages == consumed_messages
