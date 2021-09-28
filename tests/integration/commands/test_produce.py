import json
import time
from typing import Callable, List

import confluent_kafka
import pytest
from _pytest.tmpdir import TempdirFactory
from click.testing import CliRunner
from confluent_kafka.cimpl import Consumer, TopicPartition
from pytest_cases import fixture

from esque.cli.commands import esque
from esque.config import Config
from esque.errors import NoConfirmationPossibleException
from esque.io.handlers.pipe import MARKER


@fixture
def consumer_factory(unittest_config: Config) -> Callable[[str], Consumer]:
    consumers: List[Consumer] = []

    def consumer_factory_(topic: str) -> Consumer:
        consumer = Consumer(
            {
                "group.id": "asdf",
                "enable.auto.commit": False,
                "enable.partition.eof": False,
                **unittest_config.create_confluent_config(),
            }
        )
        partitions = consumer.list_topics(topic=topic).topics[topic].partitions

        consumer.assign([TopicPartition(topic=topic, partition=p, offset=0) for p in partitions])
        consumers.append(consumer)
        return consumer

    yield consumer_factory_

    for consumer in consumers:
        consumer.close()


@pytest.mark.integration
def test_produce_can_create_topic(
    consumer_factory: Callable[[str], Consumer],
    non_interactive_cli_runner: CliRunner,
    topic_id: str,
    tmpdir_factory: TempdirFactory,
):
    data = "".join([json.dumps(dict(key='"key1"', value='"value1"')) + "\n", MARKER])
    result = non_interactive_cli_runner.invoke(
        esque, args=["produce", "--no-verify", "--stdin", topic_id], input=data, catch_exceptions=False
    )
    assert result.exit_code == 0
    consumer = consumer_factory(topic_id)
    expected_messages = [(b"key1", b"value1")]
    actual_messages = []
    start = time.monotonic()
    while len(actual_messages) < 1:
        msg = consumer.poll(timeout=2)
        if msg is not None:
            actual_messages.append((msg.key(), msg.value()))
        elif time.monotonic() - start >= 20:
            raise TimeoutError("Timeout reading data from topic")

    assert expected_messages == actual_messages


@pytest.mark.integration
def test_binary_and_avro_fails(non_interactive_cli_runner: CliRunner):
    with pytest.raises(ValueError):
        non_interactive_cli_runner.invoke(
            esque, args=["produce", "--binary", "--avro", "thetopic"], catch_exceptions=False
        )


@pytest.mark.integration
def test_produce_to_non_existent_topic_fails(
    confluent_admin_client: confluent_kafka.admin.AdminClient, non_interactive_cli_runner: CliRunner, topic_id: str
):
    target_topic_id = topic_id
    data = "".join([json.dumps(dict(key='"key1"', value='"value1"')) + "\n", MARKER])
    result = non_interactive_cli_runner.invoke(esque, args=["produce", "--stdin", target_topic_id], input=data)
    assert isinstance(result.exception, NoConfirmationPossibleException)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert target_topic_id not in topics
