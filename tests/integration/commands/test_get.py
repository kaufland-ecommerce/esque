import json
import random
from string import ascii_letters
from typing import Callable, Tuple

import confluent_kafka
import pytest
from click.testing import CliRunner
from confluent_kafka import Producer

from esque.cli.commands import get_brokers, get_consumergroups, get_timestamp, get_topics
from esque.controller.topic_controller import TopicController
from esque.resources.topic import Topic
from tests.conftest import parameterized_output_formats


@pytest.mark.integration
def test_smoke_test_get_topics(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_topics, catch_exceptions=False)
    assert result.exit_code == 0


@pytest.mark.integration
@parameterized_output_formats
def test_get_topics_with_output_format(non_interactive_cli_runner: CliRunner, output_format: str, loader: Callable):
    result = non_interactive_cli_runner.invoke(get_topics, ["-o", output_format], catch_exceptions=False)
    assert result.exit_code == 0
    loader(result.output)


@pytest.mark.integration
def test_get_topics_with_prefix(
    non_interactive_cli_runner: CliRunner,
    topic_controller: TopicController,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
):
    topic_base = "".join(random.choices(ascii_letters, k=5))
    prefix_1 = "ab"
    prefix_2 = "fx"
    new_topics = [prefix_1 + topic_base, prefix_2 + topic_base, prefix_1 + prefix_2 + topic_base]
    topic_controller.create_topics([Topic(new_topic, replication_factor=1) for new_topic in new_topics])

    result = non_interactive_cli_runner.invoke(get_topics, ["-p", prefix_1, "-o", "json"], catch_exceptions=False)

    assert result.exit_code == 0
    retrieved_topics = json.loads(result.output)
    assert len(retrieved_topics) > 1
    for retrieved_topic in retrieved_topics:
        assert retrieved_topic.startswith(prefix_1)


@pytest.mark.integration
def test_smoke_test_get_consumergroups(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_consumergroups, catch_exceptions=False)
    assert result.exit_code == 0


@pytest.mark.integration
@parameterized_output_formats
def test_get_consumergroups_with_output_format(
    non_interactive_cli_runner: CliRunner, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(get_consumergroups, ["-o", output_format], catch_exceptions=False)
    assert result.exit_code == 0
    loader(result.output)


@pytest.mark.integration
def test_smoke_test_get_brokers(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_brokers, catch_exceptions=False)
    assert result.exit_code == 0


@pytest.mark.integration
@parameterized_output_formats
def test_get_brokers_with_output_format(non_interactive_cli_runner: CliRunner, output_format: str, loader: Callable):
    result = non_interactive_cli_runner.invoke(get_brokers, ["-o", output_format])
    assert result.exit_code == 0
    loader(result.output)


@pytest.mark.integration
@parameterized_output_formats
def test_get_timestamps_with_output_format(
    non_interactive_cli_runner: CliRunner,
    producer: Producer,
    target_topic: Tuple[str, int],
    output_format: str,
    loader: Callable,
):
    topic_name, partitions = target_topic
    partition_center = 5

    for target_partition in range(partitions):
        for offset in range(10):
            # make sure we don't have any message after offset 'partition_center' for the 10th partition
            # so we can test the case where the offset is after the last available message
            if target_partition == 9 and offset >= partition_center:
                continue

            producer.produce(
                topic_name, value=b"", timestamp=target_partition * 10 + offset, partition=target_partition
            )
    producer.flush()

    result = non_interactive_cli_runner.invoke(get_timestamp, [topic_name, str(partition_center), "-o", output_format])
    result_data = loader(result.output)
    assert len(result_data) == partitions
    for target_partition in range(partitions):
        if target_partition == 9:
            assert result_data[target_partition]["timestamp_ms"] is None
            assert result_data[target_partition]["offset"] is None
        else:
            assert result_data[target_partition]["timestamp_ms"] == target_partition * 10 + partition_center
            assert result_data[target_partition]["offset"] == partition_center
