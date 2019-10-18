import json
import random
from string import ascii_letters
from typing import Callable

import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import get_topics, get_consumergroups, get_brokers
from esque.controller.topic_controller import TopicController
from esque.resources.topic import Topic
from tests.conftest import parameterized_output_formats


@pytest.mark.integration
def test_smoke_test_get_topics(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_topics)
    assert result.exit_code == 0


@pytest.mark.integration
@parameterized_output_formats
def test_smoke_test_get_topics(non_interactive_cli_runner: CliRunner, output_format: str, loader: Callable):
    result = non_interactive_cli_runner.invoke(get_topics, ["-o", output_format])
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
    confluent_admin_client.poll(timeout=1)

    result = non_interactive_cli_runner.invoke(get_topics, ["-p", prefix_1, "-o", "json"])

    assert result.exit_code == 0
    retrieved_topics = json.loads(result.output)
    assert len(retrieved_topics) > 1
    for retrieved_topic in retrieved_topics:
        assert retrieved_topic.startswith(prefix_1)


@pytest.mark.integration
def test_smoke_test_get_consumergroups(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_consumergroups)
    assert result.exit_code == 0


@pytest.mark.integration
@parameterized_output_formats
def test_smoke_test_get_consumergroups_with_output_format(
    non_interactive_cli_runner: CliRunner, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(get_consumergroups, ["-o", output_format])
    assert result.exit_code == 0
    loader(result.output)


@pytest.mark.integration
def test_smoke_test_get_brokers(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_brokers)
    assert result.exit_code == 0


@pytest.mark.integration
@parameterized_output_formats
def test_smoke_test_get_brokers_with_output_format(
    non_interactive_cli_runner: CliRunner, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(get_brokers, ["-o", output_format])
    assert result.exit_code == 0
    loader(result.output)
