import random
from string import ascii_letters

import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import get_topics, get_consumergroups, get_brokers
from esque.controller.topic_controller import TopicController
from esque.resources.topic import Topic


@pytest.mark.integration
def test_smoke_test_get_topics_interactive(interactive_cli_runner: CliRunner):  # TODO: delete whole test
    result = interactive_cli_runner.invoke(get_topics)
    print(result.output)  # TODO: delete
    assert result.exit_code == 0


@pytest.mark.integration
def test_smoke_test_get_topics(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_topics)
    print(result.output)  # TODO: delete
    assert result.exit_code == 0


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
    for new_topic in new_topics:
        topic_controller.create_topics([Topic(new_topic, replication_factor=1)])
    confluent_admin_client.poll(timeout=1)

    result = non_interactive_cli_runner.invoke(get_topics, [prefix_1])

    assert result.exit_code == 0
    retrieved_topics = result.output.split("\n")
    assert len(retrieved_topics) > 1
    for retrieved_topic in retrieved_topics:
        if retrieved_topic:
            assert retrieved_topic[: len(prefix_1)] == prefix_1


@pytest.mark.integration
def test_smoke_test_get_consumergroups(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_consumergroups)
    print(result.output)  # TODO: delete
    assert result.exit_code == 0


@pytest.mark.integration
def test_smoke_test_get_brokers(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(get_brokers)
    print(result.output)  # TODO: delete
    assert result.exit_code == 0
