import random
from concurrent.futures import Future
from string import ascii_letters

import confluent_kafka
import pytest
from click.testing import CliRunner
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, TopicPartition

from esque.cli.commands import delete_consumer_group, delete_topics, get_consumergroups, get_topics
from esque.config import Config
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.resources.consumergroup import ConsumerGroup


def randomly_generated_consumer_groups(filled_topic, unittest_config: Config) -> str:
    randomly_generated_consumer_group = "".join(random.choices(ascii_letters, k=8))
    _config = unittest_config.create_confluent_config()
    _config.update(
        {
            "group.id": randomly_generated_consumer_group,
            "enable.auto.commit": False,
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )
    _consumer = confluent_kafka.Consumer(_config)
    _consumer.assign([TopicPartition(topic=filled_topic.name, partition=0, offset=0)])
    for i in range(2):
        msg = _consumer.consume(timeout=10)[0]
        _consumer.commit(msg, asynchronous=False)
    return randomly_generated_consumer_group


def randomly_generated_topics(confluent_admin_client: AdminClient) -> str:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    future: Future = confluent_admin_client.create_topics(
        [NewTopic(topic_id, num_partitions=1, replication_factor=1)]
    )[topic_id]
    while not future.done() or future.cancelled():
        if future.result():
            raise RuntimeError
    return topic_id


@pytest.mark.integration
def test_topic_deletions_multiple_cli(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient
):
    topics_to_delete = [randomly_generated_topics(confluent_admin_client) for _ in range(3)]
    remaining_topic = randomly_generated_topics(confluent_admin_client)
    topics_pre_deletion = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert all(topic in topics_pre_deletion for topic in topics_to_delete)
    assert remaining_topic in topics_pre_deletion
    assert "not_in_the_list_of_topics" not in topics_pre_deletion

    result = interactive_cli_runner.invoke(
        delete_topics, topics_to_delete + ["not_in_the_list_of_topics"], input="Y\n", catch_exceptions=False
    )
    assert result.exit_code == 0

    topics_post_deletion = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert all(topic not in topics_post_deletion for topic in topics_to_delete)
    assert remaining_topic in topics_post_deletion
    assert all(existing_topic in topics_pre_deletion for existing_topic in topics_post_deletion)


@pytest.mark.integration
def test_topic_deletions_piped(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics_to_delete = [randomly_generated_topics(confluent_admin_client) for _ in range(3)]
    remaining_topic = randomly_generated_topics(confluent_admin_client)
    topics_pre_deletion = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert all(topic in topics_pre_deletion for topic in topics_to_delete)
    assert remaining_topic in topics_pre_deletion
    assert "not_in_the_list_of_topics" not in topics_pre_deletion

    result = non_interactive_cli_runner.invoke(
        delete_topics,
        "--no-verify",
        input="\n".join(topics_to_delete + ["not_in_the_list_of_topics"]),
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    topics_post_deletion = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert all(topic not in topics_post_deletion for topic in topics_to_delete)
    assert remaining_topic in topics_post_deletion
    assert all(existing_topic in topics_pre_deletion for existing_topic in topics_post_deletion)


@pytest.mark.integration
def test_consumer_group_deletions_multiple_cli(
    interactive_cli_runner: CliRunner,
    consumergroup_controller: ConsumerGroupController,
    filled_topic,
    unittest_config: Config,
):
    consumer_groups_to_delete = [randomly_generated_consumer_groups(filled_topic, unittest_config) for _ in range(2)]
    remaining_consumer_group = randomly_generated_consumer_groups(filled_topic, unittest_config)
    consumer_groups_pre_deletion = consumergroup_controller.list_consumer_groups()
    assert all(group in consumer_groups_pre_deletion for group in consumer_groups_to_delete)
    assert remaining_consumer_group in consumer_groups_pre_deletion
    assert "not_in_the_list_of_consumers" not in consumer_groups_pre_deletion

    result = interactive_cli_runner.invoke(
        delete_consumer_group,
        consumer_groups_to_delete + ["not_in_the_list_of_consumers"],
        input="Y\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    consumer_groups_post_deletion = consumergroup_controller.list_consumer_groups()
    assert all(group not in consumer_groups_post_deletion for group in consumer_groups_to_delete)
    assert remaining_consumer_group in consumer_groups_post_deletion
    assert all(existing_group in consumer_groups_pre_deletion for existing_group in consumer_groups_post_deletion)


@pytest.mark.integration
def test_consumer_group_deletions_piped(
    non_interactive_cli_runner: CliRunner,
    consumergroup_controller: ConsumerGroupController,
    filled_topic,
    unittest_config: Config,
):
    consumer_groups_to_delete = [randomly_generated_consumer_groups(filled_topic, unittest_config) for _ in range(2)]
    remaining_consumer_group = randomly_generated_consumer_groups(filled_topic, unittest_config)
    consumer_groups_pre_deletion = consumergroup_controller.list_consumer_groups()
    assert all(group in consumer_groups_pre_deletion for group in consumer_groups_to_delete)
    assert remaining_consumer_group in consumer_groups_pre_deletion
    assert "not_in_the_list_of_consumers" not in consumer_groups_pre_deletion

    result = non_interactive_cli_runner.invoke(
        delete_consumer_group,
        "--no-verify",
        input="\n".join(consumer_groups_to_delete + ["not_in_the_list_of_consumers"]),
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    consumer_groups_post_deletion = consumergroup_controller.list_consumer_groups()
    assert all(group not in consumer_groups_post_deletion for group in consumer_groups_to_delete)
    assert remaining_consumer_group in consumer_groups_post_deletion
    assert all(existing_group in consumer_groups_pre_deletion for existing_group in consumer_groups_post_deletion)


@pytest.mark.integration
def test_topic_list_output_compatibility_for_piping(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    all_topics = non_interactive_cli_runner.invoke(get_topics, args="--hide-internal").stdout
    assert topic in all_topics
    result = non_interactive_cli_runner.invoke(delete_topics, "--no-verify", input=all_topics, catch_exceptions=False)
    assert result.exit_code == 0
    all_topics = sorted(list(confluent_admin_client.list_topics(timeout=5).topics.keys()))
    assert all_topics == ["__confluent.support.metrics", "__consumer_offsets"]


@pytest.mark.integration
def test_consumergroup_list_output_compatibility_for_piping(
    non_interactive_cli_runner: CliRunner,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    consumergroup_instance: ConsumerGroup,
):
    all_consumergroups = non_interactive_cli_runner.invoke(get_consumergroups).stdout
    assert consumergroup_instance.id in all_consumergroups
    result = non_interactive_cli_runner.invoke(
        delete_consumer_group, "--no-verify", input=all_consumergroups, catch_exceptions=False
    )
    assert result.exit_code == 0
    all_consumergroups = non_interactive_cli_runner.invoke(get_consumergroups).stdout.replace("\n", "")
    assert all_consumergroups == "[]"
