import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import create_topic
from esque.cli.options import State
from esque.resources.topic import Topic


@pytest.mark.integration
def test_create(cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str):

    cli_runner.invoke(create_topic, [topic_id])

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics


@pytest.mark.integration
def test_topic_creation_with_template_works(
    state: State, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    topic_1 = topic_id + "_1"
    topic_2 = topic_id + "_2"
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_1 not in topics
    replication_factor = 1
    num_partitions = 1
    config = {
        "cleanup.policy": "delete",
        "delete.retention.ms": "123456",
        "file.delete.delay.ms": "789101112",
        "flush.messages": "12345678910111213",
        "flush.ms": "123456789",
    }
    state.cluster.topic_controller.create_topics(
        [Topic(topic_1, replication_factor=replication_factor, num_partitions=num_partitions, config=config)]
    )
    runner = CliRunner()
    runner.invoke(create_topic, ["--no-verify", "-l", topic_1, topic_2])
    config_from_template = state.cluster.topic_controller.get_cluster_topic(topic_2)
    assert config_from_template.replication_factor == replication_factor
    assert config_from_template.num_partitions == num_partitions
    for config_key, value in config.items():
        assert config_from_template.config[config_key] == value
