import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import delete_topic


@pytest.fixture()
def basic_topic(num_partitions, topic_factory):
    yield from topic_factory(num_partitions, "basic-topic")


@pytest.fixture()
def duplicate_topic(num_partitions, topic_factory):
    yield from topic_factory(num_partitions, "basic.topic")


@pytest.mark.integration
def test_topic_deletion_works(
    cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    result = cli_runner.invoke(delete_topic, [topic], input="y\n")
    assert result.exit_code == 0

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic not in topics


@pytest.mark.integration
def test_keep_kafka_duplicated(
    cli_runner: CliRunner,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    basic_topic: str,
    duplicate_topic: str,
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert basic_topic[0] in topics
    assert duplicate_topic[0] in topics

    result = cli_runner.invoke(delete_topic, [duplicate_topic[0]], input="y\n")
    assert result.exit_code == 0

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert duplicate_topic[0] not in topics
    assert basic_topic[0] in topics
