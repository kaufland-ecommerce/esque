import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import create_topic
from esque.cli.options import State
from esque.errors import NoConfirmationPossibleException
from esque.resources.topic import Topic


@pytest.mark.integration
def test_create_without_confirmation_does_not_create_topic(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    result = interactive_cli_runner.invoke(create_topic, [topic_id])
    assert result.exit_code == 0

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics


@pytest.mark.integration
def test_create_topic_without_topic_name_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(create_topic, "--verbose")
    assert result.exit_code == 2


@pytest.mark.integration
def test_create_topic_as_argument_with_verification_works(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    result = interactive_cli_runner.invoke(create_topic, args=topic_id, input="Y\n")
    assert result.exit_code == 0
    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_create_topic_with_stdin_works(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    result = non_interactive_cli_runner.invoke(create_topic, args="--no-verify", input=topic_id)
    assert result.exit_code == 0
    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_topic_creation_stops_in_non_interactive_mode_without_no_verify(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    result = non_interactive_cli_runner.invoke(create_topic, "--verbose", input=topic_id)
    assert result.exit_code != 0 and isinstance(result.exception, NoConfirmationPossibleException)

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics


@pytest.mark.integration
def test_topic_creation_with_template_works(
    non_interactive_cli_runner: CliRunner,
    state: State,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    topic_id: str,
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
    result = non_interactive_cli_runner.invoke(create_topic, ["--no-verify", "-l", topic_1, topic_2])
    assert result.exit_code == 0
    config_from_template = state.cluster.topic_controller.get_cluster_topic(topic_2)
    assert config_from_template.replication_factor == replication_factor
    assert config_from_template.num_partitions == num_partitions
    for config_key, value in config.items():
        assert config_from_template.config[config_key] == value
