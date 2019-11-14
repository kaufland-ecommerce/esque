import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import delete_topic
from esque.errors import NoConfirmationPossibleException


@pytest.fixture()
def basic_topic(num_partitions, topic_factory):
    yield from topic_factory(num_partitions, "basic-topic")


@pytest.fixture()
def duplicate_topic(num_partitions, topic_factory):
    yield from topic_factory(num_partitions, "basic.topic")


@pytest.mark.integration
def test_topic_deletion_without_verification_does_not_work(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    result = interactive_cli_runner.invoke(delete_topic, [topic], catch_exceptions=False)
    assert result.exit_code == 0

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics


@pytest.mark.integration
def test_delete_topic_without_topic_name_fails(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient
):
    n_topics_before = len(confluent_admin_client.list_topics(timeout=5).topics)
    result = interactive_cli_runner.invoke(delete_topic)
    n_topics_after = len(confluent_admin_client.list_topics(timeout=5).topics)
    assert result.exit_code != 0
    assert n_topics_before == n_topics_after


@pytest.mark.integration
def test_topic_deletion_as_argument_works(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    result = interactive_cli_runner.invoke(delete_topic, [topic], input="y\n", catch_exceptions=False)
    assert result.exit_code == 0

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic not in topics


@pytest.mark.integration
def test_topic_deletion_as_stdin_works(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    result = non_interactive_cli_runner.invoke(delete_topic, "--no-verify", input=topic, catch_exceptions=False)
    assert result.exit_code == 0

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic not in topics


@pytest.mark.integration
def test_topic_deletion_stops_in_non_interactive_mode_without_no_verify(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    result = non_interactive_cli_runner.invoke(delete_topic, input=topic)
    assert result.exit_code != 0
    assert isinstance(result.exception, NoConfirmationPossibleException)

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics


@pytest.mark.integration
def test_keep_minus_delete_period(
    interactive_cli_runner: CliRunner,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    basic_topic: str,
    duplicate_topic: str,
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert basic_topic[0] in topics
    assert duplicate_topic[0] in topics

    result = interactive_cli_runner.invoke(delete_topic, [duplicate_topic[0]], input="y\n", catch_exceptions=False)
    assert result.exit_code == 0

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert duplicate_topic[0] not in topics
    assert basic_topic[0] in topics
