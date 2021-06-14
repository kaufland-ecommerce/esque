import confluent_kafka
import pytest
from click.testing import CliRunner

from esque.cli.commands import create_consumer_group, create_topic
from esque.cli.options import State
from esque.errors import NoConfirmationPossibleException
from esque.resources.topic import Topic


@pytest.mark.integration
def test_create_without_confirmation_does_not_create_topic(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    result = interactive_cli_runner.invoke(create_topic, [topic_id], catch_exceptions=False)
    assert result.exit_code == 0

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics


@pytest.mark.integration
def test_create_topic_without_topic_name_fails(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient
):
    n_topics_before = len(confluent_admin_client.list_topics(timeout=5).topics)
    result = non_interactive_cli_runner.invoke(create_topic)
    n_topics_after = len(confluent_admin_client.list_topics(timeout=5).topics)
    assert result.exit_code != 0
    assert n_topics_before == n_topics_after


@pytest.mark.integration
def test_create_topic_as_argument_with_verification_works(
    interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    result = interactive_cli_runner.invoke(create_topic, args=topic_id, input="Y\n", catch_exceptions=False)
    assert result.exit_code == 0
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_create_topic_with_stdin_works(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    result = non_interactive_cli_runner.invoke(
        create_topic, args="--no-verify", input=topic_id, catch_exceptions=False
    )
    assert result.exit_code == 0
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_topic_creation_stops_in_non_interactive_mode_without_no_verify(
    non_interactive_cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    result = non_interactive_cli_runner.invoke(create_topic, input=topic_id)
    assert result.exit_code != 0
    assert isinstance(result.exception, NoConfirmationPossibleException)

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
    result = non_interactive_cli_runner.invoke(
        create_topic, ["--no-verify", "-l", topic_1, topic_2], catch_exceptions=False
    )
    assert result.exit_code == 0
    config_from_template = state.cluster.topic_controller.get_cluster_topic(topic_2)
    assert config_from_template.replication_factor == replication_factor
    assert config_from_template.num_partitions == num_partitions
    for config_key, value in config.items():
        assert config_from_template.config[config_key] == value


def test_consumer_group_correct_creation(
    interactive_cli_runner: CliRunner,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    consumer_group: str,
    topic: str,
):
    first_topic = f"{topic}"
    first_cg = consumer_group + "1"
    result1 = interactive_cli_runner.invoke(
        create_consumer_group, args=[first_cg, first_topic], input="Y\n", catch_exceptions=False
    )
    assert result1.exit_code == 0

    second_topic = f"{topic}[0]"
    second_cg = consumer_group + "2"
    result2 = interactive_cli_runner.invoke(
        create_consumer_group, args=[second_cg, second_topic], input="Y\n", catch_exceptions=False
    )
    assert result2.exit_code == 0

    third_topic = f"{topic}[0]=3"
    third_cg = consumer_group + "3"
    result3 = interactive_cli_runner.invoke(
        create_consumer_group, args=[third_cg, third_topic], input="Y\n", catch_exceptions=False
    )
    assert result3.exit_code == 0

    fourth_topic = f"{topic}[]"
    fourth_cg = consumer_group + "4"
    result4 = interactive_cli_runner.invoke(
        create_consumer_group, args=[fourth_cg, fourth_topic], input="Y\n", catch_exceptions=False
    )
    assert result4.exit_code == 0

    fifth_topic = f"{topic}[]="
    fifth_cg = consumer_group + "5"
    result5 = interactive_cli_runner.invoke(
        create_consumer_group, args=[fifth_cg, fifth_topic], input="Y\n", catch_exceptions=False
    )
    assert result5.exit_code == 0

    cg_list = [gm.id for gm in confluent_admin_client.list_groups(timeout=5)]
    assert first_cg in cg_list
    assert second_cg in cg_list
    assert third_cg in cg_list
    assert fourth_cg in cg_list
    assert fifth_cg in cg_list
