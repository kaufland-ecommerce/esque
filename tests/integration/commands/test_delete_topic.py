from click.testing import CliRunner

import esque
import confluent_kafka
import pytest

from esque.cli.commands import create_topic, delete_topic


def use_confirmation_mocking(monkeypatch):
    def mock_user_confirmation():
        return True

    monkeypatch.setattr(
        esque.cli.commands, "_get_user_confirmation", mock_user_confirmation
    )  # TODO: mocking click.termui.getchar would be better but I can't get it to work :(


@pytest.mark.integration
def test_create_topic_works(
    monkeypatch, confluent_admin_client: confluent_kafka.admin.AdminClient, topic: str
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    use_confirmation_mocking(monkeypatch)

    CliRunner().invoke(delete_topic, args=topic, catch_exceptions=False)

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic not in topics


@pytest.mark.integration
def test_topic_deletion_with_stdin_works(
    monkeypatch,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    topic: str,
):
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    use_confirmation_mocking(monkeypatch)

    CliRunner().invoke(delete_topic, input=topic, catch_exceptions=False)

    # Invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic not in topics