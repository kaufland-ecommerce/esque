from click.testing import CliRunner

import esque
import confluent_kafka
import pytest

from esque.cli.commands import create_topic


def use_confirmation_mocking(monkeypatch):
    def mock_user_confirmation():
        return True

    monkeypatch.setattr(
        esque.cli.commands, "_get_user_confirmation", mock_user_confirmation
    )  # TODO: mocking click.termui.getchar would be better but I can't get it to work :(


@pytest.mark.integration
def test_create_topic_works(
    monkeypatch, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics

    use_confirmation_mocking(monkeypatch)

    CliRunner().invoke(create_topic, args=topic_id, catch_exceptions=False)

    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id in topics


@pytest.mark.integration
def test_create_topic_with_stdin_works(
    monkeypatch, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    stdin_topic = f"stdin_{topic_id}"

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert stdin_topic not in topics

    use_confirmation_mocking(monkeypatch)

    CliRunner().invoke(create_topic, input=stdin_topic, catch_exceptions=False)

    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert stdin_topic in topics