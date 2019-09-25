import click
from click.testing import CliRunner

import confluent_kafka
import pytest

from esque.cli.commands import create_topic


@pytest.mark.integration
def test_create_topic_with_stdin_works(
    monkeypatch, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    stdin_topic = f"stdin_{topic_id}"

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert stdin_topic not in topics

    def confirmation_generator_function():
        yield "y"
        yield "\r"

    confirmation_generator = confirmation_generator_function()

    def mock_confirmation(echo):
        return next(confirmation_generator)

    monkeypatch.setattr(
        click, "getchar", mock_confirmation
    )

    result = CliRunner().invoke(create_topic, input=stdin_topic, catch_exceptions=False)
    print(result.output)
    # invalidate cache
    confluent_admin_client.poll(timeout=1)
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert stdin_topic in topics
