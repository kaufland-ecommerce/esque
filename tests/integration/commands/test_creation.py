import confluent_kafka
from click.testing import CliRunner

from esque.cli.commands import create_topic


def test_create(cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str):

    cli_runner.invoke(create_topic, [topic_id])

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic_id not in topics
