import confluent_kafka
import pytest
from click.testing import CliRunner

from esque import config
from esque.cli.commands import ping
from esque.controller.topic_controller import TopicController


@pytest.mark.integration
def test_smoke_test_ping(cli_runner: CliRunner):
    result = cli_runner.invoke(ping)

    assert result.exit_code == 0


@pytest.mark.integration
def test_correct_amount_of_messages(
    mocker, cli_runner: CliRunner, confluent_admin_client: confluent_kafka.admin.AdminClient, topic_id: str
):
    config.RANDOM = "test"

    topic_controller_delete_topic = mocker.patch.object(TopicController, "delete_topic", mocker.Mock())

    result = cli_runner.invoke(ping)

    assert result.exit_code == 0
    assert topic_controller_delete_topic.call_count == 1
