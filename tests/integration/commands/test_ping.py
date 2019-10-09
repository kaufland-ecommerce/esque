import pytest
from click.testing import CliRunner

from esque import config
from esque.cli.commands import ping
from esque.controller.topic_controller import TopicController


@pytest.mark.integration
def test_smoke_test_ping(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(ping)
    print(result.output)  # TODO: delete

    assert result.exit_code == 0


@pytest.mark.integration
def test_correct_amount_of_messages(mocker, non_interactive_cli_runner: CliRunner, topic_controller: TopicController):
    topic_controller_delete_topic = mocker.patch.object(TopicController, "delete_topic", mocker.Mock())

    result = non_interactive_cli_runner.invoke(ping)

    assert result.exit_code == 0
    assert topic_controller_delete_topic.call_count == 1

    ping_topic = topic_controller.get_cluster_topic(config.PING_TOPIC)
    assert ping_topic.offsets[0].high == 10
