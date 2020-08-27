import pytest
from click.testing import CliRunner

from esque import config
from esque.cli.commands import ping
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ConsumerGroupDoesNotExistException


@pytest.mark.integration
def test_smoke_test_ping(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(ping, catch_exceptions=False)
    assert result.exit_code == 0


@pytest.mark.integration
def test_correct_amount_of_messages(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(ping, catch_exceptions=False)
    assert result.exit_code == 0

    # make sure we sent/received 10 messages
    for i in range(10):
        assert f"m_seq={i}" in result.stdout


@pytest.mark.integration
def test_offset_not_committed(
    non_interactive_cli_runner: CliRunner, consumergroup_controller: ConsumerGroupController
):
    result = non_interactive_cli_runner.invoke(ping, catch_exceptions=False)
    assert result.exit_code == 0

    # cannot use pytest.raises(ConsumerGroupDoesNotExistException) because other tests may have committed offsets
    # for this group
    try:
        data = consumergroup_controller.get_consumergroup(config.ESQUE_GROUP_ID).describe(verbose=True)
        assert config.PING_TOPIC not in data["offsets"]
    except ConsumerGroupDoesNotExistException:
        pass
