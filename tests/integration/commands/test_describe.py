import pytest
from click.testing import CliRunner

from esque.cli.commands import describe_topic


@pytest.mark.integration
def test_smoke_test_describe_topic(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic])

    assert result.exit_code == 0
