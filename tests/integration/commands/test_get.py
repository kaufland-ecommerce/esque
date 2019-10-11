import pytest
from click.testing import CliRunner

from esque.cli.commands import get_topics


@pytest.mark.integration
def test_smoke_test_get_topics(cli_runner: CliRunner):
    result = cli_runner.invoke(get_topics, ["--verbose"])

    assert result.exit_code == 0
