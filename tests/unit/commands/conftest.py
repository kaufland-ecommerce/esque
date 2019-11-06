from click.testing import CliRunner

import pytest


@pytest.fixture()
def interactive_cli_runner(mocker) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=True)
    return CliRunner()


@pytest.fixture()
def non_interactive_cli_runner(mocker):
    mocker.patch("esque.cli.helpers._isatty", return_value=False)
    return CliRunner()
