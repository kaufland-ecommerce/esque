from unittest import mock

import pytest
from click.testing import CliRunner

from esque.config import Config


@pytest.fixture()
def interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=True)
    return CliRunner()


@pytest.fixture()
def non_interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=False)
    return CliRunner()
