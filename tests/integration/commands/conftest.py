from unittest import mock

import pytest
from click.testing import CliRunner

from esque.config import Config


@pytest.fixture()
def interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=True)
    mocker.patch("esque.cli.environment.ESQUE_VERBOSE", new_callable=mock.PropertyMock, return_value="1")
    return CliRunner()


@pytest.fixture()
def non_interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=False)
    mocker.patch("esque.cli.environment.ESQUE_VERBOSE", new_callable=mock.PropertyMock, return_value="1")
    return CliRunner()
