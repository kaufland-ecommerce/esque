from unittest import mock

from click.testing import CliRunner
from pytest_cases import fixture

from esque.config import Config


@fixture()
def interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=True)
    mocker.patch("esque.cli.environment.ESQUE_VERBOSE", new_callable=mock.PropertyMock, return_value="1")
    return CliRunner()


@fixture()
def non_interactive_cli_runner(mocker: mock, unittest_config: Config) -> CliRunner:
    mocker.patch("esque.cli.helpers._isatty", return_value=False)
    mocker.patch("esque.cli.environment.ESQUE_VERBOSE", new_callable=mock.PropertyMock, return_value="1")
    return CliRunner()
