import pytest
from click.testing import CliRunner

from esque.cli.commands import describe_topic


@pytest.mark.integration
def test_smoke_test_describe_topic(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic])

    assert result.exit_code == 0


@pytest.mark.integration
def test_describe_topic_consumergroup_in_output(
    cli_runner: CliRunner, filled_topic: str, partly_read_consumer_group: str
):
    result = cli_runner.invoke(describe_topic, [filled_topic, "-C"])

    assert result.exit_code == 0
    # TODO: Would like to be able to read this as yaml. We should implement json and yaml outputs
    # output = yaml.safe_load(result.output)
    assert result.output.contains(partly_read_consumer_group)
