import json
from typing import Union, Callable

import pytest
import yaml
from click.testing import CliRunner

from esque.cli.commands import describe_topic, describe_broker
from tests.conftest import check_and_load_yaml

FORMATS_AND_LOADERS = [("yaml", check_and_load_yaml), ("json", json.loads)]

VARIOUS_IMPORTANT_BROKER_OPTIONS = [
    "advertised.host.name",
    "advertised.listeners",
    "advertised.port",
    "broker.id",
    "delete.topic.enable",
    "offsets.retention.minutes",
    "sasl.enabled.mechanisms",
    "ssl.protocol",
    "zookeeper.connect",
]


@pytest.mark.integration
def test_describe_topic_no_flag(non_interactive_cli_runner: CliRunner, topic: str):
    result = non_interactive_cli_runner.invoke(describe_topic, topic)
    assert result.exit_code == 0
    output = result.output
    check_described_topic(output)


@pytest.mark.integration
@pytest.mark.parametrize("output_format,loader", FORMATS_AND_LOADERS, ids=["yaml", "json"])
def test_describe_topic_formatted_output(
    non_interactive_cli_runner: CliRunner, topic: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_topic, [topic, "-o", output_format])
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_topic(output_dict)


@pytest.mark.integration
@pytest.mark.parametrize("output_format,loader", FORMATS_AND_LOADERS, ids=["yaml", "json"])
def test_describe_topic_from_stdin(
    non_interactive_cli_runner: CliRunner, topic: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_topic, ["-o", output_format], topic)
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_topic(output_dict)


@pytest.mark.integration
def test_describe_topic_without_topic_name_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(describe_topic)
    assert result.exit_code == 1
    assert "ERROR: Missing argument TOPIC_NAME" in result.output


@pytest.mark.integration
def test_describe_broker_no_flag(non_interactive_cli_runner: CliRunner, broker_id: str):
    result = non_interactive_cli_runner.invoke(describe_broker, broker_id)
    assert result.exit_code == 0
    output = result.output
    check_described_broker(output)


@pytest.mark.integration
@pytest.mark.parametrize("output_format,loader", FORMATS_AND_LOADERS, ids=["yaml", "json"])
def test_describe_broker_formatted_output(
    non_interactive_cli_runner: CliRunner, broker_id: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_broker, [broker_id, "-o", output_format])
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_broker(output_dict)


@pytest.mark.integration
def test_describe_broker_without_broker_id_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(describe_broker)
    assert result.exit_code == 1
    assert "ERROR: Missing argument BROKER_ID" in result.output


@pytest.mark.integration
@pytest.mark.parametrize("output_format,loader", FORMATS_AND_LOADERS, ids=["yaml", "json"])
def test_describe_broker_from_stdin(
    non_interactive_cli_runner: CliRunner, broker_id: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_broker, ["-o", output_format], broker_id)
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_broker(output_dict)


def check_described_topic(described_topic: Union[str, dict]):
    if type(described_topic) == str:
        for option in ["Topic", "Partition", "Config"]:
            assert option in described_topic
    else:
        keys = described_topic.keys()
        assert "Topic" in keys
        assert sum("Partition" in key for key in keys) == len(keys) - 2
        assert "Config" in keys


def check_described_broker(described_broker: Union[str, dict]):
    if type(described_broker) == str:
        for option in VARIOUS_IMPORTANT_BROKER_OPTIONS:
            assert option in described_broker
    else:
        keys = described_broker.keys()
        for option in VARIOUS_IMPORTANT_BROKER_OPTIONS:
            assert option in keys
