import json

import pytest
import yaml
from click.testing import CliRunner

from esque.cli.commands import describe_topic, describe_broker

NUMBER_OF_BROKER_OPTIONS = 192  # TODO: Different on gitlab and docker???


@pytest.mark.integration
def test_smoke_test_describe_topic(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic])

    assert result.exit_code == 0


@pytest.mark.integration
def test_describe_topic_to_yaml(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "yaml"])
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    check_described_topic_format(yaml_dict)


@pytest.mark.integration
def test_describe_topic_to_json(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "json"])
    output = result.output
    assert output[0] == "{"
    json_dict = json.loads(output)
    check_described_topic_format(json_dict)


@pytest.mark.integration
def test_describe_broker_to_yaml(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "yaml"])
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    check_described_broker_format(yaml_dict)


@pytest.mark.integration
def test_describe_broker_to_json(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "json"])
    output = result.output
    assert output[0] == "{"
    json_dict = json.loads(output)
    check_described_broker_format(json_dict)


@pytest.mark.integration
def test_describe_topic_from_stdin(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, ["-o", "yaml"], topic)
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    check_described_topic_format(yaml_dict)


@pytest.mark.integration
def test_describe_broker_from_stdin(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, ["-o", "json"], broker_id)
    output = result.output
    assert output[0] == "{"
    yaml_dict = json.loads(output)
    check_described_broker_format(yaml_dict)


def check_described_topic_format(described_topic: dict):
    keys = described_topic.keys()
    assert "Topic" in keys
    assert sum("Partition" in key for key in keys) == len(keys) - 2
    assert "Config" in keys


def check_described_broker_format(described_broker: dict):
    keys = described_broker.keys()
    assert len(keys) == NUMBER_OF_BROKER_OPTIONS
    config_options = [
        "advertised.host.name",
        "advertised.listeners",
        "advertised.port",
        "zookeeper.session.timeout.ms",
    ]
    for option in config_options:
        assert option in keys
