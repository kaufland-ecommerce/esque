import json

import pytest
import yaml
from click.testing import CliRunner

from esque.cli.commands import describe_topic, describe_broker


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
    keys = yaml_dict.keys()
    assert len(keys) == 3
    assert "Topic" in keys
    assert any("Partition" in key for key in keys)
    assert "Config" in keys


@pytest.mark.integration
def test_describe_topic_to_json(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "json"])
    output = result.output
    assert output[0] == "{"
    json_dict = json.loads(output)
    keys = json_dict.keys()
    assert len(keys) == 3
    assert "Topic" in keys
    assert any("Partition" in key for key in keys)
    assert "Config" in keys


@pytest.mark.integration
def test_describe_broker_to_yaml(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "yaml"])
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    keys = yaml_dict.keys()
    assert len(keys) == 191
    config_options = ["advertised.host.name", "advertised.listeners", "advertised.port", "zookeeper.session.timeout.ms"]
    for option in config_options:
        assert option in keys


@pytest.mark.integration
def test_describe_broker_to_json(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "json"])
    output = result.output
    assert output[0] == "{"
    yaml_dict = json.loads(output)
    keys = yaml_dict.keys()
    assert len(keys) == 191
    config_options = ["advertised.host.name", "advertised.listeners", "advertised.port", "zookeeper.session.timeout.ms"]
    for option in config_options:
        assert option in keys


@pytest.mark.integration
def test_describe_topic_from_stdin(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, ["-o", "yaml"], topic)
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    keys = yaml_dict.keys()
    assert len(keys) == 3
    assert "Topic" in keys
    assert any("Partition" in key for key in keys)
    assert "Config" in keys


@pytest.mark.integration
def test_describe_broker_from_stdin(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, ["-o", "json"], broker_id)
    output = result.output
    assert output[0] == "{"
    yaml_dict = json.loads(output)
    keys = yaml_dict.keys()
    assert len(keys) == 191
    config_options = ["advertised.host.name", "advertised.listeners", "advertised.port", "zookeeper.session.timeout.ms"]
    for option in config_options:
        assert option in keys
