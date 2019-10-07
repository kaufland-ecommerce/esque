import json
from typing import Union

import pytest
import yaml
from click.testing import CliRunner

from esque.cli.commands import describe_topic, describe_broker

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
def test_describe_topic_no_flag(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, topic)
    assert result.exit_code == 0
    output = result.output
    check_no_flag_output(output)
    check_described_topic(output)


@pytest.mark.integration
def test_describe_topic_to_yaml(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "yaml"])
    assert result.exit_code == 0
    output = result.output
    check_yaml_output(output)
    output_dict = yaml.safe_load(output)
    check_described_topic(output_dict)


@pytest.mark.integration
def test_describe_topic_to_json(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "json"])
    assert result.exit_code == 0
    output = result.output
    check_json_output(output)
    output_dict = json.loads(output)
    check_described_topic(output_dict)


@pytest.mark.integration
def test_describe_topic_without_topic_name_fails():
    result = CliRunner().invoke(describe_topic)
    assert result.exit_code == 1
    assert "ERROR: Missing argument TOPIC_NAME" in result.output


@pytest.mark.integration
def test_describe_broker_no_flag(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, broker_id)
    assert result.exit_code == 0
    output = result.output
    check_no_flag_output(output)
    check_described_broker(output)


@pytest.mark.integration
def test_describe_broker_to_yaml(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "yaml"])
    assert result.exit_code == 0
    output = result.output
    check_yaml_output(output)
    output_dict = yaml.safe_load(output)
    check_described_broker(output_dict)


@pytest.mark.integration
def test_describe_broker_to_json(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "json"])
    assert result.exit_code == 0
    output = result.output
    check_json_output(output)
    output_dict = json.loads(output)
    check_described_broker(output_dict)


@pytest.mark.integration
def test_describe_broker_without_broker_id_fails():
    result = CliRunner().invoke(describe_broker)
    assert result.exit_code == 1
    assert "ERROR: Missing argument BROKER_ID" in result.output


@pytest.mark.integration
def test_describe_topic_from_stdin(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, ["-o", "yaml"], topic)
    assert result.exit_code == 0
    output = result.output
    check_yaml_output(output)
    output_dict = yaml.safe_load(output)
    check_described_topic(output_dict)


@pytest.mark.integration
def test_describe_broker_from_stdin(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, ["-o", "json"], broker_id)
    output = result.output
    check_json_output(output)
    output_dict = json.loads(output)
    check_described_broker(output_dict)


def check_no_flag_output(output: str):
    assert output[0] != "{", "Non json output starts with '{'"
    assert output[-2] != "}" and output[-1] != "}", "Non json output ends with '}'"


def check_yaml_output(output: str):
    assert output[0] != "{", "Non json output starts with '{'"
    assert output[-2] != "}" and output[-1] != "}", "Non json output ends with '}'"


def check_json_output(output: str):
    assert output[0] == "{", "json output doesn't start with '{'"
    assert output[-2:] == "}\n", "json output doesn't end with '}\\n'"


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
