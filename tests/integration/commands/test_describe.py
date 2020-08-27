from typing import Callable, Union

import pytest
from click.testing import CliRunner

from esque import config
from esque.cli.commands import describe_broker, describe_topic
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import ConsumerGroupDoesNotExistException
from esque.resources.topic import Topic
from tests.conftest import parameterized_output_formats

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
    result = non_interactive_cli_runner.invoke(describe_topic, topic, catch_exceptions=False)
    assert result.exit_code == 0
    output = result.output
    check_described_topic(output)


@pytest.mark.integration
def test_describe_topic_last_timestamp_does_not_commit(
    non_interactive_cli_runner: CliRunner, topic: str, consumergroup_controller: ConsumerGroupController
):
    result = non_interactive_cli_runner.invoke(describe_topic, [topic, "--last-timestamp"], catch_exceptions=False)
    assert result.exit_code == 0
    output = result.output
    check_described_topic(output)

    # cannot use pytest.raises(ConsumerGroupDoesNotExistException) because other tests may have committed offsets
    # for this group
    try:
        data = consumergroup_controller.get_consumergroup(config.ESQUE_GROUP_ID).describe(verbose=True)
        assert topic not in data["offsets"]
    except ConsumerGroupDoesNotExistException:
        pass


@pytest.mark.integration
@parameterized_output_formats
def test_describe_topic_formatted_output(
    non_interactive_cli_runner: CliRunner, topic: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_topic, [topic, "-o", output_format], catch_exceptions=False)
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_topic(output_dict)


@pytest.mark.integration
@parameterized_output_formats
def test_describe_topic_from_stdin(
    non_interactive_cli_runner: CliRunner, topic: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_topic, ["-o", output_format], topic, catch_exceptions=False)
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_topic(output_dict)


@pytest.mark.integration
def test_describe_topic_without_topic_name_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(describe_topic)
    assert result.exit_code != 0


@pytest.mark.integration
def test_describe_broker_no_flag(non_interactive_cli_runner: CliRunner, broker_id: str):
    result = non_interactive_cli_runner.invoke(describe_broker, broker_id, catch_exceptions=False)
    assert result.exit_code == 0
    output = result.output
    check_described_broker(output)


@pytest.mark.integration
@parameterized_output_formats
def test_describe_broker_formatted_output(
    non_interactive_cli_runner: CliRunner, broker_id: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(
        describe_broker, [broker_id, "-o", output_format], catch_exceptions=False
    )
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_broker(output_dict)


@pytest.mark.integration
@parameterized_output_formats
def test_describe_broker_formatted_output_host(
    non_interactive_cli_runner: CliRunner, broker_host: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_broker, [broker_host, "-o", output_format])
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_broker(output_dict)


@pytest.mark.integration
@parameterized_output_formats
def test_describe_broker_formatted_output_host_and_port(
    non_interactive_cli_runner: CliRunner, broker_host_and_port: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(describe_broker, [broker_host_and_port, "-o", output_format])
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_broker(output_dict)


@pytest.mark.integration
def test_describe_broker_without_broker_id_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(describe_broker)
    assert result.exit_code != 0


@pytest.mark.integration
@parameterized_output_formats
def test_describe_broker_from_stdin(
    non_interactive_cli_runner: CliRunner, broker_id: str, output_format: str, loader: Callable
):
    result = non_interactive_cli_runner.invoke(
        describe_broker, ["-o", output_format], broker_id, catch_exceptions=False
    )
    assert result.exit_code == 0
    output_dict = loader(result.output)
    check_described_broker(output_dict)


def check_described_topic(described_topic: Union[str, dict]):
    topic_description_keys = ["topic", "partitions", "config"]
    if type(described_topic) == str:
        for option in topic_description_keys:
            assert option in described_topic
    else:
        assert list(described_topic.keys()) == topic_description_keys


def check_described_broker(described_broker: Union[str, dict]):
    if type(described_broker) == str:
        for option in VARIOUS_IMPORTANT_BROKER_OPTIONS:
            assert option in described_broker
    else:
        keys = described_broker.keys()
        for option in VARIOUS_IMPORTANT_BROKER_OPTIONS:
            assert option in keys


@pytest.mark.integration
@parameterized_output_formats
def test_describe_topic_consumergroup_in_output(
    non_interactive_cli_runner: CliRunner,
    filled_topic: Topic,
    partly_read_consumer_group: str,
    output_format: str,
    loader: Callable,
):
    result = non_interactive_cli_runner.invoke(
        describe_topic, ["-o", output_format, "-c", filled_topic.name], catch_exceptions=False
    )
    assert result.exit_code == 0
    output_dict = loader(result.output)

    assert partly_read_consumer_group in output_dict.get("consumergroups", None)
