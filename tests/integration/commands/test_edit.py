from unittest import mock

import click
import confluent_kafka
import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from confluent_kafka.cimpl import Producer as ConfluenceProducer
from confluent_kafka.cimpl import TopicPartition

from esque.cli.commands import esque
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.controller.topic_controller import TopicController
from esque.errors import EditCanceled
from tests.utils import produce_text_test_messages


@pytest.mark.integration
def test_edit_topic_works(
    interactive_cli_runner: CliRunner,
    monkeypatch: MonkeyPatch,
    topic_controller: TopicController,
    confluent_admin_client: confluent_kafka.admin.AdminClient,
    topic: str,
):

    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    assert topic in topics

    config_dict = {
        "config": {
            "cleanup.policy": "delete",
            "compression.type": "producer",
            "delete.retention.ms": "123456789",
            "file.delete.delay.ms": "60000",
            "flush.messages": "123456789",
            "flush.ms": "9223372036854775807",
            "follower.replication.throttled.replicas": "",
            "index.interval.bytes": "4096",
            "leader.replication.throttled.replicas": "",
            "max.message.bytes": "1000012",
            "message.downconversion.enable": "true",
            "message.format.version": "2.2-IV1",
            "message.timestamp.difference.max.ms": "123456789",
            "message.timestamp.type": "CreateTime",
            "min.cleanable.dirty.ratio": "0.5",
            "min.compaction.lag.ms": "0",
            "min.insync.replicas": "1",
            "preallocate": "false",
            "retention.bytes": "-1",
            "retention.ms": "123456789",
            "segment.bytes": "123456789",
            "segment.index.bytes": "123456789",
            "segment.jitter.ms": "0",
            "segment.ms": "123456789",
            "unclean.leader.election.enable": "true",
        }
    }

    def mock_edit_function(text=None, editor=None, env=None, require_save=None, extension=None, filename=None):
        return yaml.dump(config_dict, default_flow_style=False)

    monkeypatch.setattr(click, "edit", mock_edit_function)
    result = interactive_cli_runner.invoke(esque, args=["edit", "topic", topic], input="y\n", catch_exceptions=False)
    assert result.exit_code == 0

    topic_config_dict = topic_controller.get_cluster_topic(topic).as_dict(only_editable=True)
    for key, value in config_dict["config"].items():
        assert (key, topic_config_dict["config"][key]) == (key, value)


@pytest.mark.integration
def test_edit_topic_without_topic_name_fails(non_interactive_cli_runner: CliRunner):
    result = non_interactive_cli_runner.invoke(esque, args=["edit", "topic"])
    assert result.exit_code != 0


@pytest.mark.integration
def test_edit_topic_calls_validator(mocker: mock, topic, interactive_cli_runner, topic_controller):
    validator_mock = mocker.patch("esque.validation.validate_editable_topic_config", side_effect=EditCanceled())
    config_dict = {
        "config": {
            "cleanup.policy": "delete",
            "compression.type": "producer",
            "delete.retention.ms": "123456789",
            "segment.jitter.ms": "0",
            "segment.ms": "123456789",
            "unclean.leader.election.enable": "true",
        }
    }

    mocker.patch.object(click, "edit", return_value=yaml.dump(config_dict, default_flow_style=False))
    interactive_cli_runner.invoke(esque, args=["edit", "topic", topic], input="y\n")

    (validated_config_dict,) = validator_mock.call_args[0]
    assert validated_config_dict == config_dict


@pytest.mark.integration
def test_edit_offsets(
    monkeypatch: MonkeyPatch,
    interactive_cli_runner,
    topic: str,
    producer: ConfluenceProducer,
    consumer_group: str,
    consumergroup_controller: ConsumerGroupController,
):
    produce_text_test_messages(producer=producer, topic_name=topic, amount=10)

    consumergroup_controller.commit_offsets(consumer_group, [TopicPartition(topic=topic, partition=0, offset=10)])

    consumergroup_desc_before = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    offset_config = {"offsets": [{"topic": topic, "partition": 0, "offset": 1}]}

    def mock_edit_function(text=None, editor=None, env=None, require_save=None, extension=None, filename=None):
        return yaml.dump(offset_config, default_flow_style=False)

    monkeypatch.setattr(click, "edit", mock_edit_function)
    result = interactive_cli_runner.invoke(
        esque, args=["edit", "offsets", consumer_group, "-t", topic], input="y\n", catch_exceptions=False
    )
    assert result.exit_code == 0

    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )
    assert consumergroup_desc_before["offsets"][topic][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic][0]["consumer_offset"] == 1
