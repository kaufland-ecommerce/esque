import sys

import click
import confluent_kafka
import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner

from esque.cli.commands import edit_topic
from esque.controller.topic_controller import TopicController


@pytest.mark.integration
def test_edit_topic_works(
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

    monkeypatch.setattr(sys.__stdin__, "isatty", lambda: True)
    monkeypatch.setattr(click, "edit", mock_edit_function)
    result = CliRunner().invoke(edit_topic, topic, input="y\n", catch_exceptions=False)
    assert result.exit_code == 0

    topic_config_dict = topic_controller.get_cluster_topic(topic).as_dict(only_editable=True)
    assert topic_config_dict == config_dict


@pytest.mark.integration
def test_edit_topic_without_topic_name_fails():
    result = CliRunner().invoke(edit_topic)
    assert result.exit_code != 0
    assert 'Error: Missing argument "TOPIC_NAME"' in result.output
